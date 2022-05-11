import pandas as pd

from ftx_snap_basis import *
from ftx_portfolio import *
import inspect

run_i = 0

async def refresh_universe(exchange,universe_size):
    filename = 'Runtime/configs/universe.xlsx'
    if os.path.isfile(filename):
        try:
            return pd.read_excel(filename,sheet_name=universe_size,index_col=0)
        except Exception as e:
            logging.exception('invalid Runtime/configs/universe.xlsx', exc_info=True)

    futures = pd.DataFrame(await fetch_futures(exchange, includeExpired=False)).set_index('name')
    markets = await exchange.fetch_markets()

    universe_start = datetime(2021, 12, 1)
    universe_end = datetime(2022, 3, 1)
    borrow_decile = 0.5
    #type_allowed=['perpetual']
    screening_params=pd.DataFrame(
        index=['future_volume_threshold','spot_volume_threshold','borrow_volume_threshold','open_interest_threshold'],
        data={'max':[5e4,5e4,-1,5e4],# important that sets are decreasing  :(
              'wide':[1e5,1e5,2e5,5e6],# to run say 1M after leverage
              'institutional':[5e6,5e6,-1,1e7]})# instiutionals borrow OTC

    # qualitative screening
    futures = futures[
        (futures['expired'] == False) & (futures['enabled'] == True) & (futures['type'] != "move")
        & (futures.apply(lambda f: float(find_spot_ticker(markets, f, 'ask')), axis=1) > 0.0)
        & (futures['tokenizedEquity'] != True)]

    # volume screening
    await build_history(futures,exchange)
    hy_history = await get_history(futures, end=universe_end, start_or_nb_hours=universe_start)
    futures = market_capacity(futures, hy_history, universe_filter_window=hy_history[universe_start:universe_end].index)

    with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
        for c in screening_params:# important that wide is first :(
            population = futures.loc[
                (futures['borrow_volume_decile'] > screening_params.loc['borrow_volume_threshold',c])
                & (futures['spot_volume_avg'] > screening_params.loc['spot_volume_threshold',c])
                & (futures['future_volume_avg'] > screening_params.loc['future_volume_threshold',c])
                & (futures['openInterestUsd'] > screening_params.loc['open_interest_threshold',c])]
            population.to_excel(writer, sheet_name=c)

        parameters = pd.Series(name='run_params',data={
            'run_date':datetime.today(),
            'universe_start': universe_start,
            'universe_end': universe_end,
            'borrow_decile': borrow_decile}).to_excel(writer,sheet_name='parameters')
        screening_params.to_excel(writer, sheet_name='screening_params')
        #TODO: s3_upload_file(filename, 'gof.crypto.shared', 'ftx_universe_'+str(datetime.now())+'.xlsx')

    print('refreshed universe')
    return futures

# runs optimization * [time, params]
async def perp_vs_cash(
        exchange,
        signal_horizon,
        holding_period,
        slippage_override,
        equity,
        concentration_limit,
        mktshare_limit,
        minimum_carry,
        exclusion_list,
        backtest_start = None,# None means live-only
        backtest_end = None):

    frame = inspect.currentframe()
    args, _, _, values = inspect.getargvalues(frame)
    print(f'running {[(i, values[i]) for i in args]} ')

    markets = await exchange.fetch_markets()
    futures = pd.DataFrame(await fetch_futures(exchange, includeExpired=False)).set_index('name')
    now_time = datetime.now()

    # qualitative filtering
    universe = await refresh_universe(exchange,UNIVERSE)
    universe = universe[~universe['underlying'].isin(exclusion_list)]
    type_allowed = TYPE_ALLOWED
    filtered = futures[(futures['type'].isin(type_allowed))
                       & (futures['symbol'].isin(universe.index))]
    futures = None #safety..

    # fee estimation params
    slippage_scaler = 1
    slippage_orderbook_depth = 10000

    # previous book
    if not equity is None:
        previous_weights_df = pd.DataFrame(index=[],columns=['optimalWeight'],data=0.0)
    elif EQUITY.isnumeric():
        previous_weights_df = pd.DataFrame(index=[],columns=['optimalWeight'],data=0.0)
        equity = float(EQUITY)
    elif '.xlsx' in EQUITY:
        previous_weights_df = pd.read_excel(EQUITY, sheet_name='optimized', index_col=0)['optimalWeight']
        equity = previous_weights_df.loc['total']
        previous_weights_df = previous_weights_df.drop(['USD', 'total'])
    else:
        start_portfolio = await fetch_portfolio(exchange, now_time)
        previous_weights_df = -start_portfolio.loc[
            start_portfolio['attribution'].isin(filtered.index), ['attribution', 'usdAmt']
        ].set_index('attribution').rename(columns={'usdAmt': 'optimalWeight'})
        equity = start_portfolio.loc[start_portfolio['event_type'] == 'PV', 'usdAmt'].values[0]

    # run a trajectory
    if backtest_start and backtest_end:
        point_in_time = backtest_start + signal_horizon + holding_period
    else:
        point_in_time = now_time.replace(minute=0, second=0, microsecond=0)-timedelta(hours=1)
        backtest_start = point_in_time
        backtest_end = point_in_time

    ## ----------- enrich/carry filter, get history, populate concentration limit
    enriched = await enricher(exchange, filtered, holding_period, equity=equity,
                              slippage_override=slippage_override, slippage_orderbook_depth=slippage_orderbook_depth,
                              slippage_scaler=slippage_scaler,
                              params={'override_slippage': True, 'type_allowed': type_allowed, 'fee_mode': 'retail'})
    await build_history(enriched,exchange)
    hy_history = await get_history(enriched, end=backtest_end, start_or_nb_hours=backtest_start-signal_horizon-holding_period)
    enriched = market_capacity(enriched, hy_history)

    # ------- build derived data history
    (intLongCarry, intShortCarry, intUSDborrow, intBorrow, E_long, E_short, E_intUSDborrow, E_intBorrow) = forecast(
        exchange, enriched, hy_history,
        holding_period,  # to convert slippage into rate
        signal_horizon,filename='Runtime/logs/portfolio_optimizer/history.xlsx')  # historical window for expectations)
    updated = update(enriched, point_in_time, hy_history, equity,
                     intLongCarry, intShortCarry, intUSDborrow, intBorrow, E_long, E_short, E_intUSDborrow, E_intBorrow,
                     minimum_carry=0) # do not remove futures using minimum_carry
    enriched = None  # safety..

    # final filter, needs some history and good avg volumes
    filtered = updated[~np.isnan(updated['E_intCarry'])]
    filtered = filtered.sort_values(by='E_intCarry', ascending=False)
    updated = None #safety...

    # run a trajectory
    previous_time = point_in_time
    trajectory = pd.DataFrame()

    if backtest_start == backtest_end:
        previous_weights = previous_weights_df
    else:
        # set initial weights at 0
        previous_weights = pd.DataFrame()
        previous_weights['optimalWeight'] = 0

    while point_in_time <= backtest_end:
        updated = update(filtered, point_in_time, hy_history, equity,
                         intLongCarry, intShortCarry, intUSDborrow, intBorrow, E_long, E_short, E_intUSDborrow,E_intBorrow,
                         minimum_carry=minimum_carry,
                         previous_weights_index=previous_weights.index)

        optimized = cash_carry_optimizer(exchange, updated,
                                         previous_weights_df=previous_weights[
                                             previous_weights.index.isin(filtered.index)],
                                         holding_period=holding_period,
                                         signal_horizon=signal_horizon,
                                         concentration_limit=concentration_limit,
                                         mktshare_limit=mktshare_limit,
                                         equity=equity,
                                         optional_params=(['verbose'] if __debug__ else []) + (['cost_blind']
                                         if (point_in_time == backtest_start)&(backtest_start != backtest_end)
                                         else [])) # ignore costs on first time of a backtest
        # need to assign RealizedCarry to previous_time
        if not trajectory.empty:
            trajectory.loc[trajectory['time'] == previous_time,'RealizedCarry'] = \
                trajectory.loc[trajectory['time'] == previous_time,'name'].apply(
                    lambda f: optimized.loc[f,'RealizedCarry'] if f in optimized.index else 0)
        optimized['time'] = point_in_time

        # increment
        trajectory = trajectory.append(optimized.reset_index().rename({'name': 'symbol'}), ignore_index=True)
        trajectory.to_excel('Runtime/logs/portfolio_optimizer/temp_trajectory.xlsx')
        previous_weights = optimized['optimalWeight'].drop(index=['USD', 'total'])
        previous_time = point_in_time
        point_in_time += holding_period

    # for live, just send last optimized
    if backtest_start==backtest_end:
        filename = 'Runtime/ApprovedRuns/ftx_optimal_cash_carry_'+datetime.utcnow().strftime("%Y-%m-%d-%Hh")+'.xlsx'
        with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
            parameters = pd.Series({
                'run_date':datetime.today(),
                'universe':UNIVERSE,
                'exclusion_list': exclusion_list,
                'type_allowed': type_allowed,
                'signal_horizon': signal_horizon,
                'holding_period': holding_period,
                'slippage_override':slippage_override,
                'concentration_limit': concentration_limit,
                'equity':equity,
                'slippage_scaler': slippage_scaler,
                'slippage_orderbook_depth': slippage_orderbook_depth})
            optimized.to_excel(writer,sheet_name='optimized')
            parameters.to_excel(writer,sheet_name='parameters')
            updated.to_excel(writer, sheet_name='snapshot')

        shutil.copy2(filename,'Runtime/ApprovedRuns/current_weights.xlsx')

        display = optimized[['optimalWeight','ExpectedCarry','transactionCost']]
        totals = display.loc[['USD','total']]
        display = display.drop(index=['USD','total']).sort_values(by='optimalWeight',key=lambda f: np.abs(f),ascending=False).append(totals)
        #display= display[display['absWeight'].cumsum()>display.loc['total','absWeight']*.1]
        print(display)

        return optimized
    # for backtest, remove last line because RealizedCarry is wrong there
    else:
        trajectory = trajectory.drop(trajectory[trajectory['time'] == previous_time].index)
        trajectory['slippage_override'] = slippage_override
        trajectory['concentration_limit'] = concentration_limit
        trajectory['signal_horizon'] = signal_horizon
        trajectory['holding_period'] = holding_period

        global run_i
        filename = 'Runtime/logs/portfolio_optimizer/run_'+str(run_i)+'_'+datetime.utcnow().strftime("%Y-%m-%d-%Hh")+'.xlsx'
        run_i+=1
        with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
            parameters = pd.Series({
                'run_date':datetime.today(),
                'universe':UNIVERSE,
                'exclusion_list': exclusion_list,
                'type_allowed': type_allowed,
                'signal_horizon': signal_horizon,
                'holding_period': holding_period,
                'slippage_override':slippage_override,
                'concentration_limit': concentration_limit,
                'mktshare_limit': mktshare_limit,
                'equity':equity,
                'slippage_scaler': slippage_scaler,
                'slippage_orderbook_depth': slippage_orderbook_depth,
                'backtest_start':backtest_start,
                'backtest_end':backtest_end,})
            trajectory.to_excel(writer,sheet_name='trajectory')
            parameters.to_excel(writer,sheet_name='parameters')

        return trajectory

async def strategy_wrapper(**kwargs):

    if EQUITY.isnumeric() or '.xlsx' in EQUITY:
        exchange = await open_exchange(kwargs['exchange'], '')
    else:
        exchange = await open_exchange(kwargs['exchange'],EQUITY, config={'asyncio_loop':asyncio.get_running_loop()})
    await exchange.load_markets()

    result = await safe_gather([perp_vs_cash(
        exchange=exchange,
        equity=equity,
        concentration_limit=concentration_limit,
        mktshare_limit=mktshare_limit,
        minimum_carry=minimum_carry,
        exclusion_list=kwargs['exclusion_list'],
        signal_horizon=signal_horizon,
        holding_period=holding_period,
        slippage_override=slippage_override,
        backtest_start=kwargs['backtest_start'],
        backtest_end=kwargs['backtest_end'])
        for equity in kwargs['equity']
        for concentration_limit in kwargs['concentration_limit']
        for mktshare_limit in kwargs['mktshare_limit']
        for minimum_carry in kwargs['minimum_carry']
        for signal_horizon in kwargs['signal_horizon']
        for holding_period in kwargs['holding_period']
        for slippage_override in kwargs['slippage_override']])
    await exchange.close()

    return result

def strategies_main(*argv):
    argv=list(argv)
    if len(argv) == 0:
        argv.extend(['sysperp'])
    if len(argv) < 3:
        argv.extend([HOLDING_PERIOD, SIGNAL_HORIZON])
    print(f'running {argv}')
    if argv[0] == 'sysperp':
        return asyncio.run(strategy_wrapper(
            exchange='ftx',
            equity=[None],
            concentration_limit=[CONCENTRATION_LIMIT],
            mktshare_limit=[MKTSHARE_LIMIT],
            minimum_carry=[MINIMUM_CARRY],
            exclusion_list=EXCLUSION_LIST,
            signal_horizon=[argv[2]],
            holding_period=[argv[1]],
            slippage_override=[SLIPPAGE_OVERRIDE],
            backtest_start=None,backtest_end=None))[0]
    elif argv[0] == 'depth':
        global UNIVERSE
        UNIVERSE = 'max' # set universe to 'max'
        equities = [100000, 1000000, 5000000]
        results = asyncio.run(strategy_wrapper(
            exchange='ftx',
            equity=equities,
            concentration_limit=[CONCENTRATION_LIMIT],
            mktshare_limit=[MKTSHARE_LIMIT],
            minimum_carry=[MINIMUM_CARRY],
            exclusion_list=EXCLUSION_LIST,
            signal_horizon=[argv[1]],
            holding_period=[argv[2]],
            slippage_override=[SLIPPAGE_OVERRIDE],
            backtest_start=None, backtest_end=None))
        with pd.ExcelWriter('Runtime/logs/portfolio_optimizer/depth.xlsx', engine='xlsxwriter') as writer:
            for res,equity in zip(results,equities):
                res.to_excel(writer,sheet_name=str(equity))
        print(pd.concat({res.loc['total','optimalWeight']:res[['optimalWeight','ExpectedCarry']]/res.loc['total','optimalWeight'] for res in results},axis=1))
        return results
    elif argv[0] == 'backtest':
        for equity in [[1000000]]:
            for concentration_limit in [[1]]:
                for mktshare_limit in [[MKTSHARE_LIMIT]]:
                    for minimum_carry in [[MINIMUM_CARRY]]:
                        for signal_horizon in [[timedelta(hours=h) for h in [24]]]:
                            for holding_period in [[timedelta(hours=h) for h in [48]]]:
                                for slippage_override in [[0.0002]]:
                                    asyncio.run(strategy_wrapper(
                                        exchange='ftx',
                                        equity=equity,
                                        concentration_limit=concentration_limit,
                                        mktshare_limit=mktshare_limit,
                                        minimum_carry=minimum_carry,
                                        exclusion_list=EXCLUSION_LIST,
                                        signal_horizon=signal_horizon,
                                        holding_period=holding_period,
                                        slippage_override=slippage_override,
                                        backtest_start=datetime(2021,9,1),
                                        backtest_end=datetime(2022,3,1)))
        return pd.DataFrame()
    else:
        print(f'commands: sysperp [signal_horizon] [holding_period], backtest, depth [signal_horizon] [holding_period]')
        raise Exception('unknown request ' + argv[0])

if __name__ == "__main__":
    strategies_main(*sys.argv[1:])





