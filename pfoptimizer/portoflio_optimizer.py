import copy
import inspect
import datetime

from pfoptimizer.ftx_snap_basis import *
from riskpnl.ftx_risk_pnl import *
from utils.ftx_utils import Static, find_spot_ticker
from utils.config_loader import *
from histfeed.ftx_history import get_history
from utils.ccxt_utilities import open_exchange
from utils.MyLogger import build_logging

async def refresh_universe(exchange, universe_filter):
    ''' Reads from universe.json '''
    ''' futures need to be enriched first before this can run '''

    dir_name = configLoader.get_mktdata_folder_for_exchange(exchange.describe()['id'])
    universe_params = configLoader.get_universe_params()

    try:
        universe = configLoader.get_bases(universe_filter)
        return universe
    except FileNotFoundError as e:
        futures = pd.DataFrame(await Static.fetch_futures(exchange)).set_index('name')
        markets = await exchange.fetch_markets()

        universe_start = datetime(2021, 12, 1).replace(tzinfo=timezone.utc)
        universe_end = datetime(year=datetime.utcnow().replace(tzinfo=timezone.utc).year, month=datetime.utcnow().replace(tzinfo=timezone.utc).month, day=datetime.utcnow().replace(tzinfo=timezone.utc).day) - timedelta(days=1)
        borrow_decile = 0.5

        screening_params = universe_params["screening"]

        # qualitative screening
        futures = futures[
                        (futures['expired'] == False)
                        & (futures.apply(lambda f: float(find_spot_ticker(markets, f, 'ask')), axis=1) > 0.0)]
        futures = await enricher(exchange,futures,holding_period=timedelta(days=2),equity=1e6)

        # volume screening --> Assumes History is up to date
        hy_history = await get_history(dir_name, futures, start_or_nb_hours=universe_start, end=universe_end)
        futures = market_capacity(futures, hy_history, universe_filter_window=hy_history[universe_start:universe_end].index)

        new_universe = dict()
        # We want to sort from the lowest value to the biggest because bigger items will be retagged properly. "max" should start
        sorted_screening_params = dict(sorted(screening_params.items(), key=lambda item: sum(item[1][v] for v in item[1].keys())))
        for tier in sorted_screening_params:
            population = futures.loc[(futures['borrow_volume_decile'] > screening_params[tier]['borrow_volume_threshold'])
                                    & (futures['spot_volume_avg'] > screening_params[tier]['spot_volume_threshold'])
                                    & (futures['future_volume_avg'] > screening_params[tier]['future_volume_threshold'])
                                    & (futures['openInterestUsd'] > screening_params[tier]['open_interest_threshold'])]
            dict_population = population.to_dict(orient='index')
            dict_population = {k : tier for k, v in dict_population.items()}
            # new_universe = new_universe | dict_population
            for el in dict_population:
                if not new_universe.get(el, None):
                    new_universe[el] = {"tier": [dict_population[el]]}
                else:
                    new_universe[el]["tier"].append(dict_population[el])

        # Persist the statistics of the refreshed run
        universe_params['run_date'] = datetime.today()
        universe_params['universe_start'] = universe_start
        universe_params['universe_end'] = universe_end
        universe_params['borrow_decile'] = borrow_decile

        # Persist new params... should live in a logger file
        configLoader.persist_universe_params(universe_params)

        # Persist new refreshed universe
        configLoader.persist_universe(new_universe)

        logging.getLogger('pfoptimizer').info("Successfully refreshed universe")
        universe = configLoader.get_bases(universe_filter)
        return universe

# runs optimization * [time, params]
async def perp_vs_cash(
        exchange,
        config,
        signal_horizon,
        holding_period,
        slippage_override,
        concentration_limit,
        mktshare_limit,
        minimum_carry,
        exclusion_list,
        equity_override=None,
        backtest_start=None, # None means live-only
        backtest_end=None,
        optional_params=[]):# verbose,warm_start

    # Load defaults params
    param_equity = config['EQUITY']['value']
    param_type_allowed = config['TYPE_ALLOWED']['value']
    param_universe = config['UNIVERSE']['value']
    dir_name = configLoader.get_mktdata_folder_for_exchange(exchange.id)

    frame = inspect.currentframe()
    args, _, _, values = inspect.getargvalues(frame)
    logging.getLogger('pfoptimizer').info(f'running {[(i, values[i]) for i in args]} ')

    futures = pd.DataFrame(await Static.fetch_futures(exchange)).set_index('name')
    now_time = datetime.utcnow().replace(tzinfo=timezone.utc)

    # Qualitative filtering
    universe = await refresh_universe(exchange, param_universe)
    universe = [instrument_name for instrument_name in universe if instrument_name.split("-")[0] not in exclusion_list]
    # universe = universe[~universe['underlying'].isin(exclusion_list)]
    type_allowed = param_type_allowed
    filtered = futures[(futures['type'].isin(type_allowed))
                       & (futures['symbol'].isin(universe))]

    # Fee estimation params
    slippage_scaler = 1
    slippage_orderbook_depth = 10000

    # previous book
    if not equity_override is None:
        previous_weights_input = pd.DataFrame(index=[],columns=['optimalWeight'], data=0.0)
    elif param_equity.isnumeric():
        previous_weights_input = pd.DataFrame(index=[],columns=['optimalWeight'], data=0.0)
        equity_override = float(param_equity)
    elif '.xlsx' in param_equity:
        previous_weights_input = pd.read_excel(param_equity, sheet_name='optimized', index_col=0)['optimalWeight']
        equity_override = previous_weights_input.loc['total']
        previous_weights_input = previous_weights_input.drop(['USD', 'total'])
    else:
        start_portfolio = await fetch_portfolio(exchange, now_time)
        previous_weights_input = -start_portfolio.loc[
            start_portfolio['attribution'].isin(filtered.index), ['attribution', 'usdAmt']
        ].set_index('attribution').rename(columns={'usdAmt': 'optimalWeight'})
        equity_override = start_portfolio.loc[start_portfolio['event_type'] == 'PV', 'usdAmt'].values[0]

    # Run a trajectory
    log_path = os.path.join(os.sep, "tmp", "pfoptimizer")
    if not os.path.exists(log_path):
        os.umask(0)
        os.makedirs(log_path, mode=0o777)
    log_file = ''# TODO still on xlsx os.path.join(log_path, "history.csv")
    if backtest_start and backtest_end:
        trajectory_filename = os.path.join(os.sep,log_path, 'trajectory.csv')
        pnl_filename = os.path.join(os.sep, log_path, 'pnl.csv')
        if os.path.isfile(trajectory_filename) and os.path.isfile(pnl_filename):
            trajectory = pd.read_csv(trajectory_filename,parse_dates=['time'],index_col=0)
            pnl = pd.read_csv(pnl_filename,parse_dates=['end_time'],index_col=0)
            point_in_time = max(backtest_start,
                                pnl['end_time'].apply(lambda t: t.replace(tzinfo=timezone.utc)).max(),
                                trajectory['time'].apply(lambda t: t.replace(tzinfo=timezone.utc)).max())
        else:
            trajectory = pd.DataFrame()
            pnl = pd.DataFrame()
            point_in_time = backtest_start
    else:
        point_in_time = now_time.replace(minute=0, second=0, microsecond=0)-timedelta(hours=1)
        backtest_start = point_in_time
        backtest_end = point_in_time

    ## ----------- enrich/carry filter, get history, populate concentration limit
    enriched = await enricher(exchange, filtered, holding_period, equity=equity_override,
                              slippage_override=slippage_override, slippage_orderbook_depth=slippage_orderbook_depth,
                              slippage_scaler=slippage_scaler,
                              params={'override_slippage': True, 'type_allowed': type_allowed, 'fee_mode': 'retail'})

    # nb_of_days = 100
    # await get_history(dir_name, enriched, 24 * nb_of_days)
    # await build_history(enriched, exchange)
    hy_history = await get_history(dir_name, enriched, start_or_nb_hours=backtest_start-signal_horizon-holding_period, end=backtest_end)
    enriched = market_capacity(enriched, hy_history)

    (intLongCarry, intShortCarry, intUSDborrow, intBorrow, E_long, E_short, E_intUSDborrow, E_intBorrow) = \
        forecast(
            exchange, enriched, hy_history,
            holding_period,  # to convert slippage into rate
            signal_horizon,filename= log_file if 'verbose' in optional_params else ''
        )  # historical window for expectations
    updated = update(enriched, point_in_time, hy_history, equity_override,
                     intLongCarry, intShortCarry, intUSDborrow, intBorrow, E_long, E_short, E_intUSDborrow, E_intBorrow,
                     minimum_carry=0) # Do not remove futures using minimum_carry
    enriched = None  # safety

    # final filter, needs some history and good avg volumes
    filtered = updated.loc[~np.isnan(updated['E_intCarry'])]
    filtered = filtered.sort_values(by='E_intCarry', ascending=False)
    updated = None # safety

    if backtest_start == backtest_end:
        initial_weight = previous_weights_input
    else:
        # set initial weights at 0
        initial_weight = pd.DataFrame()
        initial_weight['optimalWeight'] = 0
    previous_weights = initial_weight
    prev_time = point_in_time
    prev_row = pd.Series(index=['previousWeight',
                                    'index',
                                    'spot',
                                    'mark',
                                    'RealizedCarry'],data=1)

    optimized = pd.DataFrame()
    while point_in_time <= backtest_end:
        try:
            updated = update(filtered, point_in_time, hy_history, equity_override,
                             intLongCarry, intShortCarry, intUSDborrow, intBorrow, E_long, E_short, E_intUSDborrow, E_intBorrow,
                             minimum_carry=minimum_carry,
                             previous_weights_index=previous_weights.index)

            optimized = cash_carry_optimizer(exchange, updated,
                                             previous_weights_df=previous_weights,
                                             holding_period=holding_period,
                                             signal_horizon=signal_horizon,
                                             concentration_limit=concentration_limit,
                                             mktshare_limit=mktshare_limit,
                                             equity=equity_override,
                                             optional_params = optional_params + (['cost_blind']
                                             if (point_in_time == backtest_start) & (backtest_start != backtest_end)
                                             else [])) # Ignore costs on first time of a backtest

            optimized = optimized[
                np.abs(optimized['optimalWeight']) >
                optimized.apply(lambda f: float(exchange.market(f.name)['info']['minProvideSize'])
                if f.name in exchange.markets_by_id else 0, axis=1)
                ]

            optimized['time'] = point_in_time
            previous_weights = optimized['optimalWeight'].drop(index=['USD', 'total'])

            # increment
            time = point_in_time
            row = optimized.reset_index()
            trajectory = pd.concat([trajectory,row])
            trajectory.to_csv(os.path.join(os.sep,log_path,'trajectory.csv'))

            pnl_list = []
            cash_flow = copy.deepcopy(row)
            cash_flow['end_time'] = time

            # weights
            cash_flow['amtUSD'] = cash_flow['previousWeight']
            cash_flow['bucket'] = 'weights'
            #TODO dupe ...
            cash_flow.loc[cash_flow['name']=='total','amtUSD'] = cash_flow['amtUSD'].sum()
            pnl_list += [cash_flow[['name','end_time','bucket','amtUSD']]]

            # spot_vs_index
            cash_flow['amtUSD'] = 10000*(1 - cash_flow['index']/cash_flow['spot'])
            cash_flow['bucket'] = 'spot_vs_index(bps)'
            pnl_list += [cash_flow[['name','end_time','bucket','amtUSD']].drop(cash_flow[cash_flow['name'].isin(['USD','total'])].index)]

            #carry
            cash_flow['amtUSD'] = cash_flow['RealizedCarry']*(time-prev_time).total_seconds()/365.25/24/3600
            cash_flow['bucket'] = 'carry(USD not annualized)'
            cash_flow.loc[cash_flow['name'] == 'total', 'amtUSD'] = cash_flow['amtUSD'].sum()
            pnl_list += [cash_flow[['name','end_time','bucket','amtUSD']]]

            # IR01
            cash_flow['amtUSD'] = (-cash_flow['previousWeight'] * ((cash_flow['mark'] - cash_flow['spot'])-(prev_row['mark'] - prev_row['spot']))/prev_row['spot'])
            cash_flow['bucket'] = 'IR01(USD)'
            cash_flow.loc[cash_flow['name'] == 'total', 'amtUSD'] = cash_flow['amtUSD'].sum()
            pnl_list += [cash_flow[['name','end_time','bucket','amtUSD']]]

            pnl = pd.concat([pnl]+pnl_list,axis=0)
            pnl.to_csv(os.path.join(os.sep,log_path,'pnl.csv'))

            prev_time = time
            prev_row = copy.deepcopy(row)

        except Exception as e:
            logging.getLogger('pfoptimizer').critical(str(e))
        finally:
            logging.getLogger('pfoptimizer').info(f'{str(point_in_time)} done')
            point_in_time += timedelta(hours=1)

    parameters = pd.Series({
            'run_date': datetime.today(),
            'universe': param_universe,
            'exclusion_list': exclusion_list,
            'type_allowed': type_allowed,
            'signal_horizon': signal_horizon,
            'holding_period': holding_period,
            'slippage_override': slippage_override,
            'concentration_limit': concentration_limit,
            'equity': equity_override,
            'slippage_scaler': slippage_scaler,
            'slippage_orderbook_depth': slippage_orderbook_depth})
    parameters.to_csv(os.path.join(os.sep,log_path,'parameters.csv'))

    # for live, send last optimized, and also shard them by coin.
    if backtest_start == backtest_end:
        pfoptimizer_path = os.path.join(configLoader.get_config_folder_path(), "pfoptimizer")

        # write logs
        if not os.path.exists(pfoptimizer_path):
            os.umask(0)
            os.makedirs(pfoptimizer_path, mode=0o777)
        pfoptimizer_res_filename = os.path.join(os.sep,
                                                pfoptimizer_path,
                                                'ftx_optimal_cash_carry_' + datetime.utcnow().strftime("%Y%m%d_%H%M%S"))
        optimized['exchange'] = exchange.id
        optimized['subaccount'] = exchange.headers['FTX-SUBACCOUNT']
        optimized.to_csv(f'{pfoptimizer_res_filename}_weights.csv')

        updated.to_csv(f'{pfoptimizer_res_filename}_snapshot.csv')
        parameters.to_csv(f'{pfoptimizer_res_filename}_parameters.csv')

        # stdout display
        display = optimized[['optimalWeight', 'ExpectedCarry', 'transactionCost']]
        totals = display.loc[['USD', 'total']]
        display = display.drop(index=['USD', 'total']).sort_values(by='optimalWeight',key=lambda f: np.abs(f),ascending=False).append(totals)
        #display= display[display['absWeight'].cumsum()>display.loc['total','absWeight']*.1]
        logging.getLogger('pfoptimizer').info(display)

        # send bus message
        pfoptimizer_res_last_filename = os.path.join(pfoptimizer_path, "current_weights.csv")
        optimized.to_csv(pfoptimizer_res_last_filename)
        optimized = optimized.drop(index=['USD', 'total']).sort_values(by='optimalWeight', key=lambda f: np.abs(f),
                                                                       ascending=False)
        for i in range(optimized.shape[0]):
            pfoptimizer_res_last_filename = os.path.join(pfoptimizer_path, f"weight_shard_{i}.csv")
            optimized.iloc[[i]].to_csv(pfoptimizer_res_last_filename)

        return optimized

    return trajectory

async def strategy_wrapper(**kwargs):

    if kwargs['equity'][0].isnumeric() or '.xlsx' in kwargs['equity'][0]:
        exchange = await open_exchange(kwargs['exchange_name'], '')
        if kwargs['equity'][0].isnumeric():
            equity_override = kwargs['equity'][0]
        else:
            raise Exception('not implemented')
    else:
        exchange = await open_exchange(kwargs['exchange_name'],
                                       kwargs['equity'][0],
                                       config={'asyncio_loop':asyncio.get_running_loop()})
        equity_override = None
    await exchange.load_markets()

    coroutines = [perp_vs_cash(
        exchange=exchange,
        config=kwargs['config'],
        equity_override=equity_override,
        concentration_limit=concentration_limit,
        mktshare_limit=mktshare_limit,
        minimum_carry=minimum_carry,
        exclusion_list=kwargs['exclusion_list'],
        signal_horizon=signal_horizon,
        holding_period=holding_period,
        slippage_override=slippage_override,
        backtest_start=kwargs['backtest_start'],
        backtest_end=kwargs['backtest_end'],
        optional_params=['verbose'] if (__debug__ and kwargs['backtest_start']==kwargs['backtest_end']) else [])
        for equity in kwargs['equity']
        for concentration_limit in kwargs['concentration_limit']
        for mktshare_limit in kwargs['mktshare_limit']
        for minimum_carry in kwargs['minimum_carry']
        for signal_horizon in kwargs['signal_horizon']
        for holding_period in kwargs['holding_period']
        for slippage_override in kwargs['slippage_override']]

    result = await safe_gather(coroutines)
    await exchange.close()

    return result

def main(*args,**kwargs):
    '''
        args:
            run_type = ["sysperp", "backtest", "depth"] (mandatory param)
            exchange = ["ftx"] (mandatory param)
        kwargs:
            subaccount = ["SysPerp"] (mandatory param for sysperp)
            config = /home/david/config/pfoptimizer_params.json (optionnal)
   '''

    logger = build_logging("pfoptimizer")

    run_type = args[1]
    if run_type not in ['backtest','depth','sysperp']:
        logger.critical('run_type {} not found'.format(kwargs['run_type']))

    exchange_name = args[2]
    if exchange_name not in ['ftx']:
        logger.critical('exchange_name {} not found'.format(kwargs['exchange_name']))

    if run_type == 'sysperp':
        subaccount = kwargs['subaccount']

    config = configLoader.get_pfoptimizer_params(dirname=kwargs['config'] if 'config' in kwargs else None)

    logger.critical(f'Running main {run_type} exchange {exchange_name} config {config}')

    if run_type == 'sysperp':
        res = asyncio.run(strategy_wrapper(
            exchange_name=exchange_name,
            config=config,
            equity=[subaccount],
            concentration_limit=[config["CONCENTRATION_LIMIT"]["value"]],
            mktshare_limit=[config["MKTSHARE_LIMIT"]["value"]],
            minimum_carry=[config["MINIMUM_CARRY"]["value"]],
            exclusion_list=config['EXCLUSION_LIST']["value"],
            signal_horizon=[parse_time_param(config['SIGNAL_HORIZON']['value'])],
            holding_period=[parse_time_param(config['HOLDING_PERIOD']['value'])],
            slippage_override=[config["SLIPPAGE_OVERRIDE"]["value"]],
            backtest_start=None,
            backtest_end=None))[0]
    elif run_type == 'depth':
        global UNIVERSE
        UNIVERSE = 'max'  # set universe to 'max'
        equities = [100000, 1000000, 5000000]
        res = asyncio.run(strategy_wrapper(
            exchange_name=exchange_name,
            equity=equities,
            concentration_limit=[config["CONCENTRATION_LIMIT"]["value"]],
            mktshare_limit=[config["MKTSHARE_LIMIT"]["value"]],
            minimum_carry=[config["MINIMUM_CARRY"]["value"]],
            exclusion_list=config['EXCLUSION_LIST']["value"],
            signal_horizon=[parse_time_param(config['SIGNAL_HORIZON']['value'])],
            holding_period=[parse_time_param(config['HOLDING_PERIOD']['value'])],
            slippage_override=[config["SLIPPAGE_OVERRIDE"]["value"]],
            backtest_start=None,
            backtest_end=None))
        with pd.ExcelWriter('Runtime/logs/portfolio_optimizer/depth.xlsx', engine='xlsxwriter') as writer:
            for res, equity in zip(res, equities):
                res.to_excel(writer, sheet_name=str(equity))
        logger.info(pd.concat({res.loc['total', 'optimalWeight']: res[['optimalWeight', 'ExpectedCarry']] / res.loc['total', 'optimalWeight'] for res in res}, axis=1))
    elif run_type == 'backtest':
        for equity in [[config["EQUITY"]["value"]]]:
            for concentration_limit in [[config["CONCENTRATION_LIMIT"]["value"]]]:
                for mktshare_limit in [[config["MKTSHARE_LIMIT"]["value"]]]:
                    for minimum_carry in [[config["MINIMUM_CARRY"]["value"]]]:
                        for sig_horizon in [[parse_time_param(h) for h in [config["SIGNAL_HORIZON"]["value"]]]]:
                            for hol_period in [[parse_time_param(h) for h in [config["HOLDING_PERIOD"]["value"]]]]:
                                for slippage_override in [[config["SLIPPAGE_OVERRIDE"]["value"]]]:
                                    asyncio.run(strategy_wrapper(
                                        exchange_name=exchange_name,
                                        equity=equity,
                                        concentration_limit=concentration_limit,
                                        mktshare_limit=mktshare_limit,
                                        minimum_carry=minimum_carry,
                                        exclusion_list=config["EXCLUSION_LIST"]["value"],
                                        signal_horizon=sig_horizon,
                                        holding_period=hol_period,
                                        slippage_override=slippage_override,
                                        backtest_start= datetime(2021,2,17).replace(tzinfo=timezone.utc),#.replace(minute=0, second=0, microsecond=0)-timedelta(days=2),# live start was datetime(2022,6,21,19),
                                        backtest_end = datetime.utcnow().replace(tzinfo=timezone.utc).replace(minute=0, second=0, microsecond=0)-timedelta(hours=1)))
        logger.critical("pfoptimizer terminated successfully...")
        return pd.DataFrame()
    else:
        logger.critical(f'commands: sysperp [signal_horizon] [holding_period], backtest, depth [signal_horizon] [holding_period]')

    logger.critical("pfoptimizer terminated successfully...")
    return res