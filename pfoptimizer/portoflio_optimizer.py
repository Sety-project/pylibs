import inspect
import asyncio
import sys
import logging
import os
import numpy as np
import shutil
import datetime
from datetime import timedelta
import pandas as pd

from pfoptimizer.ftx_snap_basis import *

from riskpnl.ftx_portfolio import *

from utils.ftx_utils import fetch_futures, find_spot_ticker
from utils.portfolio_optimizer_utils import *
from utils.async_utils import safe_gather
from utils.config_loader import *

from histfeed.ftx_history import get_history, build_history

from utils.ccxt_utilities import open_exchange

run_i = 0

LOGGER_NAME = __name__

async def refresh_universe(exchange, universe_filter):
    ''' Reads from universe.json '''
    ''' futures need to be enriched first before this can run '''

    logger = logging.getLogger(LOGGER_NAME)
    universe = configLoader.get_bases(universe_filter)
    dir_name = configLoader.get_mktdata_folder_for_exchange(exchange.describe()['id'])
    universe_params = configLoader.get_universe_params()

    if universe:
        return universe
    else:
        futures = pd.DataFrame(await fetch_futures(exchange, includeExpired=False)).set_index('name')
        markets = await exchange.fetch_markets()

        universe_start = datetime(2021, 12, 1)
        universe_end = datetime(2022, 5, 1)
        borrow_decile = 0.5

        screening_params = universe_params["screening"]

        #type_allowed=['perpetual']
        # screening_params=pd.DataFrame(
        #     index=['future_volume_threshold','spot_volume_threshold','borrow_volume_threshold','open_interest_threshold'],
        #     data={'max':[5e4,5e4,-1,5e4],# important that sets are decreasing  :(
        #           'wide':[1e5,1e5,2e5,5e6],# to run say 1M after leverage
        #           'institutional':[5e6,5e6,-1,1e7]})# instiutionals borrow OTC

        # qualitative screening
        futures = futures[
                        (futures['expired'] == False)
                        & (futures.apply(lambda f: float(find_spot_ticker(markets, f, 'ask')), axis=1) > 0.0)]
        futures = await enricher(exchange,futures,holding_period=timedelta(days=2),equity=1e6)

        # volume screening
        # await build_history(futures,exchange) --> Assumes History is up to date
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
            dict_population = {k : v | {"tier": tier} for k, v in dict_population.items()}
            new_universe = new_universe | dict_population
            # print(new_universe)

        # with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
        #     for c in screening_params:# important that wide is first :( c is among [max, wide, institutional]
        #         population = futures.loc[
        #             (futures['borrow_volume_decile'] > screening_params.loc['borrow_volume_threshold',c])
        #             & (futures['spot_volume_avg'] > screening_params.loc['spot_volume_threshold',c])
        #             & (futures['future_volume_avg'] > screening_params.loc['future_volume_threshold',c])
        #             & (futures['openInterestUsd'] > screening_params.loc['open_interest_threshold',c])]
        #         population.to_excel(writer, sheet_name=c)

        # Need to persist the statistics of the refresh run in new_universe dict
        universe_params['run_date'] = datetime.today()
        universe_params['universe_start'] = universe_start
        universe_params['universe_end'] = universe_end
        universe_params['borrow_decile'] = borrow_decile

        # Persist new params... should live in a logger file
        # configLoader.persist_universe_params(new_params)

        logger.info("Successfully refreshed universe")

        return new_universe

# runs optimization * [time, params]
async def perp_vs_cash(
        exchange_name,
        exchange,
        signal_horizon,
        holding_period,
        slippage_override,
        equity,
        concentration_limit,
        mktshare_limit,
        minimum_carry,
        exclusion_list,
        backtest_start=None, # None means live-only
        backtest_end=None):

    # Load defaults params
    pf_params = configLoader.get_pfoptimizer_params()
    param_equity = pf_params['EQUITY']['value']
    param_type_allowed = pf_params['TYPE_ALLOWED']['value']
    param_universe = pf_params['UNIVERSE']['value']
    dir_name = configLoader.get_mktdata_folder_for_exchange(exchange_name)

    frame = inspect.currentframe()
    args, _, _, values = inspect.getargvalues(frame)
    print(f'running {[(i, values[i]) for i in args]} ')

    futures = pd.DataFrame(await fetch_futures(exchange, includeExpired=False)).set_index('name')
    now_time = datetime.now()

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
    if not equity is None:
        previous_weights_df = pd.DataFrame(index=[],columns=['optimalWeight'], data=0.0)
    elif param_equity.isnumeric():
        previous_weights_df = pd.DataFrame(index=[],columns=['optimalWeight'], data=0.0)
        equity = float(param_equity)
    elif '.xlsx' in param_equity:
        previous_weights_df = pd.read_excel(param_equity, sheet_name='optimized', index_col=0)['optimalWeight']
        equity = previous_weights_df.loc['total']
        previous_weights_df = previous_weights_df.drop(['USD', 'total'])
    else:
        start_portfolio = await fetch_portfolio(exchange, now_time)
        previous_weights_df = -start_portfolio.loc[
            start_portfolio['attribution'].isin(filtered.index), ['attribution', 'usdAmt']
        ].set_index('attribution').rename(columns={'usdAmt': 'optimalWeight'})
        equity = start_portfolio.loc[start_portfolio['event_type'] == 'PV', 'usdAmt'].values[0]

    # Run a trajectory
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

    # nb_of_days = 100
    # await get_history(dir_name, enriched, 24 * nb_of_days)
    # await build_history(enriched, exchange)
    hy_history = await get_history(dir_name, enriched, start_or_nb_hours=backtest_start-signal_horizon-holding_period, end=backtest_end)
    enriched = market_capacity(enriched, hy_history)

    # ------- build derived data history
    log_path = os.path.join(os.sep, "tmp", "pfoptimizer")
    if not os.path.exists(log_path):
        os.umask(0)
        os.makedirs(log_path, mode=0o777)
    log_file = os.path.join(log_path, "history.xlsx")

    (intLongCarry, intShortCarry, intUSDborrow, intBorrow, E_long, E_short, E_intUSDborrow, E_intBorrow) = \
        forecast(
                exchange, enriched, hy_history,
                holding_period,  # to convert slippage into rate
                signal_horizon,filename=log_file
        )  # historical window for expectations
    updated = update(enriched, point_in_time, hy_history, equity,
                     intLongCarry, intShortCarry, intUSDborrow, intBorrow, E_long, E_short, E_intUSDborrow, E_intBorrow,
                     minimum_carry=0) # Do not remove futures using minimum_carry
    enriched = None  # safety

    # final filter, needs some history and good avg volumes
    filtered = updated.loc[~np.isnan(updated['E_intCarry'])]
    filtered = filtered.sort_values(by='E_intCarry', ascending=False)
    updated = None # safety

    # Run a trajectory
    previous_time = point_in_time
    trajectory = pd.DataFrame()

    if backtest_start == backtest_end:
        previous_weights = previous_weights_df
    else:
        # set initial weights at 0
        previous_weights = pd.DataFrame()
        previous_weights['optimalWeight'] = 0

    debug_mode = False #__debug__
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
                                         optional_params=(['verbose'] if debug_mode else []) + (['cost_blind']
                                         if (point_in_time == backtest_start) & (backtest_start != backtest_end)
                                         else [])) # Ignore costs on first time of a backtest
        # Need to assign RealizedCarry to previous_time
        if not trajectory.empty:
            trajectory.loc[trajectory['time'] == previous_time,'RealizedCarry'] = \
                trajectory.loc[trajectory['time'] == previous_time,'name'].apply(
                    lambda f: optimized.loc[f,'RealizedCarry'] if f in optimized.index else 0)
        optimized['time'] = point_in_time

        # increment
        trajectory = trajectory.append(optimized.reset_index().rename({'name': 'symbol'}), ignore_index=True)
        trajectory_file = os.path.join(log_path, "tmp_trajectory.xlsx")
        trajectory.to_excel(trajectory_file)
        previous_weights = optimized['optimalWeight'].drop(index=['USD', 'total'])
        previous_time = point_in_time
        point_in_time += holding_period

    # for live, just send last optimized
    if backtest_start == backtest_end:
        pfoptimizer_path = os.path.join(configLoader.get_config_folder_path(), "pfoptimizer")
        if not os.path.exists(pfoptimizer_path):
            os.umask(0)
            os.makedirs(pfoptimizer_path, mode=0o777)
        pfoptimizer_res_filename = os.path.join(pfoptimizer_path, 'ftx_optimal_cash_carry_' + datetime.utcnow().strftime("%Y%m%d_%H%M%S") + '.xlsx')
        with pd.ExcelWriter(pfoptimizer_res_filename, engine='xlsxwriter') as writer:
            parameters = pd.Series({
                'run_date':datetime.today(),
                'universe':param_universe,
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

        pfoptimizer_res_last_filename = os.path.join(pfoptimizer_path, "current_weights.xlsx")
        shutil.copy2(pfoptimizer_res_filename, pfoptimizer_res_last_filename)

        display = optimized[['optimalWeight', 'ExpectedCarry', 'transactionCost']]
        totals = display.loc[['USD', 'total']]
        display = display.drop(index=['USD', 'total']).sort_values(by='optimalWeight',key=lambda f: np.abs(f),ascending=False).append(totals)
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
        log_file = os.path.join(log_path, 'run_'+str(run_i)+'_'+datetime.utcnow().strftime("%Y%m%d_%H%M%S")+'.xlsx')
        run_i += 1
        with pd.ExcelWriter(log_file, engine='xlsxwriter') as writer:
            parameters = pd.Series({
                'run_date': datetime.today(),
                'universe': param_universe,
                'exclusion_list': exclusion_list,
                'type_allowed': type_allowed,
                'signal_horizon': signal_horizon,
                'holding_period': holding_period,
                'slippage_override':slippage_override,
                'concentration_limit': concentration_limit,
                'mktshare_limit': mktshare_limit,
                'equity': equity,
                'slippage_scaler': slippage_scaler,
                'slippage_orderbook_depth': slippage_orderbook_depth,
                'backtest_start': backtest_start,
                'backtest_end': backtest_end,})
            trajectory.to_excel(writer, sheet_name='trajectory')
            parameters.to_excel(writer, sheet_name='parameters')

        return trajectory

async def strategy_wrapper(**kwargs):

    pf_params = configLoader.get_pfoptimizer_params()

    if pf_params['EQUITY']['value'].isnumeric() or '.xlsx' in pf_params['EQUITY']['value']:
        exchange = await open_exchange(kwargs['exchange_name'], '')
    else:
        exchange = await open_exchange(kwargs['exchange_name'],
                                       pf_params['EQUITY']['value'],
                                       config={'asyncio_loop':asyncio.get_running_loop()})
    await exchange.load_markets()

    coroutines = [perp_vs_cash(
        exchange_name=kwargs['exchange_name'],
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
        for slippage_override in kwargs['slippage_override']]

    result = await safe_gather(coroutines)
    await exchange.close()

    return result

def main(*args):
    '''
        Parameters could be passed in any order
        @params:
           # For now, defaults to exchange_name = ftx
           run_type = ["sysperp", "backtest", "depth"] (mandatory param)
           exchange = ["ftx"] (mandatory param)
        @Example runs:
            - main (this will read the config from the config file, this is how docker will call)
            - main 2h 2d (this is when running local, to debug faster)
            - main ftx sysperp/backtest/depth [signal_horizon] [holding_period]
            - main ftx sysperp [signal_horizon] [holding_period], backtest, depth [signal_horizon] [holding_period]
   '''

    set_logger("pfoptimizer")
    logger = logging.getLogger(LOGGER_NAME)

    # RUN_MODE = ["prod", "debug"]
    RUN_TYPES = ["sysperp", "backtest", "depth"]
    EXCHANGE_NAMES_AVAILABLE = ["ftx"]

    # Reads config to collect params
    pf_params = configLoader.get_pfoptimizer_params()

    args = list(*args)[1:]

    # Getting exchange name, only ftx handled for now
    try:
        exchange_name = [x for x in args if x in EXCHANGE_NAMES_AVAILABLE][0]
        if exchange_name != 'ftx':
            logger.critical(f"The exchange_name param should be 'ftx' but {exchange_name} was passed. Only ftx is handled for the moment")
            logger.critical("---> Terminating...")
            sys.exit(1)
    except IndexError:
        logger.critical("Cannot find the exchange_name param. The exchange_name param should be passed explicitly : only 'ftx' is handled for the moment")
        logger.critical("---> Terminating...")
        sys.exit(1)

    # Getting RUN_MODE
    # try:
    #     run_mode = [x for x in args if x in RUN_MODE][0]
    # except IndexError:
    #     logger.critical(f"Cannot find the exchange_name param. The exchange_name param should be passed explicitly among : {RUN_MODE}")
    #     logger.critical("---> Terminating...")
    #     sys.exit(1)

    # Getting RUN_TYPES
    try:
        run_type = [x for x in args if x in RUN_TYPES][0]
    except IndexError:
        logger.critical(f"Cannot read the run_type param from config/{configLoader.get_pfoptimizer_params_filename()}")
        logger.critical("---> Terminating...")
        sys.exit(1)

    # Getting the SIGNAL_HORIZON and the HOLDING_HORIZON
    try:
        horizons = [x for x in args if x[0].isnumeric()]
        if len(horizons) == 1:
            logger.info(f"Should explicitly pass 2 horizons, or None to use default config")
            logger.critical("---> Terminating...")
            sys.exit(1)
        elif len(horizons) == 2:
            logger.info(f"Guessing horizons from params")
            logger.info(f"Assuming holding_horizon is smaller than signal_horizon ")
            horizons = sorted([parse_time_param(x) for x in horizons])
            holding_period = horizons[0]
            signal_horizon = horizons[1]
        else:
            # Using config horizons
            logger.info(f"No horizons were found or could not implicit horizons --> defaulting to config/{configLoader.get_pfoptimizer_params_filename()}...")
            holding_period = parse_time_param(pf_params["HOLDING_PERIOD"]["value"])
            signal_horizon = parse_time_param(pf_params["SIGNAL_HORIZON"]["value"])
    except IndexError:
            logger.critical(f"Cannot read the SIGNAL_HORIZON or the HOLDING_PERIOD params from config/{configLoader.get_pfoptimizer_params_filename()}")
            logger.critical("---> Terminating...")
            sys.exit(1)

    logger.info(f'Running main {run_type} holding_period={holding_period} signal_horizon={signal_horizon}')

    if run_type == 'sysperp':
        res = asyncio.run(strategy_wrapper(
            exchange_name=exchange_name,
            equity=[None],
            concentration_limit=[pf_params["CONCENTRATION_LIMIT"]["value"]],
            mktshare_limit=[pf_params["MKTSHARE_LIMIT"]["value"]],
            minimum_carry=[pf_params["MINIMUM_CARRY"]["value"]],
            exclusion_list=pf_params['EXCLUSION_LIST']["value"],
            signal_horizon=[signal_horizon],
            holding_period=[holding_period],
            slippage_override=[pf_params["SLIPPAGE_OVERRIDE"]["value"]],
            backtest_start=None,
            backtest_end=None))[0]
    elif run_type == 'depth':
        global UNIVERSE
        UNIVERSE = 'max'  # set universe to 'max'
        equities = [100000, 1000000, 5000000]
        res = asyncio.run(strategy_wrapper(
            exchange_name=exchange_name,
            equity=equities,
            concentration_limit=list(pf_params["CONCENTRATION_LIMIT"]["value"]),
            mktshare_limit=list(pf_params["MKTSHARE_LIMIT"]["value"]),
            minimum_carry=list(pf_params["MINIMUM_CARRY"]["value"]),
            exclusion_list=pf_params["EXCLUSION_LIST"]["value"],
            signal_horizon=[signal_horizon],
            holding_period=[holding_period],
            slippage_override=list(pf_params["SLIPPAGE_OVERRIDE"]["value"]),
            backtest_start=None,
            backtest_end=None))
        with pd.ExcelWriter('Runtime/logs/portfolio_optimizer/depth.xlsx', engine='xlsxwriter') as writer:
            for res, equity in zip(res, equities):
                res.to_excel(writer, sheet_name=str(equity))
        print(pd.concat({res.loc['total', 'optimalWeight']: res[['optimalWeight', 'ExpectedCarry']] / res.loc['total', 'optimalWeight'] for res in res}, axis=1))
    elif run_type == 'backtest':
        for equity in [[1000000]]:
            for concentration_limit in [[1]]:
                for mktshare_limit in [[pf_params["MKTSHARE_LIMIT"]["value"]]]:
                    for minimum_carry in [[pf_params["MINIMUM_CARRY"]["value"]]]:
                        for sig_horizon in [[timedelta(hours=h) for h in [24]]]:
                            for hol_period in [[timedelta(hours=h) for h in [48]]]:
                                for slippage_override in [[0.0002]]:
                                    asyncio.run(strategy_wrapper(
                                        exchange_name=exchange_name,
                                        equity=equity,
                                        concentration_limit=concentration_limit,
                                        mktshare_limit=mktshare_limit,
                                        minimum_carry=minimum_carry,
                                        exclusion_list=pf_params["EXCLUSION_LIST"]["value"],
                                        signal_horizon=sig_horizon,
                                        holding_period=hol_period,
                                        slippage_override=slippage_override,
                                        backtest_start=datetime(2021, 9, 1),
                                        backtest_end=datetime(2022, 3, 1)))
        logger.info("pfoptimizer terminated successfully...")
        return pd.DataFrame()
    else:
        logger.critical(f'commands: sysperp [signal_horizon] [holding_period], backtest, depth [signal_horizon] [holding_period]')

    logger.info("pfoptimizer terminated successfully...")
    return res

def parse_time_param(param):
    if 'mn' in param:
        horizon = int(param.split('mn')[0])
        horizon = timedelta(minutes=horizon)
    elif 'h' in param:
        horizon = int(param.split('h')[0])
        horizon = timedelta(hours=horizon)
    elif 'd' in param:
        horizon = int(param.split('d')[0])
        horizon = timedelta(days=horizon)
    elif 'w' in param:
        horizon = int(param.split('w')[0])
        horizon = timedelta(weeks=horizon)
    return horizon

def set_logger(app_name):
    ''' Function that sets a logger for the app, for debugging purposes '''

    time_date_stamp = strftime("%Y%m%d_%H%M%S")
    filepath = os.path.join(os.sep, 'tmp', app_name)
    if not os.path.exists(filepath):
        os.umask(0)
        os.makedirs(filepath, mode=0o777)

    logging.basicConfig()
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.INFO)

    # Create file handler which logs even debug messages
    filename = os.path.join(filepath, app_name + '_' + time_date_stamp + '.log')
    fh = logging.FileHandler(filename)
    fh.setFormatter(logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s'))
    fh.setLevel(logging.DEBUG)
    logger.addHandler(fh)

    logger.info("Logger ready")