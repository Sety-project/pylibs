from tradeexecutor.binance.api import BinanceAPI
from tradeexecutor.interface.builders import build_VenueAPI
from utils.io_utils import *
from utils.api_utils import api
from utils.async_utils import *

history_start = datetime(2023, 9, 1).replace(tzinfo=timezone.utc)

# all rates annualized, all volumes daily in usd
async def get_history(dirname,
                      futures,
                      start_or_nb_hours,
                      end=datetime.utcnow().replace(tzinfo=timezone.utc).replace(minute=0, second=0, microsecond=0),
                      resample='8h'):
    data = pd.concat(await safe_gather((
            [async_read_csv(dirname + os.sep + f + '_funding.csv', index_col=0, parse_dates=True)
             for f in futures[futures['type'] == 'perpetual'].index] +
            [async_read_csv(dirname + os.sep + f + '_futures.csv', index_col=0, parse_dates=True)
             for f in futures.index] +
            [async_read_csv(dirname + os.sep + f + '_price.csv', index_col=0, parse_dates=True)
             for f in futures['spot_ticker'].unique()] +
            [async_read_csv(dirname + os.sep + f + '_borrow.csv', index_col=0, parse_dates=True)
             for f in set(list(futures['underlying'].unique()) + ['USDT', 'USDC', 'FDUSD'])]
    )), join='outer', axis=1)

    start = end - timedelta(hours=start_or_nb_hours) if isinstance(start_or_nb_hours, int) else start_or_nb_hours
    data.index = [t.replace(tzinfo=timezone.utc) for t in data.index]
    data = data[~data.index.duplicated()].sort_index()[start:end]
    data = data.ffill().resample(resample).last()
    return data


async def build_history(futures,
                        exchange,
                        dirname=configLoader.get_mktdata_folder_path(),
                        end=datetime.utcnow().replace(tzinfo=timezone.utc).replace(minute=0, second=0, microsecond=0),
                        frequency: timedelta = '1h'):  # Runtime/Mktdata_database
    '''for now, increments local files and then uploads to s3'''
    logger = logging.getLogger('histfeed')
    logger.info("BUILDING COROUTINES")

    coroutines = []
    for _, f in futures[futures['type'] == 'perpetual'].iterrows():
        csv_name = os.path.join(dirname, f'{f.name}_funding.csv')
        csv = pd.read_csv(csv_name, index_col=0, parse_dates=True) if os.path.isfile(csv_name) else None
        start = max(csv.index).replace(tzinfo=timezone.utc) + timedelta(hours=8) if csv is not None else history_start
        if start < end:
            logger.info(f"Adding coroutine {csv_name}")
            coroutines.append(exchange.funding_history(f, start, end, dirname))

    for _, f in futures.iterrows():
        csv_name = os.path.join(dirname, f'{f.name}_futures.csv')
        csv = pd.read_csv(csv_name, index_col=0, parse_dates=True) if os.path.isfile(csv_name) else None
        start = max(csv.index).replace(tzinfo=timezone.utc) + pd.Timedelta(
            frequency) if csv is not None else history_start
        if start < end:
            logger.info(f"Adding coroutine {csv_name}")
            coroutines.append(exchange.rate_history(f, end, start, frequency, dirname))

    for f in futures['spot_ticker'].unique():
        csv_name = os.path.join(dirname, f'{f}_price.csv')
        csv = pd.read_csv(csv_name, index_col=0, parse_dates=True) if os.path.isfile(csv_name) else None
        start = max(csv.index).replace(tzinfo=timezone.utc) + pd.Timedelta(
            frequency) if csv is not None else history_start
        if start < end:
            logger.info(f"Adding coroutine {csv_name}")
            coroutines.append(exchange.spot_history(f, end, start, frequency, dirname))

    for f in set(list(futures['underlying'].unique()) + ['USDT', 'FDUSD']):
        csv_name = os.path.join(dirname, f'{f}_borrow.csv')
        csv = pd.read_csv(csv_name, index_col=0, parse_dates=True) if os.path.isfile(csv_name) else None
        start = max(csv.index).replace(tzinfo=timezone.utc) + timedelta(hours=8) if csv is not None else history_start
        if start < end:
            logger.info(f"Adding coroutine {csv_name}")
            coroutines.append(exchange.borrow_history(f, end, start, dirname))

    if coroutines:
        # run all coroutines
        logger.info(f"Gathered {len(coroutines)} coroutines, processing them {safe_gather_limit} at a time")
        await safe_gather(coroutines)
    else:
        logger.info("0 coroutines gathered, nothing to do")

    # static values for non spot Margin underlyings
    otc_file = configLoader.get_static_params_used()
    for f in list(futures.loc[futures['spotMargin'] == False, 'spot_ticker'].unique()):
        try:
            spot_csv = pd.read_csv(os.path.join(dirname, f.replace('/', '') + '_price.csv'), index_col=0,
                                   parse_dates=True)
            coin = f.split('/')[0]
            to_csv(pd.DataFrame(index=spot_csv.index,
                                columns=[coin + '_rate_borrow'],
                                data=otc_file.loc[coin, 'borrow'] if futures.loc[f, 'spotMargin'] == 'OTC' else 999
                                ),
                   os.path.join(dirname, coin + '_borrow.csv'),
                   mode='a', header=False)
        except Exception as e:
            logger.warning(e, exc_info=True)


async def correct_history(futures, exchange, hy_history, dirname=configLoader.get_mktdata_folder_path()):
    '''for now, increments local files and then uploads to s3'''

    logger = logging.getLogger('histfeed')
    logger.info("CORRECTING HISTORY")

    coroutines = []
    for _, f in futures[futures['type'] == 'perpetual'].iterrows():
        csv_name = os.path.join(dirname, f'{f.name}_funding.csv')
        coroutines.append(async_to_csv(hy_history[[exchange.market(f['symbol'])['id'] + '_rate_funding']], csv_name))
        logger.info("Adding coroutine for correction " + csv_name)

    for _, f in futures.iterrows():
        csv_name = os.path.join(dirname, f'{f.name}_futures.csv')
        future_id = exchange.market(f['symbol'])['id']
        column_names = [future_id + '_mark_' + field for field in ['o', 'h', 'l', 'c', 'volume']]
        column_names += [future_id + '_indexes_' + field for field in ['o', 'h', 'l', 'c', 'volume']]
        column_names += [future_id + '_rate_' + field for field in
                         ['h', 'l', 'c'] + (['T'] if f['type'] == 'future' else [])]
        coroutines.append(async_to_csv(hy_history[column_names], csv_name))
        logger.info("Adding coroutine for correction " + csv_name)

    for f in futures['spot_ticker'].unique():
        csv_name = os.path.join(dirname, f'{f}_price.csv')
        column_names = [f + '_price_' + field for field in ['o', 'h', 'l', 'c', 'volume']]
        coroutines.append(async_to_csv(hy_history[column_names], csv_name))
        logger.info("Adding coroutine for correction " + csv_name)

    for f in set(list(futures.loc[futures['spotMargin'] == True, 'base'].unique()) + ['USDT', 'FDUSD', 'USDC']):
        csv_name = os.path.join(dirname, f'{f}_borrow.csv')
        column_names = [f + '_rate_' + field for field in ['borrow', 'size']]
        coroutines.append(async_to_csv(hy_history[column_names], csv_name))
        logger.info("Adding coroutine for correction " + csv_name)

    # run all coroutines
    await safe_gather(coroutines)


async def history_main_wrapper(run_type, exchange_name, universe_name, nb_days=1, frequency='1h'):
    parameters = configLoader.get_executor_params(order='listen_binance')
    exchange = await build_VenueAPI(parameters['venue_api'])

    def _array_concat(a, b):
        return (a or []) + (b or [])

    exchange.array_concat = _array_concat

    universe = []  # configLoader.get_bases(universe_name)
    nb_days = int(nb_days)
    futures = pd.DataFrame(await BinanceAPI.Static.fetch_futures(exchange)).set_index('name')

    # universe should be either 'all', either a universe name, or a list of currencies
    dir_name = configLoader.get_mktdata_folder_for_exchange(exchange_name)

    # In case universe was not max, is_wide, is_institutional
    # universe = [id for id, data in exchange.markets_by_id.items() if data['base'] in [x.upper() for x in [universe]] and data['contract']]

    if universe != []:
        futures = futures[futures.index.isin(universe)]
    else:
        futures = futures.sort_values('openInterestUsd', ascending=False)

    logger = logging.getLogger('histfeed')

    # Volume Screening
    if run_type == 'build':
        logger.info("Building history for build")
        await build_history(futures, exchange, dir_name, frequency=frequency)
        await exchange.close()
    elif run_type == 'correct':
        raise Exception('bug. does not correct')
        logger.info("Building history for correct")
        hy_history = await get_history(dir_name, futures, history_start)
        end = datetime.utcnow().replace(tzinfo=timezone.utc) - timedelta(days=nb_days)
        await correct_history(futures, exchange, hy_history[:end])
        await build_history(futures, exchange, dir_name, frequency=frequency)
        await exchange.close()
    elif run_type == 'get':
        logger.info("Getting history...")
        hy_history = await get_history(dir_name, futures, 24 * nb_days)
        await exchange.close()
        return hy_history


@api
def main(*args, **kwargs):
    '''
        example: histfeed get ftx wide 5 1h
        args:
           run_type = "build", "correct", "get"
           exchange = "binance"
           universe = "institutional", "wide", "max", "all"
           nb_days = int
           frequency = str
   '''
    # __logger is NOT an argument, it's provided by @api
    logger = kwargs.pop('__logger')

    # Make sure the exchange repo exists in mktdata/, if not creates it
    mktdata_exchange_repo = configLoader.get_mktdata_folder_for_exchange(args[1])
    if not os.path.exists(mktdata_exchange_repo):
        os.umask(0)
        os.makedirs(mktdata_exchange_repo, mode=0o777)

    result = asyncio.run(history_main_wrapper(*args, **kwargs))
    return result
