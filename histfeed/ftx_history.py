import logging

import sys
import pandas as pd
import os
from pathlib import Path
from time import strftime
from utils.ftx_utils import *
from utils.io_utils import *
from utils.config_loader import *

history_start = datetime(2019, 11, 26)
LOGGER_NAME = __name__

# all rates annualized, all volumes daily in usd
async def get_history(dirname,
                      futures,
                      start_or_nb_hours,
                      end=datetime.now(tz=None).replace(minute=0,second=0,microsecond=0)
                      ):
    data = pd.concat(await safe_gather((
            [async_from_parquet(dirname + os.sep + f + '_funding.parquet')
             for f in futures[futures['type'] == 'perpetual'].index] +
            [async_from_parquet(dirname + os.sep + f + '_futures.parquet')
             for f in futures.index] +
            [async_from_parquet(dirname + os.sep + f + '_price.parquet')
             for f in futures['underlying'].unique()] +
            [async_from_parquet(dirname + os.sep + f + '_borrow.parquet')
             for f in (list(futures['underlying'].unique()) + ['USD'])]
    )), join='outer', axis=1)

    # convert borrow size in usd
    for f in futures['underlying'].unique():
        data[f + '_rate_size']*=data[f + '_price_o']

    start = end - timedelta(hours=start_or_nb_hours) if isinstance(start_or_nb_hours, int) else start_or_nb_hours
    return data[~data.index.duplicated()].sort_index()[start:end]

async def build_history(futures,
                        exchange,
                        dirname=configLoader.get_mktdata_folder_path(),
                        end=datetime.now(tz=None).replace(minute=0,second=0,microsecond=0),
                        timeframe = '1h'): # Runtime/Mktdata_database
    '''for now, increments local files and then uploads to s3'''
    logger = logging.getLogger(LOGGER_NAME)
    logger.info("BUILDING COROUTINES")

    coroutines = []
    for _, f in futures[futures['type'] == 'perpetual'].iterrows():
        parquet_name = os.path.join(dirname, f.name + '_funding.parquet')
        parquet = from_parquet(parquet_name) if os.path.isfile(parquet_name) else None
        start = max(parquet.index)+timedelta(hours=1) if parquet is not None else history_start
        if start < end:
            logger.info("Adding coroutine " + parquet_name)
            coroutines.append(funding_history(f, exchange, start, end, dirname))

    for _, f in futures.iterrows():
        parquet_name = os.path.join(dirname, f.name + '_futures.parquet')
        parquet = from_parquet(parquet_name) if os.path.isfile(parquet_name) else None
        start = max(parquet.index) + timedelta(hours=1) if parquet is not None else history_start
        if start < end:
            logger.info("Adding coroutine " + parquet_name)
            coroutines.append(rate_history(f, exchange, end, start, timeframe, dirname))

    for f in futures['underlying'].unique():
        parquet_name = os.path.join(dirname, f + '_price.parquet')
        parquet = from_parquet(parquet_name) if os.path.isfile(parquet_name) else None
        start = max(parquet.index) + timedelta(hours=1) if parquet is not None else history_start
        if start < end:
            logger.info("Adding coroutine " + parquet_name)
            coroutines.append(spot_history(f + '/USD', exchange, end, start, timeframe, dirname))

    for f in list(futures.loc[futures['spotMargin'] == True, 'underlying'].unique()) + ['USD']:
        parquet_name = os.path.join(dirname, f + '_borrow.parquet')
        parquet = from_parquet(parquet_name) if os.path.isfile(parquet_name) else None
        start = max(parquet.index) + timedelta(hours=1) if parquet is not None else history_start
        if start < end:
            logger.info("Adding coroutine " + parquet_name)
            coroutines.append(borrow_history(f, exchange, end, start, dirname))

    if coroutines:
        # run all coroutines
        logger.info(f"Gathered {len(coroutines)} coroutines, processing them by batches of {safe_gather_limit}")
        await safe_gather(coroutines)
    else:
        logger.info("0 coroutines gathered, nothing to do")

    # static values for non spot Margin underlyings
    otc_file = configLoader.get_static_params_used()
    for f in list(futures.loc[futures['spotMargin'] == False, 'underlying'].unique()):
        spot_parquet = from_parquet(os.path.join(dirname, f + '_price.parquet'))
        to_parquet(pd.DataFrame(index=spot_parquet.index,
                                columns=[f + '_rate_borrow'],
                                data=otc_file.loc[f,'borrow'] if futures.loc[f+'-PERP','spotMargin'] == 'OTC' else 999
                                ),
                   os.path.join(dirname, f +'_borrow.parquet'),
                   mode='a')

        to_parquet(pd.DataFrame(index=spot_parquet.index,
                                columns=[f + '_rate_size'],
                                data=otc_file.loc[f,'size'] if futures.loc[f+'-PERP','spotMargin'] == 'OTC' else 0
                                ),
                   os.path.join(dirname, f + '_borrow.parquet'),
                   mode='a')

async def correct_history(futures,exchange,hy_history,dirname=configLoader.get_mktdata_folder_path()):
    '''for now, increments local files and then uploads to s3'''

    logger = logging.getLogger(LOGGER_NAME)
    logger.info("CORRECTING HISTORY")

    coroutines = []
    for _, f in futures[futures['type'] == 'perpetual'].iterrows():
        parquet_name = os.path.join(dirname, f'{f.name}_funding.parquet')
        coroutines.append(async_to_parquet(hy_history[[exchange.market(f['symbol'])['id'] + '_rate_funding']],parquet_name))
        logger.info("Adding coroutine for correction " + parquet_name)

    for _, f in futures.iterrows():
        parquet_name = os.path.join(dirname, f'{f.name}_futures.parquet')
        future_id = exchange.market(f['symbol'])['id']
        column_names = [future_id + '_mark_' + field for field in ['o', 'h', 'l', 'c', 'volume']]
        column_names += [future_id + '_indexes_' + field for field in ['o', 'h', 'l', 'c', 'volume']]
        column_names += [future_id + '_rate_' + field for field in ['h', 'l', 'c'] + (['T'] if f['type']=='future' else [])]
        coroutines.append(async_to_parquet(hy_history[column_names], parquet_name))
        logger.info("Adding coroutine for correction " + parquet_name)

    for f in futures['underlying'].unique():
        parquet_name = os.path.join(dirname, f'{f}_price.parquet')
        column_names = [f + '_price_' + field for field in ['o', 'h', 'l', 'c', 'volume']]
        coroutines.append(async_to_parquet(hy_history[column_names], parquet_name))
        logger.info("Adding coroutine for correction " + parquet_name)

    for f in list(futures.loc[futures['spotMargin'] == True, 'underlying'].unique()) + ['USD']:
        parquet_name = os.path.join(dirname, f'{f}_borrow.parquet')
        column_names = [f + '_rate_' + field for field in ['borrow', 'size']]
        coroutines.append(async_to_parquet(hy_history[column_names], parquet_name))
        logger.info("Adding coroutine for correction " + parquet_name)

    # run all coroutines
    await safe_gather(coroutines)

### only perps, only borrow and funding, only hourly, time is fixing / payment time.
async def borrow_history(coin,
                         exchange,
                         end=(datetime.now(tz=timezone.utc).replace(minute=0,second=0,microsecond=0)),
                         start=(datetime.now(tz=timezone.utc).replace(minute=0,second=0,microsecond=0))-timedelta(days=30),
                         dirname=''):
    max_funding_data = int(500)  # in hours, limit is 500 only
    resolution = exchange.describe()['timeframes']['1h']

    e = end.timestamp()
    s = start.timestamp()
    f = max_funding_data * int(resolution)
    start_times = [int(round(e - k * f)) for k in range(1 + int((e - s) / f)) if e - k * f > s] + [s]

    data = pd.concat(await safe_gather([
        fetch_borrow_rate_history(exchange,coin,start_time,start_time+f-int(resolution))
            for start_time in start_times]),axis=0,join='outer')
    if len(data)==0:
        return pd.DataFrame(columns=[coin+'_rate_borrow',coin+'_rate_size'])

    data = data.astype(dtype={'time': 'int64'}).set_index('time')[['rate','size']]
    data.rename(columns={'rate':coin+'_rate_borrow','size':coin+'_rate_size'},inplace=True)
    data.index = [datetime.fromtimestamp(x / 1000) for x in data.index]
    data=data[~data.index.duplicated()].sort_index()

    if dirname != '': await async_to_parquet(data, os.path.join(dirname, coin + '_borrow.parquet'),mode='a')

######### annualized funding for perps, time is fixing / payment time.
async def funding_history(future,
                          exchange,
                          start=(datetime.now(tz=timezone.utc).replace(minute=0, second=0, microsecond=0))-timedelta(days=30),
                          end=(datetime.now(tz=timezone.utc).replace(minute=0, second=0, microsecond=0)),
                          dirname=''):

    max_funding_data = int(500)  # in hour. limit is 500 :(
    resolution = exchange.describe()['timeframes']['1h']

    e = end.timestamp()
    s = start.timestamp()
    f = max_funding_data * int(resolution)
    start_times=[int(round(s+k*f)) for k in range(1+int((e-s)/f)) if s+k*f<e]

    lists = await safe_gather([
        exchange.fetch_funding_rate_history(exchange.market(future['symbol'])['symbol'], params={'start_time':start_time, 'end_time':start_time+f})
                                for start_time in start_times])
    funding = [y for x in lists for y in x]

    if len(funding)==0:
        return pd.DataFrame(columns=[exchange.market(future['symbol'])['id'] + '_rate_funding'])

    data = pd.DataFrame(funding)
    data['time']=data['timestamp'].astype(dtype='int64')
    data[exchange.market(future['symbol'])['id'] + '_rate_funding']=data['fundingRate']*365.25*24
    data=data[['time',exchange.market(future['symbol'])['id'] + '_rate_funding']].set_index('time')
    data.index = [datetime.fromtimestamp(x / 1000) for x in data.index]
    data = data[~data.index.duplicated()].sort_index()

    if dirname != '': await async_to_parquet(data, os.path.join(dirname,
                                                                exchange.market(future['symbol'])['id'] + '_funding.parquet'),
                                             mode='a')

#### annualized rates for futures and perp, volumes are daily
async def rate_history(future,exchange,
                 end=datetime.now(tz=timezone.utc).replace(minute=0,second=0,microsecond=0),
                 start=datetime.now(tz=timezone.utc).replace(minute=0,second=0,microsecond=0)-timedelta(days=30),
                 timeframe='1h',
                 dirname=''):
    max_mark_data = int(1500)
    resolution = exchange.describe()['timeframes'][timeframe]


    e = end.timestamp()
    s = start.timestamp()
    f = max_mark_data * int(resolution)
    start_times=[int(round(s+k*f)) for k in range(1+int((e-s)/f)) if s+k*f<e]

    mark_indexes = await safe_gather([
        exchange.fetch_ohlcv(exchange.market(future['symbol'])['symbol'], timeframe=timeframe, params=params) # volume is for max_mark_data*resolution
            for start_time in start_times
                for params in [{'start_time':start_time, 'end_time':start_time+f-int(resolution)},
                               {'start_time':start_time, 'end_time':start_time+f-int(resolution),'price':'index'}]])
    mark = [y for x in mark_indexes[::2] for y in x]
    indexes = [y for x in mark_indexes[1::2] for y in x]

    if ((len(indexes) == 0) | (len(mark) == 0)):
        return pd.DataFrame(columns=
                         [exchange.market(future['symbol'])['id'] + '_mark_' + c for c in ['t', 'o', 'h', 'l', 'c', 'volume']]
                        +[exchange.market(future['symbol'])['id'] + '_indexes_' + c for c in ['t', 'o', 'h', 'l', 'c', 'volume']]
                        +[exchange.market(future['symbol'])['id'] + '_rate_' + c for c in ['T','c','h','l']])
    column_names = ['t', 'o', 'h', 'l', 'c', 'volume']

    ###### indexes
    indexes = pd.DataFrame([dict(zip(column_names,row)) for row in indexes], dtype=float).astype(dtype={'t': 'int64'}).set_index('t')
    indexes['volume'] = indexes['volume']* 24 * 3600 / int(resolution)

    ###### marks
    mark = pd.DataFrame([dict(zip(column_names,row)) for row in mark]).astype(dtype={'t': 'int64'}).set_index('t')
    mark['volume']=mark['volume']*24*3600/int(resolution)

    mark.columns = ['mark_' + column for column in mark.columns]
    indexes.columns = ['indexes_' + column for column in indexes.columns]
    data = mark.join(indexes, how='inner')

    ########## rates from index to mark
    if future['type'] == 'future':
        expiry_time = dateutil.parser.isoparse(future['expiry']).timestamp()
        data['rate_T'] = data.apply(lambda t: (expiry_time - int(t.name) / 1000) / 3600 / 24 / 365.25, axis=1)

        data['rate_c'] = data.apply(
            lambda y: calc_basis(y['mark_c'],
                                 indexes.loc[y.name, 'indexes_c'], future['expiryTime'],
                                 datetime.fromtimestamp(int(y.name / 1000), tz=None)), axis=1)
        data['rate_h'] = data.apply(
            lambda y: calc_basis(y['mark_h'], indexes.loc[y.name, 'indexes_h'], future['expiryTime'],
                                 datetime.fromtimestamp(int(y.name / 1000), tz=None)), axis=1)
        data['rate_l'] = data.apply(
            lambda y: calc_basis(y['mark_l'], indexes.loc[y.name, 'indexes_l'], future['expiryTime'],
                                 datetime.fromtimestamp(int(y.name / 1000), tz=None)), axis=1)
    elif future['type'] == 'perpetual': ### 1h funding = (mark/spot-1)/24
        data['rate_T'] = None
        data['rate_c'] = (mark['mark_c'] / indexes['indexes_c'] - 1)*365.25
        data['rate_h'] = (mark['mark_h'] / indexes['indexes_h'] - 1)*365.25
        data['rate_l'] = (mark['mark_l'] / indexes['indexes_l'] - 1)*365.25
    else:
        print('what is ' + future['symbol'] + ' ?')
        return
    
    data.columns = [exchange.market(future['symbol'])['id'] + '_' + c for c in data.columns]
    data.index = [datetime.fromtimestamp(x / 1000) for x in data.index]
    data = data[~data.index.duplicated()].sort_index()

    if dirname != '': await async_to_parquet(data,
                                             os.path.join(dirname,
                                                          exchange.market(future['symbol'])['id'] + '_futures.parquet'),
                                             mode='a')

## populates future_price or spot_price depending on type
async def spot_history(symbol, exchange,
                       end= (datetime.now(tz=timezone.utc).replace(minute=0,second=0,microsecond=0)),
                       start= (datetime.now(tz=timezone.utc).replace(minute=0,second=0,microsecond=0))-timedelta(days=30),
                       timeframe='1h',
                       dirname=''):
    max_mark_data = int(1500)
    resolution = exchange.describe()['timeframes'][timeframe]


    e = end.timestamp()
    s = start.timestamp()
    f = max_mark_data * int(resolution)
    start_times=[int(round(s+k*f)) for k in range(1+int((e-s)/f)) if s+k*f<e]

    spot_lists = await safe_gather([
        exchange.fetch_ohlcv(symbol, timeframe=timeframe, params={'start_time':start_time, 'end_time':start_time+f-int(resolution)})
                                for start_time in start_times])
    spot = [y for x in spot_lists for y in x]

    column_names = ['t', 'o', 'h', 'l', 'c', 'volume']
    if len(spot)==0:
        return pd.DataFrame(columns=[symbol.replace('/USD', '') + '_price_' + c for c in column_names])

    ###### spot
    data = pd.DataFrame(columns=column_names, data=spot).astype(dtype={'t': 'int64', 'volume': 'float'}).set_index('t')
    data['volume'] = data['volume'] * 24 * 3600 / int(resolution)
    data.columns = [symbol.replace('/USD', '') + '_price_' + column for column in data.columns]
    data.index = [datetime.fromtimestamp(x / 1000) for x in data.index]
    data = data[~data.index.duplicated()].sort_index()
    if dirname!='': await async_to_parquet(data,
                                           os.path.join(dirname,
                                                        symbol.replace('/USD', '') + '_price.parquet'),
                                           mode='a')

async def fetch_trades_history(symbol,
                               exchange,
                               start= (datetime.now(tz=timezone.utc).replace(minute=0,second=0,microsecond=0))-timedelta(days=30),
                               end=(datetime.now(tz=timezone.utc).replace(minute=0, second=0, microsecond=0)),
                               frequency=timedelta(minutes=1),
                               dirname=''):

    max_trades_data = int(5000)  # in trades, limit is 5000 :(
    print('trades_history: ' + symbol)

    ### grab data per batch of 5000, try weekly
    trades=[]
    start_time = start.timestamp()
    end_time = end.timestamp()

    new_trades = []
    while start_time < end.timestamp():
        new_trades = (await exchange.publicGetMarketsMarketNameTrades(
            {'market_name': symbol, 'start_time': start_time, 'end_time': end_time}
                                                        ))['result']
        trades.extend(new_trades)

        if len(new_trades) > 0:
            last_trade_time = dateutil.parser.isoparse(new_trades[0]['time']).timestamp()
            if last_trade_time > end.timestamp(): break
            if (len(new_trades)<max_trades_data)&(end_time>end.timestamp()): break
            start_time = last_trade_time if len(new_trades)==max_trades_data else end_time
        else:
            start_time=end_time
        end_time = start_time + 15*60

    if len(trades)==0:
        vwap = pd.DataFrame(columns=['size','volume','count','vwap','vwvol','liquidation_intensity'])
        vwap.columns = [symbol.split('/USD')[0] + '_trades_' + column for column in vwap.columns]
        return {'symbol':exchange.market(symbol)['symbol'],
                'coin':exchange.market(symbol)['base'],
                'vwap':vwap[symbol.split('/USD')[0] + '_trades_'+'vwap'],
                'vwvol': vwap[symbol.split('/USD')[0] + '_trades_'+'vwvol'],
                'volume':vwap[symbol.split('/USD')[0] + '_trades_'+'volume'],
                'liquidation_intensity':vwap[symbol.split('/USD')[0] + '_trades_'+'liquidation_intensity']}

    data = pd.DataFrame(data=trades)
    data[['size','price']] = data[['size','price']].astype(float)
    data['volume'] = data['size'] * data['price']
    data['square'] = data['size'] * data['price']*data['price']
    data['count'] = 1
    data['liquidation_volume'] = data['size'] * data['price'] * data['liquidation'].apply(lambda flag: 1 if flag else 0)

    data['time']=data['time'].apply(lambda t: dateutil.parser.isoparse(t).replace(tzinfo=timezone.utc))
    data.set_index('time',inplace=True)

    vwap = data[['size','volume','square','count','liquidation_volume']].resample(frequency).sum()
    vwap['vwap'] = vwap['volume']/vwap['size']
    vwap['vwvol'] = (vwap['square'] / vwap['size']-vwap['vwap']*vwap['vwap']).apply(np.sqrt)
    vwap['liquidation_intensity'] = vwap['liquidation_volume'] / vwap['volume']

    vwap.columns = [symbol.split('/USD')[0] + '_trades_' + column for column in vwap.columns]
    #data.index = [datetime.fromtimestamp(x / 1000) for x in data.index]
    vwap = vwap[~vwap.index.duplicated()].sort_index().ffill()

    if dirname != '':
        parquet_filename = os.path.join(dirname, symbol.split('/USD')[0] + "_trades.parquet")
        vwap.to_parquet(parquet_filename)

    return {'symbol':exchange.market(symbol)['symbol'],
            'coin':exchange.market(symbol)['base'],
            'vwap':vwap[symbol.split('/USD')[0] + '_trades_vwap'],
            'vwvol':vwap[symbol.split('/USD')[0] + '_trades_vwvol'],
            'volume':vwap[symbol.split('/USD')[0] + '_trades_volume'],
            'liquidation_intensity':vwap[symbol.split('/USD')[0] + '_trades_liquidation_intensity']}

async def ftx_history_main_wrapper(exchange_name, run_type, universe, nb_of_days,timeframe,dirname):

    exchange = await open_exchange(exchange_name,'')
    futures = pd.DataFrame(await fetch_futures(exchange, includeExpired=True)).set_index('name')
    await exchange.load_markets()

    #universe should be either 'all', either a universe name, or a list of currencies
    # In case universe was not max, is_wide, is_institutional
    # universe = [id for id, data in exchange.markets_by_id.items() if data['base'] in [x.upper() for x in [universe]] and data['contract']]

    if universe != []:
        futures = futures[futures.index.isin(universe)]

    logger = logging.getLogger(LOGGER_NAME)

    # Volume Screening
    if run_type == 'build':
        logger.info(f'Building history for build timeframe={timeframe}')
        await build_history(futures, exchange, dirname, timeframe=timeframe)
        await exchange.close()
    elif run_type == 'correct':
        raise Exception('bug. does not correct')
        logger.info("correcting HOURLY history")
        hy_history = await get_history(dirname, futures, history_start)
        end = datetime.now()-timedelta(days=nb_of_days)
        await correct_history(futures, exchange, hy_history[:end])
        await build_history(futures, exchange, dirname) # only correct the hourly
        await exchange.close()
    elif run_type == 'get':
        logger.info("Getting history...")
        hy_history = await get_history(dirname, futures, 24 * nb_of_days)
        await exchange.close()
        return hy_history

def main(*args):
    '''
        Parameters could be passed in any order
        @params:
           # For now, defaults to exchange_name = ftx
           run_type = Build or correct
           universe = [list of symbols to build the histFeed for]
        @Example runs:
            - main build ftx wide 100
            - main ftx 100 all
            - main correct ftx max
   '''

    set_logger('histfeed')
    logger = logging.getLogger(LOGGER_NAME)

    RUN_TYPES = ["build", "correct", "get"]
    EXCHANGE_NAMES_AVAILABLE = ["ftx"]

    args = list(*args)[1:]
    if len(args) < 3:
        logger.critical("Cannot run histfeed from the provided params.")
        logger.critical(f'Parameters passed are {[arg for arg in args]}]')
        logger.critical("Missing parameters")
        logger.critical("3 to 5 parameters should be passed : build/correct, exchange_name, universe_filter and, optionally, the number of days, timeframe")
        logger.critical("---> Terminating...")
        return -1
    elif len(args) > 5:
        logger.critical("Cannot run histfeed from the provided params.")
        logger.critical(f'Parameters passed are {[arg for arg in args]}]')
        logger.critical("Too many parameters")
        logger.critical("3 to 5 parameters should be passed : build/correct, exchange_name, universe_filter and, optionally, the number of days, timeframe")
        logger.critical("---> Terminating...")
    else:
        # Getting exchange name, only ftx handled for now
        try:
            exchange_name = [x for x in args if x in EXCHANGE_NAMES_AVAILABLE][0]
        except IndexError:
            logger.critical("Cannot find the exchange_name param. The exchange_name param should be passed explicitly : only 'ftx' is handled for the moment")
            logger.critical("---> Terminating...")
            sys.exit(1)

        # Getting the universe from the params passed
        try:
            universe_filter = [x for x in args if x in configLoader.get_universe_pool()][0]
        except IndexError:
            logger.critical(f"Cannot find the universe_filter param. The universe_filter param should be explicitly in {configLoader.get_universe_pool()}")
            logger.critical("---> Terminating...")
            sys.exit(1)

        # Try loading the config
        try:
            universe = configLoader.get_bases(universe_filter)
        except FileNotFoundError as err:
            logger.critical(err)
            logger.critical("---> Terminating...")
            sys.exit(1)

        # Getting the run_type
        try:
            run_type = [x for x in args if x in RUN_TYPES][0]
        except IndexError as e:
            logger.critical(f"Cannot find the run_type param. The run_type param should be passed explicitly : build/correct {str(e)}")
            logger.critical("---> Terminating...")
            sys.exit(1)

        # Getting the nb_of_days, or defaulting to 100
        try:
            nb_of_days = [int(x) for x in args if x.isnumeric()][0]
        except IndexError:
            nb_of_days = 100
            logger.info("Cannot find the nb_of_days param. Defaulting to nb_of_days=100")

        ftx_timeframes = ['15s','1m','5m','15m','1h','4h','1d','3d','1w','2w','1M']
        # Getting the nb_of_days, or defaulting to 100
        try:
            timeframe = [x for x in args if x in ftx_timeframes][0]
            if timeframe != '1h':
                dirname = os.path.join(os.sep, 'tmp', f'{exchange_name}_{universe_filter}_{timeframe}')
                if not os.path.exists(dirname):
                    os.umask(0)
                    os.makedirs(dirname, mode=0o777)
            else:
                dirname = configLoader.get_mktdata_folder_for_exchange(exchange_name)

        except IndexError:
            timeframe = '1h'
            logger.info(f"Cannot find the timeframe param. Defaulting to timeframe=1h")
            dirname = configLoader.get_mktdata_folder_for_exchange(exchange_name)

        # Make sure the exchange repo exists in mktdata/, if not creates it
        mktdata_exchange_repo = configLoader.get_mktdata_folder_for_exchange(exchange_name)
        if not os.path.exists(mktdata_exchange_repo):
            os.umask(0)
            os.makedirs(mktdata_exchange_repo, mode=0o777)

        # Running histfeed
        logger.info(f'histfeed running with params {[arg for arg in args]}')
        result = asyncio.run(ftx_history_main_wrapper(exchange_name, run_type, universe, nb_of_days, timeframe,dirname))
        return result
        logger.info("histfeed terminated successfully...")

def set_logger(app_name):
    ''' Function that sets a logger for the app, for debugging purposes '''

    time_date_stamp = strftime("%Y%m%d_%H%M%S")
    filepath = os.path.join(os.sep, 'tmp', app_name)
    if not os.path.exists(filepath):
        os.umask(0)
        os.makedirs(filepath, mode=0o777)

    logging.basicConfig()
    #filename=os.path.join(filepath, 'histfeed_' + time_date_stamp + '.log'),
                        # filemode='a',
                        # format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                        # datefmt='%H:%M:%S',
                        # level=logging.DEBUG

    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.INFO)

    # Create file handler which logs even debug messages
    filename = os.path.join(filepath, app_name + '_' + time_date_stamp + '.log')
    fh = logging.FileHandler(filename)
    fh.setFormatter(logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s'))
    fh.setLevel(logging.DEBUG)
    logger.addHandler(fh)

    logger.info("Logger set")