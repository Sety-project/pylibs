from tradeexecutor.binance_api import BinanceAPI
from utils.io_utils import *
from utils.api_utils import api
from utils.async_utils import *
from utils.ccxt_utilities import calc_basis

history_start = datetime(2021, 11, 26).replace(tzinfo=timezone.utc)

# all rates annualized, all volumes daily in usd
async def get_history(dirname,
                      futures,
                      start_or_nb_hours,
                      end=datetime.utcnow().replace(tzinfo=timezone.utc).replace(minute=0,second=0,microsecond=0)
                      ):
    data = pd.concat(await safe_gather((
            [async_read_csv(dirname + os.sep + f + '_funding.csv',index_col=0,parse_dates=True)
             for f in futures[futures['type'] == 'perpetual'].index] +
            [async_read_csv(dirname + os.sep + f + '_futures.csv',index_col=0,parse_dates=True)
             for f in futures.index] +
            [async_read_csv(dirname + os.sep + f + '_price.csv',index_col=0,parse_dates=True)
             for f in futures['underlying'].unique()] +
            [async_read_csv(dirname + os.sep + f + '_borrow.csv',index_col=0,parse_dates=True)
             for f in (list(futures['underlying'].unique()) + ['USD'])]
    )), join='outer', axis=1)

    # convert borrow size in usd
    for f in futures['underlying'].unique():
        data[f + '_rate_size']*=data[f + '_price_o']

    start = end - timedelta(hours=start_or_nb_hours) if isinstance(start_or_nb_hours, int) else start_or_nb_hours
    data.index = [t.replace(tzinfo=timezone.utc) for t in data.index]
    return data[~data.index.duplicated()].sort_index()[start:end]

async def build_history(futures,
                        exchange,
                        dirname=configLoader.get_mktdata_folder_path(),
                        end=datetime.utcnow().replace(tzinfo=timezone.utc).replace(minute=0,second=0,microsecond=0),
                        frequency: timedelta = '1h'): # Runtime/Mktdata_database
    '''for now, increments local files and then uploads to s3'''
    logger = logging.getLogger('histfeed')
    logger.info("BUILDING COROUTINES")

    coroutines = []
    for _, f in futures[futures['type'] == 'perpetual'].iterrows():
        csv_name = os.path.join(dirname, f.name + '_funding.csv')
        csv = pd.read_csv(csv_name,index_col=0,parse_dates=True) if os.path.isfile(csv_name) else None
        start = max(csv.index).replace(tzinfo=timezone.utc)+timedelta(hours=8) if csv is not None else history_start
        if start < end:
            logger.info("Adding coroutine " + csv_name)
            coroutines.append(funding_history(f, exchange, start, end, dirname))

    for _, f in futures.iterrows():
        csv_name = os.path.join(dirname, f.name + '_futures.csv')
        csv = pd.read_csv(csv_name,index_col=0,parse_dates=True) if os.path.isfile(csv_name) else None
        start = max(csv.index).replace(tzinfo=timezone.utc) + pd.Timedelta(frequency) if csv is not None else history_start
        if start < end:
            logger.info("Adding coroutine " + csv_name)
            coroutines.append(rate_history(f, exchange, end, start, frequency, dirname))

    for f in futures['spot_ticker'].unique():
        csv_name = os.path.join(dirname, f.replace('/','') + '_price.csv')
        csv = pd.read_csv(csv_name,index_col=0,parse_dates=True) if os.path.isfile(csv_name) else None
        start = max(csv.index).replace(tzinfo=timezone.utc) + pd.Timedelta(frequency) if csv is not None else history_start
        if start < end:
            logger.info("Adding coroutine " + csv_name)
            coroutines.append(spot_history(f, exchange, end, start, frequency, dirname))

    for f in list(futures.loc[futures['spotMargin'] == True, 'underlying'].unique()) + ['USDT','BUSD']:
        csv_name = os.path.join(dirname, f + '_borrow.csv')
        csv = pd.read_csv(csv_name,index_col=0,parse_dates=True) if os.path.isfile(csv_name) else None
        start = max(csv.index).replace(tzinfo=timezone.utc) + timedelta(hours=1) if csv is not None else history_start
        if start < end:
            logger.info("Adding coroutine " + csv_name)
            coroutines.append(borrow_history(f, exchange, end, start, dirname))

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
            spot_csv = pd.read_csv(os.path.join(dirname, f.replace('/','') + '_price.csv'),index_col=0,parse_dates=True)
            coin = f.split('/')[0]
            to_csv(pd.DataFrame(index=spot_csv.index,
                                    columns=[coin + '_rate_borrow'],
                                    data=otc_file.loc[coin,'borrow'] if futures.loc[f,'spotMargin'] == 'OTC' else 999
                                    ),
                       os.path.join(dirname, coin +'_borrow.csv'),
                       mode='a',header=False)
        except Exception as e:
            logger.warning(e,exc_info=True)
async def correct_history(futures,exchange,hy_history,dirname=configLoader.get_mktdata_folder_path()):
    '''for now, increments local files and then uploads to s3'''

    logger = logging.getLogger('histfeed')
    logger.info("CORRECTING HISTORY")

    coroutines = []
    for _, f in futures[futures['type'] == 'perpetual'].iterrows():
        csv_name = os.path.join(dirname, f'{f.name}_funding.csv')
        coroutines.append(async_to_csv(hy_history[[exchange.market(f['symbol'])['id'] + '_rate_funding']],csv_name))
        logger.info("Adding coroutine for correction " + csv_name)

    for _, f in futures.iterrows():
        csv_name = os.path.join(dirname, f'{f.name}_futures.csv')
        future_id = exchange.market(f['symbol'])['id']
        column_names = [future_id + '_mark_' + field for field in ['o', 'h', 'l', 'c', 'volume']]
        column_names += [future_id + '_indexes_' + field for field in ['o', 'h', 'l', 'c', 'volume']]
        column_names += [future_id + '_rate_' + field for field in ['h', 'l', 'c'] + (['T'] if f['type']=='future' else [])]
        coroutines.append(async_to_csv(hy_history[column_names], csv_name))
        logger.info("Adding coroutine for correction " + csv_name)

    for f in futures['underlying'].unique():
        csv_name = os.path.join(dirname, f'{f}_price.csv')
        column_names = [f + '_price_' + field for field in ['o', 'h', 'l', 'c', 'volume']]
        coroutines.append(async_to_csv(hy_history[column_names], csv_name))
        logger.info("Adding coroutine for correction " + csv_name)

    for f in list(futures.loc[futures['spotMargin'] == True, 'underlying'].unique()) + ['USD']:
        csv_name = os.path.join(dirname, f'{f}_borrow.csv')
        column_names = [f + '_rate_' + field for field in ['borrow', 'size']]
        coroutines.append(async_to_csv(hy_history[column_names], csv_name))
        logger.info("Adding coroutine for correction " + csv_name)

    # run all coroutines
    await safe_gather(coroutines)

### only perps, only borrow and funding, only hourly, time is fixing / payment time.
@ignore_error
async def borrow_history(coin,
                         exchange,
                         end=(datetime.now(tz=timezone.utc).replace(minute=0,second=0,microsecond=0)),
                         start=(datetime.now(tz=timezone.utc).replace(minute=0,second=0,microsecond=0))-timedelta(days=30),
                         dirname=''):
    max_funding_data = int(92)  # in days, limit is 92 only
    resolution = pd.Timedelta(exchange.describe()['timeframes']['1d']).total_seconds()

    e = end.timestamp()
    s = start.timestamp()
    f = max_funding_data * resolution
    start_times = [int(round(e - k * f)) for k in range(1 + int((e - s) / f)) if e - k * f > s] + [s]

    lists = await safe_gather([
        exchange.fetch_borrow_rate_history(coin,since=int(start_time*1000), limit=max_funding_data)
            for start_time in start_times])
    borrow = [y for x in lists for y in x]

    data = pd.DataFrame(borrow)
    data = data.set_index('timestamp')[['rate']]*365.25
    data.rename(columns={'rate':coin+'_rate_borrow'},inplace=True)
    data.index = [datetime.fromtimestamp(x / 1000) for x in data.index]
    data=data[~data.index.duplicated()].sort_index()

    if dirname != '': await async_to_csv(data, os.path.join(dirname, coin + '_borrow.csv'),mode='a',header=False)

######### annualized funding for perps, time is fixing / payment time.
@ignore_error
async def funding_history(future,
                          exchange,
                          start=(datetime.now(tz=timezone.utc).replace(minute=0, second=0, microsecond=0))-timedelta(days=30),
                          end=(datetime.now(tz=timezone.utc).replace(minute=0, second=0, microsecond=0)),
                          dirname=''):

    max_funding_data = int(100)  # in hour. limit is 100 :(
    resolution = pd.Timedelta(exchange.describe()['timeframes']['1h']).total_seconds()

    e = end.timestamp()
    s = start.timestamp()
    f = max_funding_data * resolution
    start_times=[int(round(s+k*f)) for k in range(1+int((e-s)/f)) if s+k*f<e]

    lists = await safe_gather([
        exchange.fetch_funding_rate_history(exchange.market(future['symbol'])['symbol'], params={'startTime': start_time*1000, 'endTime': (start_time+f)*1000})
                                for start_time in start_times])
    funding = [y for x in lists for y in x]

    if len(funding)>0:
        data = pd.DataFrame(funding)
        data['time']=data['timestamp'].astype(dtype='int64')
        data[exchange.market(future['symbol'])['id'] + '_rate_funding']=data['fundingRate']*365.25*8
        data=data[['time',exchange.market(future['symbol'])['id'] + '_rate_funding']].set_index('time')
        data.index = [datetime.fromtimestamp(x / 1000) for x in data.index]
        data = data[~data.index.duplicated()].sort_index()

        if dirname != '': await async_to_csv(data, os.path.join(dirname,
                                                                    exchange.market(future['symbol'])['id'] + '_funding.csv'),
                                             mode='a',header=False)

#### annualized rates for futures and perp, volumes are daily
@ignore_error
async def rate_history(future,exchange,
                 end=datetime.now(tz=timezone.utc).replace(minute=0,second=0,microsecond=0),
                 start=datetime.now(tz=timezone.utc).replace(minute=0,second=0,microsecond=0)-timedelta(days=30),
                 timeframe='1h',
                 dirname=''):
    symbol = exchange.market(future['symbol'])['symbol']

    max_mark_data = int(500)
    resolution = pd.Timedelta(exchange.describe()['timeframes'][timeframe]).total_seconds()

    e = end.timestamp()
    s = start.timestamp()
    f = max_mark_data * resolution
    start_times=[int(round(s+k*f)) for k in range(1+int((e-s)/f)) if s+k*f<e]

    mark_indexes = await safe_gather([
        exchange.fetch_ohlcv(symbol, timeframe=timeframe, params=params) # volume is for max_mark_data*resolution
            for start_time in start_times
                for params in [{'startTime':start_time*1000, 'endTime':(start_time+f-resolution)*1000},
                               {'start_time':start_time, 'end_time':start_time+f-resolution,'price':'index'}]])
    mark = [y for x in mark_indexes[::2] for y in x]
    indexes = [y for x in mark_indexes[1::2] for y in x]
    column_names = ['t', 'o', 'h', 'l', 'c', 'volume']

    ###### indexes
    indexes = pd.DataFrame([dict(zip(column_names,row)) for row in indexes], dtype=float).astype(dtype={'t': 'int64'}).set_index('t')
    indexes['volume'] = indexes['volume']* 24 * 3600 / resolution

    ###### marks
    mark = pd.DataFrame([dict(zip(column_names,row)) for row in mark]).astype(dtype={'t': 'int64'}).set_index('t')
    mark['volume']=mark['volume']*24*3600/resolution

    mark.columns = ['mark_' + column for column in mark.columns]
    indexes.columns = ['indexes_' + column for column in indexes.columns]

    ##### openInterestUsd
    max_oi_data = 500
    e = end.timestamp()
    s = start.timestamp()
    f = max_oi_data * resolution
    start_times=[int(round(s+k*f)) for k in range(1+int((e-s)/f)) if s+k*f<e]

    openInterest_list = await safe_gather([
        exchange.fetch_open_interest_history(symbol, timeframe='5m' if timeframe=='1m' else timeframe, since=int(start_time*1000), limit=max_oi_data)
        for start_time in start_times])
    openInterest_list = [y for x in openInterest_list for y in x]
    openInterest = pd.DataFrame(openInterest_list, columns=['timestamp','openInterestAmount']).astype(dtype={'timestamp': 'int64'}).set_index('timestamp')

    data = mark.join(indexes, how='inner').join(openInterest, how='inner')

    ########## rates from index to mark
    if future['type'] == 'future':
        expiry_time = dateutil.parser.isoparse(future['expiry']).timestamp()
        data['rate_T'] = data.apply(lambda t: (expiry_time - int(t.name) / 1000) / 3600 / 24 / 365.25, axis=1)

        data['rate_c'] = data.apply(
            lambda y: calc_basis(y['mark_c'],
                                 indexes.loc[y.name, 'indexes_c'], future['expiryTime'],
                                 datetime.fromtimestamp(int(y.name / 1000), tz=timezone.utc)), axis=1)
        data['rate_h'] = data.apply(
            lambda y: calc_basis(y['mark_h'], indexes.loc[y.name, 'indexes_h'], future['expiryTime'],
                                 datetime.fromtimestamp(int(y.name / 1000), tz=timezone.utc)), axis=1)
        data['rate_l'] = data.apply(
            lambda y: calc_basis(y['mark_l'], indexes.loc[y.name, 'indexes_l'], future['expiryTime'],
                                 datetime.fromtimestamp(int(y.name / 1000), tz=timezone.utc)), axis=1)
    elif future['type'] == 'perpetual': ### 1h funding = (mark/spot-1)/24
        data['rate_T'] = None
        data['rate_c'] = (mark['mark_c'] / indexes['indexes_c'] - 1)*365.25
        data['rate_h'] = (mark['mark_h'] / indexes['indexes_h'] - 1)*365.25
        data['rate_l'] = (mark['mark_l'] / indexes['indexes_l'] - 1)*365.25
    else:
        raise Exception('what is ' + future['symbol'] + ' ?')

    data.columns = [exchange.market(future['symbol'])['id'] + '_' + c for c in data.columns]
    data.index = [datetime.fromtimestamp(x / 1000) for x in data.index]
    data = data[~data.index.duplicated()].sort_index()

    if dirname != '': await async_to_csv(data,
                                             os.path.join(dirname,
                                                          exchange.market(future['symbol'])['id'] + '_futures.csv'),
                                             mode='a',header=False)

## populates future_price or spot_price depending on type
@ignore_error
async def spot_history(symbol, exchange,
                       end= (datetime.now(tz=timezone.utc).replace(minute=0,second=0,microsecond=0)),
                       start= (datetime.now(tz=timezone.utc).replace(minute=0,second=0,microsecond=0))-timedelta(days=30),
                       timeframe='1h',
                       dirname=''):
    max_mark_data = int(500)
    resolution = pd.Timedelta(exchange.describe()['timeframes'][timeframe]).total_seconds()

    e = end.timestamp()
    s = start.timestamp()
    f = max_mark_data * resolution
    start_times=[int(round(s+k*f)) for k in range(1+int((e-s)/f)) if s+k*f<e]

    spot_lists = await safe_gather([
        exchange.fetch_ohlcv(symbol, timeframe=timeframe, params={'startTime':start_time*1000, 'endTime':(start_time+f-resolution)*1000})
                                for start_time in start_times])
    spot = [y for x in spot_lists for y in x]
    column_names = ['t', 'o', 'h', 'l', 'c', 'volume']

    ###### spot
    data = pd.DataFrame(columns=column_names, data=spot).astype(dtype={'t': 'int64', 'volume': 'float'}).set_index('t')
    data['volume'] = data['volume'] * 24 * 3600 / resolution
    data.columns = [symbol.replace('/', '') + '_price_' + column for column in data.columns]
    data.index = [datetime.fromtimestamp(x / 1000) for x in data.index]
    data = data[~data.index.duplicated()].sort_index()
    if dirname!='': await async_to_csv(data,
                                           os.path.join(dirname,
                                                        symbol.replace('/', '') + '_price.csv'),
                                           mode='a',header=False)

@ignore_error
async def fetch_trades_history(symbol,
                               exchange,
                               start= (datetime.now(tz=timezone.utc).replace(minute=0,second=0,microsecond=0))-timedelta(days=30),
                               end=(datetime.now(tz=timezone.utc).replace(minute=0, second=0, microsecond=0)),
                               frequency=timedelta(minutes=1),
                               dirname=''):

    max_trades_data = int(5000)  # in trades, limit is 5000 :(

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
        vwap.columns = [symbol.split('/USDT')[0] + '_trades_' + column for column in vwap.columns]
        return {'symbol':exchange.market(symbol)['symbol'],
                'coin':exchange.market(symbol)['base'],
                'vwap':vwap[symbol.split('/USDT')[0] + '_trades_'+'vwap'],
                'vwvol': vwap[symbol.split('/USDT')[0] + '_trades_'+'vwvol'],
                'volume':vwap[symbol.split('/USDT')[0] + '_trades_'+'volume'],
                'liquidation_intensity':vwap[symbol.split('/USDT')[0] + '_trades_'+'liquidation_intensity']}

    vwap = vwap_from_list(frequency, trades)
    vwap.columns = [symbol.split('/USDT')[0] + '_trades_' + column for column in vwap.columns]
    # data.index = [datetime.fromtimestamp(x / 1000) for x in data.index]
    vwap = vwap[~vwap.index.duplicated()].sort_index().ffill()

    if dirname != '':
        csv_filename = os.path.join(dirname, symbol.split('/USDT')[0] + "_trades.csv")
        vwap.to_csv(csv_filename)

    return {'vwap':vwap[symbol.split('/USDT')[0] + '_trades_vwap'],
            'vwvol':vwap[symbol.split('/USDT')[0] + '_trades_vwvol'],
            'volume':vwap[symbol.split('/USDT')[0] + '_trades_volume'],
            'liquidation_intensity':vwap[symbol.split('/USDT')[0] + '_trades_liquidation_intensity']}

def vwap_from_list(frequency, trades: list[dict]) -> pd.DataFrame:
    '''needs ['size', 'price', 'liquidation', 'time' as isostring]'''
    data = pd.DataFrame(data=trades)
    data[['size', 'price']] = data[['size', 'price']].astype(float)
    data['volume'] = data['size'] * data['price']
    data['square'] = data['size'] * data['price'] * data['price']
    data['count'] = 1
    data['liquidation_volume'] = data['size'] * data['price'] * data['liquidation'].apply(lambda flag: 1 if flag else 0)
    data['time'] = data['time'].apply(lambda t: dateutil.parser.isoparse(t).replace(tzinfo=timezone.utc))
    data.set_index('time', inplace=True)
    vwap = data[['size', 'volume', 'square', 'count', 'liquidation_volume']].resample(frequency).sum()
    vwap['vwap'] = vwap['volume'] / vwap['size']
    vwap['vwvol'] = (vwap['square'] / vwap['size'] - vwap['vwap'] * vwap['vwap']).apply(np.sqrt)
    vwap['liquidation_intensity'] = vwap['liquidation_volume'] / vwap['volume']
    return vwap[~vwap.index.duplicated()].sort_index().ffill()

async def history_main_wrapper(run_type, exchange_name, universe_name, nb_days=1, frequency='1h'):
    parameters = configLoader.get_executor_params(order='listen_binance', dirname='prod')
    exchange = await BinanceAPI.build(parameters['venue_api'])

    universe = configLoader.get_bases(universe_name)
    nb_days = int(nb_days)
    futures = pd.DataFrame(await BinanceAPI.Static.fetch_futures(exchange)).set_index('name')
    await exchange.load_markets()

    #universe should be either 'all', either a universe name, or a list of currencies
    dir_name = configLoader.get_mktdata_folder_for_exchange(exchange_name)

    # In case universe was not max, is_wide, is_institutional
    # universe = [id for id, data in exchange.markets_by_id.items() if data['base'] in [x.upper() for x in [universe]] and data['contract']]

    if universe != []:
        futures = futures[futures.index.isin(universe)]

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
        end = datetime.utcnow().replace(tzinfo=timezone.utc)-timedelta(days=nb_days)
        await correct_history(futures, exchange, hy_history[:end])
        await build_history(futures, exchange, dir_name, frequency=frequency)
        await exchange.close()
    elif run_type == 'get':
        logger.info("Getting history...")
        hy_history = await get_history(dir_name, futures, 24 * nb_days)
        await exchange.close()
        return hy_history

@api
def main(*args,**kwargs):
    '''
        example: histfeed get ftx wide 5 1h
        args:
           run_type = "build", "correct", "get"
           exchange = "ftx"
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