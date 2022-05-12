from utils.ftx_utils import *
from utils.io_utils import *
import pandas as pd
import os
from pathlib import Path

history_start = datetime(2019, 11, 26)

# all rates annualized, all volumes daily in usd
async def get_history(futures, start_or_nb_hours, end = datetime.now(tz=None).replace(minute=0,second=0,microsecond=0),
        dirname = os.path.join(Path.home(), 'mktdata')):
    data = pd.concat(await safe_gather((
            [async_from_parquet(dirname+'/'+f+'_funding.parquet')
             for f in futures[futures['type'] == 'perpetual'].index] +
            [async_from_parquet(dirname+'/'+f+'_futures.parquet')
             for f in futures.index] +
            [async_from_parquet(dirname+'/'+f+'_price.parquet')
             for f in futures['underlying'].unique()] +
            [async_from_parquet(dirname+'/'+f+'_borrow.parquet')
             for f in (list(futures['underlying'].unique()) + ['USD'])]
    )), join='outer', axis=1)

    # convert borrow size in usd
    for f in futures['underlying'].unique():
        data[f + '/rate/size']*=data[f + '/price/o']

    start = end - timedelta(hours=start_or_nb_hours) if isinstance(start_or_nb_hours,int) else start_or_nb_hours
    return data[~data.index.duplicated()].sort_index()[start:end]

async def build_history(futures,exchange,
        end = (datetime.now(tz=None).replace(minute=0,second=0,microsecond=0)),
        dirname = os.path.join(Path.home(), 'mktdata')): # Runtime/Mktdata_database
    '''for now, increments local files and then uploads to s3'''

    coroutines = []
    for _, f in futures[futures['type'] == 'perpetual'].iterrows():
        parquet_name = os.path.join(dirname, f.name + '_funding.parquet')
        parquet = from_parquet(parquet_name) if os.path.isfile(parquet_name) else None
        start = max(parquet.index)+timedelta(hours=1) if parquet is not None else history_start
        if start < end:
            coroutines.append(funding_history(f, exchange, start, end, dirname))

    for _, f in futures.iterrows():
        parquet_name = os.path.join(dirname, f.name + '_futures.parquet')
        parquet = from_parquet(parquet_name) if os.path.isfile(parquet_name) else None
        start = max(parquet.index) + timedelta(hours=1) if parquet is not None else history_start
        if start < end:
            coroutines.append(rate_history(f, exchange, end, start, '1h', dirname))

    for f in futures['underlying'].unique():
        parquet_name = os.path.join(dirname, f + '_price.parquet')
        parquet = from_parquet(parquet_name) if os.path.isfile(parquet_name) else None
        start = max(parquet.index) + timedelta(hours=1) if parquet is not None else history_start
        if start < end:
            coroutines.append(spot_history(f + '/USD', exchange, end, start , '1h', dirname))

    for f in list(futures.loc[futures['spotMargin'] == True, 'underlying'].unique()) + ['USD']:
        parquet_name = os.path.join(dirname, f + '_borrow.parquet')
        parquet = from_parquet(parquet_name) if os.path.isfile(parquet_name) else None
        start = max(parquet.index) + timedelta(hours=1) if parquet is not None else history_start
        if start < end:
            coroutines.append(borrow_history(f, exchange, end, start, dirname))

    # run all coroutines
    await safe_gather(coroutines)

    # static values for non spot Margin underlyings
    otc_file = pd.read_excel(os.path.join(Path.home(), 'mktdata', 'static_params.xlsx'),sheet_name='used').set_index('coin')
    for f in list(futures.loc[futures['spotMargin'] == False, 'underlying'].unique()):
        spot_parquet = from_parquet(os.path.join(dirname, f + '_price.parquet'))
        to_parquet(pd.DataFrame(index=spot_parquet.index, columns=[f + '/rate/borrow'],
                                data=otc_file.loc[f,'borrow'] if futures.loc[f+'-PERP','spotMargin'] == 'OTC' else 999
                                ),os.path.join(dirname, f +'_borrow.parquet'),mode='a')
        to_parquet(pd.DataFrame(index=spot_parquet.index, columns=[f + '/rate/size'],
                                data=otc_file.loc[f,'size'] if futures.loc[f+'-PERP','spotMargin'] == 'OTC' else 0
                                ),os.path.join(dirname, f + '_borrow.parquet'),mode='a')

    #os.system("aws s3 sync Runtime/Mktdata_database/ s3://hourlyftx/Mktdata_database")

async def correct_history(futures,exchange,hy_history,dirname=os.path.join(Path.home(), 'mktdata')):
    '''for now, increments local files and then uploads to s3'''

    coroutines = []
    for _, f in futures[futures['type'] == 'perpetual'].iterrows():
        parquet_name = os.path.join(dirname, f.name, '_funding.parquet')
        coroutines.append(async_to_parquet(hy_history[[exchange.market(f['symbol'])['id'] + '/rate/funding']],parquet_name))

    for _, f in futures.iterrows():
        parquet_name = os.path.join(dirname, f.name, '_futures.parquet')
        future_id = exchange.market(f['symbol'])['id']
        column_names = [future_id + '/mark/' + field for field in ['o', 'h', 'l', 'c', 'volume']]
        column_names += [future_id + '/indexes/' + field for field in ['o', 'h', 'l', 'c', 'volume']]
        column_names += [future_id + '/rate/' + field for field in ['h', 'l', 'c'] + (['T'] if f['type']=='future' else [])]
        coroutines.append(async_to_parquet(hy_history[column_names], parquet_name))

    for f in futures['underlying'].unique():
        parquet_name = os.path.join(dirname, f, '_price.parquet')
        column_names = [f + '/price/' + field for field in ['o', 'h', 'l', 'c', 'volume']]
        coroutines.append(async_to_parquet(hy_history[column_names], parquet_name))

    for f in list(futures.loc[futures['spotMargin'] == True, 'underlying'].unique()) + ['USD']:
        parquet_name = os.path.join(dirname, f, '_borrow.parquet')
        column_names = [f + '/rate/' + field for field in ['borrow', 'size']]
        coroutines.append(async_to_parquet(hy_history[column_names], parquet_name))

    # run all coroutines
    await safe_gather(coroutines)

### only perps, only borrow and funding, only hourly
async def borrow_history(coin,
                         exchange,
                         end=(datetime.now(tz=timezone.utc).replace(minute=0,second=0,microsecond=0)),
                         start=(datetime.now(tz=timezone.utc).replace(minute=0,second=0,microsecond=0))-timedelta(days=30),
                         dirname=''):
    max_funding_data = int(500)  # in hour. limit is 500 :(
    resolution = exchange.describe()['timeframes']['1h']

    e = end.timestamp()
    s = start.timestamp()
    f = max_funding_data * int(resolution)
    start_times = [e - k * f for k in range(1 + int((e - s) / f)) if e - k * f > s] + [s]

    data = pd.concat(await safe_gather([
        fetch_borrow_rate_history(exchange,coin,start_time,start_time+f-int(resolution))
            for start_time in start_times]),axis=0,join='outer')
    if len(data)==0:
        return pd.DataFrame(columns=[coin+'/rate/borrow',coin+'/rate/size'])

    data = data.astype(dtype={'time': 'int64'}).set_index(
        'time')[['rate','size']]
    data.rename(columns={'rate':coin+'/rate/borrow','size':coin+'/rate/size'},inplace=True)
    data.index = [datetime.fromtimestamp(x / 1000) for x in data.index]
    data=data[~data.index.duplicated()].sort_index()

    if dirname != '': await async_to_parquet(data, os.path.join(dirname, coin + '_borrow.parquet'),mode='a')

    return data

######### annualized funding for perps
async def funding_history(future,
                          exchange,
                          start=(datetime.now(tz=timezone.utc).replace(minute=0,second=0,microsecond=0))-timedelta(days=30),
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
        return pd.DataFrame(columns=[exchange.market(future['symbol'])['id'] + '/rate/funding'])

    data = pd.DataFrame(funding)
    data['time']=data['timestamp'].astype(dtype='int64')
    data[exchange.market(future['symbol'])['id'] + '/rate/funding']=data['fundingRate']*365.25*24
    data=data[['time',exchange.market(future['symbol'])['id'] + '/rate/funding']].set_index('time')
    data.index = [datetime.fromtimestamp(x / 1000) for x in data.index]
    data = data[~data.index.duplicated()].sort_index()

    if dirname != '': await async_to_parquet(data, os.path.join(dirname,
                                                                exchange.market(future['symbol'])['id'] + '_funding.parquet'),
                                             mode='a')

    return data

async def fetch_trades_history(symbol,exchange,
                 start= (datetime.now(tz=timezone.utc).replace(minute=0,second=0,microsecond=0))-timedelta(days=30),
                    end=(datetime.now(tz=timezone.utc).replace(minute=0, second=0, microsecond=0)),
                         frequency=timedelta(minutes=1),
                    dirname=''):
    max_trades_data = int(5000)  # in trades. limit is 5000 :(
    print('trades_history: ' + symbol)

    ### grab data per batch of 5000, try weekly
    trades=[]
    end_time = (start + timedelta(hours=1)).timestamp()
    start_time = start.timestamp()

    while start_time < end.timestamp():
        new_trades =  (await exchange.publicGetMarketsMarketNameTrades(
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
        end_time = (datetime.fromtimestamp(start_time) + timedelta(
            hours=1)).timestamp()

    if len(trades)==0:
        vwap=pd.DataFrame(columns=['size','volume','count','vwap'])
        vwap.columns = [symbol.split('/USD')[0] + '/trades/' + column for column in vwap.columns]
        return {'symbol':exchange.market(symbol)['symbol'],'coin':exchange.market(symbol)['base'],'vwap':vwap}

    data = pd.DataFrame(data=trades)
    data[['size','price']] = data[['size','price']].astype(float)
    data['volume'] = data['size'] * data['price']
    data['square'] = data['size'] * data['price']*data['price']
    data['count'] = 1

    data['time']=data['time'].apply(lambda t: dateutil.parser.isoparse(t).replace(tzinfo=None))
    data.set_index('time',inplace=True)

    vwap=data[['size','volume','square','count']].resample(frequency).sum()
    vwap['vwap']=vwap['volume']/vwap['size']
    vwap['vwsp'] = np.sqrt(vwap['square'] / vwap['size']-vwap['vwap']*vwap['vwap'])

    vwap.columns = [symbol.split('/USD')[0] + '/trades/' + column for column in vwap.columns]
    #data.index = [datetime.fromtimestamp(x / 1000) for x in data.index]
    vwap = vwap[~vwap.index.duplicated()].sort_index().ffill()

    parquet_filename = os.path.join(dirname, symbol.split('/USD')[0] + "_trades.parquet")
    if dirname != '': vwap.to_parquet(parquet_filename)

    return {'symbol':exchange.market(symbol)['symbol'],'coin':exchange.market(symbol)['base'],'vwap':vwap}


#### annualized rates for futures and perp, volumes are daily
async def rate_history(future,exchange,
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

    mark_indexes = await safe_gather([
        exchange.fetch_ohlcv(exchange.market(future['symbol'])['symbol'], timeframe=timeframe, params=params) # volume is for max_mark_data*resolution
            for start_time in start_times
                for params in [{'start_time':start_time, 'end_time':start_time+f-int(resolution)},
                               {'start_time':start_time, 'end_time':start_time+f-int(resolution),'price':'index'}]])
    mark = [y for x in mark_indexes[::2] for y in x]
    indexes = [y for x in mark_indexes[1::2] for y in x]

    if ((len(indexes) == 0) | (len(mark) == 0)):
        return pd.DataFrame(columns=
                         [exchange.market(future['symbol'])['id'] + '/mark/' + c for c in ['t', 'o', 'h', 'l', 'c', 'volume']]
                        +[exchange.market(future['symbol'])['id'] + '/indexes/'  + c for c in ['t', 'o', 'h', 'l', 'c', 'volume']]
                        +[exchange.market(future['symbol'])['id'] + '/rate/' + c for c in ['T','c','h','l']])
    column_names = ['t', 'o', 'h', 'l', 'c', 'volume']

    ###### indexes
    indexes = pd.DataFrame([dict(zip(column_names,row)) for row in indexes], dtype=float).astype(dtype={'t': 'int64'}).set_index('t')
    indexes['volume'] = indexes['volume']* 24 * 3600 / int(resolution)

    ###### marks
    mark = pd.DataFrame([dict(zip(column_names,row)) for row in mark]).astype(dtype={'t': 'int64'}).set_index('t')
    mark['volume']=mark['volume']*24*3600/int(resolution)

    mark.columns = ['mark/' + column for column in mark.columns]
    indexes.columns = ['indexes/' + column for column in indexes.columns]
    data = mark.join(indexes, how='inner')

    ########## rates from index to mark
    if future['type'] == 'future':
        expiry_time = dateutil.parser.isoparse(future['expiry']).timestamp()
        data['rate/T'] = data.apply(lambda t: (expiry_time - int(t.name) / 1000) / 3600 / 24 / 365.25, axis=1)

        data['rate/c'] = data.apply(
            lambda y: calc_basis(y['mark/c'],
                                 indexes.loc[y.name, 'indexes/c'], future['expiryTime'],
                                 datetime.fromtimestamp(int(y.name / 1000), tz=None)), axis=1)
        data['rate/h'] = data.apply(
            lambda y: calc_basis(y['mark/h'], indexes.loc[y.name, 'indexes/h'], future['expiryTime'],
                                 datetime.fromtimestamp(int(y.name / 1000), tz=None)), axis=1)
        data['rate/l'] = data.apply(
            lambda y: calc_basis(y['mark/l'], indexes.loc[y.name, 'indexes/l'], future['expiryTime'],
                                 datetime.fromtimestamp(int(y.name / 1000), tz=None)), axis=1)
    elif future['type'] == 'perpetual': ### 1h funding = (mark/spot-1)/24
        data['rate/T'] = None
        data['rate/c'] = (mark['mark/c'] / indexes['indexes/c'] - 1)*365.25
        data['rate/h'] = (mark['mark/h'] / indexes['indexes/h'] - 1)*365.25
        data['rate/l'] = (mark['mark/l'] / indexes['indexes/l'] - 1)*365.25
    else:
        print('what is ' + future['symbol'] + ' ?')
        return
    data.columns = [exchange.market(future['symbol'])['id'] + '/' + c for c in data.columns]
    data.index = [datetime.fromtimestamp(x / 1000) for x in data.index]
    data = data[~data.index.duplicated()].sort_index()

    if dirname != '': await async_to_parquet(data,
                                             os.path.join(dirname,
                                                          exchange.market(future['symbol'])['id'] + '_futures.parquet'),
                                             mode='a')

    return data

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
        return pd.DataFrame(columns=[symbol.replace('/USD','') + '/price/' + c for c in column_names])

    ###### spot
    data = pd.DataFrame(columns=column_names, data=spot).astype(dtype={'t': 'int64', 'volume': 'float'}).set_index('t')
    data['volume'] = data['volume'] * 24 * 3600 / int(resolution)
    data.columns = [symbol.replace('/USD','') + '/price/' + column for column in data.columns]
    data.index = [datetime.fromtimestamp(x / 1000) for x in data.index]
    data = data[~data.index.duplicated()].sort_index()
    if dirname!='': await async_to_parquet(data,
                                           os.path.join(dirname,
                                                        symbol.replace('/USD', '') + '_price.parquet'),
                                           mode='a')

    return data

async def ftx_history_main_wrapper(*argv):
    exchange = await open_exchange(argv[2],'')
    futures = pd.DataFrame(await fetch_futures(exchange, includeExpired=True)).set_index('name')
    await exchange.load_markets()

    #argv[1] is either 'all', either a universe name, or a list of currencies
    filename = 'Runtime/configs/universe.xlsx'
    filename = '/universe.xlsx'
    filename = os.path.join(Path.home(), 'mktdata', 'universe.xlsx')
    try:
        universe_list=pd.read_excel(filename, sheet_name='screening_params', index_col=0).columns
    except:
        universe_list=pd.DataFrame()

    if isinstance(argv[1],str) and argv[1] in universe_list:
        universe = pd.read_excel(filename,sheet_name=argv[1],index_col=0).index
    elif argv[1]=='all': universe=exchange.markets_by_id
    else: universe = [id for id,data in exchange.markets_by_id.items() if data['base'] in [x.upper() for x in [argv[1]]] and data['contract']]
    futures = futures[futures.index.isin(universe)]

    # volume screening
    if argv[0] == 'build':
        await build_history(futures, exchange)
    elif argv[0] == 'correct':
        hy_history = await get_history(futures, history_start)
        end = datetime.now()-timedelta(days=int(argv[3]))
        await correct_history(futures,exchange,hy_history[:end])
        await build_history(futures, exchange)
    elif argv[0] == 'get':
        pass
    else:
        raise Exception(f'unknown command{argv[0]}: use build,correct,get')

    hy_history = await get_history(futures, 24*int(argv[3]))
    await exchange.close()
    return hy_history

def ftx_history_main(*argv):
    argv=list(argv)
    if len(argv) < 1:
        argv.extend(['build']) # build or correct
    if len(argv) < 2:
        argv.extend(['wide']) # universe name, or list of currencies, or 'all'
    if len(argv) < 3:
        argv.extend(['ftx']) # exchange_name
    if len(argv) < 4:
        argv.extend([100])# nb days

    return asyncio.run(ftx_history_main_wrapper(*argv))

if __name__ == "__main__":
    ftx_history_main(*sys.argv[1:])