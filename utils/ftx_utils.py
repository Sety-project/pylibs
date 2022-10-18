from utils.ccxt_utilities import *
import pandas as pd


#index_list=['DEFI_PERP','SHIT_PERP','ALT_PERP','MID_PERP','DRGN_PERP','PRIV_PERP']
#publicGetIndexesIndexNameWeights()GET /indexes/{index_name}/weights
#index_table=pd.DataFrame()

# C'est pour contribuer a ccxt, pour normaliser leur data
# ouvrir un objet exchange ccxt pour faire okex
# appeler fetch_candles

def getUnderlyingType(coin_detail_item):
    if coin_detail_item['usdFungible'] == True:
        return 'usdFungible'
    if ('tokenizedEquity' in coin_detail_item.keys()):
        if (coin_detail_item['tokenizedEquity'] == True):
            return 'tokenizedEquity'
    if coin_detail_item['fiat'] == True:
        return 'fiat'

    return 'crypto'

def find_spot_ticker(markets,future,query):
    try:
        spot_found=next(item for item in markets if
                        (item['base'] == future['underlying'])
                        &(item['quote'] == 'USD')
                        &(item['type'] == 'spot'))
        return spot_found['info'][query]
    except:
        return np.NaN

def mkt_at_size(order_book, side, target_depth=10000.):
    ''' returns average px of a mkt order of size target_depth (in USD)
    side='bids' or 'asks'
    '''
    mktdepth = pd.DataFrame(order_book[side])
    mid = 0.5 * (order_book['bids'][0][0] + order_book['asks'][0][0])

    if target_depth==0:
        return (order_book[side][0][0],mid)

    mktdepth['px'] = (mktdepth[0]*mktdepth[1]).cumsum()/mktdepth[1].cumsum()
    mktdepth['size'] = (mktdepth[0]*mktdepth[1]).cumsum()

    interpolator = mktdepth.set_index('size')['px']
    interpolator[float(target_depth)] = np.NaN
    interpolator.interpolate(method='index',inplace=True)

    return {'mid':mid,'side':interpolator[target_depth],'slippage':interpolator[target_depth]/mid-1.0}

def sweep_price_depths(order_book, depths):
    ''' depths = a ('bids','asks') dict of (USD) depths list
    returns a ('bids','asks') dict of (depths,sweep price) tuple list '''
    sweep_prices = {'bids':[],'asks':[]}
    for side in ['bids','asks']:
        book_on_side = order_book[side]
        depths_on_side = depths[side]
        n_on_side = len(depths_on_side)
        depth_idx = 0
        depth = 0
        for pair in book_on_side:
            depth += pair[0] * pair[1]
            if depth > depths_on_side[depth_idx]:
                sweep_prices[side] += [(depths_on_side[depth_idx],pair[0])]
                if depth_idx < n_on_side:
                    depth_idx += 1
                else:
                    break
    return sweep_prices

async def mkt_speed(exchange, symbol, target_depth=10000):
    '''
    returns time taken to trade a certain target_depth (in USD)
    side='bids' or 'asks' '''
    nowtime = datetime.now(tz=timezone.utc)
    trades=pd.DataFrame(await exchange.fetch_trades(symbol))
    if trades.shape[0] == 0: return 9999999
    nowtime = nowtime + timedelta(microseconds=int(0.5*(nowtime.microsecond-datetime.utcnow().replace(tzinfo=timezone.utc).microsecond))) ### to have unbiased ref

    trades['size']=(trades['price']*trades['amount']).cumsum()

    interpolator=trades.set_index('size')['timestamp']
    interpolator[float(target_depth)]=np.NaN
    interpolator.interpolate(method='index',inplace=True)
    res=interpolator[float(target_depth)]
    return nowtime-datetime.fromtimestamp(res / 1000, tz=timezone.utc)

async def vwap(exchange,symbol,start_time,end_time,freq):
    '''vwap calc, disregard pagination :('''
    trade_list = pd.DataFrame(await exchange.publicGetMarketsMarketNameTrades(
        {'market_name': symbol, 'start_time': start_time/1000, 'end_time': end_time/1000}
    )['result'], dtype=float)
    trade_list['time']=trade_list['time'].apply(dateutil.parser.isoparse)
    trade_list['amt']=trade_list.apply(lambda x: x['size']*(1 if x['side'] else -1),axis=1)
    trade_list['amtUSD'] = trade_list['amt']*trade_list['price']

    vwap=trade_list.set_index('time')[['amt','amtUSD']].resample(freq).sum()
    vwap['vwap']=vwap['amtUSD']/vwap['amt']

    return vwap.drop(columns='amtUSD').ffill()

async def underlying_vol(exchange,symbol,start_time,end_time):
    raise Exception('bugged')
    trade_list = pd.DataFrame(await exchange.publicGetMarketsMarketNameTrades(
        {'market_name': symbol, 'start_time': start_time/1000, 'end_time': end_time/1000}
    )['result'], dtype=float)
    trade_list['timestamp']=trade_list['time'].apply(dateutil.parser.isoparse).apply(datetime.timestamp)
    trade_list['amt']=trade_list.apply(lambda x: x['size']*(1 if x['side'] else -1),axis=1)
    trade_list['amtUSD'] = trade_list['amt']*trade_list['price']

    # in case duplicated index
    trade_list=trade_list.set_index('timestamp')[['amt','amtUSD']].groupby(level=0).sum().reset_index()
    trade_list['price'] = trade_list['amtUSD']/trade_list['amt']
    vol = np.sqrt((trade_list['price'].diff()*trade_list['price'].diff()/trade_list['timestamp'].diff()).median())

    return vol

'''
market data helpers
'''
# time in mili, rate annualized, size is an open interest in USD
async def fetch_borrow_rate_history(exchange, coin,start_time,end_time,params={}):
    request = {
        'coin': coin,
        'start_time': start_time,
        'end_time': end_time
    }

    response = await exchange.publicGetSpotMarginHistory(exchange.extend(request, params))

    if len(exchange.safe_value(response, 'result', []))==0: return pd.DataFrame()
    result = pd.DataFrame(exchange.safe_value(response, 'result', [])).astype({'coin':str,'time':str,'size':float,'rate':float})
    result['time']=result['time'].apply(lambda t:dateutil.parser.isoparse(t).timestamp()*1000)
    result['rate']*=24*365.25*(1+500 * exchange.loaded_fees['trading'][(coin if coin!='USD' else 'BTC') +'/USD']['taker'])
    result['size']*=1 # borrow size is an open interest
    original = result

    # response = await exchange.fetch_borrow_rate_history(coin,since=start_time*1000,limit=int((end_time-start_time)/3600))
    # response = [x|{'size':x['info']['size']} for x in response]
    # result = pd.DataFrame(response).astype({'rate':float,'size':float}).rename(columns={'timestamp':'time','currency':'coin'})
    # result['rate'] *= 24 * 365.25
    # result = result[['coin','time','size','rate']]

    return original

def collateralWeightInitial(future):
    '''not in API. Empirically = collateralWeight'''
    return max(0.01,future['collateralWeight'])
    if future['underlying'] in ['BUSD','FTT','HUSD','TUSD','USD','USDC','USDP','WUSDC']:
        return future['collateralWeight']
    elif future['underlying'] in ['AUD','BRL','BRZ','CAD','CHF','EUR','GBP','HKD','SGD','TRY','ZAR']:
        return future['collateralWeight']-0.01
    elif future['underlying'] in ['BTC','USDT','WBTC','WUSDT']:
        return future['collateralWeight']-0.025
    else:
        return max(0.01,future['collateralWeight']-0.05)


