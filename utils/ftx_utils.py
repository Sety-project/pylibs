from ccxt_utilities import *

#index_list=['DEFI_PERP','SHIT_PERP','ALT_PERP','MID_PERP','DRGN_PERP','PRIV_PERP']
#publicGetIndexesIndexNameWeights()GET /indexes/{index_name}/weights
#index_table=pd.DataFrame()

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

async def mkt_at_size(exchange, symbol, side, target_depth=10000.):
    ''' returns average px of a mkt order of size target_depth (in USD)
    side='bids' or 'asks'
    '''
    order_book = await exchange.fetch_order_book(symbol)
    mktdepth = pd.DataFrame(order_book[side])
    other_side = 'bids' if side=='asks' else 'asks'
    mid = 0.5 * (order_book[side][0][0] + order_book[other_side][0][0])

    if target_depth==0:
        return (order_book[side][0][0],mid)

    mktdepth['px'] = (mktdepth[0]*mktdepth[1]).cumsum()/mktdepth[1].cumsum()
    mktdepth['size'] = (mktdepth[0]*mktdepth[1]).cumsum()

    interpolator = mktdepth.set_index('size')['px']
    interpolator[float(target_depth)] = np.NaN
    interpolator.interpolate(method='index',inplace=True)

    return {'mid':mid,'side':interpolator[target_depth],'slippage':interpolator[target_depth]/mid-1.0,'symbol':symbol}

def sweep_price(exchange, symbol, size):
    ''' fast version of mkt_at_size for use in executer
    slippage of a mkt order: https://www.sciencedirect.com/science/article/pii/S0378426620303022'''
    depth = 0
    previous_side = exchange.orderbooks[symbol]['bids' if size >= 0 else 'asks'][0][0]
    for pair in exchange.orderbooks[symbol]['bids' if size>=0 else 'asks']:
        depth += pair[0] * pair[1]
        if depth > size:
            break
        previous_side = pair[0]
    depth=0
    previous_opposite = exchange.orderbooks[symbol]['bids' if size < 0 else 'asks'][0][0]
    for pair in exchange.orderbooks[symbol]['bids' if size<0 else 'asks']:
        depth += pair[0] * pair[1]
        if depth > size:
            break
        previous_opposite = pair[0]
    return {'side':previous_side,'opposite':previous_opposite}

async def mkt_speed(exchange, symbol, target_depth=10000):
    '''
    returns time taken to trade a certain target_depth (in USD)
    side='bids' or 'asks' '''
    nowtime = datetime.now(tz=timezone.utc)
    trades=pd.DataFrame(await exchange.fetch_trades(symbol))
    if trades.shape[0] == 0: return 9999999
    nowtime = nowtime + timedelta(microseconds=int(0.5*(nowtime.microsecond-datetime.now().microsecond))) ### to have unbiased ref

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

async def fetch_coin_details(exchange):
    coin_details = pd.DataFrame((await exchange.publicGetWalletCoins())['result']).astype(dtype={'collateralWeight': 'float','indexPrice': 'float'}).set_index('id')

    borrow_rates = pd.DataFrame((await exchange.private_get_spot_margin_borrow_rates())['result']).astype(dtype={'coin': 'str', 'estimate': 'float', 'previous': 'float'}).set_index('coin')[['estimate']]
    borrow_rates[['estimate']]*=24*365.25
    borrow_rates.rename(columns={'estimate':'borrow'},inplace=True)

    lending_rates = pd.DataFrame((await exchange.private_get_spot_margin_lending_rates())['result']).astype(dtype={'coin': 'str', 'estimate': 'float', 'previous': 'float'}).set_index('coin')[['estimate']]
    lending_rates[['estimate']] *= 24 * 365.25
    lending_rates.rename(columns={'estimate': 'lend'}, inplace=True)

    borrow_volumes = pd.DataFrame((await exchange.public_get_spot_margin_borrow_summary())['result']).astype(dtype={'coin': 'str', 'size': 'float'}).set_index('coin')
    borrow_volumes.rename(columns={'size': 'borrow_open_interest'}, inplace=True)

    all = pd.concat([coin_details,borrow_rates,lending_rates,borrow_volumes],join='outer',axis=1)
    all = all.loc[coin_details.index] # borrow summary has beed seen containing provisional underlyings
    all.loc[coin_details['spotMargin'] == False,'borrow']= None ### hope this throws an error...
    all.loc[coin_details['spotMargin'] == False, 'lend'] = 0

    return all

# time in mili, rate annualized, size is an open interest in USD
async def fetch_borrow_rate_history(exchange, coin,start_time,end_time,params={}):
    start_time = datetime.now().timestamp() - 499 * 3600
    end_time = datetime.now().timestamp()

    request = {
        'coin': coin,
        'start_time': start_time,
        'end_time': end_time
    }

    try:
        response = await exchange.publicGetSpotMarginHistory(exchange.extend(request, params))
    except Exception as e:
        logging.exception(e,exc_info=True)
        return pd.DataFrame()

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

### get all static fields TODO: could just append coindetails if it wasn't for index,imf factor,positionLimitWeight
async def fetch_futures(exchange,includeExpired=False,includeIndex=False,params={}):
    response = await exchange.publicGetFutures(params)
    fetched = await exchange.fetch_markets()
    expired = await exchange.publicGetExpiredFutures(params) if includeExpired==True else []
    coin_details = await fetch_coin_details(exchange)
    otc_file = pd.read_excel('Runtime/configs/static_params.xlsx',sheet_name='used').set_index('coin')

    #### for IM calc
    account_leverage = (await exchange.privateGetAccount())['result']
    if float(account_leverage['leverage']) >= 50: print("margin rules not implemented for leverage >=50")

    markets = exchange.safe_value(response, 'result', []) + exchange.safe_value(expired, 'result', [])

    perp_list = [f['name'] for f in markets if f['type'] == 'perpetual' and f['enabled']]
    funding_rates = await safe_gather([exchange.publicGetFuturesFutureNameStats({'future_name': f})
                              for f in perp_list])
    funding_rates = {name: float(rate['result']['nextFundingRate']) * 24 * 365.325 for name,rate in zip(perp_list,funding_rates)}

    result = []
    for i in range(0, len(markets)):
        market = markets[i]
        underlying = exchange.safe_string(market, 'underlying')
        mark =  exchange.safe_number(market, 'mark')
        imfFactor =  exchange.safe_number(market, 'imfFactor')
        expiryTime = dateutil.parser.isoparse(exchange.safe_string(market, 'expiry')).replace(tzinfo=None) if exchange.safe_string(market, 'type') == 'future' else np.NaN
        if exchange.safe_string(market,'type') == 'future':
            future_carry = calc_basis(mark, market['index'], expiryTime, datetime.now())
        elif market['name'] in perp_list:
            future_carry = funding_rates[exchange.safe_string(market, 'name')]
        else:
            future_carry = 0

        ## eg ADA has no coin details
        if not underlying in coin_details.index:
            if not includeIndex: continue
        try:## eg DMG-PERP doesn't exist (IncludeIndex = True)
            symbol = exchange.market(exchange.safe_string(market, 'name'))['symbol']
        except Exception as e:
            continue
            #symbol=exchange.safe_string(market, 'name')#TODO: why ?

        result.append({
            'ask':  exchange.safe_number(market, 'ask'),
            'bid':  exchange.safe_number(market, 'bid'),
            'change1h':  exchange.safe_number(market, 'change1h'),
            'change24h':  exchange.safe_number(market, 'change24h'),
            'changeBod':  exchange.safe_number(market, 'changeBod'),
            'volumeUsd24h':  exchange.safe_number(market, 'volumeUsd24h'),
            'volume':  exchange.safe_number(market, 'volume'),
            'symbol': exchange.safe_string(market, 'name'),
            "enabled": exchange.safe_value(market, 'enabled'),
            "expired": exchange.safe_value(market, 'expired'),
            "expiry": exchange.safe_string(market, 'expiry') if exchange.safe_string(market, 'expiry') else 'None',
            'index':  exchange.safe_number(market, 'index'),
            'imfFactor':  exchange.safe_number(market, 'imfFactor'),
            'last':  exchange.safe_number(market, 'last'),
            'lowerBound':  exchange.safe_number(market, 'lowerBound'),
            'mark':  exchange.safe_number(market, 'mark'),
            'name': exchange.safe_string(market, 'name'),
            "perpetual": exchange.safe_value(market, 'perpetual'),
            #'positionLimitWeight': exchange.safe_value(market, 'positionLimitWeight'),
            #"postOnly": exchange.safe_value(market, 'postOnly'),
            'priceIncrement': exchange.safe_value(market, 'priceIncrement'),
            'sizeIncrement': exchange.safe_value(market, 'sizeIncrement'),
            'underlying': exchange.safe_string(market, 'underlying'),
            'upperBound': exchange.safe_value(market, 'upperBound'),
            'type': exchange.safe_string(market, 'type'),
         ### additionnals
            'new_symbol': exchange.market(exchange.safe_string(market, 'name'))['symbol'],
            'openInterestUsd': exchange.safe_number(market,'openInterestUsd'),
            'account_leverage': float(account_leverage['leverage']),
            'collateralWeight':coin_details.loc[underlying,'collateralWeight'] if underlying in coin_details.index else 'coin_details not found',
            'underlyingType': getUnderlyingType(coin_details.loc[underlying]) if underlying in coin_details.index else 'index',
            'spot_ticker': exchange.safe_string(market, 'underlying')+'/USD',
            'cash_borrow': coin_details.loc[underlying,'borrow'] if underlying in coin_details.index and coin_details.loc[underlying,'spotMargin'] else None,
            'future_carry': future_carry,
            'spotMargin': 'OTC' if underlying in otc_file.index else (coin_details.loc[underlying,'spotMargin'] if underlying in coin_details.index else 'coin_details not found'),
            'tokenizedEquity':coin_details.loc[underlying,'tokenizedEquity'] if underlying in coin_details.index else 'coin_details not found',
            'usdFungible':coin_details.loc[underlying,'usdFungible'] if underlying in coin_details.index else 'coin_details not found',
            'fiat':coin_details.loc[underlying,'fiat'] if underlying in coin_details.index else 'coin_details not found',
            'expiryTime':expiryTime
            })

    return result