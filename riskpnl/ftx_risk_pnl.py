import time as t
from utils.ftx_utils import *
from utils.config_loader import *
from riskpnl.post_trade import *

async def carry_portfolio_greeks(exchange,futures,params={'positive_carry_on_balances':False}):
    '''
    list of dicts positions (resp. balances) assume unique 'future' (resp. 'coin')
    positions need netSize, future, initialMarginRequirement, maintenanceMarginRequirement, realizedPnl, unrealizedPnl
    balances need coin, total
    careful: carry on balances cannot be overal positive.
    '''
    markets = await exchange.fetch_markets()
    futures = await fetch_futures(exchange)

    balances=pd.DataFrame((await exchange.fetch_balance(params={}))['info']['result'],dtype=float)#'showAvgPrice':True})
    balances=balances[(balances['total']!=0.0)&(balances['coin']!='USD')].fillna(0.0)
    balances['spotDelta'] = balances.apply(lambda f: f['total'] * (1.0 if f['coin'] == 'USD' else float(futures[f['coin'] + '/USD']['info']['price'])), axis=1)

    positions = pd.DataFrame([r['info'] for r in await exchange.fetch_positions(params={}) if r['info']['netSize']!=0],dtype=float)#'showAvgPrice':True})

    greeks = pd.DataFrame(columns=pd.MultiIndex.from_tuples([], names=['underlyingType',"underlying", "margining", "expiry","name","contractType"]))
    updated=str(datetime.now())
    rho=0.4

    for x in positions:
        if float(x['optimalWeight']) !=0.0:

            future_item=next(item for item in futures if item['symbol'] == x['future'])
            coin = future_item['underlying']
            underlyingType=getUnderlyingType(coin_details.loc[coin]) if coin in coin_details.index else 'index'
            funding_stats =(await exchange.publicGetFuturesFutureNameStats({'future_name': future_item['name']}))['result']

            size = float(x['netSize'])
            chg = float(future_item['change24h'])
            f=float(future_item['mark'])
            s = float(future_item['index'])
            if future_item['type']=='perpetual':
                t=0.0
                carry= - size*s*float(funding_stats['nextFundingRate'])*24*365.25
            else:
                days_diff = (dateutil.parser.isoparse(future_item['expiry']) - datetime.now(tz=timezone.utc))
                t=days_diff.days/365.25
                carry = - size*f * np.log(f / s) / t

            margin_coin = 'USD'  ## always USD on FTX
            if margin_coin == 'USD':
                greeks[(underlyingType,
                    str(coin),
                    margin_coin,
                    future_item['expiry'],
                    future_item['name'],
                    future_item['type'])]= pd.Series({
                        (updated,'PV'):0,
                        (updated, 'ref'): f,
                        (updated,'Delta'):size*f,
                        (updated,'ShadowDelta'):size*f*(1+rho*t),
                        (updated,'Gamma'):size*f*rho*t*(1+rho*t),
                        (updated,'IR01'):size*t*f/10000,
                        (updated,'Carry'):carry,
                        (updated,'collateralValue'):0,
                        (updated,'IM'): float(x['initialMarginRequirement'])*np.abs(size)*f,
                        (updated,'MM'): float(x['maintenanceMarginRequirement'])*np.abs(size)*f,
                            })
            else:
                greeks[(underlyingType,
                    str(coin),
                    margin_coin,
                    future_item['expiry'],
                    future_item['name'],
                    future_item['type'])] = pd.Series({
                    (updated, 'PV'): 0,
                    (updated, 'ref'): f,
                    (updated, 'Delta'): size / f*s,
                    (updated, 'ShadowDelta'): size / f * s * (1 + rho * t),
                    (updated, 'Gamma'): size / f *s* rho * t * (1 + rho * t),
                    (updated, 'IR01'): size*t*s/f/10000,
                    (updated, 'Carry'): carry,
                    (updated, 'collateralValue'): 0,
                    (updated, 'IM'): float(x['collateralUsed']),
                    (updated, 'MM'): float(x['maintenanceMarginRequirement']) * size ,
                })

            margin_cash=float(x['realizedPnl'])+float(x['unrealizedPnl'])
            try:
                for item in balances:
                    if item['coin'] == margin_coin: item['total']=float(item['total'])+margin_cash
            except:
                balances.append({'total':margin_cash,'coin':margin_coin})

#        margin_greeks=pd.Series(index=list(zip([updated]*10,['PV','ref','Delta','ShadowDelta','Gamma','IR01','Carry','collateralValue','IM','MM'])),
#                   data=[margin_cash,1.0,0.0,0.0,0,0,0,margin_cash,0,0])# + float(x['realizedPnl'])
#        if (margin_coin, 'USD', None, margin_coin, 'spot') in greeks.columns:
#            greeks[('usdFungible',margin_coin, 'USD', None, margin_coin, 'spot')]=margin_greeks.add(greeks[(margin_coin, 'USD', None, margin_coin, 'spot')],fill_value=0.0)
#        else:
#            greeks[('usdFungible',margin_coin, 'USD', None, margin_coin, 'spot')]=margin_greeks ### 'usdFungible' for now...

    stakes = pd.DataFrame((await exchange.privateGetStakingBalances())['result']).set_index('coin')
    for x in balances:
        try:
            market_item = next(item for item in markets if item['id'] == x['coin']+'/USD')
            s = float(market_item['info']['price'])
            chg = float(market_item['info']['change24h'])
        except: ## fails for USD
            s = 1.0
            chg = 0.0

        coin = x['coin']
        underlyingType=getUnderlyingType(coin_details.loc[coin])

        size=float(x['total'])
        if size!=0:
            staked = float(stakes.loc[coin,'staked']) if coin in stakes.index else 0
            collateralValue=size*s*(coin_details.loc[coin,'collateralWeight'] if size>0 else 1)-staked*s
            ### weight(initial)=weight(total)-5% for all but stablecoins/ftt(0) and BTC (2.5)
            im=(1.1 / (coin_details.loc[coin,'collateralWeight']-0.05) - 1) * s * -size if (size<0) else 0.0
            mm=(1.03 / (coin_details.loc[coin,'collateralWeight']-0.05) - 1) * s * -size if (size<0) else 0.0
            ## prevent positive carry on balances (no implicit lending/staking)
            carry=size*s* (float(coin_details.loc[coin,('borrow')]) if (size<0) else 0)
            delta = size*s if coin!='USD' else 0

            newgreeks=pd.Series({
                    (updated,'PV'):size*s,
                    (updated, 'ref'): s,
                    (updated,'Delta'):delta,
                    (updated,'ShadowDelta'):delta,
                    (updated,'Gamma'):0,
                    (updated,'IR01'):0,
                    (updated,'Carry'):carry,
                    (updated,'collateralValue'): collateralValue,
                    (updated,'IM'): im,
                    (updated,'MM'): mm})
            if (underlyingType,coin,'USD',None,coin,'spot') in greeks.columns:
                greeks[(underlyingType,
                        coin,
                        'USD',
                        None,
                        coin,
                        'spot')] = greeks[(underlyingType,
                        coin,
                        'USD',
                        None,
                        coin,
                        'spot')] + newgreeks
            else:
                greeks[(underlyingType,
                coin,
                'USD',
                None,
                coin,
                'spot')]=newgreeks

    ## add a sum column
    greeks.sort_index(axis=1, level=[0, 1, 3, 5], ascending=[True, True, True, True],inplace=True)
    greeks[('sum',
            None,
            None,
            None,
            None,
            None)] = greeks.sum(axis=1)
    return greeks

async def live_risk_wrapper(exchange_name='ftx',subaccount='SysPerp'):
    exchange = await open_exchange(exchange_name,subaccount)

    # contruct markets_by_id
    futures = pd.DataFrame(await fetch_futures(exchange,includeIndex=True)).set_index('name')
    risk = await live_risk(exchange,futures)
    await exchange.close()
    return risk

async def live_risk(exchange,futures):

    all_balances = pd.DataFrame((await exchange.fetch_balance(params={}))['info']['result'],dtype=float)#'showAvgPrice':True})
    #balances = all_balances[(all_balances['total']!=0.0)&(all_balances['coin']!='USD')].fillna(0.0)
    balances = all_balances[abs(all_balances['usdValue']) > 1].fillna(0.0)
    balances['spotDelta'] = balances.apply(lambda f: f['total'] * (1.0 if f['coin'] == 'USD' else float(exchange.market(f['coin']+'/USD')['info']['price'])), axis=1)

    def borrow(balance):
        if balance['coin'] != "USD":
            s = float(exchange.market(balance['coin']+'/USD')['info']['price'])
            N = min(0,balance['total'])
            r = float(futures.loc[balance['coin']+'-PERP', 'cash_borrow'] if balance['coin'] != 'USD' else futures.iloc[0]['quote_borrow'])
            return N * s * r
        else:
            return 0
    balances['borrow'] = balances.apply(lambda f: borrow(f), axis=1)

    positions = pd.DataFrame([r['info'] for r in await exchange.fetch_positions(params={}) if r['info']['netSize']!=0],dtype=float)#'showAvgPrice':True})

    if not positions.empty:
        positions = positions[positions['netSize'] != 0.0].fillna(0.0)
        positions['coin'] = positions['future'].apply(lambda f: futures.loc[f,'underlying'])
        positions['futureMark'] = positions.apply(lambda f: futures.loc[f['future'],'mark'], axis=1)
        positions['futureIndex'] = positions.apply(lambda f: futures.loc[f['future'],'index'], axis=1)
        positions['futureDelta'] = positions['netSize'] * positions['futureMark']

        positions = positions.sort_values(by=['coin'], ascending=True)

        def future_carry(position):
            s = position['futureIndex']
            N = position['netSize']
            r = float(futures.loc[position['future'], 'future_carry'])
            return - N * s * r
        positions['future_carry'] = positions.apply(lambda f: future_carry(f),axis=1)

        def IR01(position): # per percent, not bps
            f = position['futureMark']
            N = position['netSize']
            days_diff = futures.loc[position['future'], 'expiryTime'] - datetime.now(tz=timezone.utc)
            t = days_diff.days / 365.25
            return N * f * t / 100
        positions['IR01'] = positions.apply(lambda f: IR01(f),axis=1)

        result = balances.merge(positions, how='outer', on='coin').fillna(0.0)

    else:
        result=balances
        result[['futureDelta','futureMark','futureIndex','future_carry','IR01']]=0

    # Find the dollar index
    usd_index = result[result['coin']=='USD'].index[0]
    # Create pv column = spot delta
    result['pv'] = result['spotDelta']

    result['netDelta'] = result['futureDelta'] + result['spotDelta'] #- result[result['netDelta']]
    # Put spotDelta and netDelta at 0 for USD
    result.at[usd_index, 'spotDelta'] = 0
    result.at[usd_index, 'netDelta'] = 0

    result['netCarry'] = result['future_carry'] + result['borrow']
    result['spotMark'] = balances['coin'].apply(lambda f: 1.0 if f=='USD' else float(exchange.market(f+'/USD')['info']['price']))

    # Move dollar to the last line
    idx = result.index.tolist()
    idx.pop(usd_index)
    result = result.reindex(idx + [usd_index])

    # Sort by coin before print
    result = result.sort_values(by=['coin'], ascending=True)
    result.loc['total', ['futureDelta', 'spotDelta', 'netDelta', 'future_carry', 'borrow', 'netCarry', 'IR01', 'pv']] = result[['futureDelta', 'spotDelta', 'netDelta', 'future_carry', 'borrow', 'netCarry', 'IR01', 'pv']].sum()
    result.loc['total', 'coin'] = 'total'

    return result[['coin','futureDelta', 'spotDelta', 'netDelta', 'future_carry', 'borrow', 'netCarry', 'IR01', 'pv']].set_index('coin')

# diff is in coin
async def diff_portoflio(exchange,future_weights) -> pd.DataFrame():
    ''' Compares current risk vs optimal risk '''
    future_weights = future_weights[future_weights['name'].isin(['USD','total'])==False]
    future_weights['optimalUSD'] = -future_weights['optimalWeight']

    cash_weights = future_weights.copy()
    cash_weights['name']=cash_weights['name'].apply(lambda x: exchange.market(x)['base']+'/USD')
    cash_weights['optimalUSD'] *= -1
    target = future_weights.append(cash_weights)

    async def fetch_balances_positions(exchange: ccxt.Exchange) -> pd.DataFrame:
        positions = pd.DataFrame([r['info'] for r in await exchange.fetch_positions(params={})]).rename(
            columns={'future': 'name', 'netSize': 'total'})  # 'showAvgPrice':True})

        balances = pd.DataFrame((await exchange.fetch_balance(params={}))['info']['result'])  # 'showAvgPrice':True})
        balances = balances[balances['coin'] != 'USD']
        balances['name'] = balances['coin'].apply(lambda c: c+'/USD')

        current = positions.append(balances)[['name', 'total']]
        current['total'] = current['total'].astype(float)
        current = current[current['total'] != 0]

        return current

    # get portfolio in coin
    current=await fetch_balances_positions(exchange)

    # join, diff, coin
    result = target.set_index('name')[['optimalUSD']].join(current.set_index('name')[['total']],how='outer')
    result=result.fillna(0.0).reset_index().rename(columns={'total':'currentCoin'})

    result['name'] = result['name'].apply(lambda x: x.replace('_LOCKED', ''))
    result['coin'] = result['name'].apply(lambda x: exchange.market(x)['base'])
    #we ignore the basis for scaling the perps. Too volatile. If spot absent use perp.
    result['spot_price']=result['coin'].apply(lambda x: float(exchange.market(x+('/USD' if (x+'/USD') in exchange.markets else '-PERP'))['info']['price']))
    result['optimalCoin'] = result['optimalUSD'] / result['spot_price']
    result['currentUSD'] = result['currentCoin'] * result['spot_price']
    result['minProvideSize'] = result['name'].apply(lambda f: float(exchange.market(f)['info']['minProvideSize']))
    result['diffCoin']= result.apply(lambda f: float(exchange.amount_to_precision(exchange.market(f['name'])['symbol'],f['optimalCoin']-f['currentCoin'])),axis=1)
    result['diffUSD'] = result['diffCoin']*result['spot_price']

    result = result[np.abs(result['diffCoin'])>0]
    return result

async def diff_portoflio_wrapper(*argv):
    exchange= await open_exchange(*argv)
    await exchange.load_markets()
    future_weights = pd.read_csv('~/config/pfoptimizer/current_weights.csv')
    diff = await diff_portoflio(exchange,future_weights)
    await exchange.close()
    return diff

async def fetch_portfolio(exchange,time):
    # fetch mark,spot and balances as closely as possible

    # shoot rest requests
    n_requests = int(safe_gather_limit / 3)
    p = [getattr(exchange, coro)(params={'dummy': i})
         for i in range(n_requests)
         for coro in ['fetch_markets', 'fetch_balance', 'fetch_positions']]
    results = await safe_gather(p)


    # avg to reduce impact of latency
    markets_list = []
    for result in results[0::3]:
        res = pd.DataFrame(result).set_index('id')
        res['price'] = res['info'].apply(lambda f: float(f['price']) if f['price'] else -9999999)
        markets_list.append(res[['price']])
    markets = sum(markets_list) / len(markets_list)

    balances_list = [pd.DataFrame(r['info']['result']).set_index('coin')[['total']].astype(float) for r in
                     results[1::3]]
    balances = sum(balances_list) / len(balances_list)
    var = sum([bal * bal for bal in balances_list]) / len(balances_list) - balances * balances
    balances.reset_index(inplace=True)

    positions_list = [
        pd.DataFrame([r['info'] for r in result]).set_index('future')[['netSize', 'unrealizedPnl']].astype(float) for
        result in results[2::3]]
    positions = sum(positions_list) / len(positions_list)
    var = sum([pos * pos for pos in positions_list]) / len(positions_list) - positions * positions
    positions.reset_index(inplace=True)

    # go ahead
    futures = pd.DataFrame(await fetch_futures(exchange, includeExpired=True, includeIndex=True))
    futures = futures.set_index('name')
    if not positions.empty:
        positions = positions[positions['netSize'] != 0.0].fillna(0.0)
        unrealizedPnL= positions['unrealizedPnl'].sum()
        positions['coin'] = 'USD'
        positions['coinAmt'] = positions['netSize']
        positions['time']=time.replace(tzinfo=None)
        positions['event_type'] = 'delta'
        positions['attribution'] = positions['future']
        positions['mark'] = positions['attribution'].apply(lambda f: markets.loc[f,'price'])
        positions['spot'] = positions['attribution'].apply(lambda f:
                                                           float(exchange.market((futures.loc[f,'underlying']+'/USD') if (futures.loc[f,'underlying']+'/USD') in exchange.markets
                                                                                 else f)['info']['price']))
        positions['usdAmt'] = positions['coinAmt'] * positions['mark']

        positions.loc[~positions['attribution'].isin(futures.index),'rate'] = 0.0

        positions.loc[positions['attribution'].isin(futures[futures['type'] == 'perpetual'].index),'rate'] = \
        positions.loc[positions['attribution'].isin(futures[futures['type'] == 'perpetual'].index),'mark']/ \
        positions.loc[positions['attribution'].isin(futures[futures['type'] == 'perpetual'].index),'spot']-1.0

        positions.loc[positions['attribution'].isin(futures[futures['type']=='future'].index),'additional']= \
        positions.loc[positions['attribution'].isin(futures[futures['type']=='future'].index)].apply(lambda f:
                    calc_basis(f['mark'], f['spot'],futures.loc[f['future'],'expiryTime'], time)
                                                           ,axis=1)
    else: unrealizedPnL=0.0

    balances = balances[balances['total'] != 0.0].fillna(0.0)
    balances['coinAmt'] =balances['total']
    balances.loc[balances['coin']=='USD','coinAmt'] += unrealizedPnL
    balances['time']=time.replace(tzinfo=None)
    balances['event_type'] = 'delta'
    balances['coin'] = balances['coin'].apply(lambda f:f.replace('_LOCKED', ''))
    balances['attribution'] = balances['coin']
    balances['spot'] = balances['coin'].apply(lambda f: 1.0 if f == 'USD' else float(markets.loc[f + '/USD', 'price']))
    balances['mark'] = balances['spot']
    balances['usdAmt'] = balances['coinAmt'] * balances['mark']
    balances['additional'] = unrealizedPnL

    PV = pd.DataFrame(index=['total'],columns=['time','coin','coinAmt','event_type','attribution','spot','mark'])
    PV.loc['total','time'] = time.replace(tzinfo=None)
    PV.loc['total','coin'] = 'USD'
    PV.loc['total','coinAmt'] = (balances['coinAmt'] * balances['mark']).sum() + unrealizedPnL
    PV.loc['total','event_type'] = 'PV'
    PV.loc['total','attribution'] = 'USD'
    PV.loc['total','spot'] = 1.0
    PV.loc['total','mark'] = 1.0
    PV.loc['total', 'usdAmt'] = PV.loc['total','coinAmt']

    IM = pd.DataFrame(index=['total'],
                      columns=['time','coin','coinAmt','event_type','attribution','spot','mark'])
    IM.loc['total','time'] = time.replace(tzinfo=None)
    IM.loc['total','coin'] = 'USD'
    account_data = pd.DataFrame((await exchange.privateGetAccount())['result'])[['marginFraction', 'totalPositionSize', 'initialMarginRequirement']]
    if not account_data.empty:
        account_data=account_data.iloc[0].astype(float)
        IM.loc['total','coinAmt'] = PV.loc['total','coinAmt']- account_data['marginFraction']*account_data['totalPositionSize'] +account_data['initialMarginRequirement']*account_data['totalPositionSize']
    else:   IM.loc['total','coinAmt'] = 0
    IM.loc['total','event_type'] = 'IM'
    IM.loc['total','attribution'] = 'USD'
    IM.loc['total','spot'] = 1.0
    IM.loc['total','mark'] = 1.0
    IM.loc['total', 'usdAmt'] = IM.loc['total','coinAmt']

    return pd.concat([
        balances[['time','coin','coinAmt','event_type','attribution','spot','mark', 'usdAmt','additional']],#TODO: rate to be removed
        positions[['time','coin','coinAmt','event_type','attribution','spot','mark', 'usdAmt','additional']] if not positions.empty else pd.DataFrame(),
        PV[['time','coin','coinAmt','event_type','attribution','spot','mark', 'usdAmt']],
        IM[['time','coin','coinAmt','event_type','attribution','spot','mark', 'usdAmt']]
    ],axis=0,ignore_index=True)

async def compute_plex(exchange,start,end,start_portfolio,end_portfolio):
    futures = pd.DataFrame(await fetch_futures(exchange, includeExpired=True, includeIndex=True)).set_index('name')
    start_time = start.timestamp()
    end_time = end.timestamp()
    params={'start_time':start_time,'end_time':end_time}

    cash_flows=pd.DataFrame(columns=['time','coin','USDamt','event_type','attribution'])

    deposits= pd.DataFrame((await exchange.privateGetWalletDeposits(params))['result'],dtype=float)
    if not deposits.empty:
        deposits=deposits[deposits['status']=='complete']
        deposits['USDamt']=deposits['size']
        deposits['attribution'] = deposits['coin']
        deposits['event_type'] = 'deposit_'+deposits['status']
        cash_flows=cash_flows.append(deposits[['time','coin','USDamt','event_type','attribution']],ignore_index=True)
    withdrawals = pd.DataFrame((await exchange.privateGetWalletWithdrawals(params))['result'],dtype=float)
    if not withdrawals.empty:
        withdrawals = withdrawals[withdrawals['status'] == 'complete']
        withdrawals['USDamt'] = -withdrawals['size']
        withdrawals['attribution'] = withdrawals['coin']
        withdrawals['event_type'] = 'withdrawal_'+withdrawals['status']
        cash_flows=cash_flows.append(withdrawals[['time','coin','USDamt','event_type','attribution']],ignore_index=True)

    funding = pd.DataFrame((await exchange.privateGetFundingPayments(params))['result'], dtype=float)
    if not funding.empty:
        funding['coin'] = 'USD'
        funding['USDamt'] = -funding['payment']
        funding['attribution'] = funding['future']
        funding['event_type'] = 'funding'
        cash_flows = cash_flows.append(funding[['time', 'coin', 'USDamt', 'event_type','attribution']], ignore_index=True)

    borrow= pd.DataFrame((await exchange.privateGetSpotMarginBorrowHistory(params))['result'],dtype=float)
    if not borrow.empty:
        borrow['USDamt']=-borrow['cost']
        borrow['attribution'] = borrow['coin']
        borrow['event_type'] = 'borrow'
        cash_flows=cash_flows.append(borrow[['time','coin','USDamt','event_type','attribution']],ignore_index=True)
    lending = pd.DataFrame((await exchange.privateGetSpotMarginLendingHistory(params))['result'],dtype=float)
    if not lending.empty:
        lending['USDamt'] = lending['proceeds']
        lending['attribution'] = lending['coin']
        lending['event_type'] = 'lending'
        cash_flows=cash_flows.append(lending[['time','coin','USDamt','event_type','attribution']],ignore_index=True)

    airdrops = pd.DataFrame((await exchange.privateGetWalletAirdrops(params))['result'],dtype=float)
    if not airdrops.empty:
        airdrops = airdrops[airdrops['status'] == 'complete']
        airdrops['USDamt'] = airdrops['size']
        airdrops['attribution'] = airdrops['coin']
        airdrops['event_type'] = 'airdrops'+'_'+airdrops['status']
        cash_flows=cash_flows.append(airdrops[['time','coin','USDamt','event_type','attribution']],ignore_index=True)

    staking = pd.DataFrame((await exchange.privateGetStakingStakingRewards(params))['result'],dtype=float)
    if not staking.empty:
        staking['event_type'] = 'staking'
        staking['USDamt'] = staking['size']
        staking['attribution']=staking['coin']
        cash_flows=cash_flows.append(staking[['time','coin','USDamt','event_type','attribution']],ignore_index=True)

    trades = pd.DataFrame((await exchange.privateGetFills(params))['result'], dtype=float)
    future_trades=pd.DataFrame()
    if not trades.empty:
        trades['baseCurrency'] = trades['baseCurrency'].apply(lambda s: s.replace('_LOCKED', '') if s else s)
        trades['feeCurrency'] = trades['feeCurrency'].apply(lambda s: s.replace('_LOCKED', '') if s else s)
        #trades=trades[trades['type']=='order'] # TODO: dealt with unlock and otc later
        # spot trade first, attribute to baseccy
        base_trades=trades[~trades['future'].isin(futures.index)]
        base_trades['coin'] = base_trades['baseCurrency']
        base_trades['attribution'] = base_trades['baseCurrency']
        base_trades['USDamt'] = base_trades['size'] * base_trades['side'].apply(lambda side: 1 if side == 'buy' else -1)
        base_trades['event_type'] ='spot_trade'
        cash_flows = cash_flows.append(base_trades[['time', 'coin', 'USDamt', 'event_type', 'attribution']],
                                       ignore_index=True)

        # tx Fees
        txFees = base_trades[base_trades['fee'] > 0.00000000001]
        if not txFees.empty:
            txFees['coin'] = txFees['feeCurrency']
            txFees['attribution'] = txFees['attribution']
            txFees['USDamt'] = -txFees['fee']
            txFees['event_type'] = 'txFee'
            cash_flows = cash_flows.append(txFees[['time', 'coin', 'USDamt', 'event_type', 'attribution']],
                                       ignore_index=True)

        quote_trades = trades[~trades['future'].isin(futures.index)]
        quote_trades['coin'] = base_trades['quoteCurrency']
        quote_trades['attribution'] = base_trades['baseCurrency'] # yes, it's base not quote!
        quote_trades['USDamt'] = - quote_trades['size'] * quote_trades['side'].apply(lambda side: 1 if side == 'buy' else -1) * quote_trades['price']
        quote_trades['event_type'] ='spot_trade'
        cash_flows = cash_flows.append(quote_trades[['time', 'coin', 'USDamt', 'event_type', 'attribution']],
                                       ignore_index=True)

        # tx Fees
        txFees = quote_trades[quote_trades['fee'] > 0.00000000001]
        if not txFees.empty:
            txFees['coin'] = txFees['feeCurrency']
            txFees['attribution'] = txFees['attribution']
            txFees['USDamt'] = -txFees['fee']
            txFees['event_type'] = 'txFee'
            cash_flows = cash_flows.append(txFees[['time', 'coin', 'USDamt', 'event_type', 'attribution']],
                                           ignore_index=True)

        # futures more tricky
        future_trades=trades[trades['future'].isin(futures.index)]
        if not future_trades.empty:
            future_trades['coin'] = 'USD'
            future_trades['attribution'] = future_trades['future']
            future_trades['USDamt'] = 0 # when spot available
            future_trades['event_type'] = 'future_trade'
            cash_flows=cash_flows.append(future_trades[['time','coin','USDamt','event_type','attribution']],ignore_index=True)

            # tx Fees
            txFees = future_trades[future_trades['fee']>0.00000000001]
            if not txFees.empty:
                txFees['coin'] = txFees['feeCurrency']
                txFees['attribution'] = txFees['future']
                txFees['USDamt'] = -txFees['fee']
                txFees['event_type'] = 'txFee'
                cash_flows = cash_flows.append(txFees[['time', 'coin', 'USDamt', 'event_type','attribution']], ignore_index=True)

    # now fetch EOD.
    async def fetch_EOD(symbol_list,point_in_time,portfolio):
        # read what you can
        result = portfolio.loc[portfolio['attribution'].isin(symbol_list), ['attribution', 'spot', 'mark']]

        # fetch the rest
        for f in set(symbol_list)-set(result['attribution']):
            if type(f)!=str: continue
            if f in futures.index:
                spot_ticker = futures.loc[f.replace('_LOCKED',''), 'underlying'] + '/USD'
                mark_ticker = futures.loc[futures['symbol'] == f, 'symbol'].values[0]
                # some have no spot
                spot_ticker = spot_ticker if spot_ticker in exchange.markets else mark_ticker
            else:
                spot_ticker = f.replace('_LOCKED', '')
                if '/USD' not in f: spot_ticker += '/USD'
                mark_ticker = spot_ticker

            params={'start_time':point_in_time.timestamp(), 'end_time':point_in_time.timestamp()+15}
            result=result.append(pd.Series(
                {'attribution': f.replace('_LOCKED', ''),
                'spot':(await exchange.fetch_ohlcv(spot_ticker, timeframe='15s', params=params))[0][1],
                'mark':(await exchange.fetch_ohlcv(mark_ticker, timeframe='15s', params=params))[0][1]
                 }),ignore_index=True)
            logging.info('had to snap '+f)

        result.set_index('attribution',inplace=True)
        return result[~result.index.duplicated()]

    end_of_day = await fetch_EOD(cash_flows['attribution'].append(start_portfolio['attribution']).unique(),end,end_portfolio)

    # rescale inflows, or recompute if needed
    cash_flows['USDamt'] *= cash_flows['coin'].apply(lambda f: end_of_day.loc[f,'spot'])
    cash_flows['end_mark'] = cash_flows['attribution'].apply(lambda f: end_of_day.loc[f,'mark'])
    cash_flows['start_time']=cash_flows['time']

    if not future_trades.empty:# TODO: below code relies on trades ordering being preserved to this point :(
        cash_flows.loc[cash_flows['event_type']=='future_trade','USDamt']=(\
            future_trades['size'] * future_trades['side'].apply(lambda side: 1 if side == 'buy' else -1)*\
            (end_of_day.loc[future_trades['future'],'mark'].values-future_trades['price'])).values
    if not start_portfolio.empty:
        start_of_day = await fetch_EOD(start_portfolio['attribution'].unique(), start,start_portfolio)

        mkt_risk = start_portfolio.copy()
        mkt_risk['end_spot'] = mkt_risk['attribution'].apply(lambda f: end_of_day.loc[f, 'spot'])
        mkt_risk['start_spot'] = mkt_risk['attribution'].apply(lambda f: start_of_day.loc[f, 'spot'])
        mkt_risk['end_mark'] = mkt_risk['attribution'].apply(lambda f: end_of_day.loc[f, 'mark' if f in futures.index else 'spot'])
        mkt_risk['start_mark'] = mkt_risk['attribution'].apply(lambda f: start_of_day.loc[f,'mark' if f in futures.index else 'spot'])
        mkt_risk['start_time']=start
        mkt_risk['time']=end

        # spots have delta
        spots = mkt_risk[~mkt_risk['attribution'].isin(futures.index)]
        if not spots.empty:
            spots['delta_pnl'] = spots['coinAmt'] * (spots['end_spot'] - spots['start_spot'])
            spots['USDamt'] = spots['delta_pnl']
            spots['event_type']='delta'
            spots=spots[spots['USDamt']!=0.0]
            cash_flows = cash_flows.append(spots[['start_time','time','coin','USDamt','event_type','attribution','end_mark']], ignore_index=True)

        # perps also have premium -> IR01
        perp_portfolio = mkt_risk[mkt_risk['attribution'].isin(futures[futures['type'] == 'perpetual'].index)]
        if not perp_portfolio.empty:
            perp_portfolio['delta_pnl'] = perp_portfolio['coinAmt'] * (perp_portfolio['end_spot'] - perp_portfolio['start_spot'])
            perp_portfolio['USDamt'] = perp_portfolio['delta_pnl']
            perp_portfolio['event_type'] = 'delta'
            cash_flows = cash_flows.append(perp_portfolio[['start_time','time', 'coin', 'USDamt', 'event_type', 'attribution', 'end_mark']],
                                           ignore_index=True)

            perp_portfolio['premium_pnl'] = perp_portfolio['coinAmt'] * (perp_portfolio['end_mark'] - perp_portfolio['start_mark']) - perp_portfolio['delta_pnl']
            perp_portfolio['USDamt'] = perp_portfolio['premium_pnl']
            perp_portfolio['event_type'] = 'IR01'
            cash_flows = cash_flows.append(perp_portfolio[['start_time','time', 'coin', 'USDamt', 'event_type', 'attribution', 'end_mark']],ignore_index=True)

        # futures also rolldown, and IR01 = totalpnl-mkt_risk-rolldown
        future_portfolio = mkt_risk[mkt_risk['attribution'].isin(futures[futures['type'] == 'future'].index)]
        if not future_portfolio.empty:
            future_portfolio['avg_mark']=(future_portfolio['start_mark']+future_portfolio['end_mark'])/2
            future_portfolio['T'] = futures.loc[future_portfolio['attribution'],'expiryTime'].values
            future_portfolio['avg_t'] = future_portfolio['T'].apply(lambda t: start+timedelta(seconds=(min([t,end])-start).total_seconds()/2))
            future_portfolio['start_rate'] = future_portfolio.apply(lambda f:
                        calc_basis(f['start_mark'],f['start_spot'],f['T'],start)
                                                                   ,axis=1)
            future_portfolio['end_rate'] = future_portfolio.apply(lambda f:
                        calc_basis(f['end_mark'],f['end_spot'],f['T'],end)
                                                                   ,axis=1)
            dt=(end-start).total_seconds()/365.25/24/3600
            future_portfolio['rolldown'] = - future_portfolio['coinAmt']*future_portfolio['avg_mark']* \
                                           (future_portfolio['end_rate']+future_portfolio['start_rate'])*dt/2

            future_portfolio['USDamt']=future_portfolio['rolldown']
            future_portfolio['event_type']='funding'
            cash_flows = cash_flows.append(future_portfolio[['start_time','time','coin','USDamt','event_type','attribution','end_mark']], ignore_index=True)

            future_portfolio['delta_pnl'] = future_portfolio['coinAmt'] * (future_portfolio['end_spot'] / future_portfolio['start_spot'] - 1.0) * future_portfolio['start_mark']
            future_portfolio['USDamt'] = future_portfolio['delta_pnl']
            future_portfolio['event_type'] = 'delta'
            cash_flows = cash_flows.append(future_portfolio[['start_time','time', 'coin', 'USDamt', 'event_type', 'attribution', 'end_mark']],
                                           ignore_index=True)

            future_portfolio['IR01'] = future_portfolio['coinAmt']*future_portfolio['avg_mark']* \
                                       (future_portfolio['end_rate']-future_portfolio['start_rate'])* \
                                       (future_portfolio['T']-future_portfolio['avg_t']).apply(lambda t: t.total_seconds())/365.25/24/3600
            future_portfolio['USDamt'] = future_portfolio['IR01']
            future_portfolio['event_type'] = 'IR01'
            cash_flows = cash_flows.append(
                future_portfolio[['start_time','time', 'coin', 'USDamt', 'event_type', 'attribution', 'end_mark']],
                ignore_index=True)

            future_portfolio['FX-IR_gamma'] = future_portfolio['coinAmt'] * (future_portfolio['end_mark'] - future_portfolio['start_mark']) \
                        - future_portfolio['delta_pnl'] \
                        - future_portfolio['rolldown'] \
                        - future_portfolio['IR01']
            future_portfolio['USDamt']=future_portfolio['FX-IR_gamma']
            future_portfolio['event_type']='cross_effect' # includes xgamma
            cash_flows = cash_flows.append(future_portfolio[['start_time','time','coin','USDamt','event_type','attribution','end_mark']], ignore_index=True)

    unexplained=pd.DataFrame()
    unexplained['USDamt']=end_portfolio.loc[end_portfolio['event_type']=='PV','coinAmt'].values - \
                start_portfolio.loc[start_portfolio['event_type']=='PV','coinAmt'].values - \
                cash_flows['USDamt'].sum()
    unexplained['coin'] = 'USD'
    unexplained['attribution'] = 'USD'
    unexplained['event_type'] = 'unexplained'
    unexplained['start_time'] = start
    unexplained['time'] = end
    unexplained['end_mark'] = 1.0
    cash_flows = cash_flows.append(unexplained[['start_time','time', 'coin', 'USDamt', 'event_type', 'attribution', 'end_mark']], ignore_index=True)

    cash_flows['start_time'] = cash_flows['start_time'].apply(lambda t: t if type(t)!=str else dateutil.parser.isoparse(t).replace(tzinfo=None))
    cash_flows['time'] = cash_flows['time'].apply(lambda t: t if type(t)!=str else dateutil.parser.isoparse(t).replace(tzinfo=None))
    cash_flows['underlying'] = cash_flows['attribution'].apply(lambda f:
                            futures.loc[f,'underlying'] if f in futures.index
                            else f)

    return cash_flows.sort_values(by='time',ascending=True)

async def run_plex_wrapper(exchange_name='ftx',subaccount='debug'):
    exchange = await open_exchange(exchange_name,subaccount)
    plex= await run_plex(exchange)
    await exchange.close()
    return plex

async def run_plex(exchange,dirname='/tmp/pnl/'):

    filename = dirname+'portfolio_history_'+exchange.describe()['id']+('_'+exchange.headers['FTX-SUBACCOUNT'] if 'FTX-SUBACCOUNT' in exchange.headers else '')+'.xlsx'
    if not os.path.exists(dirname):
        os.umask(0)
        os.makedirs(dirname, mode=0o777)
    if not os.path.isfile(filename):
        risk_history = pd.DataFrame()
        risk_history = risk_history.append(pd.DataFrame(index=[0], data=dict(
            zip(['time', 'coin', 'coinAmt', 'event_type', 'attribution', 'spot', 'mark'],
                [datetime(2021, 12, 6), 'USD', 0.0, 'delta', 'USD', 1.0, 1.0]))))
        risk_history = risk_history.append(pd.DataFrame(index=[0], data=dict(
            zip(['time', 'coin', 'coinAmt', 'event_type', 'attribution', 'spot', 'mark'],
                [datetime(2021, 12, 6), 'USD', 0.0, 'PV', 'USD', 1.0, 1.0]))))
        pnl_history = pd.DataFrame()
        with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
            risk_history.to_excel(writer, sheet_name='risk')
            pnl_history.to_excel(writer, sheet_name='pnl')

    risk_history = pd.read_excel(filename,sheet_name='risk',index_col=0)
    pnl_history = pd.read_excel(filename, sheet_name='pnl', index_col=0)
    start_time = risk_history['time'].max()
    start_portfolio = risk_history[(risk_history['time']<start_time+timedelta(milliseconds= 1000)) \
                &(risk_history['time']>start_time-timedelta(milliseconds= 1000))] #TODO: precision!

    end_time = datetime.now() - timedelta(seconds=14)  # 16s is to avoid looking into the future to fetch prices
    end_portfolio = await fetch_portfolio(exchange, end_time) # it's live in fact, end_time just there for records

    pnl = await compute_plex(exchange,start=start_time,end=end_time,start_portfolio=start_portfolio,end_portfolio=end_portfolio)#margintest
    pnl.sort_values(by='time',ascending=True,inplace=True)
    summary=pnl[pnl['time']>datetime(2022,6,21,19)].pivot_table(values='USDamt',
                            index='time',
                            columns='event_type',
                            aggfunc='sum',
                            margins=True,
                            fill_value=0.0)

    with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
        risk_history.append(end_portfolio,ignore_index=True).to_excel(writer, sheet_name='risk')
        pnl_history.append(pnl, ignore_index=True).to_excel(writer, sheet_name='pnl')
        summary.to_excel(writer, sheet_name='summary')

    return summary

def ftx_portoflio_main(*argv):
    ''' TODO: improve input params'''
    #pd.set_option('display.float_format',lambda x: '{:,.3f}'.format(x))
    #f'{float(f"{i:.1g}"):g}'

    argv=list(argv)
    if len(argv) == 0:
        argv.extend(['plex'])
    print(f'running {argv}')
    if argv[0] == 'fromoptimal':
        if len(argv) < 3:
            argv.extend(['ftx', 'SysPerp'])

        diff=asyncio.run(diff_portoflio_wrapper(argv[1], argv[2]))
        diff=diff.append(pd.Series({'coin': 'total', 'name': 'total'}).append(diff.sum(numeric_only=True)),ignore_index=True)
        print(diff.loc[diff['diffUSD'].apply(np.abs)>1,['coin','name','currentUSD','optimalUSD','diffUSD']].round(decimals=0))
        return diff

    elif argv[0] == 'risk':
        if len(argv) < 3:
            argv.extend(['ftx', 'SysPerp'])
        while True:
            risk=asyncio.run(live_risk_wrapper(argv[1], argv[2]))
            print(risk.astype(int))
            t.sleep(5)
        return risk

    elif argv[0] == 'plex':
        if len(argv) < 3:
            argv.extend(['ftx', 'SysPerp'])

        plex = asyncio.run(run_plex_wrapper(*argv[1:]))
        print(plex.astype(int))
        return plex

    elif argv[0] == 'log_reader':
        if argv[0] == 'log_reader' and len(argv) < 2:
            argv.extend(['latest'])

        log = summarize_exec_logs(prefix=argv[1])
        return log

    elif argv[0] == 'batch_log_reader':
        log = batch_summarize_exec_logs()
        return log

    else:
        print(f'commands: fromOptimal[ftx, SysPerp]\nrisk[ftx, SysPerp],plex[ftx, SysPerp,timedelta(days=1)],log_reader[latest],batch_log_reader[]')


def main(*args):
    ftx_portoflio_main(*sys.argv[1:])
