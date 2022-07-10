import asyncio
import os
import time as t
from datetime import timedelta

import dateutil.parser
import numpy as np
import pandas as pd

from utils.MyLogger import ExecutionLogger
from utils.config_loader import configLoader

from utils.ftx_utils import *
from utils.MyLogger import *
from utils.api_utils import MyModules,api


async def live_risk_wrapper(exchange,subaccount,nb_runs='1'):
    exchange_obj = await open_exchange(exchange,subaccount)

    # contruct markets_by_id
    futures = pd.DataFrame(await Static.fetch_futures(exchange_obj)).set_index('name')
    for i in range(int(nb_runs)):
        if i>0: t.sleep(30)
        risk = await live_risk(exchange_obj,subaccount,futures)
        logging.getLogger('pnl').info(risk)

    await exchange_obj.close()
    return risk

async def live_risk(exchange,subaccount,futures):
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
async def diff_portoflio(exchange,future_weights=pd.DataFrame()) -> pd.DataFrame():
    ''' Compares current risk vs optimal risk. Only names in future_weights are touched. All if future_weight empty.'''
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
    # get portfolio in coin.
    current = await fetch_balances_positions(exchange)

    if not future_weights.empty:
        future_weights = future_weights[future_weights['name'].isin(['USD', 'total']) == False]
        future_weights['optimalUSD'] = -future_weights['optimalWeight']

        cash_weights = future_weights.copy()
        cash_weights['name'] = cash_weights['name'].apply(lambda x: exchange.market(x)['base'] + '/USD')
        cash_weights['optimalUSD'] *= -1
        target = future_weights.append(cash_weights)
        result = target.set_index('name')[['optimalUSD']].join(current.set_index('name')[['total']], how='left')
    else:
        result = current.set_index('name')[['total']]
        result['optimalUSD'] = 0

    # join, diff, coin. Only names in future_weights are touched.

    result = result.fillna(0.0).reset_index().rename(columns={'total':'currentCoin'})

    result['name'] = result['name'].apply(lambda x: x.replace('_LOCKED', ''))
    result['coin'] = result['name'].apply(lambda x: exchange.market(x)['base'])
    #we ignore the basis for scaling the perps. Too volatile. If spot absent use perp.
    result['spot_price']=result['coin'].apply(lambda x: float(exchange.market(x+('/USD' if (x+'/USD') in exchange.markets else '-PERP'))['info']['price']))
    result['optimalCoin'] = result['optimalUSD'] / result['spot_price']
    result['currentUSD'] = result['currentCoin'] * result['spot_price']
    result['minProvideSize'] = result['name'].apply(lambda f: float(exchange.market(f)['info']['minProvideSize']))
    if result.empty:
        result['diffCoin'] = 0
    else:
        result['diffCoin']= result['optimalCoin']-result['currentCoin']
    result['diffUSD'] = result['diffCoin']*result['spot_price']

    result = result[np.abs(result['diffCoin'])>0]
    return result

async def diff_portoflio_wrapper(*args,**kwargs):
    exchange = await open_exchange(*args)
    await exchange.load_markets()
    filename = os.path.join(os.sep,configLoader.get_config_folder_path(),
                            'pfoptimizer',
                            kwargs['config'] if 'config' in kwargs else '',
                            kwargs['filename'] if 'filename' in kwargs else 'current_weights.csv')
    future_weights = configLoader.get_current_weights(filename)
    diff = await diff_portoflio(exchange,future_weights)
    await exchange.close()
    return diff

async def fetch_portfolio(exchange,time):
    state = await syncronized_state(exchange)

    # go ahead
    futures = pd.DataFrame(await Static.fetch_futures(exchange))
    futures = futures.set_index('name')
    positions = pd.DataFrame(state['positions']).reset_index().rename(columns={'index':'future'})
    markets = pd.DataFrame(state['markets'])
    balances = pd.DataFrame(state['balances']).reset_index().rename(columns={'index':'coin'})
    if not positions.empty:
        unrealizedPnL = positions['unrealizedPnl'].sum()
        positions['coin'] = 'USD'
        positions['coinAmt'] = positions['netSize']
        positions['time'] = time
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


    balances['coinAmt'] =balances['total']
    balances.loc[balances['coin']=='USD','coinAmt'] += unrealizedPnL
    balances['time'] = time
    balances['event_type'] = 'delta'
    balances['coin'] = balances['coin'].apply(lambda f:f.replace('_LOCKED', ''))
    balances['attribution'] = balances['coin']
    balances['spot'] = balances['coin'].apply(lambda f: 1.0 if f == 'USD' else float(markets.loc[f + '/USD', 'price']))
    balances['mark'] = balances['spot']
    balances['usdAmt'] = balances['coinAmt'] * balances['mark']
    balances['additional'] = unrealizedPnL

    PV = pd.DataFrame(index=['total'],columns=['time','coin','coinAmt','event_type','attribution','spot','mark'])
    PV.loc['total','time'] = time
    PV.loc['total','coin'] = 'USD'
    PV.loc['total','coinAmt'] = (balances['coinAmt'] * balances['mark']).sum() + unrealizedPnL
    PV.loc['total','event_type'] = 'PV'
    PV.loc['total','attribution'] = 'USD'
    PV.loc['total','spot'] = 1.0
    PV.loc['total','mark'] = 1.0
    PV.loc['total', 'usdAmt'] = PV.loc['total','coinAmt']

    IM = pd.DataFrame(index=['total'],
                      columns=['time','coin','coinAmt','event_type','attribution','spot','mark'])
    IM.loc['total','time'] = time
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
    futures = pd.DataFrame(await Static.fetch_futures(exchange)).set_index('name')
    start_time = start.timestamp()
    end_time = end.timestamp()
    params={'start_time':start_time,'end_time':end_time}

    # time: paydate, coin:numeraire, USDamt: cn amount first, will be converted at EOD later, event_type:..., attribution: for risk attribution (may be != coin)

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
            coros = await safe_gather([exchange.fetch_ohlcv(ticker, timeframe='15s', params=params) for ticker in [spot_ticker,mark_ticker]])
            result=result.append(pd.Series(
                {'attribution': f.replace('_LOCKED', ''),
                'spot':coros[0][0][1],
                'mark':coros[1][0][1]
                 }),ignore_index=True)
            logging.info('had to snap '+f)

        result.set_index('attribution',inplace=True)
        return result[~result.index.duplicated()]

    end_of_day = await fetch_EOD(cash_flows['attribution'].append(start_portfolio['attribution']).unique(),end,end_portfolio)

    # rescale inflows, or recompute if needed
    cash_flows['USDamt'] *= cash_flows['coin'].apply(lambda f: end_of_day.loc[f,'spot'])
    cash_flows['end_mark'] = cash_flows['attribution'].apply(lambda f: end_of_day.loc[f,'mark'])
    cash_flows['time'] = cash_flows['time'].apply(lambda t:dateutil.parser.parse(t).replace(tzinfo=timezone.utc))

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
    unexplained['USDamt']=end_portfolio.loc[end_portfolio['event_type']=='PV','usdAmt'].values - \
                start_portfolio.loc[start_portfolio['event_type']=='PV','usdAmt'].values - \
                cash_flows['USDamt'].sum()
    unexplained['coin'] = 'USD'
    unexplained['attribution'] = 'USD'
    unexplained['event_type'] = 'unexplained'
    unexplained['start_time'] = start
    unexplained['time'] = end
    unexplained['end_mark'] = 1.0
    cash_flows = cash_flows.append(unexplained[['start_time','time', 'coin', 'USDamt', 'event_type', 'attribution', 'end_mark']], ignore_index=True)

    cash_flows['underlying'] = cash_flows['attribution'].apply(lambda f:
                            futures.loc[f,'underlying'] if f in futures.index
                            else f)

    return cash_flows.sort_values(by='time',ascending=True)

async def risk_and_pnl_wrapper(exchange,subaccount,period='write_all'):
    exchange_obj = await open_exchange(exchange,subaccount)
    plex = await risk_and_pnl(exchange_obj,period)
    await exchange_obj.close()
    return plex

async def risk_and_pnl(exchange,period):
    end_time = datetime.utcnow().replace(tzinfo=timezone.utc)
    dirname = os.path.join(os.sep,'tmp','pnl')

    # initialize directory and previous_risk if needed
    if not os.path.exists(dirname):
        os.umask(0)
        os.makedirs(dirname, mode=0o777)

    risk_filename = os.path.join(os.sep,dirname,'all_risk.csv')
    if not os.path.isfile(risk_filename):
        previous_risk = pd.DataFrame()
        previous_risk = previous_risk.append(pd.DataFrame(index=[0], data=dict(
            zip(['time', 'coin', 'coinAmt', 'event_type', 'attribution', 'spot', 'mark'],
                [end_time - timedelta(hours=1), 'USD', 0.0, 'delta', 'USD', 1.0, 1.0]))))
        previous_risk = previous_risk.append(pd.DataFrame(index=[0], data=dict(
            zip(['time', 'coin', 'coinAmt', 'event_type', 'attribution', 'spot', 'mark'],
                [end_time - timedelta(hours=1), 'USD', 0.0, 'PV', 'USD', 1.0, 1.0]))))
        previous_risk.to_csv(risk_filename)

    # need to retrieve previous risk / marks
    previous_risk = pd.read_csv(risk_filename,index_col=0)
    previous_risk['time'] = previous_risk['time'].apply(lambda date: pd.to_datetime(date).replace(tzinfo=timezone.utc))
    start_time = previous_risk['time'].max()
    start_portfolio = previous_risk[(previous_risk['time']<start_time+timedelta(milliseconds= 1000)) \
                &(previous_risk['time']>start_time-timedelta(milliseconds= 1000))] #TODO: precision!

    # calculate risk
    # 14s is to avoid looking into the future to fetch prices
    end_portfolio = await fetch_portfolio(exchange, end_time - timedelta(seconds=14)) # it's live in fact, end_time just there for records
    # calculate pnl
    pnl = await compute_plex(exchange,start=start_time,end=end_time,start_portfolio=start_portfolio,end_portfolio=end_portfolio)
    pnl.sort_values(by='time',ascending=True,inplace=True)

    # return pnl period is specified
    if period != 'write_all':
        period = parse_time_param(period)
        result = pnl[pnl['time']>end_time-period]
        logging.getLogger('pnl').info(result)
        return result
    else:
        # accrue and archive risk otherwise
        end_portfolio.to_csv(os.path.join(dirname,end_time.strftime("%Y%m%d_%H%M%S") + '_risk.json'))
        pnl.to_csv(os.path.join(dirname, end_time.strftime("%Y%m%d_%H%M%S") + '_pnl.json'))

        all_risk = pd.concat([previous_risk,end_portfolio],axis=0)
        all_risk.to_csv(risk_filename)

        pnl_filename = os.path.join(os.sep, dirname, 'all_pnl.csv')
        if os.path.isfile(pnl_filename):
            all_pnl = pd.concat([pd.read_csv(pnl_filename,index_col=0),pnl],axis=0)
            #all_pnl['time'] = all_pnl['time'].apply(lambda t: pd.to_datetime(t).replace(tzinfo=timezone.utc))
            #all_pnl['start_time'] = all_pnl['start_time'].apply(lambda t: pd.to_datetime(t).replace(tzinfo=timezone.utc))
            all_pnl.to_csv(pnl_filename)
        else:
            pnl.to_csv(pnl_filename)


@api
def main(*args,**kwargs):
    '''
        examples:
            riskpnl risk ftx debug nb_runs=10
            riskpnl plex ftx debug period=2d
            riskpnl fromoptimal ftx debug
        args:
            run_type = ["risk", "plex", "batch_summarize_exec_logs", "fromoptimal"]
            exchange = ["ftx"]
            subaccount = any
        kwargs:
            nb_runs = integer (optionnal for risk, default=1)
            period = nb days (optional for run_type="basis", default="all" )
            dirname = str (optional for run_type="batch_summarize_exec_logs", default=/tmp/tradexecutor)
            filename = (optional for fromoptimal, default=current_weights.csv)
            config = /home/david/config/pfoptimizer_params.json (optionnal for fromoptimal)
   '''
    logger = kwargs.pop('__logger')

    if args[0] == 'fromoptimal':
        diff=asyncio.run(diff_portoflio_wrapper(*args[1:],**kwargs))
        diff=diff.append(pd.Series({'coin': 'total', 'name': 'total'}).append(diff.sum(numeric_only=True)),ignore_index=True)
        return diff.loc[diff['diffUSD'].apply(np.abs)>1,['coin','name','currentUSD','optimalUSD','diffUSD']].round(decimals=0)

    elif args[0] == 'risk':
        risk = asyncio.run(live_risk_wrapper(*args[1:],**kwargs))
        return risk

    elif args[0] == 'plex':
        plex = asyncio.run(risk_and_pnl_wrapper(*args[1:],**kwargs))
        return plex.pivot_table(index=['underlying','attribution'],columns='event_type',values='USDamt',aggfunc='sum',margins=True)

    elif args[0] == 'batch_summarize_exec_logs':
        log = ExecutionLogger.batch_summarize_exec_logs(*args[1:],**kwargs)
        return log
    else:
        logger.critical(f'commands: fromoptimal[exchange, subaccount]\nrisk[exchange, subaccount,nb_runs=1],plex[exchange, subaccount,period=write_all],batch_summarize_exec_logs[exchange=ftx, subaccount= ,dirname=/tmp/tradeexecutor]')
