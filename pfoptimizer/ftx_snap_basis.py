finite_diff_rel_step = 1e-4

from ftx_portfolio import BasisMarginCalculator
from ftx_history import *
from portfolio_optimizer_utils import *

def market_capacity(futures, hy_history, universe_filter_window=[]):
    if len(universe_filter_window) == 0:
        universe_filter_window = hy_history.index
    borrow_decile=0.5
    futures['borrow_volume_decile'] =0
    futures.loc[futures['spotMargin']!=False,'borrow_volume_decile'] = futures[futures['spotMargin']!=False].apply(lambda f:
                                                                                                                   (hy_history.loc[universe_filter_window,f['underlying']+'/rate/size']).quantile(q=borrow_decile)
                                                                                                                   ,axis=1)
    futures['spot_volume_avg'] = futures.apply(lambda f:
                                               (hy_history.loc[universe_filter_window,f['underlying'] + '/price/volume'] ).mean()
                                               ,axis=1)
    futures['future_volume_avg'] = futures.apply(lambda f:
                                                 (hy_history.loc[universe_filter_window,f.name + '/mark/volume']).mean()
                                                 ,axis=1)
    # only consider borrow if short
    futures['concentration_limit_long']=futures.apply(lambda f:
                                                      min([f['openInterestUsd'],f['spot_volume_avg']/24,f['future_volume_avg']/24])
                                                      ,axis=1)
    futures['concentration_limit_short'] = futures.apply(lambda f:
                                                         min([0 if f['spotMargin']==False else f['borrow_volume_decile'],
                                                              f['openInterestUsd'],f['spot_volume_avg'] / 24, f['future_volume_avg'] / 24])
                                                         , axis=1)
    futures['carry_native_spot'] = futures['carry_mid']*futures.apply(
        lambda f: f['concentration_limit_long'],# if futures['direction']==1 else f['concentration_limit_short'],
        axis=1)
    futures['carry_external_spot'] = futures['carry_mid'] * futures.apply(
        lambda f: min([f['openInterestUsd'],f['future_volume_avg']/24]),
        axis=1)

    return futures

# adds info, transcation costs, and basic screening
async def enricher(exchange,futures,holding_period,equity,
                   slippage_override= -999, slippage_orderbook_depth= 0,
                   slippage_scaler= 1.0, params={'override_slippage': True,'type_allowed':['perpetual'],'fee_mode':'retail'}):
    markets = await exchange.fetch_markets()
    otc_file = pd.read_excel('Runtime/configs/static_params.xlsx',sheet_name='used').set_index('coin')

    # basic screening
    futures = futures[
        (futures['expired'] == False) & (futures['enabled'] == True) & (futures['type'] != "move")
        & (futures.apply(lambda f: float(find_spot_ticker(markets, f, 'ask')), axis=1) > 0.0)
        & (futures['tokenizedEquity'] != True)
        & (futures['type'].isin(params['type_allowed'])==True)]

    ########### add borrows
    coin_details = pd.DataFrame((await exchange.publicGetWalletCoins())['result'], dtype=float).set_index('id')
    borrows = await fetch_coin_details(exchange)
    futures = pd.merge(futures, borrows[['borrow', 'lend', 'borrow_open_interest']], how='left', left_on='underlying',
                       right_index=True)
    futures['quote_borrow'] = float(borrows.loc['USD', 'borrow'])
    futures['quote_lend'] = float(borrows.loc['USD', 'lend'])
    ########### naive basis for all futures
    if not futures[futures['type'] == 'perpetual'].empty:
        list = await safe_gather([exchange.publicGetFuturesFutureNameStats({'future_name': f})
            for f in futures[futures['type'] == 'perpetual'].index])
        list = [float(l['result']['nextFundingRate'])*24*365.325 for l in list]
        futures.loc[futures['type'] == 'perpetual','basis_mid'] = list

    if not futures[futures['type'] == 'future'].empty:
        futures.loc[futures['type'] == 'future', 'basis_mid'] = futures[futures['type'] == 'future'].apply(
            lambda f: calc_basis(f['mark'], f['index'], f['expiryTime'], datetime.now()), axis=1)

    #### fill in borrow for spotMargin==False ot OTC override
    futures.loc[futures['spotMargin']==False,'borrow']=999
    futures.loc[futures['spotMargin'] == False, 'lend'] = -999
    futures.loc[futures['spotMargin']=='OTC','borrow']=futures.loc[futures['spotMargin']=='OTC','underlying'].apply(lambda f:otc_file.loc[f,'borrow'])
    futures.loc[futures['spotMargin'] == 'OTC', 'lend'] = futures.loc[
        futures['spotMargin'] == 'OTC', 'underlying'].apply(lambda f: otc_file.loc[f, 'lend'])
    futures.loc[futures['spotMargin'] == 'OTC', 'borrow_open_interest'] = futures.loc[
        futures['spotMargin'] == 'OTC', 'underlying'].apply(lambda f: otc_file.loc[f, 'size'])

    # transaction costs
    costs = await fetch_rate_slippage(futures, exchange, holding_period,
                                    slippage_override, slippage_orderbook_depth, slippage_scaler,
                                    params)
    futures = futures.join(costs, how = 'outer')

    # spot carries
    futures['carryLong']=futures['basis_mid']-futures['quote_borrow']
    futures['carryShort']=futures['basis_mid']-futures['quote_borrow']+futures['borrow']
    futures['direction_mid']=0
    futures['carry_mid'] = 0
    futures.loc[(futures['carryShort']+futures['carryLong']<0)&(futures['carryShort']<0),'direction_mid']=-1
    futures.loc[(futures['carryShort']+futures['carryLong']>0)&(futures['carryLong']>0),'direction_mid']=1
    futures.loc[futures['direction_mid']==-1, 'carry_mid'] = -futures['carryShort']
    futures.loc[futures['direction_mid']==1, 'carry_mid'] = futures['carryLong']

    ##### max weights ---> TODO CHECK formulas, short < long no ??
    future_im = futures.apply(lambda f:
                              (f['imfFactor'] * np.sqrt(equity / f['mark'])).clip(min=1 / f['account_leverage']),
                              axis=1)
    futures['MaxLongWeight'] = 1 / (1.1 + (future_im - futures['collateralWeight']))
    futures['MaxShortWeight'] = -1 / (future_im + 1.1 / futures.apply(lambda f:collateralWeightInitial(f),axis=1) - 1)
    futures.loc[futures['spotMargin']==False,'MaxShortWeight']=0

    return futures.drop(columns=['carryLong','carryShort'])

def enricher_wrapper(exchange_name: str,type: str,depth: int) ->pd.DataFrame():
    async def enricher_subwrapper(exchange_name,type,depth):

        exchange = await open_exchange(exchange_name,'')
        markets = await exchange.fetch_markets()
        futures = pd.DataFrame(await fetch_futures(exchange,includeIndex=False)).set_index('name')
        futures = futures[
            (futures['expired'] == False) & (futures['enabled'] == True) & (futures['type'] != "move")
            & (futures.apply(lambda f: float(find_spot_ticker(markets, f, 'ask')), axis=1) > 0.0)
            & (futures['tokenizedEquity'] != True)]

        filtered = futures[~futures['underlying'].isin(EXCLUSION_LIST)]
        type = ['future','perpetual'] if type == 'all' else [type]
        enriched = await enricher(exchange, filtered, timedelta(weeks=1), equity=1.0,
                                  slippage_override=-999, slippage_orderbook_depth=depth,
                                  slippage_scaler=1.0,
                                  params={'override_slippage': False, 'type_allowed': type, 'fee_mode': 'retail'})
        await build_history(futures, exchange)
        hy_history = await get_history(enriched,start_or_nb_hours=48)
        (intLongCarry, intShortCarry, intUSDborrow, intBorrow, E_long, E_short, E_intUSDborrow, E_intBorrow) = forecast(
            exchange, enriched, hy_history,
            HOLDING_PERIOD, SIGNAL_HORIZON,  # to convert slippage into rate
            filename='Runtime/logs/portfolio_optimizer/history.xlsx')  # historical window for expectations)
        point_in_time = max(hy_history.index)
        updated = update(enriched, point_in_time, hy_history, depth,
                                     intLongCarry, intShortCarry, intUSDborrow, intBorrow, E_long, E_short,
                                     E_intUSDborrow, E_intBorrow)

        data = market_capacity(updated,hy_history).sort_values(by='E_intCarry',key=np.abs,ascending=False)

        await exchange.close()
        return data
    return asyncio.run(enricher_subwrapper(exchange_name,type,depth))

def update(futures,point_in_time,history,equity,
           intLongCarry, intShortCarry, intUSDborrow,intBorrow,E_long,E_short,E_intUSDborrow,E_intBorrow,
           minimum_carry=0.0,
           previous_weights_index=[]):
    ####### spot quantities. Not used by optimizer. Careful about foresight bias when using those !
    # add borrows
    futures['borrow']=futures['underlying'].apply(lambda f:history.loc[point_in_time,f + '/rate/borrow'])
    futures['lend'] = futures['underlying'].apply(lambda f:history.loc[point_in_time, f + '/rate/borrow'])*.9 # TODO:lending rate
    futures['quote_borrow'] = history.loc[point_in_time, 'USD/rate/borrow']
    futures['quote_lend'] = history.loc[point_in_time, 'USD/rate/borrow'] * .9  # TODO:lending rate

    # spot basis
    futures.loc[futures['type'] == 'perpetual', 'basis_mid'] = futures[futures['type'] == 'perpetual'].apply(
        lambda f: history.loc[point_in_time, f.name + '/rate/funding'],axis=1)
    futures['mark'] = futures.apply(
        lambda f: history.loc[point_in_time, f.name + '/mark/o'],axis=1)
    futures['index'] = futures.apply(
        lambda f: history.loc[point_in_time, f.name + '/indexes/o'],axis=1)
    futures.loc[futures['type'] == 'future','expiryTime'] = futures.loc[futures['type'] == 'future'].apply(
        lambda f: history.loc[point_in_time, f.name + '/rate/T'],axis=1)
    futures.loc[futures['type'] == 'future', 'basis_mid'] = futures[futures['type'] == 'future'].apply(
        lambda f: calc_basis(f['mark'], f['index'],
                             dateutil.parser.isoparse(f['expiry']).replace(tzinfo=None),
                             point_in_time), axis=1)

    # spot carries
    futures['carryLong']=futures['basis_mid']-futures['quote_borrow']
    futures['carryShort']=futures['basis_mid']-futures['quote_borrow']+futures['borrow']
    futures['direction_mid']=0
    futures['carry_mid'] = 0
    futures.loc[(futures['carryShort']+futures['carryLong']<0)&(futures['carryShort']<0),'direction_mid']=-1
    futures.loc[(futures['carryShort']+futures['carryLong']>0)&(futures['carryLong']>0),'direction_mid']=1
    futures.loc[futures['direction_mid']==-1, 'carry_mid'] = -futures['carryShort']
    futures.loc[futures['direction_mid']==1, 'carry_mid'] = futures['carryLong']

    ####### expectations. This is what optimizer uses.

    # carry expectation at point_in_time. no need for -1h to avoid foresight, as funding is a TWAP
    futures['intLongCarry']  = intLongCarry.loc[point_in_time]
    futures['intShortCarry'] = intShortCarry.loc[point_in_time]
    futures['intBorrow'] = intBorrow.loc[point_in_time]
    futures['intUSDborrow']  = intUSDborrow.loc[point_in_time]
    futures['E_long']        = E_long.loc[point_in_time]
    futures['E_short']       = E_short.loc[point_in_time]
    futures['E_intBorrow'] = E_intBorrow.loc[point_in_time]
    futures['E_intUSDborrow']= E_intUSDborrow.loc[point_in_time]

    ##### assume direction only depends on sign(E[long]-E[short]), no integral.
    # Freeze direction into Carry_t and assign max weights. filter out assets with too little carry.
    futures['direction'] = 0
    futures.loc[(futures['E_long'] * futures['MaxLongWeight'] - futures['E_short'] * futures['MaxShortWeight'] < 0)
                &(futures['E_short']<-minimum_carry)
                &(futures['spotMargin']!=False),
                'direction'] = -1
    futures.loc[(futures['E_long'] * futures['MaxLongWeight'] - futures['E_short'] * futures['MaxShortWeight'] > 0)
                & (futures['E_long'] > minimum_carry),
                'direction'] = 1

    # remove low yielding underlyings, except if were in previous portfolio.
    futures = futures[(futures['direction']!=0)|(futures.index.isin(previous_weights_index))]

    # compute realized=\int(carry) and E[\int(carry)]. We're done with direction so remove the max leverage.
    futures['intCarry'] = 0
    futures.loc[futures['direction'] < 0,'intCarry'] = futures.loc[futures['direction'] < 0,'intShortCarry']
    futures.loc[futures['direction'] > 0,'intCarry'] = futures.loc[futures['direction'] > 0,'intLongCarry']
    futures['E_intCarry'] = 0
    futures.loc[futures['direction'] < 0,'E_intCarry'] = futures.loc[futures['direction'] < 0, 'E_short']
    futures.loc[futures['direction'] > 0,'E_intCarry'] = futures.loc[futures['direction'] > 0,'E_long']
    # TODO: covar pre-update
    # C_int = integralCarry_t.ewm(times=hy_history.index, halflife=signal_horizon, axis=0).cov().loc[point_in_time]

    return futures

# return rolling expectations of integrals
def forecast(exchange, futures, hy_history,
             holding_period,  # to convert slippage into rate
             signal_horizon,  # historical window for expectations
             filename=''):             # use external rather than order book
    dated = futures[futures['type'] == 'future']
    ### remove blanks for this
    hy_history = hy_history.fillna(method='ffill',limit=2).dropna(axis=1,how='all')
    # TODO: check hy_history is hourly
    holding_hours = int(holding_period.total_seconds() / 3600)

    #---------- compute max leveraged \int{carry moments}, long and short. To find direction, not weights.
    # for perps, compute carry history to estimate moments.
    # TODO: we no longer have tx costs in carries # 0*f['bid_rate_slippage']
    # TODO: doesn't work with futures (needs point in time argument)

    # 1: spot time series
    LongCarry = futures.apply(lambda f:
                              (- hy_history['USD/rate/borrow']+
                               hy_history[f.name + '/rate/' + ('funding' if f['type']=='perpetual' else 'c')]),
                              axis=1).T
    LongCarry.columns=futures.index.tolist()

    ShortCarry = futures.apply(lambda f:
                               (- hy_history['USD/rate/borrow']+
                                hy_history[f.name + '/rate/' + ('funding' if f['type']=='perpetual' else 'c')]
                                + hy_history[f['underlying'] + '/rate/borrow']),
                               axis=1).T
    ShortCarry.columns = futures.index.tolist()

    Borrow = futures.apply(lambda f: hy_history[f['underlying'] + '/rate/borrow'],
                           axis=1).T
    Borrow.columns = futures.index.tolist()
    USDborrow = hy_history['USD/rate/borrow']

    # 2: integrals, and their median.
    intLongCarry = LongCarry.rolling(holding_hours).mean()
    intLongCarry[dated.index]= LongCarry[dated.index]
    E_long = intLongCarry.rolling(int(signal_horizon.total_seconds()/3600)).mean()
    E_long[dated.index] = intLongCarry[dated.index]

    intShortCarry = ShortCarry.rolling(holding_hours).mean()
    intShortCarry[dated.index] = ShortCarry[dated.index]
    E_short = intShortCarry.rolling(int(signal_horizon.total_seconds()/3600)).mean()
    E_short[dated.index] = intShortCarry[dated.index]

    intBorrow = Borrow.rolling(holding_hours).mean()
    E_intBorrow = intBorrow.rolling(int(signal_horizon.total_seconds()/3600)).mean()

    intUSDborrow = USDborrow.rolling(holding_hours).mean()
    E_intUSDborrow = intUSDborrow.rolling(int(signal_horizon.total_seconds()/3600)).mean()

    # TODO:3: spot premium (approximated using last funding) assumed to converge to median -> IR01 pnl
    # specifically, assume IR01 pnl of f-E[f] and then yield E[f]. More conservative than f stays better than E[f]...as long as holding>1d.
    # E[f] = E_long + E_usdBorrow
    #E_long += futures.apply(lambda f:
    #                    (hy_history[f.name + '/rate/funding']-E_long-E_intUSDborrow)*365.25*24*3600/holding_period.total_seconds() if f['type']=='perpetual' else
    #                    (hy_history[f.name + '/rate/c']-E_long-E_intUSDborrow)*365.25*24*3600/holding_period.total_seconds(),
    #                        axis=1)
    #E_short += futures.apply(lambda f:
    #                    (hy_history[f.name + '/rate/funding'] - E_long - E_intUSDborrow) * 365.25 * 24 * 3600 / holding_period.total_seconds() if f['type'] == 'perpetual' else
    #                    (hy_history[f.name + '/rate/c'] - E_long - E_intUSDborrow) * 365.25 * 24 * 3600 / holding_period.total_seconds(),
    #                        axis=1)

    if filename!='':
        with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
            futures.to_excel(writer, sheet_name='futureinfo')
            for col in futures.index:
                all = pd.concat([intLongCarry[col],
                                 intShortCarry[col],
                                 E_long[col],
                                 E_short[col],
                                 intBorrow[col],
                                 E_intBorrow[col]],axis = 1)
                all.columns = ['intLongCarry', 'intShortCarry', 'E_long','E_short','intBorrow','E_intBorrow']
                all['intUSDborrow']=intUSDborrow
                all['E_intUSDborrow'] = E_intUSDborrow
                all.to_excel(writer, sheet_name=col)

    return (intLongCarry,intShortCarry,intUSDborrow,intBorrow,E_long,E_short,E_intUSDborrow,E_intBorrow)

#-------------------- transaction costs---------------
# add slippage, fees and speed. Pls note we will trade both legs, at entry and exit.
# Slippage override =spread to mid for a single leg, with fees = avg taker/maker.
# Otherwise calculate from orderbook (override Only live is supported).
async def fetch_rate_slippage(input_futures, exchange: ccxt.Exchange,holding_period,
                              slippage_override: int = -999, slippage_orderbook_depth: float = 0,
                              slippage_scaler: float = 1.0,params={'override_slippage':True,'fee_mode':'retail'}) -> None:
    futures=input_futures.copy()
    point_in_time=datetime.now()
    markets=await exchange.fetch_markets()
    if params['override_slippage']==True:
        futures['spot_ask'] = slippage_override
        futures['spot_bid'] = -slippage_override
        futures['future_ask'] = slippage_override
        futures['future_bid'] = -slippage_override
    else: ## rubble calc:
        trading_fees = await exchange.fetch_trading_fees()
        futures_fees = futures['new_symbol'].apply(lambda f: (0.6*0.00015-0.0001+2*0.00006) if (params['fee_mode']=='hr') \
            else (trading_fees[f]['taker']+trading_fees[f]['maker']*0))
        spot_fees = futures.set_index('underlying').apply(lambda f: (0.6 * 0.00015 - 0.0001 + 2 * 0.00006) if (params['fee_mode'] == 'hr') \
            else (trading_fees[f.name + '/USD']['taker'] + trading_fees[f.name + '/USD']['maker'] * 0),axis=1)
        spot_fees = spot_fees[~spot_fees.index.duplicated()]
        spot_fees.index = [x+'/USD' for x in spot_fees.index]
        fees = futures_fees.append(spot_fees)

        #maker fees 0 with 26 FTT staked
        ### relative semi-spreads incl fees, and speed
        if slippage_orderbook_depth==0:
            futures['spot_ask'] = futures.apply(lambda f: fees[f['underlying']]+0.5*(float(find_spot_ticker(markets, f, 'ask'))/float(find_spot_ticker(markets, f, 'bid'))-1), axis=1)*slippage_scaler
            futures['spot_bid'] = -futures['spot_ask']
            futures['future_ask'] = fees[futures['underlying']].values+0.5*(futures['ask'].astype(float)/futures['bid'].astype(float)-1)*slippage_scaler
            futures['future_bid'] = -futures['future_ask']
            #futures['speed']=0##*futures['future_ask'] ### just 0
        else:
            futures['spot_ask'] = await safe_gather(
                [mkt_at_size(exchange, f, 'asks', slippage_orderbook_depth) for
                  f in futures['spot_ticker'].values])
            futures['spot_bid'] = await safe_gather(
                [mkt_at_size(exchange, f, 'bids', slippage_orderbook_depth) for
                  f in futures['spot_ticker'].values])
            futures['future_ask'] = await safe_gather(
                [mkt_at_size(exchange, f, 'asks', slippage_orderbook_depth) for
                 f in futures['symbol'].values])
            futures['future_bid'] = await safe_gather(
                [mkt_at_size(exchange, f, 'bids', slippage_orderbook_depth) for
                 f in futures['symbol'].values])

            futures[['spot_ask','spot_bid','future_ask','future_bid']] = futures[['spot_ask','spot_bid','future_ask','future_bid']].applymap(lambda x:
                        x['slippage'] * slippage_scaler + fees[x['symbol']])

    #### rate slippage assuming perps are rolled every perp_holding_period
    #### use both bid and ask for robustness, but don't x2 for entry+exit
    futures['expiryTime'] = futures.apply(lambda x:
                                          x['expiryTime'] if x['type'] == 'future'
                                          else point_in_time + holding_period,
                                          axis=1)  # .replace(tzinfo=timezone.utc)

    # buy is negative, sell is positive
    buy_slippage=futures['future_bid'] - futures['spot_ask']
    bid_rate_slippage = futures.apply(lambda f: \
                                          (f['future_bid']- f['spot_ask']) \
                                          / np.max([1, (f['expiryTime'] - point_in_time).total_seconds()/3600])*365.25*24,axis=1) # no less than 1h
    sell_slippage=futures['future_ask'] - futures['spot_bid']
    ask_rate_slippage = futures.apply(lambda f: \
                                          (f['future_ask'] - f['spot_bid']) \
                                          / np.max([1, (f['expiryTime'] - point_in_time).total_seconds()/3600])*365.25*24,axis=1) # no less than 1h

    holding_hours = int(holding_period.total_seconds() / 3600)
    return pd.DataFrame({
        'buy_slippage': buy_slippage*365.25*24/holding_hours,
        'sell_slippage': sell_slippage*365.25*24/holding_hours,
        'bid_rate_slippage':bid_rate_slippage,
        'ask_rate_slippage':ask_rate_slippage,
    })
def transaction_cost_calculator(dx,buy_slippage,sell_slippage):
    return sum([
        dx[i] * buy_slippage[i] if dx[i] > 0
        else dx[i] * sell_slippage[i]
        for i in range(len(dx))
    ])

###### use convex optimiziation
# https://docs.scipy.org/doc/scipy/reference/tutorial/optimize.html#sequential-least-squares-programming-slsqp-algorithm-method-slsqp
def cash_carry_optimizer(exchange, futures,
                         previous_weights_df,
                         holding_period,  # to convert slippag into rate
                         signal_horizon,  # historical window for expectations
                         concentration_limit,
                         mktshare_limit,
                         equity,# for markovitz
                         optional_params=[]):             # use external rather than order book
    # freeze universe, from now on must ensure order is the same
    futures = futures.join(previous_weights_df, how='left', lsuffix='_')
    previous_weights = futures['optimalWeight'].fillna(0.0)

    intCarry=futures['intCarry'].values
    intBorrow = futures['intBorrow'].values
    intUSDborrow=futures['intUSDborrow'].values[0]
    E_intCarry = futures['E_intCarry'].values
    E_intBorrow = futures['E_intBorrow'].values
    E_intUSDborrow=futures['E_intUSDborrow'].values[0]
    buy_slippage=futures['buy_slippage'].values
    sell_slippage = futures['sell_slippage'].values

    ##### optimization functions

    ### objective is E_int but:
    # - do not earn USDborrow if usd balance (1-sum)>0
    objective = lambda x: -(
            np.dot(x,E_intCarry) \
            + E_intUSDborrow * min([equity,sum(x)]) \
            + (0 if ('cost_blind' in optional_params) else
               transaction_cost_calculator(x - xt,buy_slippage,sell_slippage))
    )
    #+ marginal_coin_penalty*sum(np.array([np.min[1,np.abs(i)/0.001] for i in x]))

    objective_jac= lambda x: -(
            E_intCarry
            + (E_intUSDborrow if sum(x)<equity else np.zeros(len(x)))
            + (0 if ('cost_blind' in optional_params) else
               (np.array( [buy_slippage[i] if (x - xt)[i] > 0
                           else sell_slippage[i] for i in range(len(x - xt))])))
    )

    #subject to weight bounds, margin and loss probability ceiling
    # TODO: covar pre-update
    #loss_tolerance_constraint = {'type': 'ineq',
    #            'fun': lambda x: loss_tolerance - norm(loc=np.dot(x,E_int), scale=np.dot(x,np.dot(C_int,x))).cdf(0)}
    futures_dict = futures[['new_symbol', 'account_leverage','imfFactor','mark']].set_index('new_symbol').to_dict()
    spot_dict = futures[['underlying','collateralWeight','index']].set_index('underlying').to_dict()
    excess_margin = BasisMarginCalculator(futures['account_leverage'].values[0],
                                                 spot_dict['collateralWeight'],
                                                 futures_dict['imfFactor'],
                                                 equity,
                                                 spot_dict['index'],#TODO: strictly speaking whould be price of spot
                                                 futures_dict['mark'])
    margin_constraint = {'type': 'ineq',
                         'fun': lambda x: excess_margin.shockedEstimate(x)['totalIM']}
    stopout_constraint = {'type': 'ineq',
                          'fun': lambda x: excess_margin.shockedEstimate(x)['totalMM']}

    # bounds: mktshare and concentration
    # scipy understands [0,0] bounds
    lower_bound = futures.apply(lambda future: 0 if future['direction'] > 0 else
    max(-concentration_limit*equity,-mktshare_limit*future['concentration_limit_short']) # 0 bounds if short  + no spotMargin...invalid for institutionals
                                ,axis=1)
    upper_bound = futures.apply(lambda future: 0 if future['direction'] < 0 else
    min(concentration_limit*equity,mktshare_limit*future['concentration_limit_long'])
                                , axis=1)
    bounds = scipy.optimize.Bounds(lb=np.asarray(lower_bound.values,dtype=object),
                                   ub=np.asarray(upper_bound.values,dtype=object))

    # --------- verbose callback function: breaks down pnl during optimization
    progress_display=[]
    def callbackF(x, progress_display, print_with_flag=None):
        if print_with_flag:
            progress_display += [pd.Series({
                'E_int': np.dot(x, E_intCarry),
                'usdBorrowRefund': E_intUSDborrow * min([equity, sum(x)]),
                'tx_cost': + sum([(x - xt)[i] * buy_slippage[i] if (x - xt)[i] > 0
                                  else (x - xt)[i] * sell_slippage[i] for i in
                                  range(len(x - xt))]),
                #TODO: covar pre-update
                # 'loss_tolerance_constraint': loss_tolerance_constraint['fun'](x),
                'margin_constraint': margin_constraint['fun'](x),
                'stopout_constraint': stopout_constraint['fun'](x),
                'success': print_with_flag
            }).append(pd.Series(index=futures.index, data=x))]
            with pd.ExcelWriter('Runtime/logs/portfolio_optimizer/paths.xlsx', engine='xlsxwriter') as writer:
                pd.concat(progress_display, axis=1).to_excel(writer, sheet_name='optimPath')
        return []

    xt = previous_weights.values
    if 'frozen_weights' in futures.columns:
        res=futures[['frozen_weights']].rename({'frozen_weights':'x'}).to_numpy()
    else:
        # guess:
        # - normalized carry expectation, rescaled to max margins
        # - previous weights
        #x0=equity*np.array(E_intCarry)/sum(E_intCarry)
        #x1 = x0/np.max([1-margin_constraint['fun'](x0)/equity,1-stopout_constraint['fun'](x0)/equity])
        x1 = xt*0
        if 'verbose' in optional_params:
            callbackF(x1, progress_display,'initial')

        res = scipy.optimize.minimize(objective, x1, method='SLSQP', jac=objective_jac,
                                      constraints = [margin_constraint,stopout_constraint], # ,loss_tolerance_constraint
                                      bounds = bounds,
                                      callback= (lambda x:callbackF(x,progress_display,'interim')) if 'verbose' in optional_params else None,
                                      options = {'ftol': 1e-2, 'disp': False, 'finite_diff_rel_step' : finite_diff_rel_step, 'maxiter': 20*len(x1)})
        if (not res['success']) and not res['message'] == 'Iteration limit reached':
            # cheeky ignore that exception:
            # https://github.com/scipy/scipy/issues/3056 -> SLSQP is unfomfortable with numerical jacobian when solution is on bounds, but in fact does converge.
            logging.error(res['message'])
            raise Exception(res['message'])

    callbackF(res['x'], progress_display,res['message'])

    def summarize():
        summary=pd.DataFrame()
        summary['spotBenchmark']=futures['mark']/futures['index']-1.0
        summary['ExpectedBenchmark']=(E_intCarry+E_intUSDborrow- futures['direction'].apply(lambda  f: 0 if f>0 else 1.0).values*E_intBorrow)/365.25
        summary['FundingBenchmark'] = futures['basis_mid']/365.25
        summary['currentWeight'] = previous_weights
        summary['optimalWeight'] = res['x']
        summary['ExpectedCarry'] = res['x'] * (E_intCarry+E_intUSDborrow)
        summary['RealizedCarry'] = xt*(intCarry+intUSDborrow)
        summary['excessIM'] = summary.apply(lambda f:excess_margin.shockedEstimate(res['x'])['IM'].loc[[f.name.split('-PERP')[0]+'/USD',f.name.split('-PERP')[0]+'/USD:USD']].sum(),axis=1)
        summary['excessMM'] = summary.apply(lambda f:excess_margin.shockedEstimate(res['x'])['MM'].loc[[f.name.split('-PERP')[0]+'/USD',f.name.split('-PERP')[0]+'/USD:USD']].sum(),axis=1)

        weight_move=summary['optimalWeight']-previous_weights
        summary['transactionCost']=weight_move*futures['buy_slippage']
        summary.loc[weight_move<0, 'transactionCost'] = weight_move[weight_move < 0] * sell_slippage[weight_move < 0]

        summary.loc['USD', 'spotBenchmark'] = futures.iloc[0]['quote_borrow']
        summary.loc['USD', 'ExpectedBenchmark'] = E_intUSDborrow
        summary.loc['USD', 'FundingBenchmark'] = futures.iloc[0]['quote_borrow'] /365.25
        summary.loc['USD', 'currentWeight'] = equity - sum(previous_weights.values)
        summary.loc['USD', 'optimalWeight'] = equity-sum(res['x'])
        summary.loc['USD', 'ExpectedCarry'] = np.min([0,equity-sum(res['x'])])* E_intUSDborrow
        summary.loc['USD', 'RealizedCarry'] = np.min([0,equity-previous_weights.sum()])* intUSDborrow
        summary.loc['USD', 'excessIM'] = excess_margin.shockedEstimate(res['x'])['totalIM']-summary['excessIM'].sum()
        summary.loc['USD', 'excessMM'] = excess_margin.shockedEstimate(res['x'])['totalMM']-summary['excessMM'].sum()
        summary.loc['USD', 'transactionCost'] = 0

        summary.loc['total', 'spotBenchmark'] = summary['spotBenchmark'].mean()
        summary.loc['total', 'ExpectedBenchmark'] = (E_intCarry+E_intUSDborrow).mean()
        summary.loc['total', 'FundingBenchmark'] = summary['FundingBenchmark'].mean()
        summary.loc['total', 'currentWeight'] = equity
        summary.loc['total', 'optimalWeight'] = summary['optimalWeight'].sum() ## 000....
        summary.loc['total', 'ExpectedCarry'] = summary['ExpectedCarry'].sum()
        summary.loc['total', 'RealizedCarry'] = summary['RealizedCarry'].sum()
        summary.loc['total', 'excessIM'] = summary['excessIM'].sum()
        summary.loc['total', 'excessMM'] = summary['excessMM'].sum()
        # TODO: covar pre-update
        #futures.loc['total', 'lossProbability'] = loss_tolerance - loss_tolerance_constraint['fun'](res['x'])
        summary.loc['total', 'transactionCost'] = summary['transactionCost'].sum()
        summary.columns.names=['field']

        return summary
    return summarize()