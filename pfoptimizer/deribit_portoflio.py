import copy
import math

import numpy as np
import scipy

from deribit_history import *

class black_scholes:
    @staticmethod
    def d1(S, K, V, T):
        return (math.log(S / float(K)) + (V ** 2 / 2) * T) / (V * math.sqrt(T))

    @staticmethod
    def d2(S, K, V, T):
        return black_scholes.d1(S, K, V, T) - (V * math.sqrt(T))

    @staticmethod
    def pv(S, K, V, T, cp):
        if cp == 'C':
            return S * scipy.stats.norm.cdf(black_scholes.d1(S, K, V, T)) - K * scipy.stats.norm.cdf(
                black_scholes.d2(S, K, V, T))
        elif cp == 'P':
            return K * scipy.stats.norm.cdf(-black_scholes.d2(S, K, V, T)) - S * scipy.stats.norm.cdf(
                -black_scholes.d1(S, K, V, T))
        else:
            return black_scholes.pv(S, K, V, T, 'P') + black_scholes.pv(S, K, V, T, 'C')

    @staticmethod
    def delta(S, K, V, T, cp):
        '''for a 1% move'''
        delta = scipy.stats.norm.cdf(black_scholes.d1(S, K, V, T))
        if cp == 'C':
            delta = delta
        elif cp == 'P':
            delta = (delta - 1)
        elif cp =='S':
            delta = (2 * delta - 1)

        return delta * S * 0.01

    @staticmethod
    def gamma(S, K, V, T, cp):
        '''for a 1% move'''
        gamma = scipy.stats.norm.pdf(black_scholes.d1(S, K, V, T)) / (S * V * math.sqrt(T))
        return gamma * S * 0.01 * S * 0.01 * (1 if cp != 'S' else 2)

    @staticmethod
    def vega(S, K, V, T, cp):
        '''for a 10% move'''
        vega = (S * math.sqrt(T) * scipy.stats.norm.pdf(black_scholes.d1(S, K, V, T)))
        return vega * V * 0.1 * (1 if cp != 'S' else 2)

    @staticmethod
    def theta(S, K, V, T, cp):
        '''for 1h'''
        theta = -((S * V * scipy.stats.norm.pdf(black_scholes.d1(S, K, V, T))) / (2 * math.sqrt(T)))
        return theta / 24/365.25 * (1 if cp != 'S' else 2)

slippage = {'delta':0, # 1 means 1%
            'gamma':0, # 1 means 1%
            'vega':0,# 1 means 10% relative
            'theta':0, # 1 means 1d
            'rho':0}

class Instrument:
    '''everything is coin margined so beware confusions:
    self.notional is in USD, except cash.
    all greeks are in coin and have signature: (self,market,optionnal **kwargs)
    +ve Delta still means long coin, though, and is in USD'''
    def __init__(self,market):
        self.symbol = None
    def pv(self, market):
        raise Exception('not implemented')
    def cash_flow(self, market, prev_market):
        return {'drop_from_pv': 0, 'accrues': 0}

class Cash(Instrument):
    '''size in coin'''
    def __init__(self, market, currency):
        self.symbol = currency+'-CASH:'
        self.currency = currency
    def pv(self, market):
        return 1

class InverseFuture(Instrument):
    '''a long perpetual (size in USD, strike in coinUSD) is a short USDcoin(notional in USD, 1/strike in USDcoin
    what we implement is more like a fwd..
    '''
    greeks_available = ['delta', 'theta']#,'IR01'
    def __init__(self, market, underlying, strike_coinUSD, maturity):
        self.strike = 1 / strike_coinUSD
        self.maturity = maturity # timestamp. TODO: in fact only fri 8utc for 1w,2w,1m,and 4 IMM
        self.symbol = underlying+'-FUTURE:' + str(pd.to_datetime(maturity, unit='s'))
    
    def pv(self, market):
        T = (self.maturity - market['t'])
        df = np.exp(-market['r']*T)
        fwd = market['fwd'].interpolate(self.maturity)
        return df * (self.strike - 1 / fwd)
    def delta(self, market, maturity_timestamp=None):
        '''for a 1% move'''
        if maturity_timestamp and np.abs(maturity_timestamp - self.maturity)> 1/24/60:
            return 0.0

        T = (self.maturity - market['t'])
        fwd = market['fwd'].interpolate(self.maturity)
        return np.exp(-market['r']*T) * 0.01 / fwd
    def theta(self, market):
        '''for 1 day'''
        T = (self.maturity - market['t'])
        df = np.exp(-market['r'] * T)
        return - market['r'] * df / 24/365.25
    def cash_flow(self, market, prev_market):
        '''cash settled in coin'''
        if prev_market['t'] < self.maturity and market['t'] >= self.maturity:
            cash_flow = (1 / market['spot'] - self.strike)
        else:
            cash_flow = 0
        return {'drop_from_pv': cash_flow, 'accrues': 0}
    def margin_value(self, market):
        return -6e-3 / market['spot']

class InversePerpetual(Instrument):
    '''a long perpetual (size in USD, strike in coinUSD) is a short USDcoin(notional in USD, 1/strike in USDcoin
    what we implement is more like a fwd..'''
    greeks_available = ['delta']
    def __init__(self, market, underlying, strike_coinUSD):
        self.strike = 1 / strike_coinUSD
        self.symbol = underlying+'-PERPETUAL:'

    def pv(self, market):
        fwd = market['fwd'].interpolate(0) ## conventionally 0d
        return (self.strike - 1 / fwd)
    def delta(self, market):
        '''for a -1% move of 1/f'''
        fwd = market['fwd'].interpolate(0)  ## conventionally 0d
        return 0.01 / fwd
    def cash_flow(self, market, prev_market):
        '''accrues every millisecond.
        8h Funding Rate = Maximum (0.05%, Premium Rate) + Minimum (-0.05%, Premium Rate)
        TODO: doesn't handle intra period changes'''
        cash_flow = market['fundingRate'] * (market['t'] - prev_market['t'])/3600/24/365.25 / market['spot']
        return {'drop_from_pv': 0, 'accrues': cash_flow}
    def margin_value(self, market):
        return -6e-3 / market['spot']

class Option(Instrument):
    '''a coinUSD call(size in coin, strike in coinUSD) is a USDcoin put(strike*size in USD,1/strike in USDTBC)
    notional in USD, strike in coin, maturity in sec, callput = C or P '''
    greeks_available = ['delta', 'gamma', 'vega', 'theta']#, 'IR01']
    def __init__(self, market, underlying, strike_coinUSD, maturity, call_put):
        self.strike = 1 / strike_coinUSD
        self.maturity = maturity # TODO: in fact only 8utc for 1d,2d, fri 1w,2w,3w,1m,2m,3m and 4 IMM
        if call_put == 'C':
            self.call_put = 'P'
        elif call_put == 'P':
            self.call_put = 'C'
        elif call_put == 'S':
            self.call_put = 'S'
        else:
            raise Exception("unknown option type")
        self.symbol = underlying + '-' + call_put + ':' + str(pd.to_datetime(maturity, unit='s')) + '+' + str(int(strike_coinUSD))

    def pv(self, market):
        T = (self.maturity - market['t'])
        df = np.exp(-market['r']*T)
        fwd = market['fwd'].interpolate(self.maturity)
        vol = market['vol'].interpolate(self.strike,self.maturity)
        return df * black_scholes.pv(1 / fwd, self.strike, vol,
                                                T / 3600 / 24 / 365.25, self.call_put) if self.maturity > market['t'] else 0
    def delta(self, market, maturity_timestamp=None):
        '''parallel delta by default.
        delta by tenor is maturity_timestamp (within 1s)'''
        if maturity_timestamp and np.abs(maturity_timestamp - self.maturity)> 1/24/60:
            return 0.0
        else:
            T = (self.maturity - market['t'])
            df = np.exp(-market['r']*T)
            fwd = market['fwd'].interpolate(self.maturity)
            vol = market['vol'].interpolate(self.strike,self.maturity)
            return df * black_scholes.delta(1 / fwd, self.strike, vol,
                                                       T / 3600 / 24 / 365.25, self.call_put) if self.maturity > market['t'] else 0
    def gamma(self, market):
        T = (self.maturity - market['t'])
        df = np.exp(-market['r']*T)
        fwd = market['fwd'].interpolate(self.maturity)
        vol = market['vol'].interpolate(self.strike,self.maturity)
        return df * black_scholes.gamma(1 / fwd, self.strike, vol,
                                                   T / 3600 / 24 / 365.25, self.call_put) if self.maturity > market['t'] else 0
    def vega(self, market, maturity_timestamp=None):
        '''vega by tenor maturity_timestamp(within 1s)'''
        if maturity_timestamp and np.abs(maturity_timestamp - self.maturity)> 1/24/60:
            return 0.0
        else:
            T = (self.maturity - market['t'])
            df = np.exp(-market['r'] * T)
            fwd = market['fwd'].interpolate(self.maturity)
            vol = market['vol'].interpolate(self.strike,self.maturity)
            return df * black_scholes.vega(1 / fwd, self.strike, vol,
                                                      T / 3600 / 24 / 365.25, self.call_put) if self.maturity > market['t'] else 0
    def theta(self, market):
        T = (self.maturity - market['t'])
        df = np.exp(-market['r']*T)
        fwd = market['fwd'].interpolate(self.maturity)
        vol = market['vol'].interpolate(self.strike,self.maturity)
        theta_fwd = df * black_scholes.theta(1 / fwd, self.strike, vol,
                                                  T / 3600 / 24 / 365.25, self.call_put) if self.maturity > market['t'] else 0
        return theta_fwd - market['r'] * df / 24/365.25

    def cash_flow(self, market, prev_market):
        '''cash settled in coin'''
        if prev_market['t'] < self.maturity and market['t'] >= self.maturity:
            fwd = market['spot']
            cash_flow = max(0,(1 if self.call_put == 'C' else -1)*(1 / fwd - self.strike))
        else:
            cash_flow = 0
        return {'drop_from_pv': cash_flow, 'accrues': 0}

    def margin_value(self, market, notional):
        fwd = market['spot']
        if self.maturity > market['t']:
            intrisinc = max(0.15 - (1 / fwd - self.strike) * (1 if self.call_put == 'C' else -1) * np.sign(notional),0.1) / market['spot']
            mark = self.pv(market)
            vega = self.vega(market) * 0.45 * np.power(30*24*3600/(self.maturity-market['t']),0.3)
            pin = 0.01 / market['spot'] if notional<0 else 0
            return - (intrisinc+mark+vega+pin)
        else:
            return 0

class Position:
    def __init__(self,instrument,notional_USD,label=None):
        self.instrument = instrument
        self.notional = notional_USD
        self.new_deal_pnl = 0
        self.label = label # this str is handy to describe why position is here, eg gamma_hedge, delta_hedge...
    def apply(self, greek, market, **kwargs):
        return self.notional * getattr(self.instrument, greek)(market,**kwargs) if hasattr(self.instrument,greek) else 0
    def margin_value(self, market):
        kwargs = {'notional':self.notional} if type(self.instrument) == Option else {}
        return self.notional * getattr(self.instrument, 'margin_value')(market, **kwargs)

    def process_margin_call(self, market, prev_market):
        '''apply to prev_portfolio !! '''
        self.new_deal_pnl = 0
        return self.notional * (self.instrument.pv(market) - self.instrument.pv(prev_market))
    def cash_flow(self, market, prev_market):
        return {mode: self.notional * self.instrument.cash_flow(market,prev_market)[mode] for mode in ['drop_from_pv','accrues']}

class Portfolio():
    '''
    a list of position mixin
    hedging buisness logic is here
    '''
    def __init__(self,currency, notional_coin,market):
        self.positions = [Position(Cash(market, currency), notional_coin)]
        self.greeks_cache = dict()

    def target_greek(self, market,
                     greek,
                     greek_params,
                     hedge_instrument,
                     hedge_params,
                     target):
        '''
        targets a value of a greek
        first unwinds all instruments of type hedge_mode['replace'], or just adds if hedge_mode['replace']={}
        '''
        hedge_instrument_greek = getattr(hedge_instrument, greek)(market,**greek_params)
        assert hedge_instrument_greek, f'zero {greek} {hedge_instrument.symbol}'
    
        if 'replace_label' in hedge_params:
            positions_to_replace = [position for position in self.positions if
                                    position.label == hedge_params['replace_label']]

            [self.new_trade(position.instrument,
                            -position.notional,
                            price_coin='slippage',
                            market=market,
                            label=hedge_params['replace_label']
                            ) for position in positions_to_replace]
        portfolio_greek = self.apply(greek, market, **greek_params)
    
        hedge_notional = (target - portfolio_greek) / hedge_instrument_greek
        self.new_trade(hedge_instrument,
                       hedge_notional,
                       price_coin='slippage',
                       market=market,
                       label=hedge_params['replace_label'])

    def vol_flattener(self, market, underlying, vega_target, vol_maturity_timestamp, gamma_maturity_timestamp):
        '''
        This is where the hedging steps are detailed
        '''

        # 1: target vega, replacing back legs
        hedge_instrument = Option(market, underlying, market['fwd'].interpolate(vol_maturity_timestamp), vol_maturity_timestamp, 'S')
        self.target_greek(market,
                          greek= 'vega', greek_params= {'maturity_timestamp':vol_maturity_timestamp},
                          hedge_instrument=hedge_instrument,
                          hedge_params={'replace_label': 'vega_leg'}, target= vega_target)

        # 2: hedge gamma, replacing front legs
        hedge_instrument = Option(market, underlying, market['fwd'].interpolate(gamma_maturity_timestamp), gamma_maturity_timestamp, 'S')
        self.target_greek(market,
                          greek='gamma', greek_params={},
                          hedge_instrument=hedge_instrument,
                          hedge_params={'replace_label': 'gamma_hedge'}, target=0)

        # 3: hedge delta, replacing InverseForward
        hedge_instrument = InverseFuture(market, underlying, market['fwd'].interpolate(vol_maturity_timestamp), vol_maturity_timestamp)
        self.target_greek(market,
                          greek='delta', greek_params={},
                          hedge_instrument=hedge_instrument,
                          hedge_params={'replace_label': 'delta_hedge'}, target=0)

    def process_settlements(self, market, prev_market):
        '''apply to prev_portfolio !'''
        new_positions = copy.deepcopy(self.positions)

        cash_flows = sum([getattr(position, 'cash_flow')(market, prev_market)[mode]
                          for position in new_positions for mode in ['drop_from_pv','accrues']])
        margin_call = sum([getattr(position, 'process_margin_call')(market, prev_market)
                           for position in new_positions])
        new_positions[0].notional += cash_flows + margin_call

        # clean-up unwound positions
        for position in new_positions[1:]:
            if (hasattr(position.instrument, 'maturity') and market['t'] > position.instrument.maturity) \
                    or np.abs(position.notional) < 1e-18:
                new_positions.remove(position)

        return new_positions
    def apply(self, greek, market,**kwargs):
        result = sum([position.apply(greek,market,**kwargs) for position in self.positions])
        return result

    def new_trade(self, instrument, notional_USD, price_coin = 'slippage', market = None, label=None):
        position = next(iter(x for x in self.positions if x.instrument.symbol == instrument.symbol), 'new_position')
        if position == 'new_position':
            position = Position(instrument,notional_USD,label)
            self.positions += [position]
        else:
            position.notional += notional_USD

        # slippage
        if price_coin == 'slippage':
            assert market is not None , 'market is None'
            #empty: slippage using parallel greeks
            kwargs = {}
            slippage_cost = sum(
                slippage[greek] * np.abs(self.apply(greek,market, **kwargs)) for greek in instrument.__class__.greeks_available)
            new_deal_pnl = slippage_cost * np.sign(notional_USD)
        else:
            new_deal_pnl = price_coin - instrument.pv(market)

        # pay cost.
        position.new_deal_pnl = new_deal_pnl
        self.positions[0].notional -= notional_USD * new_deal_pnl

def display_current(portfolio,prev_portfolio, market,prev_market):
    predictors = {'delta':lambda x: x.apply(greek='delta', market=prev_market)*(1-prev_market['spot']/market['spot'])*100,
                  'gamma':lambda x: 0.5 * x.apply(greek='gamma', market=prev_market)*(1-prev_market['spot']/market['spot'])**2 *100*100,
                  'vega':lambda x: x.apply(greek='vega', market=prev_market)*(market['vol'].interpolate(x.instrument.strike,x.instrument.maturity)/prev_market['vol'].interpolate(x.instrument.strike,x.instrument.maturity)-1)*10 if type(x.instrument) == Option else 0,
                  'theta':lambda x: x.apply(greek='theta', market=prev_market)*(market['t']-prev_market['t'])/3600,
                  'IR01':lambda x: x.apply(greek='delta', market=prev_market)*(1-market['spot']/prev_market['spot']*prev_market['fwd'].interpolate(x.instrument.maturity)/market['fwd'].interpolate(x.instrument.maturity))*100 if type(x.instrument) in [InverseFuture,Option] else 0}

    # 1: mkt
    display_market = pd.Series({('mkt',data,'total'):market[data] for data in ['t', 'spot', 'fwd', 'vol', 'fundingRate', 'borrow', 'r']})

    # 2 : greeks
    greeks = pd.Series(
        {('risk',greek,position.instrument.symbol.split(':')[0]):
             position.apply(greek, market)
         for greek in ['pv'] + list(predictors.keys()) for position in portfolio.positions})

    # 3: predict = mkt move for prev_portfolio
    predict = pd.Series(
        {('predict',greek,position.instrument.symbol.split(':')[0]):
             predictor(position)
         for greek,predictor in predictors.items() for position in prev_portfolio.positions[1:]})

    # 4: actual = prev_portfolio cash_flows + new deal + unexplained
    actual = pd.Series(
        {('actual', 'unexplained', p0.instrument.symbol.split(':')[0]): # the unexplained = dPV-predict, for old portoflio only
             p0.apply('pv',market) - p0.apply('pv',prev_market) # dPV
             - p0.cash_flow(market,prev_market)['drop_from_pv'] # we don't do it for cash so avoid double counting since PV above has cash_flows
             - predict[('predict',slice(None),p0.instrument.symbol.split(':')[0])].sum() # - the predict
         for p0 in prev_portfolio.positions[1:]} # ....only for old positions. New positions contribute to new_deal only.
        |{('actual', 'cash_flow', p0.instrument.symbol.split(':')[0]):
              p0.cash_flow(market,prev_market)['drop_from_pv']+p0.cash_flow(market,prev_market)['accrues']
          for p0 in prev_portfolio.positions[1:]}
        |{('actual', 'new_deal', p1.instrument.symbol.split(':')[0]):
              p1.new_deal_pnl # new deals only contribute here.
          for p1 in portfolio.positions[1:]})
    # check new_deal accrues only once.
    issues = [p0.instrument.symbol for p0 in prev_portfolio.positions[1:] if p0.new_deal_pnl]
    if len(issues)>0:
          print('new_deal_pnl double count'+ ''.join([p0.instrument.symbol for p0 in prev_portfolio.positions[1:] if p0.new_deal_pnl]))

    # add totals and mkt
    new_data = pd.concat([greeks,predict,actual],axis=0).unstack(level=2)
    new_data['total'] = new_data.sum(axis=1)
    new_data = new_data.stack()
    new_data = pd.concat([display_market,new_data],axis=0)

    new_data.name = pd.to_datetime(market['t'], unit='s')
    return new_data.sort_index(axis=0,level=[0,1,2])

def strategies_main(*argv):
    argv = list(argv)
    if len(argv) == 0:
        argv.extend(['ETH'])
    if len(argv) < 2:
        argv.extend([1])
    if len(argv) < 3:
        argv.extend([7])
    if len(argv) < 4:
        argv.extend([30])
    if len(argv) < 5:
        argv.extend([100])
    print(f'running {argv}')

    currency = argv[0]
    volfactor = argv[1]
    gamma_tenor = argv[2]*24*3600
    vol_tenor = argv[3]*24*3600
    backtest_window = argv[4]

    ## get history
    history = deribit_history_main('get',currency,'deribit',backtest_window)

    ## format history
    history.reset_index(inplace=True)
    history.rename(columns={'index':'t',
                            currency + '-PERPETUAL/indexes/o':'spot',
                            currency+'/fwd':'fwd',
                            currency+'/vol':'vol',
                            currency + '-PERPETUAL/rate/funding':'fundingRate'},inplace=True)
    history['t'] = history['t'].apply(lambda t: t.timestamp())
    history['vol'] = history['vol'].apply(lambda v: v.scale(volfactor))
    history['borrow'] = 0
    history['r'] = history['borrow']
    history.ffill(limit=2,inplace=True)
    history.bfill(inplace=True)

    ## new portfolio
    prev_mkt = history.iloc[0]
    portfolio = Portfolio(currency,notional_coin=1,market=prev_mkt)
    prev_portfolio = copy.deepcopy(portfolio)
    vega_target = -0.1 # silly start value

    ## run backtest
    series = []
    for run_i,market in history.iterrows():
        # all book-keeping
        portfolio.positions = prev_portfolio.process_settlements(market, prev_mkt)

        # all the hedging steps
        portfolio.vol_flattener(market,currency, vega_target, market['t']+vol_tenor, market['t']+gamma_tenor)
        vega_target *= portfolio.apply(greek='vega',market=market,maturity_timestamp=market['t']+vol_tenor)

        # all the info
        series += [display_current(portfolio,prev_portfolio,market,prev_mkt)]

        # ...and move on
        prev_mkt = market
        prev_portfolio = copy.deepcopy(portfolio)

        if run_i%100==0:
            print('{} at {}'.format(run_i,datetime.now()))

    display = pd.concat(series,axis=1)
    display.loc[(['predict','actual'],slice(None),slice(None))] = display.loc[(['predict','actual'],slice(None),slice(None))].cumsum(axis=1)

    filename = f'Runtime/logs/deribit_portfolio/run_{argv[1]}_{datetime.utcnow().strftime("%Y-%m-%d-%Hh")}' # should be linear 1 to 1
    with pd.ExcelWriter(filename+'.xlsx', engine='xlsxwriter',mode='w') as writer:
        display.T.to_excel(writer,sheet_name=f'{argv[1]}_{argv[2]}_{argv[3]}')
        pd.DataFrame(index=['params'],
                     data={'underlying': [currency], 'vol_tenor': [vol_tenor], 'gamma_tenor': [gamma_tenor]}
                     ).to_excel(writer, sheet_name=f'params')
    display.to_pickle(filename+'.pickle')

if __name__ == "__main__":
    strategies_main(*sys.argv[1:])