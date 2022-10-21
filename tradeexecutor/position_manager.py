import numpy as np

from riskpnl.ftx_margin import MarginCalculator
from utils.io_utils import myUtcNow
from tradeexecutor.interface.StrategyEnabler import StrategyEnabler
from tradeexecutor.venue_api import GmxAPI
from abc import abstractmethod

class PositionManager(StrategyEnabler):
    '''PositionManager owns pv,risk,margin. Even underlyings not being executed (but only one exchange)
    Records historical risks.
    structure is {symbol:[{'delta','delta_timestamp','delta_id',some static data},...]}
    able to reconcile to an exchange'''
    class LimitBreached(Exception):
        def __init__(self,check_frequency,limit):
            super().__init__()
            self.delta_limit = limit
            self.check_frequency = check_frequency

    def __init__(self,parameters):
        super().__init__(parameters)
        self.delta_adjustment: dict = dict({key: 0 for key in parameters['symbols']})

        self.pv = None
        self.margin = None
        self.limit = None
        self.risk_reconciliations = []

    def serialize(self) -> list[dict]:
        result = self.risk_reconciliations
        self.risk_reconciliations = []
        return result

    @staticmethod
    async def build(parameters):
        if parameters['exchange'] == 'gmx':
            result = GMXPositionManager(parameters)
        else:
            result = FtxPositionManager(parameters)

        result.limit = PositionManager.LimitBreached(parameters['check_frequency'], parameters['delta_limit'])

        for symbol in parameters['symbols']:
            result.data[symbol] = {'delta': 0,
                              'delta_timestamp': myUtcNow(),
                              'delta_id': 0}
        return result

    @abstractmethod
    def adjusted_delta(self,symbol):
        raise NotImplementedError

    @abstractmethod
    @StrategyEnabler.unless_reconciled
    def reconcile(self):
        raise NotImplementedError

class FtxPositionManager(PositionManager):
    def __init__(self,parameters):
        super().__init__(parameters)
        self.markets = None

    def process_fill(self, fill):
        symbol = fill['symbol']
        px = fill['price']
        fill_size = fill['amount'] * (1 if fill['side'] == 'buy' else -1)

        # update risk_state
        if symbol not in self.data:
            self.data[symbol] = {'delta': 0, 'delta_id': 0, 'delta_timestamp': myUtcNow()}
        data = self.data[symbol]
        data['delta'] += fill_size * px
        data['delta_id'] = max(data['delta_id'], int(fill['order']))
        data['delta_timestamp'] = fill['timestamp']

        # update margin
        self.margin.add_instrument(symbol, fill_size)

        if 'verbose' in self.strategy.parameters['options'] and symbol in self.strategy:
            current = self.data[symbol]['delta']
            target = self.strategy[symbol]['target'] * px if 'target' in self.strategy[symbol] else None
            self.strategy.logger.warning('{} risk at {} ms: [current {}, target {}]'.format(
                symbol,
                self.data[symbol]['delta_timestamp'],
                current,
                target))

    @StrategyEnabler.unless_reconciled
    async def reconcile(self):
        # if not already done, Initializes a margin calculator that can understand the margin of an order
        # reconcile the margin of an exchange with the margin we calculate
        # pls note it needs the exchange pv, which is known after reconcile
        self.margin = self.margin if self.margin else await MarginCalculator.margin_calculator_factory(self.strategy.venue_api)
        self.markets = self.markets if self.markets else self.strategy.venue_api.markets

        # recompute risks, margins
        previous_delta = {symbol:{'delta':data['delta']} for symbol, data in self.data.items()}
        previous_pv = self.pv

        # adjust delta by parents
        for parent in self.strategy.parents.values():
            await parent.venue_api.reconcile()

        await self.strategy.venue_api.reconcile()
        state = self.strategy.venue_api.state
        self.delta_adjustment = {key: sum(parent.venue_api.partial_delta(key, normalized=True) * parent.position_manager.adjusted_delta(lp_token) * self.strategy.venue_api.mid(key)
                                          for lp_token, parent in self.strategy.parents.items())
                                 for key in self.data}

        risk_timestamp = myUtcNow()

        # delta is noisy for perps, so override to delta 1.
        self.pv = 0
        for coin, balance in state.balances['total'].items():
            if coin != 'USD':
                symbol = coin + '/USD'
                mid = state.markets['price'][self.strategy.venue_api.market_id(symbol)]
                delta = balance * mid
                if symbol not in self.data:
                    self.data[symbol] = {'delta_id': 0}
                self.data[symbol]['delta'] = delta
                self.data[symbol]['delta_timestamp'] = risk_timestamp
                self.pv += delta

        self.pv += state.balances['total']['USD']  # doesn't contribute to delta, only pv !

        for name, position in state.positions['netSize'].items():
            symbol = self.strategy.venue_api.market(name)['symbol']
            delta = position * self.strategy.venue_api.mid(symbol)

            if symbol not in self.data:
                self.data[symbol] = {'delta_id': 0}
            self.data[symbol]['delta'] = delta
            self.data[symbol]['delta_timestamp'] = risk_timestamp

        # update IM
        await self.margin.refresh(self.strategy.venue_api, balances=state.balances['total'], positions=state.positions['netSize'],
                                  im_buffer=self.parameters['im_buffer'] * self.pv)

        delta_error = {symbol: self.data[symbol]['delta'] - (previous_delta[symbol]['delta'] if symbol in previous_delta else 0)
                       for symbol in self.data}
        self.risk_reconciliations += [{'symbol':symbol_,
                                       'delta_timestamp': self.data[symbol_]['delta_timestamp'],
                                       'delta': self.data[symbol_]['delta'],
                                       'netDelta': self.coin_delta(symbol_),
                                       'pv': self.pv,
                                       'estimated_IM': self.margin.estimate('IM'),
                                       'actual_IM': self.margin.actual_IM,
                                       'pv_error': self.pv - (previous_pv or 0),
                                       'total_delta_error': sum(delta_error.values())}
                                      for symbol_ in self.data]
        self.check_limit()

    def check_limit(self):
        absolute_risk = dict()
        for symbol in self.data:
            coin = self.markets[symbol]['base']
            if coin not in absolute_risk:
                absolute_risk[coin] = abs(self.coin_delta(symbol))
        if sum(absolute_risk.values()) > self.pv * self.limit.delta_limit:
            self.strategy.logger.info(
                f'absolute_risk {absolute_risk} > {self.pv * self.limit.delta_limit}')
        if self.margin.actual_IM < self.pv / 100:
            self.strategy.logger.info(f'IM {self.margin.actual_IM}  < 1%')

    def trim_to_margin(self, weights: dict):
        '''trim size to margin allocation (equal for all running symbols)'''
        marginal_IM = sum([self.margin.order_marginal_cost(symbol,
                                                           size,
                                                           self.strategy.venue_api.tickers[symbol]['mid'],
                                                           'IM')
                           for symbol,size in weights.items()])
        estimated_IM = self.margin.estimate('IM')
        actual_IM = self.margin.actual_IM

        if self.margin.actual_IM + marginal_IM < self.margin.IM_buffer:
            self.strategy.logger.info(
                f'actual_IM {self.margin.actual_IM} < IM_buffer {self.margin.IM_buffer} (estimated_IM {estimated_IM} / actual_IM {actual_IM} / marginal_IM {marginal_IM})')
            return {symbol: 0 for symbol in weights}

        marginal_IM = marginal_IM if abs(marginal_IM) > 1e-9 else np.sign(marginal_IM) * 1e-9
        if actual_IM + marginal_IM < self.margin.IM_buffer:
            trim_factor = np.clip((self.margin.IM_buffer - actual_IM) / marginal_IM,a_min=0,a_max=1)
        else:
            trim_factor = 1.0
        trimmed_size = {symbol: size*trim_factor for symbol, size in weights.items()}
        return trimmed_size

    def adjusted_delta(self, symbol):
        '''includes delta adjustment'''
        return self.data[symbol]['delta'] + (self.delta_adjustment[symbol] if symbol in self.delta_adjustment else 0)

    def coin_delta(self,symbol):
        '''includes delta adjustment'''
        coin = self.markets[symbol]['base']
        return sum(self.adjusted_delta(symbol_)
                       for symbol_ in self.data
                       if self.markets[symbol_]['base'] == coin)

    def delta_bounds(self, symbol):
        ''''''
        coinDelta = self.coin_delta(symbol)
        coin = self.markets[symbol]['base']
        coin_delta_plus = coinDelta + sum(self.margin.open_orders[symbol_]['longs']
                                        for symbol_ in self.margin.open_orders
                                        if self.markets[symbol_]['base'] == coin)
        coin_delta_minus = coinDelta + sum(self.margin.open_orders[symbol_]['shorts']
                                         for symbol_ in self.margin.open_orders
                                         if self.markets[symbol_]['base'] == coin)
        total_delta = sum(self.adjusted_delta(symbol_)
                          for symbol_, data in self.data.items())
        total_delta_plus = total_delta + sum(self.margin.open_orders[symbol_]['longs']
                                             for symbol_ in self.margin.open_orders)
        total_delta_minus = total_delta + sum(self.margin.open_orders[symbol_]['shorts']
                                              for symbol_ in self.margin.open_orders)

        global_delta = coinDelta + self.parameters['global_beta'] * (total_delta - coinDelta)
        global_delta_plus = coin_delta_plus + self.parameters['global_beta'] * (total_delta_plus - coin_delta_plus)
        global_delta_minus = coin_delta_minus + self.parameters['global_beta'] * (total_delta_minus - coin_delta_minus)

        return {'global_delta': global_delta,
                'global_delta_plus': global_delta_plus,
                'global_delta_minus': global_delta_minus}

class GMXPositionManager(PositionManager):
    def __init__(self,parameters):
        super().__init__(parameters)

    def adjusted_delta(self, symbol):
        return self.data[symbol]['delta']

    @StrategyEnabler.unless_reconciled
    async def reconcile(self):
        '''we could call balance of but we just read the target'''
        previous_delta = {symbol: {'delta': data['delta']} for symbol, data in self.data.items()}
        previous_pv = self.pv
        
        risk_timestamp = myUtcNow()
        await self.strategy.venue_api.reconcile()
        glp_position = self.strategy.venue_api.depositBalances()
        self.pv = glp_position * self.strategy.venue_api.actualAum['total']
        self.data['GLP']['delta'] = self.pv

        delta_error = {symbol: self.data[symbol]['delta'] - (previous_delta[symbol]['delta'] if symbol in previous_delta else 0)
                       for symbol in ['GLP']}
        self.risk_reconciliations += [{'symbol': 'GLP',
                                       'delta_timestamp': risk_timestamp,
                                       'delta': self.data[symbol_]['delta'],
                                       #'netDelta': self.coin_delta(symbol_),
                                       'pv': self.pv,
                                       'pv_error': self.pv - (previous_pv or 0),
                                       'total_delta_error': sum(delta_error.values())}
                                      for symbol_ in ['GLP']]