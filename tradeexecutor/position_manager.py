import numpy as np

from riskpnl.ftx_margin import MarginCalculator
from utils.ftx_utils import syncronized_state
from utils.io_utils import myUtcNow


class PositionManager(dict):
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
        super().__init__()
        self.parameters = parameters
        self.strategy = None
        self.pv = None
        self.margin = None
        self.markets = None
        self.limit = None
        self.risk_reconciliations = []

    def to_dict(self):
        return self.risk_reconciliations

    @staticmethod
    async def build(parameters):
        if not parameters:
            return None
        else:
            result = PositionManager(parameters)
            result.limit = PositionManager.LimitBreached(parameters['check_frequency'], parameters['delta_limit'])

            for symbol in parameters['symbols']:
                result[symbol] = {'delta': 0,
                                  'delta_timestamp': myUtcNow(),
                                  'delta_id': 0}
            return result

    def process_fill(self, fill):
        symbol = fill['symbol']
        # update risk_state
        if symbol not in self:
            self[symbol] = {'delta': 0, 'delta_id': 0, 'delta_timestamp': myUtcNow()}
        data = self[symbol]
        fill_size = fill['amount'] * (1 if fill['side'] == 'buy' else -1) * fill['price']
        data['delta'] += fill_size
        data['delta_id'] = max(data['delta_id'], int(fill['order']))
        data['delta_timestamp'] = fill['timestamp']

        # update margin
        self.margin.add_instrument(symbol, fill_size)

        if 'verbose' in self.parameters['options']:
            current = self[symbol]['delta']
            target = self.strategy[symbol]['target'] * fill['price']
            diff = (self.strategy[symbol]['target'] - self[symbol]['delta']) * fill['price']
            initial = self.strategy[symbol]['target'] * fill['price'] - diff
            self.strategy.logger.warning('{} risk at {} ms: {}% done [current {}, initial {}, target {}]'.format(
                symbol,
                self[symbol]['delta_timestamp'],
                (current - initial) / diff * 100,
                current,
                initial,
                target))

    async def reconcile(self):

        # if not already done, Initializes a margin calculator that can understand the margin of an order
        # reconcile the margin of an exchange with the margin we calculate
        # pls note it needs the exchange pv, which is known after reconcile
        self.margin = self.margin if self.margin else await MarginCalculator.margin_calculator_factory(self.strategy.venue_api)
        self.markets = self.markets if self.markets else self.strategy.venue_api.markets

        # recompute risks, margins
        previous_delta = {symbol:{'delta':data['delta']} for symbol,data in self.items()}
        previous_pv = self.pv

        state = await syncronized_state(self.strategy.venue_api)
        risk_timestamp = myUtcNow()

        # delta is noisy for perps, so override to delta 1.
        self.pv = 0
        for coin, balance in state['balances']['total'].items():
            if coin != 'USD':
                symbol = coin + '/USD'
                mid = state['markets']['price'][self.strategy.venue_api.market_id(symbol)]
                delta = balance * mid
                if symbol not in self:
                    self[symbol] = {'delta_id': 0}
                self[symbol]['delta'] = delta
                self[symbol]['delta_timestamp'] = risk_timestamp
                self.pv += delta

        self.pv += state['balances']['total']['USD']  # doesn't contribute to delta, only pv !

        for name, position in state['positions']['netSize'].items():
            symbol = self.strategy.venue_api.market(name)['symbol']
            delta = position * self.strategy.venue_api.mid(symbol)

            if symbol not in self:
                self[symbol] = {'delta_id': 0}
            self[symbol]['delta'] = delta
            self[symbol]['delta_timestamp'] = risk_timestamp

        # update IM
        await self.margin.refresh(self.strategy.venue_api, balances=state['balances']['total'], positions=state['positions']['netSize'],
                                  im_buffer=self.parameters['im_buffer'] * self.pv)

        delta_error = {symbol: self[symbol]['delta'] - (previous_delta[symbol]['delta'] if symbol in previous_delta else 0)
                       for symbol in self}
        self.risk_reconciliations = {symbol_:{
                                       'delta_timestamp': self[symbol_]['delta_timestamp'],
                                       'delta': self[symbol_]['delta'],
                                       'netDelta': self.coin_delta(symbol_),
                                       'pv': self.pv,
                                       'estimated_IM': self.margin.estimate('IM'),
                                       'actual_IM': self.margin.actual_IM,
                                       'pv_error': self.pv - (previous_pv or 0),
                                       'total_delta_error': sum(delta_error.values())}
                                      for symbol_ in self}

    def check_limit(self):
        absolute_risk = dict()
        for symbol in self:
            coin = self.markets[symbol]['base']
            if coin not in absolute_risk:
                absolute_risk[coin] = abs(self.coin_delta(symbol))
        if sum(absolute_risk.values()) > self.pv * self.limit.delta_limit:
            self.strategy.logger.info(
                f'absolute_risk {absolute_risk} > {self.pv * self.limit.delta_limit}')
        if self.margin.actual_IM < self.pv / 100:
            self.strategy.logger.info(f'IM {self.margin.actual_IM}  < 1%')

    def trim_to_margin(self, mid, size, symbol):
        '''trim size to margin allocation (equal for all running symbols)'''
        marginal_IM = self.margin.order_marginal_cost(symbol, size, mid, 'IM')
        estimated_IM = self.margin.estimate('IM')
        actual_IM = self.margin.actual_IM

        if self.margin.actual_IM < self.margin.IM_buffer:
            self.strategy.logger.info(
                f'estimated_IM {estimated_IM} / actual_IM {actual_IM} / marginal_IM {marginal_IM}')
            return 0

        marginal_IM = marginal_IM if abs(marginal_IM) > 1e-9 else np.sign(marginal_IM) * 1e-9
        if actual_IM + marginal_IM < self.margin.IM_buffer:
            trim_factor = np.clip((self.margin.IM_buffer - actual_IM) / marginal_IM,a_min=0,a_max=1)
        else:
            trim_factor = 1.0
        trimmed_size = size * trim_factor
        if trim_factor < 1:
            self.strategy.logger.info(f'trimmed {size} {symbol} by {trim_factor}')
        return trimmed_size

    def coin_delta(self,symbol):
        coin = self.markets[symbol]['base']
        return sum(data['delta']
                       for symbol_, data in self.items()
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
        total_delta = sum(data['delta']
                          for symbol_, data in self.items())
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
