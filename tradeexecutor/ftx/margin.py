from tradeexecutor.ftx.api import FtxAPI
from tradeexecutor.utils.config_loader import *
from tradeexecutor.utils.ftx_utils import ftx_collateralWeightInitial
import numpy as np

class MarginCalculator:
    '''low level class to compute margins
    weights is position size in usd
    '''
    def __init__(self, account_leverage, collateralWeight, imfFactor):  # collateralWeight,imfFactor by symbol
        # Load defaults params

        self._account_leverage = account_leverage
        self._collateralWeight = {f'{coin}/USD': collateralWeight[coin]
                                  for coin, data in collateralWeight.items() } |{'USD/USD' :1.0}
        self._imfFactor = imfFactor | {'USD/USD:USD':0.0}
        self._collateralWeightInitial = {f'{coin}/USD': ftx_collateralWeightInitial
            ({'underlying': coin, 'collateralWeight': data})
                                         for coin, data in collateralWeight.items() } |{'USD/USD' :1.0}

        self.actual_futures_IM = None   # dict, keys by id not symbol
        self.actual_IM = None           # float
        self.actual_MM = None           # float

        self.balances_and_positions = dict() # {symbol: {'size': size, 'spot_or_swap': spot or future}}...includes USD/USD
        self.open_orders = dict() # {symbol: {'long':2,'short':1}}

        self.IM_buffer = None # need to wait to have pv...
        self.type_func = None
        self.mid_func = None

    # --------------- slow explicit factory / reconcilators -------------

    @staticmethod
    async def margin_calculator_factory(exchange):
        '''factory from exchange'''
        # get static parameters
        futures = pd.DataFrame(await FtxAPI.Static.fetch_futures(exchange))
        account_leverage = float(futures.iloc[0]['account_leverage'])
        collateralWeight = futures.set_index('underlying')['collateralWeight'].to_dict()
        imfFactor = futures.set_index('new_symbol')['imfFactor'].to_dict()
        initialized = MarginCalculator(account_leverage, collateralWeight, imfFactor, )
        initialized.type_func = lambda s: exchange.markets[s]['type']
        initialized.mid_func = exchange.mid

        await initialized.refresh(exchange)

        return initialized

    async def refresh(self,exchange, im_buffer = None, balances = None, positions = None):
        await self.set_instruments(exchange,balances,positions)
        await self.set_open_orders(exchange)
        await self.update_actual(exchange)
        self.IM_buffer = im_buffer

    async def set_instruments(self, exchange, balances=None, positions=None):
        '''reset balances_and_positions and get from rest request'''
        self.balances_and_positions = dict()

        if balances is None or positions is None:
            await exchange.reconcile()
            balances = exchange.state.balances['total']
            positions = exchange.state.positions['netSize']

        for name,position in positions.items():
            symbol = exchange.market(name)['symbol']
            self.balances_and_positions[symbol] = {'size': position,
                                                       'spot_or_swap': 'swap'}
        for coin, balance in balances.items():
            if coin in exchange.currencies and balance != 0:
                self.balances_and_positions[f'{coin}/USD'] = {'size' :balance,
                                                              'spot_or_swap' :'spot'}

    async def set_open_orders(self, exchange):
        '''reset open orders and add all orders by side'''
        self.open_orders = dict()

        external_orders = await exchange.fetch_open_orders()
        for order in external_orders:
            self.add_open_order(order)

    async def update_actual(self, exchange):
        account_information = (await exchange.privateGetAccount())['result']

        self.actual_futures_IM = {position['future']: float(position['collateralUsed'])
                                  for position in account_information['positions'] if float(position['netSize']) != 0}
        totalPositionSize = float(account_information['totalPositionSize']) if float(
            account_information['totalPositionSize']) > 0 else 0.0
        openMarginFraction = float(account_information['openMarginFraction']) if account_information[
            'openMarginFraction'] else 0.0
        self.actual_IM = float(totalPositionSize) * (
                float(openMarginFraction) - float(account_information['initialMarginRequirement']))
        self.actual_MM = float(totalPositionSize) * (
                float(openMarginFraction) - float(account_information['maintenanceMarginRequirement']))
        pv = account_information['totalAccountValue']

    # --------------- quick update methods -------------

    def add_instrument(self ,symbol ,size):
        '''size in coin'''
        initial = self.balances_and_positions[symbol]['size'] if symbol in self.balances_and_positions else 0
        self.balances_and_positions[symbol] = {'size' :size + initial,
                                               'spot_or_swap' : self.type_func(symbol)}

    def add_open_order(self, order):
        symbol = order['symbol']
        side = order['side']
        amount = float(order['amount'])
        if symbol not in self.open_orders:
            self.open_orders[symbol] = {'spot_or_swap' :self.type_func(symbol), 'longs': 0, 'shorts': 0}
        self.open_orders[symbol]['longs' if side == 'buy' else 'shorts'] += amount

    # --------------- calculators -------------

    def swap_IM(self ,symbol ,size ,mark):
        amount = np.abs(size)
        return - mark * amount * max(1.0 / self._account_leverage,
                              self._imfFactor[symbol] * np.sqrt(amount / mark))
    def swap_MM(self ,symbol ,size ,mark):
        amount = np.abs(size)
        return min(- 0.03 * amount * mark, 0.6 * self.future_IM(symbol ,size ,mark))
    def spot_IM(self ,symbol ,size ,mark):
        # https://help.ftx.com/hc/en-us/articles/360031149632
        if size < 0:
            collateral = size
            im_short = size * max(1.1 / self._collateralWeightInitial[symbol] - 1,
                                     self._imfFactor[f'{symbol}:USD'] * np.sqrt(np.abs(size)))
        else:
            collateral = size * min(self._collateralWeight[symbol],
                                      1.1 / (1 + self._imfFactor[f'{symbol}:USD'] * np.sqrt(np.abs(size))))
            im_short = 0
        return mark * (collateral + im_short)
    def spot_MM(self, symbol, size, mark):
        raise Exception('fix formula (split collateral out')
        # https://help.ftx.com/hc/en-us/articles/360053007671-Spot-Margin-Trading-Explainer
        return min(- 0.03 * np.abs(size) * mark, 0.6 * self.future_IM(symbol, size, mark))

    def estimate(self, IM_or_MM, return_details = False):
        '''returns {symbol:margin}'''

        # first, compute margin for existing positions
        margins = {symbol :self.__getattribute__(instrument['spot_or_swap']+'_'+IM_or_MM)(
            symbol,
            instrument['size'],
            self.mid_func(symbol))
            for symbol ,instrument in self.balances_and_positions.items()}

        # modify for open orders
        for symbol ,open_order in self.open_orders.items():
            size_list = []
            size_list.append(self.balances_and_positions[symbol]['size'] \
                if symbol in self.balances_and_positions else 0)
            size_list.append(size_list[0] + open_order['longs'])
            size_list.append(size_list[0] - open_order['shorts'])

            spot_or_swap = self.type_func(symbol)
            margins[symbol] = min(self.__getattribute__(spot_or_swap+ '_' + IM_or_MM)(
                symbol, _size, self.mid_func(symbol)) for _size in size_list)

        return margins if return_details else sum(margins.values())

    def order_marginal_cost(self, symbol, size, mid, IM_or_MM):
        '''margin impact of an order'''

        size_list_before = []
        size_list_before.append(self.balances_and_positions[symbol]['size'] \
            if symbol in self.balances_and_positions else 0)
        size_list_before.append(size_list_before[0] + \
                              (self.open_orders[symbol]['longs'] if symbol in self.open_orders else 0))
        size_list_before.append(size_list_before[0] - \
                              (self.open_orders[symbol]['shorts'] if symbol in self.open_orders else 0))

        size_list_after = [ size_before + size for size_before in size_list_before]

        spot_or_swap = self.type_func(symbol)
        new_margin = min(self.__getattribute__(spot_or_swap + '_' + IM_or_MM)(
            symbol, _size, mid) for _size in size_list_after)
        old_margin = min(self.__getattribute__(spot_or_swap + '_' + IM_or_MM)(
            symbol, _size, mid) for _size in size_list_before)

        return new_margin - old_margin

class BasisMarginCalculator(MarginCalculator):
    '''implement specific contraints for carry trades, w/risk mgmt
    x will be spot weights in USD
    spot_marks, future_marks'''

    def __init__(self, account_leverage, collateralWeight, imfFactor,
                 equity, spot_marks, future_marks):
        super().__init__(account_leverage, collateralWeight, imfFactor)

        self._equity = equity
        self.spot_marks = spot_marks
        self.future_marks = future_marks

        pf_params = configLoader.get_pfoptimizer_params()
        self._long_blowup = float(pf_params['LONG_BLOWUP']['value'])
        self._short_blowup = float(pf_params['SHORT_BLOWUP']['value'])
        self._nb_blowups = int(pf_params['NB_BLOWUPS']['value'])
        self._open_order_headroom = float(pf_params['OPEN_ORDERS_HEADROOM']['value'])

    def futureMargins(self, weights):
        '''weights = {symbol: 'weight, 'mark'''
        im_fut = {symbol:
            abs(data['weight']) * max(1.0 / self._account_leverage,
                                      self._imfFactor[symbol] * np.sqrt(abs(data['weight']) / data['mark']))
            for symbol, data in weights.items()}
        mm_fut = {symbol:
            max([0.03 * data['weight'], 0.6 * im_fut[symbol]])
            for symbol, data in weights.items()}

        return (im_fut, mm_fut)

    def spotMargins(self, weights):
        # https://help.ftx.com/hc/en-us/articles/360031149632
        collateral = {coin+'/USD':
            data['weight'] if data['weight'] < 0
            else data['weight'] * min(self._collateralWeight[coin+'/USD'],
                                      1.1 / (1 + self._imfFactor[coin + '/USD:USD'] * np.sqrt(
                                          abs(data['weight']) / data['mark'])))
            for coin, data in weights.items()}
        # https://help.ftx.com/hc/en-us/articles/360053007671-Spot-Margin-Trading-Explainer
        im_short = {coin+'/USD':
            0 if data['weight'] > 0
            else -data['weight'] * max(1.1 / self._collateralWeightInitial[coin+'/USD'] - 1,
                                       self._imfFactor[coin + '/USD:USD'] * np.sqrt(abs(data['weight']) / data['mark']))
            for coin, data in weights.items()}
        mm_short = {coin+'/USD':
            0 if data['weight'] > 0
            else -data['weight'] * max(1.03 / self._collateralWeightInitial[coin+'/USD'] - 1,
                                       0.6 * self._imfFactor[coin + '/USD:USD'] * np.sqrt(
                                           abs(data['weight']) / data['mark']))
            for coin, data in weights.items()}

        return (collateral, im_short, mm_short)

    def shockedEstimate(self, x):
        ''' blowsup carry trade with a spike situation on the nb_blowups biggest deltas
         long: new freeColl = (1+ds)w-(ds+blow)-fut_mm(1+ds+blow)
         freeColl move = w ds-(ds+blow)-mm(ds+blow) = blow(1-mm) -ds(1-w+mm) ---> ~blow
         short: new freeColl = -(1+ds)+(ds+blow)-fut_mm(1+ds+blow)-spot_mm(1+ds)
         freeColl move = blow - fut_mm(ds+blow)-spot_mm ds = blow(1-fut_mm)-ds(fut_mm+spot_mm) ---> ~blow
        '''
        future_weights = {symbol: {'weight': -x[i], 'mark': self.future_marks[symbol]}
                          for i, symbol in enumerate(self.future_marks)}
        spot_weights = {coin: {'weight': x[i], 'mark': self.spot_marks[coin]}
                        for i, coin in enumerate(self.spot_marks)}
        usd_balance = self._equity - sum(x)

        # blowup symbols are _nb_blowups the biggest weights
        blowup_idx = np.argpartition(np.apply_along_axis(abs, 0, x),
                                     -self._nb_blowups)[-self._nb_blowups:]
        blowups = np.zeros(len(x))
        for j in range(len(blowup_idx)):
            i = blowup_idx[j]
            blowups[i] = x[i] * self._long_blowup if x[i] > 0 else -x[i] * self._short_blowup

        # assume all coins go either LONG_BLOWUP or SHORT_BLOWUP..what is the margin impact incl future pnl ?
        # up...
        future_up = {
            symbol: {'weight': data['weight'] * (1 + self._long_blowup), 'mark': data['mark'] * (1 + self._long_blowup)}
            for symbol, data in future_weights.items()}
        spot_up = {
            coin: {'weight': data['weight'] * (1 + self._long_blowup), 'mark': data['mark'] * (1 + self._long_blowup)}
            for coin, data in spot_weights.items()}
        (collateral_up, im_short_up, mm_short_up) = self.spotMargins(spot_up)
        (im_fut_up, mm_fut_up) = self.futureMargins(future_up)
        sum_MM_up = sum(x for x in collateral_up.values()) - \
                    sum(x for x in mm_fut_up.values()) - \
                    sum(x for x in mm_short_up.values()) - \
                    sum(x) * self._long_blowup  # add futures pnl

        # down...
        future_down = {
            symbol: {'weight': data['weight'] * (1 - self._short_blowup), 'mark': data['mark'] * (1 - self._short_blowup)}
            for symbol, data in future_weights.items()}
        spot_down = {
            coin: {'weight': data['weight'] * (1 - self._short_blowup), 'mark': data['mark'] * (1 - self._short_blowup)}
            for coin, data in spot_weights.items()}
        (collateral_down, im_short_down, mm_short_down) = self.spotMargins(spot_down)
        (im_fut_down, mm_fut_down) = self.futureMargins(future_down)
        sum_MM_down = sum(x for x in collateral_down.values()) - \
                      sum(x for x in mm_fut_down.values()) - \
                      sum(x for x in mm_short_down.values()) + \
                      sum(x) * self._short_blowup  # add the futures pnl

        # flat + a blowup_idx only shock
        (collateral, im_short, mm_short) = self.spotMargins(spot_weights)
        (im_fut, mm_fut) = self.futureMargins(future_weights)
        MM = pd.DataFrame([collateral]).T - pd.DataFrame([mm_short]).T
        MM = pd.concat([MM, -pd.DataFrame([mm_fut]).T])  # MM.append(-pd.DataFrame([mm_fut]).T)
        sum_MM = sum(MM[0]) - sum(blowups)  # add the futures pnl

        # aggregate
        IM = pd.DataFrame([collateral]).T - pd.DataFrame([im_short]).T
        IM = pd.concat([IM, -pd.DataFrame([im_fut]).T])  # IM.append(-pd.DataFrame([im_fut]).T)
        totalIM = usd_balance - 0.1 * max([0, -usd_balance]) + sum(
            IM[0]) - self._equity * self._open_order_headroom
        totalMM = usd_balance - 0.03 * max([0, -usd_balance]) + min([sum_MM, sum_MM_up, sum_MM_down])

        return {'totalIM': totalIM,
                'totalMM': totalMM,
                'IM': IM,
                'MM': MM}