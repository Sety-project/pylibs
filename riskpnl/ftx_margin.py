from utils.ftx_utils import *
from utils.config_loader import *

class MarginCalculator:
    '''low level class to compute margins
    weights is position size in usd
    '''
    def __init__(self, account_leverage, collateralWeight, imfFactor):  # collateralWeight,imfFactor by symbol
        # Load defaults params

        self._account_leverage = account_leverage
        self._collateralWeight = {f'{coin}/USD': collateralWeight[coin]
                                  for coin, data in collateralWeight.items() } |{'USD/USD' :1.0}
        self._imfFactor = imfFactor
        self._collateralWeightInitial = {f'{coin}/USD': collateralWeightInitial
            ({'underlying': coin, 'collateralWeight': data})
                                         for coin, data in collateralWeight.items() } |{'USD/USD' :1.0}
        self.estimated_IM = None        # dict
        self.estimated_MM = None        # dict
        self.actual_futures_IM = None   # dict, keys by id not symbol
        self.actual_IM = None           # float
        self.actual_MM = None           # float

        self.balances_and_positions = dict() # {symbol: {'size': size, 'spot_or_future': spot or future}}...includes USD/USD
        self.open_orders = dict() # {symbol: {'long':2,'short':1}}

    # --------------- slow explicit factory / reconcilators -------------

    @staticmethod
    async def margin_calculator_factory(exchange):
        '''factory from exchange'''
        # get static parameters
        futures = pd.DataFrame(await fetch_futures(exchange))
        account_leverage = float(futures.iloc[0]['account_leverage'])
        collateralWeight = futures.set_index('underlying')['collateralWeight'].to_dict()
        imfFactor = futures.set_index('new_symbol')['imfFactor'].to_dict()
        initialized = MarginCalculator(account_leverage, collateralWeight, imfFactor)

        await initialized.refresh(exchange)

        return initialized

    async def refresh(self,exchange, risks = None):
        await self.set_instruments(exchange,risks)
        self.set_open_orders(exchange)
        await self.update_actual(exchange)

    async def set_instruments(self ,exchange, risks = None):
        '''reset balances_and_positions and get from rest request'''
        self.balances_and_positions = dict()

        if risks is None:
            await safe_gather([exchange.fetch_balance(), exchange.fetch_positions()])
        balances = risks[0]
        positions = risks[1]
        for position in positions:
            if float(position['info']['netSize']) != 0:
                symbol = position['symbol']
                self.balances_and_positions[symbol] = {'size': float(position['info']['netSize']),
                                                       'spot_or_future': 'future'}
        for coin, balance in balances.items():
            if coin in exchange.currencies and balance['total'] != 0:
                self.balances_and_positions[f'{coin}/USD'] = {'size' :balance['total'],
                                                              'spot_or_future' :'spot'}

    def set_open_orders(self, exchange):
        '''reset open orders and add all orders by side'''
        self.open_orders = dict()

        for open_order_history in exchange.filter_order_histories(state_set=exchange.openStates):
            clientOrderId = open_order_history[-1]['clientOrderId']
            symbol = exchange.latest_value(clientOrderId, 'symbol')
            side = (1 if exchange.latest_value(clientOrderId, 'side') == 'buy' else -1)
            amount = exchange.latest_value(clientOrderId, 'remaining') * side
            instrument_type = exchange.market(symbol)['spot_or_future']
            self.add_open_order(symbol, side, amount, instrument_type)

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

    # --------------- quick update methods -------------

    def add_instrument(self ,symbol ,size ,instrument_type):
        '''size in coin'''
        increment = self.balances_and_positions[symbol]['size'] if symbol in self.balances_and_positions else 0
        self.balances_and_positions[symbol] = {'size' :size + increment,
                                               'spot_or_future' :instrument_type}

    def add_open_order(self, symbol, side, amount, instrument_type):
        if symbol not in self.open_orders:
            self.open_orders[symbol] = {'spot_or_future' :instrument_type, 'longs': 0, 'shorts': 0}
        self.open_orders[symbol]['longs' if side > 0 else 'shorts'] += amount

    # --------------- calculators -------------

    def future_IM(self ,symbol ,size ,mark):
        amount = np.abs(size)
        return - amount * max(1.0 / self._account_leverage,
                              self._imfFactor[symbol] * np.sqrt(amount / mark))
    def future_MM(self ,symbol ,size ,mark):
        amount = np.abs(size)
        return min(- 0.03 * amount, 0.6 * self.future_IM(symbol ,size ,mark))
    def spot_IM(self ,symbol ,size ,mark):
        amount = np.abs(size)
        # https://help.ftx.com/hc/en-us/articles/360031149632
        if size < 0:
            collateral = amount
            im_short = -amount * max(1.1 / self._collateralWeightInitial[symbol] - 1,
                                     self._imfFactor[symbol] * np.sqrt(amount / mark))
        else:
            collateral = amount * min(self._collateralWeight[symbol],
                                      1.1 / (1 + self._imfFactor[symbol] * np.sqrt(amount / mark)))
            im_short = 0
        return collateral - im_short
    def spot_MM(self, symbol, size, mark):
        # https://help.ftx.com/hc/en-us/articles/360053007671-Spot-Margin-Trading-Explainer
        return min(- 0.03 * np.abs(size), 0.6 * self.future_IM(symbol, size, mark))

    def estimate(self, exchange, IM_or_MM, return_details = False):
        '''returns {symbol:margin}'''

        # first, compute margin for existing positions
        margins = {symbol :self.__getattribute__(self ,instrument['spot_or_future' ] +'_ ' +IM_or_MM)(
            symbol,
            instrument['size'],
            exchange.mid(symbol))
            for symbol ,instrument in self.balances_and_positions.items()}

        # modify for open orders
        for symbol ,open_order in self.open_orders.items():
            size_list = []
            size_list[0] = self.balances_and_positions[symbol]['size'] \
                if symbol in self.balances_and_positions else 0
            size_list[1] = size_list[0] + open_order['longs']
            size_list[2] = size_list[0] - open_order['shorts']

            spot_or_future = 'spot' if exchange.markets[symbol]['spot'] else 'future'
            margins[symbol] = min(MarginCalculator.__getattribute__(self, spot_or_future + '_' + IM_or_MM)(
                symbol, _size, exchange.mid(symbol)) for _size in size_list)

        return margins if return_details else sum(margins.values())

    def order_marginal_cost(self, symbol, size, mid, IM_or_MM, spot_or_future):
        '''margin impact of an order'''

        size_list_before = []
        size_list_before[0] = self.balances_and_positions[symbol]['size'] \
            if symbol in self.balances_and_positions else 0
        size_list_before[1] = size_list_before[0] + \
                              (self.open_orders[symbol]['longs'] if symbol in self.open_orders else 0)
        size_list_before[2] = size_list_before[0] - \
                              (self.open_orders[symbol]['shorts'] if symbol in self.open_orders else 0)

        size_list_after = size_list_before + [size] * 3

        new_margin = min(MarginCalculator.__getattribute__(self, spot_or_future + '_' + IM_or_MM)(
            symbol, _size, mid) for _size in size_list_after)
        old_margin = min(MarginCalculator.__getattribute__(self, spot_or_future + '_' + IM_or_MM)(
            symbol, _size, mid) for _size in size_list_before)

        return new_margin - old_margin

class BasisMarginCalculator(MarginCalculator):
    '''implement specific contraints for carry trades, w/risk mgmt
    x will be spot weights in USD
    spot_marks, future_marks'''

    def __init__(self, account_leverage, collateralWeight, imfFactor,
                 equity, spot_marks, future_marks,
                 long_blowup=None, short_blowup=None, nb_blowups=None):
        super().__init__(account_leverage, collateralWeight, imfFactor)

        if long_blowup is None:
            long_blowup = self.LONG_BLOWUP
        if short_blowup is None:
            short_blowup = self.SHORT_BLOWUP
        if nb_blowups is None:
            nb_blowups = self.NB_BLOWUPS

        self._equity = equity
        self.spot_marks = spot_marks
        self.future_marks = future_marks
        self._long_blowup = float(long_blowup)
        self._short_blowup = float(short_blowup)
        self._nb_blowups = int(nb_blowups)

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
            symbol: {'weight': data['weight'] * (1 + self.LONG_BLOWUP), 'mark': data['mark'] * (1 + self.LONG_BLOWUP)}
            for symbol, data in future_weights.items()}
        spot_up = {
            coin: {'weight': data['weight'] * (1 + self.LONG_BLOWUP), 'mark': data['mark'] * (1 + self.LONG_BLOWUP)}
            for coin, data in spot_weights.items()}
        (collateral_up, im_short_up, mm_short_up) = self.spotMargins(spot_up)
        (im_fut_up, mm_fut_up) = self.futureMargins(future_up)
        sum_MM_up = sum(x for x in collateral_up.values()) - \
                    sum(x for x in mm_fut_up.values()) - \
                    sum(x for x in mm_short_up.values()) - \
                    sum(x) * self.LONG_BLOWUP  # add futures pnl

        # down...
        future_down = {
            symbol: {'weight': data['weight'] * (1 - self.SHORT_BLOWUP), 'mark': data['mark'] * (1 - self.SHORT_BLOWUP)}
            for symbol, data in future_weights.items()}
        spot_down = {
            coin: {'weight': data['weight'] * (1 - self.SHORT_BLOWUP), 'mark': data['mark'] * (1 - self.SHORT_BLOWUP)}
            for coin, data in spot_weights.items()}
        (collateral_down, im_short_down, mm_short_down) = self.spotMargins(spot_down)
        (im_fut_down, mm_fut_down) = self.futureMargins(future_down)
        sum_MM_down = sum(x for x in collateral_down.values()) - \
                      sum(x for x in mm_fut_down.values()) - \
                      sum(x for x in mm_short_down.values()) + \
                      sum(x) * self.SHORT_BLOWUP  # add the futures pnl

        # flat + a blowup_idx only shock
        (collateral, im_short, mm_short) = self.spotMargins(spot_weights)
        (im_fut, mm_fut) = self.futureMargins(future_weights)
        MM = pd.DataFrame([collateral]).T - pd.DataFrame([mm_short]).T
        MM = pd.concat([MM, -pd.DataFrame([mm_fut]).T])  # MM.append(-pd.DataFrame([mm_fut]).T)
        sum_MM = sum(MM[0]) - sum(blowups)  # add the futures pnl

        # aggregate
        IM = pd.DataFrame([collateral]).T - pd.DataFrame([im_short]).T
        IM = pd.concat([IM, -pd.DataFrame([im_fut]).T])  # IM.append(-pd.DataFrame([im_fut]).T)
        totalIM = self._equity - sum(x) - 0.1 * max([0, sum(x) - self._equity]) + sum(
            IM[0]) - self._equity * self.OPEN_ORDERS_HEADROOM
        totalMM = self._equity - sum(x) - 0.03 * max([0, sum(x) - self._equity]) + min([sum_MM, sum_MM_up, sum_MM_down])

        return {'totalIM': totalIM,
                'totalMM': totalMM,
                'IM': IM,
                'MM': MM}