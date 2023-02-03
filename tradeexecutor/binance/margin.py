from tradeexecutor.binance.api import BinanceAPI
from utils.ftx_utils import *
from utils.config_loader import *

class MarginCalculator:
    ## TODO: confusing! https://www.binance.com/en/support/faq/binance-portfolio-margin-account-maintenance-margin-ratio-calculation-5e33af8e6ac04d57a1d0a3398e0a0752
    '''low level class to compute margins
    weights is position size in usd
    '''
    def __init__(self, maintMarginPercent, requiredMarginPercent, PortfolioCollateralRate):  # collateralWeight,imfFactor by symbol
        # Load defaults params

        # TODO: self._account_leverage = account_leverage
        self._maintMarginPercent = maintMarginPercent
        self._requiredMarginPercent = requiredMarginPercent
        self._PortfolioCollateralRate = PortfolioCollateralRate

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
        futures = pd.DataFrame(await exchange.fetch_futures())

        maintMarginPercent = futures['maintMarginPercent'].to_dict()
        requiredMarginPercent = futures['requiredMarginPercent'].to_dict()
        PortfolioCollateralRate = futures['PortfolioCollateralRate'].to_dict()

        initialized = MarginCalculator(maintMarginPercent, requiredMarginPercent, PortfolioCollateralRate)
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
            balances = exchange.state.balances
            positions = exchange.state.positions

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

        exchange.options['warnOnFetchOpenOrdersWithoutSymbol'] = False
        external_orders = sum([await exchange.fetch_open_orders(params={'type':instrument_type}) for instrument_type in ['future','delivery','margin']],[])
        for order in external_orders:
            self.add_open_order(order)

    async def update_actual(self, exchange: BinanceAPI):
        account_information = await exchange.fetch_account_positions(params={'all':True})

        self.actual_futures_IM = {position['symbol']: position['initialMargin']
                                  for position in account_information['positions'] if position['contracts'] != 0}
        self.actual_IM = account_information['totalInitialMargin']
        self.actual_MM = account_information['totalMaintMargin']
        pv = float(account_information['totalWalletBalance'])

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
        return - mark * amount * self._requiredMarginPercent[symbol]
    def swap_MM(self ,symbol ,size ,mark):
        amount = np.abs(size)
        return amount * mark * self._maintMarginPercent[symbol]
    def spot_IM(self ,symbol ,size ,mark):
        if size < 0:
            collateral = size
            im_short = size * 0.2
        else:
            collateral = size * self._PortfolioCollateralRate[symbol]
            im_short = 0
        return mark * (collateral + im_short)
    def spot_MM(self, symbol, size, mark):
        if size < 0:
            collateral = size
            mm_short = size * 0.1
        else:
            collateral = size * self._PortfolioCollateralRate[symbol]
            mm_short = 0
        return mark * (collateral + mm_short)

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

    def __init__(self, maintMarginPercent, requiredMarginPercent, PortfolioCollateralRate,
                 equity, spot_marks, future_marks):
        super().__init__(maintMarginPercent, requiredMarginPercent, PortfolioCollateralRate)

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
            abs(data['weight']) * (self._requiredMarginPercent[symbol] if symbol in self._requiredMarginPercent else 99)
            for symbol, data in weights.items()}
        mm_fut = {symbol:
            abs(data['weight']) * (self._maintMarginPercent[symbol] if symbol in self._maintMarginPercent else 99)
            for symbol, data in weights.items()}

        return (im_fut, mm_fut)

    def spotMargins(self, weights):
        collateral = {coin+'/USDT':
            data['weight'] if data['weight'] < 0
            else data['weight'] * (self._PortfolioCollateralRate[coin+'/USDT:USDT'] if (coin+'/USDT:USDT') in self._PortfolioCollateralRate else 0.0)
            for coin, data in weights.items()}
        im_short = {coin+'/USDT':
            0 if data['weight'] > 0
            else -data['weight'] * 0.2
            for coin, data in weights.items()}
        mm_short = {coin+'/USDT':
            0 if data['weight'] > 0
            else -data['weight'] * 0.1
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
        totalIM = usd_balance - 0.2 * max([0, -usd_balance]) + sum(
            IM[0]) - self._equity * self._open_order_headroom
        totalMM = usd_balance - 0.1 * max([0, -usd_balance]) + min([sum_MM, sum_MM_up, sum_MM_down])

        return {'totalIM': totalIM,
                'totalMM': totalMM,
                'IM': IM,
                'MM': MM}