from tradeexecutor.okx.api import OkxAPI
from utils.ftx_utils import *
from utils.config_loader import *

class MarginCalculator:
    '''low level class to compute margins
    weights is position size in usd
    '''
    def __init__(self, account_leverage, collateralWeight, imfFactor):  # collateralWeight,imfFactor by symbol
        # Load defaults params

        # TODO: self._account_leverage = account_leverage
        # self._collateralWeight = {f'{coin}/USD': collateralWeight[coin]
        #                           for coin, data in collateralWeight.items() } |{'USD/USD' :1.0}
        # self._imfFactor = imfFactor | {'USD/USD:USD':0.0}
        # self._collateralWeightInitial = {f'{coin}/USD': collateralWeightInitial
        #     ({'underlying': coin, 'collateralWeight': data})
        #                                  for coin, data in collateralWeight.items() } |{'USD/USD' :1.0}

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
        futures = pd.DataFrame(await OkxAPI.Static.fetch_futures(exchange))
        account_leverage = 0#TODO:  float(futures.iloc[0]['account_leverage'])
        collateralWeight = 0# TODO: futures.set_index('underlying')['collateralWeight'].to_dict()
        imfFactor = 0# TODO: futures.set_index('new_symbol')['imfFactor'].to_dict()
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

    async def update_actual(self, exchange: OkxAPI):
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
