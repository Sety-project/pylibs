import asyncio, urllib
import collections, itertools, functools, json
from abc import abstractmethod
from datetime import timezone, datetime
import dateutil

import numpy as np
import pandas as pd

import ccxtpro
from web3 import Web3
from utils.async_utils import safe_gather, async_wrap, safe_gather_limit
from utils.ccxt_utilities import api_params, calc_basis
from utils.config_loader import configLoader
from utils.ftx_utils import getUnderlyingType
from utils.io_utils import myUtcNow
from tradeexecutor.interface.StrategyEnabler import StrategyEnabler

def loop(func):
    @functools.wraps(func)
    async def wrapper_loop(*args, **kwargs):
        self = args[0].strategy
        while True:
            try:
                value = await func(*args, **kwargs)
            except ccxtpro.NetworkError as e:
                self.logger.info(str(e))
                self.logger.info('reconciling after '+func.__name__+' dropped off')
                await self.reconcile()
            except Exception as e:
                self.logger.info(e, exc_info=True)
                raise e
    return wrapper_loop

def intercept_message_during_reconciliation(wrapped):
    '''decorates self.watch_xxx(message) to block incoming messages during reconciliation'''
    @functools.wraps(wrapped)
    def _wrapper(*args, **kwargs):
        self=args[0]
        if not isinstance(self, VenueAPI):
            raise Exception("only apply on venueAPI methods")
        if self.strategy.lock[f'reconciling'].locked():
            self.strategy.logger.warning(f'message during reconciliation{args[2]}')
            self.message_missed.append(args[2])
        else:
            return wrapped(*args, **kwargs)
    return _wrapper


class PegRule:
    Actions = {'repeg','stopout'}
    def __call__(self, input):
        raise NotImplementedError

class Chase(PegRule):
    def __init__(self,low,high):
        self.low = low
        self.high = high
    def __call__(self, input):
        if self.low < input < self.high:
            return None
        else:
            return 'repeg'

class PegOrStop(PegRule):
    def __init__(self,direction,chase,stop):
        self.direction = direction
        self.chase = chase
        self.stop = stop
    def __call__(self, input):
        if self.direction == 'buy':
            if input > self.stop: return 'stop'
            elif input > self.chase: return 'repeg'
            else: return None
        else:
            if input < self.stop: return 'stop'
            elif input < self.chase: return 'repeg'
            else: return None

class VenueAPI(StrategyEnabler):
    cache_size = 100000

    def __init__(self,parameters):
        super().__init__(parameters)
        self.static = None
        self.message_missed = collections.deque(maxlen=VenueAPI.cache_size)
        self.state = None
    @abstractmethod
    def get_id(self):
        raise NotImplementedError
    @abstractmethod
    async def reconcile(self):
        raise NotImplementedError
    def serialize(self):
        raise NotImplementedError

    @staticmethod
    async def build(parameters):
        if parameters['exchange'] == 'ftx':
            exchange = FtxAPI(parameters)
            exchange.verbose = False
            if 'subaccount' in parameters: {'FTX-SUBACCOUNT': parameters['subaccount'] if 'subaccount' in parameters else 'SysPerp'}
            await exchange.authenticate()
            await exchange.load_markets()
            if 'symbol' in parameters:
                symbols = parameters['symbols']
            else:
                futures = await FtxAPI.Static.fetch_futures(exchange)
                future_symbols = [future['new_symbol'] for future in futures]
                spot_symbols = [future['spot_ticker'] for future in futures if future['spot_ticker'] in exchange.markets]
                symbols = future_symbols + spot_symbols
            exchange.static = await FtxAPI.Static.build(exchange, symbols)
        elif parameters['exchange'] == 'binanceusdm':
            exchange = BinanceAPI(parameters)
            exchange.verbose = False
            #if 'subaccount' in parameters: {'FTX-SUBACCOUNT': parameters['subaccount'] if 'subaccount' in parameters else 'SysPerp'}
            await exchange.authenticate()
            await exchange.load_markets()
            if 'symbol' in parameters:
                symbols = parameters['symbols']
            else:
                futures = await BinanceAPI.Static.fetch_futures(exchange)
                future_symbols = [future['symbol'] for future in futures]
                spot_symbols = [] #[future['spot_ticker'] for future in futures if future['spot_ticker'] in exchange.markets]
                symbols = future_symbols + spot_symbols
            exchange.static = await BinanceAPI.Static.build(exchange, symbols)
        elif parameters['exchange'] == 'gmx':
            exchange = GmxAPI(parameters)
        else:
            raise NotImplementedError

        return exchange

class CeFiAPI(VenueAPI):
    '''VenueAPI implements rest calls and websocket loops to observe raw market data / order events and place orders
    send events for Strategy to action
    send events to SignalEngine for further processing'''

    class State:
        def __init__(self):
            self.markets: dict[str, float] = dict()
            self.balances: dict[str, float] = dict()
            self.positions: dict[str, float] = dict()

    # --------------------------------------------------------------------------------------------
    # ---------------------------------- various helpers -----------------------------------------
    # --------------------------------------------------------------------------------------------

    def mid(self,symbol):
        if symbol == 'USD/USD': return 1.0
        data = self.tickers[symbol] if symbol in self.tickers else self.markets[symbol]['info']
        return 0.5*(float(data['bid'])+float(data['ask']))

    def sweep_price_atomic(self, symbol, sizeUSD, include_taker_vs_maker_fee = False):
        ''' fast version of mkt_at_size for use in executer
        slippage of a mkt order: https://www.sciencedirect.com/science/article/pii/S0378426620303022
        :param symbol: '''
        book_on_side = self.orderbooks[symbol]['bids' if sizeUSD < 0 else 'asks']
        depth = 0
        for pair in book_on_side:
            depth += pair[0] * pair[1]
            if depth > sizeUSD:
                break
        res = pair[0]
        if include_taker_vs_maker_fee:
            res += res * self.static[symbol]['takerVsMakerFee'] * (1 if sizeUSD > 0 else -1)
        return res
    # --------------------------------------------------------------------------------------------
    # ---------------------------------- WS loops, processors and message handlers ---------------
    # --------------------------------------------------------------------------------------------

    # ---------------------------------- orderbook_update

    @loop
    async def monitor_order_book(self, symbol):
        '''on each top of book update, update market_state and send orders
        tunes aggressiveness according to risk.
        populates event_records, maintains pending_new
        no lock is done, so we keep collecting mktdata'''
        orderbook = await self.watch_order_book(symbol)
        self.populate_ticker(symbol, orderbook)
        if hasattr(self.strategy,'process_order_book_update'):
            await getattr(self.strategy,'process_order_book_update')(symbol, orderbook)

    def populate_ticker(self,symbol,orderbook):
        self.tickers[symbol] = {'symbol': symbol,
                                'timestamp': orderbook['timestamp'] * 1000,
                                'bid': orderbook['bids'][0][0],
                                'ask': orderbook['asks'][0][0],
                                'mid': 0.5 * (orderbook['bids'][0][0] + orderbook['asks'][0][0]),
                                'bidVolume': orderbook['bids'][0][1],
                                'askVolume': orderbook['asks'][0][1]}

    # ---------------------------------- fills

    @loop
    async def monitor_fills(self):
        '''maintains risk_state, event_records, logger.info
            #     await self.reconcile_state() is safer but slower. we have monitor_risk to reconcile'''
        fills = await self.watch_my_trades()
        if not self.strategy.lock[f'reconciling'].locked():
            for fill in fills:
                if hasattr(self.strategy, 'process_fill'):
                    await getattr(self.strategy, 'process_fill')(fill)

    # ---------------------------------- orders

    @loop
    async def monitor_orders(self,symbol):
        '''maintains orders, pending_new, event_records'''
        orders = await self.watch_orders(symbol=symbol)
        if not self.strategy.lock[f'reconciling'].locked():
            for order in orders:
                if hasattr(self.strategy, 'process_order'):
                    await getattr(self.strategy, 'process_order')(order | {'comment': 'websocket_acknowledgment'})

    # ---------------------------------- misc

    async def watch_ticker(self, symbol, params=dict()):
        '''watch_order_book is faster than watch_tickers so we DON'T LISTEN TO TICKERS. Dirty...'''
        raise Exception("watch_order_book is faster than watch_tickers so we DON'T LISTEN TO TICKERS. Dirty...")

    @loop
    async def monitor_trades(self,symbol):
        '''maintains risk_state, event_records, logger.info
            #     await self.reconcile_state() is safer but slower. we have monitor_risk to reconcile'''
        trades = await self.watch_trades(symbol=symbol)
        if hasattr(self.strategy, 'process_trade'):
            for trade in trades:
                await getattr(self.strategy, 'process_trade')(trade)

    # ---------------------------------- just implemented so we hold messages while reconciling
    @intercept_message_during_reconciliation
    def handle_my_trade(self, client, message):
        '''just implemented so we hold messages while reconciling'''
        super().handle_my_trade(client, message)

    @intercept_message_during_reconciliation
    def handle_order(self, client, message):
        '''just implemented so we hold messages while reconciling'''
        super().handle_order(client, message)

    def round_to_increment(self, sizeIncrement, amount):
        if amount >= 0:
            return np.floor(amount/sizeIncrement) * sizeIncrement
        else:
            return -np.floor(-amount / sizeIncrement) * sizeIncrement

# class FtxAPI(CeFiAPI,ccxtpro.ftx):
#     '''VenueAPI implements rest calls and websocket loops to observe raw market data / order events and place orders
#     send events for Strategy to action
#     send events to SignalEngine for further processing'''
#
#     def get_id(self):
#         return "ftx"
#
#     class Static(dict):
#         _cache = dict()  # {function_name: result}
#
#         @staticmethod
#         async def build(exchange, symbols):
#             result = FtxAPI.Static()
#             trading_fees = await exchange.fetch_trading_fees()
#             for symbol in symbols:
#                 result[symbol] = \
#                     {
#                         'priceIncrement': float(exchange.markets[symbol]['info']['priceIncrement']),
#                         'sizeIncrement': float(exchange.markets[symbol]['info']['minProvideSize']),
#                         'taker_fee': trading_fees[symbol]['taker'],
#                         'maker_fee': trading_fees[symbol]['maker'],
#                         'takerVsMakerFee': trading_fees[symbol]['taker'] - trading_fees[symbol]['maker']
#                     }
#             return result
#
#        ### get all static fields TODO: could just append coindetails if it wasn't for index,imf factor,positionLimitWeight
#         @staticmethod
#         async def fetch_futures(exchange):
#             if 'fetch_futures' in FtxAPI.Static._cache:
#                 return FtxAPI.Static._cache['fetch_futures']
#
#             includeExpired = True
#             includeIndex = False
#
#             response = await exchange.publicGetFutures()
#             expired = await exchange.publicGetExpiredFutures() if includeExpired == True else []
#             coin_details = await FtxAPI.Static.fetch_coin_details(exchange)
#
#             otc_file = configLoader.get_static_params_used()
#
#             #### for IM calc
#             account_leverage = (await exchange.privateGetAccount())['result']
#             if float(account_leverage['leverage']) >= 50: print("margin rules not implemented for leverage >=50")
#
#             markets = exchange.safe_value(response, 'result', []) + exchange.safe_value(expired, 'result', [])
#
#             perp_list = [f['name'] for f in markets if f['type'] == 'perpetual' and f['enabled']]
#             funding_rates = await safe_gather([exchange.publicGetFuturesFutureNameStats({'future_name': f})
#                                                for f in perp_list])
#             funding_rates = {name: float(rate['result']['nextFundingRate']) * 24 * 365.325 for name, rate in
#                              zip(perp_list, funding_rates)}
#
#             result = []
#             for i in range(0, len(markets)):
#                 market = markets[i]
#                 underlying = exchange.safe_string(market, 'underlying')
#                 ## eg ADA has no coin details
#                 if (underlying in ['ROSE', 'SCRT', 'AMC']) \
#                         or (exchange.safe_string(market, 'tokenizedEquity') == True) \
#                         or (exchange.safe_string(market, 'type') in ['move', 'prediction']) \
#                         or (exchange.safe_string(market, 'enabled') == False):
#                     continue
#                 if not underlying in coin_details.index:
#                     if not includeIndex: continue
#                 try:  ## eg DMG-PERP doesn't exist (IncludeIndex = True)
#                     symbol = exchange.market(exchange.safe_string(market, 'name'))['symbol']
#                 except Exception as e:
#                     continue
#
#                 mark = exchange.safe_number(market, 'mark')
#                 imfFactor = exchange.safe_number(market, 'imfFactor')
#                 expiryTime = dateutil.parser.isoparse(exchange.safe_string(market, 'expiry')).replace(
#                     tzinfo=timezone.utc) if exchange.safe_string(market, 'type') == 'future' else np.NaN
#                 if exchange.safe_string(market, 'type') == 'future':
#                     future_carry = calc_basis(mark, market['index'], expiryTime,
#                                               datetime.utcnow().replace(tzinfo=timezone.utc))
#                 elif market['name'] in perp_list:
#                     future_carry = funding_rates[exchange.safe_string(market, 'name')]
#                 else:
#                     future_carry = 0
#
#                 result.append({
#                     'ask': exchange.safe_number(market, 'ask'),
#                     'bid': exchange.safe_number(market, 'bid'),
#                     'change1h': exchange.safe_number(market, 'change1h'),
#                     'change24h': exchange.safe_number(market, 'change24h'),
#                     'changeBod': exchange.safe_number(market, 'changeBod'),
#                     'volumeUsd24h': exchange.safe_number(market, 'volumeUsd24h'),
#                     'volume': exchange.safe_number(market, 'volume'),
#                     'symbol': exchange.safe_string(market, 'name'),
#                     "enabled": exchange.safe_value(market, 'enabled'),
#                     "expired": exchange.safe_value(market, 'expired'),
#                     "expiry": exchange.safe_string(market, 'expiry') if exchange.safe_string(market,
#                                                                                              'expiry') else 'None',
#                     'index': exchange.safe_number(market, 'index'),
#                     'imfFactor': exchange.safe_number(market, 'imfFactor'),
#                     'last': exchange.safe_number(market, 'last'),
#                     'lowerBound': exchange.safe_number(market, 'lowerBound'),
#                     'mark': exchange.safe_number(market, 'mark'),
#                     'name': exchange.safe_string(market, 'name'),
#                     "perpetual": exchange.safe_value(market, 'perpetual'),
#                     # 'positionLimitWeight': exchange.safe_value(market, 'positionLimitWeight'),
#                     # "postOnly": exchange.safe_value(market, 'postOnly'),
#                     'priceIncrement': exchange.safe_value(market, 'priceIncrement'),
#                     'sizeIncrement': exchange.safe_value(market, 'sizeIncrement'),
#                     'underlying': exchange.safe_string(market, 'underlying'),
#                     'upperBound': exchange.safe_value(market, 'upperBound'),
#                     'type': exchange.safe_string(market, 'type'),
#                     ### additionnals
#                     'new_symbol': exchange.market(exchange.safe_string(market, 'name'))['symbol'],
#                     'openInterestUsd': exchange.safe_number(market, 'openInterestUsd'),
#                     'account_leverage': float(account_leverage['leverage']),
#                     'collateralWeight': coin_details.loc[
#                         underlying, 'collateralWeight'] if underlying in coin_details.index else 'coin_details not found',
#                     'underlyingType': getUnderlyingType(
#                         coin_details.loc[underlying]) if underlying in coin_details.index else 'index',
#                     'spot_ticker': exchange.safe_string(market, 'underlying') + '/USD',
#                     'cash_borrow': coin_details.loc[underlying, 'borrow'] if underlying in coin_details.index and
#                                                                              coin_details.loc[
#                                                                                  underlying, 'spotMargin'] else None,
#                     'future_carry': future_carry,
#                     'spotMargin': 'OTC' if underlying in otc_file.index else (coin_details.loc[
#                                                                                   underlying, 'spotMargin'] if underlying in coin_details.index else 'coin_details not found'),
#                     'tokenizedEquity': coin_details.loc[
#                         underlying, 'tokenizedEquity'] if underlying in coin_details.index else 'coin_details not found',
#                     'usdFungible': coin_details.loc[
#                         underlying, 'usdFungible'] if underlying in coin_details.index else 'coin_details not found',
#                     'fiat': coin_details.loc[
#                         underlying, 'fiat'] if underlying in coin_details.index else 'coin_details not found',
#                     'expiryTime': expiryTime
#                 })
#
#             FtxAPI.Static._cache['fetch_futures'] = result
#             return result
#
#         @staticmethod
#         async def fetch_coin_details(exchange):
#             if 'fetch_coin_details' in FtxAPI.Static._cache:
#                 return FtxAPI.Static._cache['fetch_coin_details']
#
#             coin_details = pd.DataFrame((await exchange.publicGetWalletCoins())['result']).astype(
#                 dtype={'collateralWeight': 'float', 'indexPrice': 'float'}).set_index('id')
#
#             borrow_rates = pd.DataFrame((await exchange.private_get_spot_margin_borrow_rates())['result']).astype(
#                 dtype={'coin': 'str', 'estimate': 'float', 'previous': 'float'}).set_index('coin')[['estimate']]
#             borrow_rates[['estimate']] *= 24 * 365.25
#             borrow_rates.rename(columns={'estimate': 'borrow'}, inplace=True)
#
#             lending_rates = pd.DataFrame((await exchange.private_get_spot_margin_lending_rates())['result']).astype(
#                 dtype={'coin': 'str', 'estimate': 'float', 'previous': 'float'}).set_index('coin')[['estimate']]
#             lending_rates[['estimate']] *= 24 * 365.25
#             lending_rates.rename(columns={'estimate': 'lend'}, inplace=True)
#
#             borrow_volumes = pd.DataFrame((await exchange.public_get_spot_margin_borrow_summary())['result']).astype(
#                 dtype={'coin': 'str', 'size': 'float'}).set_index('coin')
#             borrow_volumes.rename(columns={'size': 'borrow_open_interest'}, inplace=True)
#
#             all = pd.concat([coin_details, borrow_rates, lending_rates, borrow_volumes], join='outer', axis=1)
#             all = all.loc[coin_details.index]  # borrow summary has beed seen containing provisional underlyings
#             all.loc[coin_details['spotMargin'] == False, 'borrow'] = None  ### hope this throws an error...
#             all.loc[coin_details['spotMargin'] == False, 'lend'] = 0
#
#             FtxAPI.Static._cache['fetch_coin_details'] = all
#             return all
#
#     def __init__(self, parameters, private_endpoints=True):
#         config = {
#             'enableRateLimit': True,
#             'newUpdates': True}
#         if private_endpoints:  ## David personnal
#             config |= {'apiKey': 'ZUWyqADqpXYFBjzzCQeUTSsxBZaMHeufPFgWYgQU',
#             'secret': api_params['ftx']['key']}
#         super().__init__(parameters)
#         super(ccxtpro.ftx,self).__init__(config=config)
#         self.state = CeFiAPI.State()
#
#         self.options['tradesLimit'] = VenueAPI.cache_size # TODO: shoud be in signalengine with a different name. inherited from ccxt....
#
#         self.peg_rules: dict[str, PegRule] = dict()
#
#     async def reconcile(self):
#         # fetch mark,spot and balances as closely as possible
#         # shoot rest requests
#         n_requests = int(safe_gather_limit / 3)
#         p = [getattr(self, coro)(params={'dummy': i})
#              for i in range(n_requests)
#              for coro in ['fetch_markets', 'fetch_balance', 'fetch_positions']]
#         results = await safe_gather(p, semaphore=self.strategy.rest_semaphore)
#         # avg to reduce impact of latency
#         markets_list = []
#         for result in results[0::3]:
#             res = pd.DataFrame(result).set_index('id')
#             res['price'] = res['info'].apply(lambda f: float(f['price']) if f['price'] else -9999999)
#             markets_list.append(res[['price']])
#         markets = sum(markets_list) / len(markets_list)
#
#         balances_list = [pd.DataFrame(r['info']['result']).set_index('coin')[['total']].astype(float) for r in
#                          results[1::3]]
#         balances = sum(balances_list) / len(balances_list)
#         var = sum([bal * bal for bal in balances_list]) / len(balances_list) - balances * balances
#         balances = balances[balances['total'] != 0.0].fillna(0.0)
#
#         positions_list = [
#             pd.DataFrame([r['info'] for r in result]).set_index('future')[['netSize', 'unrealizedPnl']].astype(float)
#             for
#             result in results[2::3]]
#         positions = sum(positions_list) / len(positions_list)
#         var = sum([pos * pos for pos in positions_list]) / len(positions_list) - positions * positions
#         positions = positions[positions['netSize'] != 0.0].fillna(0.0)
#
#         self.state.markets = markets.to_dict()
#         self.state.balances = balances.to_dict()
#         self.state.positions = positions.to_dict()
#
#     async def peg_or_stopout(self, symbol, size, edit_trigger_depth=None, edit_price_depth=None, stop_depth=None):
#         size = self.round_to_increment(self.static[symbol]['sizeIncrement'], size)
#         if abs(size) == 0:
#             return
#
#         #TODO: https://help.ftx.com/hc/en-us/articles/360052595091-Ratelimits-on-FTX
#         opposite_side = self.tickers[symbol]['ask' if size>0 else 'bid']
#         mid = self.tickers[symbol]['mid']
#
#         priceIncrement = self.static[symbol]['priceIncrement']
#         sizeIncrement = self.static[symbol]['sizeIncrement']
#
#         if stop_depth is not None:
#             stop_trigger = float(self.price_to_precision(symbol,stop_depth))
#
#         #TODO: use orderbook to place before cliff; volume matters too.
#         isTaker = edit_price_depth in ['rush_in', 'rush_out', 'taker_hedge']
#         if not isTaker:
#             edit_price = float(self.price_to_precision(symbol, opposite_side - (1 if size > 0 else -1) * edit_price_depth))
#             edit_trigger = float(self.price_to_precision(symbol, edit_trigger_depth))
#         else:
#             edit_price = self.sweep_price_atomic(symbol, size * mid)
#             edit_trigger = None
#             self.strategy.logger.warning(f'{edit_price_depth} {size} {symbol}')
#
#         # remove open order dupes is any (shouldn't happen)
#         event_histories = self.strategy.order_manager.filter_order_histories([symbol], self.strategy.order_manager.openStates)
#         if len(event_histories) > 1:
#             first_pending_new = np.argmin(np.array([data[0]['timestamp'] for data in event_histories]))
#             for i,event_history in enumerate(self.strategy.order_manager.filter_order_histories([symbol], self.strategy.order_manager.cancelableStates)):
#                 if i != first_pending_new:
#                     await self.cancel_order(event_history[-1]['clientOrderId'],'duplicates')
#                     self.strategy.logger.info('canceled duplicate {} order {}'.format(symbol,event_history[-1]['clientOrderId']))
#
#         # skip if there is inflight on the spread
#         # if self.pending_new_histories(coin) != []:#TODO: rather incorporate orders_pending_new in risk, rather than block
#         #     if self.pending_new_histories(coin,symbol) != []:
#         #         self.strategy.logger.info('orders {} should not be in flight'.format([order['clientOrderId'] for order in self.pending_new_histories(coin,symbol)[-1]]))
#         #     else:
#         #         # this happens mostly between pending_new and create_order on the other leg. not a big deal...
#         #         self.strategy.logger.info('orders {} still in flight. holding off {}'.format(
#         #             [order['clientOrderId'] for order in self.pending_new_histories(coin)[-1]],symbol))
#         #     return
#         pending_new_histories = self.strategy.order_manager.filter_order_histories(self.parameters['symbols'],
#                                                                           ['pending_new'])
#         if pending_new_histories != []:
#             self.strategy.logger.info('orders {} should not be in flight'.format([order[-1]['clientOrderId'] for order in pending_new_histories]))
#             return
#
#         # if no open order, create an order
#         order_side = 'buy' if size>0 else 'sell'
#         if len(event_histories)==0:
#             await self.create_order(symbol, 'limit', order_side, abs(size), price=edit_price,
#                                                   params={'postOnly': not isTaker,
#                                                           'ioc': isTaker,
#                                                           'comment':edit_price_depth if isTaker else 'new'})
#         # if only one and it's editable, stopout or peg or wait
#         elif len(event_histories)==1 \
#                 and (self.strategy.order_manager.latest_value(event_histories[0][-1]['clientOrderId'], 'remaining') >= sizeIncrement) \
#                 and event_histories[0][-1]['state'] in self.strategy.order_manager.acknowledgedStates:
#             order = event_histories[0][-1]
#             order_distance = (1 if order['side'] == 'buy' else -1) * (opposite_side - order['price'])
#
#             # panic stop. we could rather place a trailing stop: more robust to latency, but less generic.
#             if (stop_depth and order_distance > stop_trigger) \
#                     or isTaker:
#                 size = self.strategy.order_manager.latest_value(order['clientOrderId'], 'remaining')
#                 price = self.sweep_price_atomic(symbol, size * mid)
#                 await self.create_order(symbol, 'limit', order_side, abs(size),
#                                                  price = price,
#                                                  params={'postOnly':False,
#                                                          'ioc':True,
#                                                          'comment':edit_price_depth if isTaker else 'stop'},
#                                                  previous_clientOrderId = order['clientOrderId'])
#             # peg limit order
#             elif order_distance > edit_trigger and abs(edit_price - order['price']) >= priceIncrement:
#                 await self.create_order(symbol, 'limit', order_side, abs(size),
#                                                 price=edit_price,
#                                                 params={'postOnly': True,
#                                                         'ioc':False,
#                                                         'comment':'chase'},
#                                                 previous_clientOrderId=order['clientOrderId'])
#
#     async def peg_to_level(self, symbol, size, target, edit_trigger_depth=None):
#         size = self.round_to_increment(self.static[symbol]['sizeIncrement'], size)
#         if abs(size) == 0:
#             return
#
#         #TODO: use orderbook to place before cliff; volume matters too.
#         pending_new_histories = self.strategy.order_manager.filter_order_histories(self.parameters['symbols'],
#                                                                           ['pending_new'])
#         if pending_new_histories != []:
#             self.strategy.logger.info('orders {} should not be in flight'.format([order[-1]['clientOrderId'] for order in pending_new_histories]))
#             return
#
#         event_histories = self.strategy.order_manager.filter_order_histories([symbol], self.strategy.order_manager.openStates)
#         # if no open order, create an order
#         if len(event_histories)==0:
#             await self.create_mkt_or_limit(symbol, size, target, 'new', edit_trigger_depth=edit_trigger_depth)
#         # editables: stopout or peg
#         for order in [history[-1] for history in event_histories
#                       if self.strategy.order_manager.latest_value(history[-1]['clientOrderId'], 'remaining') >= self.static[symbol]['sizeIncrement']
#                          and history[-1]['state'] in self.strategy.order_manager.cancelableStates
#                          and history[-1]['clientOrderId'] in self.peg_rules]:
#             if self.peg_rules[order['clientOrderId']](target) == 'repeg':
#                 if await self.cancel_order(order['clientOrderId'], 'edit'):
#                     size = self.strategy.order_manager.latest_value(order['clientOrderId'], 'remaining')
#                     await self.create_mkt_or_limit(symbol, size * (1 if order['side'] == 'buy' else -1), target, 'chase', edit_trigger_depth=edit_trigger_depth)
#
#     # ---------------------------------- low level
#
#     async def create_mkt_or_limit(self,symbol, size, target, comment, edit_trigger_depth=None):
#         '''mkt if price good enough, incl slippage and fees
#         limit otherwise. Mind not crossing.'''
#         order_side = 'buy' if size > 0 else 'sell'
#         opposite_side = self.tickers[symbol]['ask' if size > 0 else 'bid']
#         sweep_price = self.sweep_price_atomic(symbol, size * self.tickers[symbol]['mid'], include_taker_vs_maker_fee=True)
#         sweep_it = (sweep_price < target) if (size > 0) else (target < sweep_price)
#         if sweep_it:
#             await self.create_order(symbol, 'limit', order_side, abs(size), price=sweep_price,
#                                      params={'postOnly': False,
#                                              'ioc': True,
#                                              'comment': comment})
#         else:
#             price = target if size * (target - opposite_side) <= 0 else opposite_side
#             if size > 0:
#                 peg_rule = Chase(price - self.static[symbol]['priceIncrement'], price + edit_trigger_depth)
#             else:
#                 peg_rule = Chase(price - edit_trigger_depth, price + self.static[symbol]['priceIncrement'])
#             await self.create_order(symbol, 'limit', order_side, abs(size), price=price,
#                                     params={'postOnly': True,
#                                             'ioc': False,
#                                             'comment': comment},
#                                     peg_rule=peg_rule)
#
#     async def create_taker_hedge(self,symbol, size, comment='taker_hedge'):
#         '''trade and cancel. trade may be partial and cancel may have failed !!'''
#         sweep_price = self.sweep_price_atomic(symbol, size * self.mid(symbol))
#         coro = [self.create_order(symbol, 'limit', ('buy' if size>0 else 'sell'), abs(size), price=sweep_price,
#                                                         params={'postOnly': False,
#                                                                 'ioc': False,
#                                                                 'comment': comment})]
#         cancelable_orders = self.strategy.order_manager.filter_order_histories([symbol],
#                                                                          self.strategy.order_manager.cancelableStates)
#         coro += [self.cancel_order(order[-1]['clientOrderId'], 'cancel_symbol')
#                                for order in cancelable_orders]
#         await safe_gather(coro, semaphore=self.strategy.rest_semaphore)
#
#     async def create_order(self, symbol, type, side, amount, price=None, params=dict(),previous_clientOrderId=None,peg_rule: PegRule=None):
#         '''if not new, cancel previous first
#         if acknowledged, place order. otherwise just reconcile
#         orders_pending_new is blocking'''
#         if previous_clientOrderId is not None:
#             await self.cancel_order(previous_clientOrderId, 'edit')
#
#         trimmed_size = self.strategy.position_manager.trim_to_margin({symbol:amount * (1 if side == 'buy' else -1)})[symbol]
#         rounded_amount = self.round_to_increment(self.static[symbol]['sizeIncrement'], abs(trimmed_size))
#         if rounded_amount < self.static[symbol]['sizeIncrement']:
#             return
#         # set pending_new -> send rest -> if success, leave pending_new and give id. Pls note it may have been caught by handle_order by then.
#         clientOrderId = self.strategy.order_manager.pending_new({'symbol': symbol,
#                                                     'type': type,
#                                                     'side': side,
#                                                     'amount': rounded_amount,
#                                                     'remaining': rounded_amount,
#                                                     'price': price,
#                                                     'comment': params['comment']})
#         try:
#             # REST request
#             order = await super().create_order(symbol, type, side, rounded_amount, price, {'clientOrderId':clientOrderId} | params)
#         except Exception as e:
#             order = {'clientOrderId':clientOrderId,
#                      'timestamp':myUtcNow(),
#                      'state':'rejected',
#                      'comment':'create/'+str(e)}
#             self.strategy.order_manager.cancel_or_reject(order)
#             if isinstance(e,ccxtpro.InsufficientFunds):
#                 self.strategy.logger.info(f'{clientOrderId} too big: {rounded_amount*self.mid(symbol)}')
#             elif isinstance(e,ccxtpro.RateLimitExceeded):
#                 throttle = 200.0
#                 self.strategy.logger.info(f'{str(e)}: waiting {throttle} ms)')
#                 await asyncio.sleep(throttle / 1000)
#             else:
#                 raise e
#         else:
#             self.strategy.order_manager.sent(order)
#             if peg_rule is not None:
#                 self.peg_rules[clientOrderId] = peg_rule
#
#     async def cancel_order(self, clientOrderId, trigger):
#         '''set in flight, send cancel, set as pending cancel, set as canceled or insist'''
#         symbol = clientOrderId.split('_')[1]
#         self.strategy.order_manager.pending_cancel({'comment':trigger}
#                                           | {key: [order[key] for order in self.strategy.order_manager.data[clientOrderId] if key in order][-1]
#                                         for key in ['clientOrderId','symbol','side','amount','remaining','price']})  # may be needed
#
#         try:
#             status = await super().cancel_order(None,params={'clientOrderId':clientOrderId})
#             self.strategy.order_manager.cancel_sent({'clientOrderId':clientOrderId,
#                                         'symbol':symbol,
#                                         'status':status,
#                                         'comment':trigger})
#         except ccxtpro.CancelPending as e:
#             self.strategy.order_manager.cancel_sent({'clientOrderId': clientOrderId,
#                                         'symbol': symbol,
#                                         'status': str(e),
#                                         'comment': trigger})
#             return True
#         except ccxtpro.InvalidOrder as e: # could be in flight, or unknown
#             self.strategy.order_manager.cancel_or_reject({'clientOrderId':clientOrderId,
#                                              'status':str(e),
#                                              'state':'canceled',
#                                              'comment':trigger})
#             return False
#         except Exception as e:
#             self.strategy.logger.info(f'{clientOrderId} failed to cancel: {str(e)} --> retrying')
#             await asyncio.sleep(CeFiAPI.delay())
#             return await self.cancel_order(clientOrderId, trigger+'+')
#         else:
#             return True
#
#     async def close_dust(self):
#         data = await safe_gather([self.fetch_balance(),self.fetch_markets()], semaphore=self.strategy.rest_semaphore)
#         balances = data[0]
#         markets = data[1]
#         coros = []
#         for coin, balance in balances.items():
#             if coin in self.currencies.keys() \
#                     and coin != 'USD' \
#                     and balance['total'] != 0.0 \
#                     and abs(balance['total']) < self.static[coin+'/USD']['sizeIncrement']:
#                 size = balance['total']
#                 mid = float(self.markets[coin+'/USD']['info']['last'])
#                 if size > 0:
#                     request = {'fromCoin':coin,'toCoin':'USD','size':abs(size)}
#                 else:
#                     request = {'fromCoin':'USD','toCoin':coin,'size':abs(size)*self.mid(f'{coin}/USD')}
#                 coros += [self.privatePostOtcQuotes(request)]
#
#         quoteId_list = await safe_gather(coros, semaphore=self.strategy.rest_semaphore)
#         await safe_gather([self.privatePostOtcQuotesQuoteIdAccept({'quoteId': int(quoteId['result']['quoteId'])}, semaphore=self.strategy.rest_semaphore)
#         for quoteId in quoteId_list
#         if quoteId['success']], semaphore=self.strategy.rest_semaphore)

class BinanceAPI(CeFiAPI,ccxtpro.binanceusdm):
    '''VenueAPI implements rest calls and websocket loops to observe raw market data / order events and place orders
    send events for Strategy to action
    send events to SignalEngine for further processing'''

    def get_id(self):
        return "binance"

    class Static(dict):
        _cache = dict()  # {function_name: result}

        @staticmethod
        async def build(exchange, symbols):
            result = BinanceAPI.Static()
            trading_fees = await exchange.fetch_trading_fees()
            for symbol in symbols:
                market = exchange.market(symbol)
                result[symbol] = \
                    {
                        'priceIncrement': float(next(_filter for _filter in market['info']['filters']
                                           if _filter['filterType'] == 'PRICE_FILTER')['tickSize']),
                        'sizeIncrement': float(next(_filter for _filter in market['info']['filters']
                                          if _filter['filterType'] == 'LOT_SIZE')['stepSize']),
                        'taker_fee': trading_fees[symbol]['taker'],
                        'maker_fee': trading_fees[symbol]['maker'],
                        'takerVsMakerFee': trading_fees[symbol]['taker'] - trading_fees[symbol]['maker']
                    }
            return result

       ### get all static fields TODO: only works for perps
        @staticmethod
        async def fetch_futures(exchange):
            if 'fetch_futures' in BinanceAPI.Static._cache:
                return BinanceAPI.Static._cache['fetch_futures']

            includeExpired = False
            includeIndex = False
            includeInverse = False

            perp_markets = await exchange.fetch_markets({'type': 'future'})
            #future_markets = await exchange.fetch_markets({'type': 'delivery'})
            margin_markets = await exchange.fetch_markets({'type': 'margin'})

            future_list = [f for f in perp_markets
                           if (('status' in f['info'] and f['info']['status'] == 'TRADING')) # only for linear perps it seems
                           and (f['info']['underlyingType'] == 'COIN' or includeIndex)]

            otc_file = configLoader.get_static_params_used()

            perp_list = [f['id'] for f in future_list if f['expiry'] == None]
            funding_rates = await safe_gather([exchange.fetch_funding_rate(symbol=f) for f in perp_list])
            # perp_tickers = await exchange.fetch_tickers(symbols=[f['id'] for f in future_list], params={'type':'future'})
            # delivery_tickers = await exchange.fetch_tickers(symbols=[f['id'] for f in future_list], params={'type': 'delivery'})
            open_interests = await safe_gather([exchange.fapiPublicGetOpenInterest({'symbol':f}) for f in perp_list])

            result = []
            for i in range(0, len(funding_rates)):
                funding_rate = funding_rates[i]
                market = next(m for m in perp_markets if m['symbol'] == funding_rate['symbol'])
                margin_market = next((m for m in margin_markets if m['symbol'] == funding_rate['symbol']), None)
                open_interest = next((m for m in open_interests if m['symbol'] == market['id']), None)

                index = funding_rate['indexPrice']
                mark = funding_rate['markPrice']
                expiryTime = dateutil.parser.isoparse(market['expiryDatetime']).replace(
                    tzinfo=timezone.utc) if bool(market['delivery']) else np.NaN
                if bool(market['delivery']):
                    future_carry = calc_basis(mark, index, expiryTime,
                                              datetime.utcnow().replace(tzinfo=timezone.utc))
                elif bool(market['swap']):
                    future_carry = funding_rate['fundingRate']*3*365.25
                else:
                    future_carry = 0

                result.append({
                    'symbol': exchange.safe_string(market, 'symbol'),
                    'index': index,
                    'mark': mark,
                    'name': exchange.safe_string(market, 'id'),
                    'perpetual': bool(exchange.safe_value(market, 'swap')),
                    'priceIncrement': float(next(_filter for _filter in market['info']['filters']
                                                 if _filter['filterType'] == 'PRICE_FILTER')['tickSize']),
                    'sizeIncrement': float(next(_filter for _filter in market['info']['filters']
                                                if _filter['filterType'] == 'LOT_SIZE')['stepSize']),
                    'mktSizeIncrement':
                        float(next(_filter for _filter in market['info']['filters']
                             if _filter['filterType'] == 'MARKET_LOT_SIZE')['stepSize']),
                    'underlying': market['base'],
                    'quote': market['quote'],
                    'type': 'perpetual' if market['swap'] else None,
                    'underlyingType': exchange.safe_number(market, 'underlyingType'),
                    'underlyingSubType': exchange.safe_number(market, 'underlyingSubType'),
                    'spot_ticker': '{}/{}'.format(market['base'], market['quote']),
                    'spotMargin': margin_market['info']['isMarginTradingAllowed'] if margin_market else False,
                    'cash_borrow': None,
                    'future_carry': future_carry,
                    'openInterestUsd': float(open_interest['openInterest'])*mark,
                    'expiryTime': expiryTime
                })

            BinanceAPI.Static._cache['fetch_futures'] = result
            return result

    def __init__(self, parameters, private_endpoints=True):
        config = {
            'enableRateLimit': True,
            'newUpdates': True}
        if private_endpoints:  ## David personnal
            config |= {'apiKey': 'V2KfGbMd9Zd9fATONTESrbtUtkEHFcVDr6xAI4KyGBjKs7z08pQspTaPhqITwh1M',
            'secret': api_params['binance']['key']}
        super().__init__(parameters)
        super(ccxtpro.binance,self).__init__(config=config)
        self.state = CeFiAPI.State()

        self.options['tradesLimit'] = VenueAPI.cache_size # TODO: shoud be in signalengine with a different name. inherited from ccxt....

        self.peg_rules: dict[str, PegRule] = dict()

    async def reconcile(self):
        raise NotImplementedError
        # fetch mark,spot and balances as closely as possible
        # shoot rest requests
        n_requests = int(safe_gather_limit / 3)
        p = [getattr(self, coro)(params={'dummy': i})
             for i in range(n_requests)
             for coro in ['fetch_markets', 'fetch_balance', 'fetch_positions']]
        results = await safe_gather(p, semaphore=self.strategy.rest_semaphore)
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
        balances = balances[balances['total'] != 0.0].fillna(0.0)

        positions_list = [
            pd.DataFrame([r['info'] for r in result]).set_index('future')[['netSize', 'unrealizedPnl']].astype(float)
            for
            result in results[2::3]]
        positions = sum(positions_list) / len(positions_list)
        var = sum([pos * pos for pos in positions_list]) / len(positions_list) - positions * positions
        positions = positions[positions['netSize'] != 0.0].fillna(0.0)

        self.state.markets = markets.to_dict()
        self.state.balances = balances.to_dict()
        self.state.positions = positions.to_dict()

class GmxAPI(VenueAPI):
    GLPAdd = "0x01234181085565ed162a948b6a5e88758CD7c7b8"
    GLPABI = """[{"inputs":[],"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"address","name":"spender","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Transfer","type":"event"},{"inputs":[{"internalType":"address","name":"_account","type":"address"}],"name":"addAdmin","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_account","type":"address"}],"name":"addNonStakingAccount","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"admins","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_owner","type":"address"},{"internalType":"address","name":"_spender","type":"address"}],"name":"allowance","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"},{"internalType":"address","name":"","type":"address"}],"name":"allowances","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_spender","type":"address"},{"internalType":"uint256","name":"_amount","type":"uint256"}],"name":"approve","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_account","type":"address"}],"name":"balanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"balances","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_account","type":"address"},{"internalType":"uint256","name":"_amount","type":"uint256"}],"name":"burn","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_receiver","type":"address"}],"name":"claim","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"decimals","outputs":[{"internalType":"uint8","name":"","type":"uint8"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"gov","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"id","outputs":[{"internalType":"string","name":"_name","type":"string"}],"stateMutability":"pure","type":"function"},{"inputs":[],"name":"inPrivateTransferMode","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"isHandler","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"isMinter","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_account","type":"address"},{"internalType":"uint256","name":"_amount","type":"uint256"}],"name":"mint","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"name","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"nonStakingAccounts","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"nonStakingSupply","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_account","type":"address"},{"internalType":"address","name":"_receiver","type":"address"}],"name":"recoverClaim","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_account","type":"address"}],"name":"removeAdmin","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_account","type":"address"}],"name":"removeNonStakingAccount","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_gov","type":"address"}],"name":"setGov","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_handler","type":"address"},{"internalType":"bool","name":"_isActive","type":"bool"}],"name":"setHandler","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"bool","name":"_inPrivateTransferMode","type":"bool"}],"name":"setInPrivateTransferMode","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"string","name":"_name","type":"string"},{"internalType":"string","name":"_symbol","type":"string"}],"name":"setInfo","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_minter","type":"address"},{"internalType":"bool","name":"_isActive","type":"bool"}],"name":"setMinter","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address[]","name":"_yieldTrackers","type":"address[]"}],"name":"setYieldTrackers","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_account","type":"address"}],"name":"stakedBalance","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"symbol","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"totalStaked","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"totalSupply","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_recipient","type":"address"},{"internalType":"uint256","name":"_amount","type":"uint256"}],"name":"transfer","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_sender","type":"address"},{"internalType":"address","name":"_recipient","type":"address"},{"internalType":"uint256","name":"_amount","type":"uint256"}],"name":"transferFrom","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_token","type":"address"},{"internalType":"address","name":"_account","type":"address"},{"internalType":"uint256","name":"_amount","type":"uint256"}],"name":"withdrawToken","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"","type":"uint256"}],"name":"yieldTrackers","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"}]"""
    VaultAdd = "0x9ab2De34A33fB459b538c43f251eB825645e8595"
    VaultABI = """[{"inputs":[],"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"address","name":"account","type":"address"},{"indexed":false,"internalType":"address","name":"token","type":"address"},{"indexed":false,"internalType":"uint256","name":"tokenAmount","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"usdgAmount","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"feeBasisPoints","type":"uint256"}],"name":"BuyUSDG","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"bytes32","name":"key","type":"bytes32"},{"indexed":false,"internalType":"uint256","name":"size","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"collateral","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"averagePrice","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"entryFundingRate","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"reserveAmount","type":"uint256"},{"indexed":false,"internalType":"int256","name":"realisedPnl","type":"int256"}],"name":"ClosePosition","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"address","name":"token","type":"address"},{"indexed":false,"internalType":"uint256","name":"feeUsd","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"feeTokens","type":"uint256"}],"name":"CollectMarginFees","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"address","name":"token","type":"address"},{"indexed":false,"internalType":"uint256","name":"feeUsd","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"feeTokens","type":"uint256"}],"name":"CollectSwapFees","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"address","name":"token","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount","type":"uint256"}],"name":"DecreaseGuaranteedUsd","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"address","name":"token","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount","type":"uint256"}],"name":"DecreasePoolAmount","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"bytes32","name":"key","type":"bytes32"},{"indexed":false,"internalType":"address","name":"account","type":"address"},{"indexed":false,"internalType":"address","name":"collateralToken","type":"address"},{"indexed":false,"internalType":"address","name":"indexToken","type":"address"},{"indexed":false,"internalType":"uint256","name":"collateralDelta","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"sizeDelta","type":"uint256"},{"indexed":false,"internalType":"bool","name":"isLong","type":"bool"},{"indexed":false,"internalType":"uint256","name":"price","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"fee","type":"uint256"}],"name":"DecreasePosition","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"address","name":"token","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount","type":"uint256"}],"name":"DecreaseReservedAmount","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"address","name":"token","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount","type":"uint256"}],"name":"DecreaseUsdgAmount","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"address","name":"token","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount","type":"uint256"}],"name":"DirectPoolDeposit","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"address","name":"token","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount","type":"uint256"}],"name":"IncreaseGuaranteedUsd","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"address","name":"token","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount","type":"uint256"}],"name":"IncreasePoolAmount","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"bytes32","name":"key","type":"bytes32"},{"indexed":false,"internalType":"address","name":"account","type":"address"},{"indexed":false,"internalType":"address","name":"collateralToken","type":"address"},{"indexed":false,"internalType":"address","name":"indexToken","type":"address"},{"indexed":false,"internalType":"uint256","name":"collateralDelta","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"sizeDelta","type":"uint256"},{"indexed":false,"internalType":"bool","name":"isLong","type":"bool"},{"indexed":false,"internalType":"uint256","name":"price","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"fee","type":"uint256"}],"name":"IncreasePosition","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"address","name":"token","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount","type":"uint256"}],"name":"IncreaseReservedAmount","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"address","name":"token","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount","type":"uint256"}],"name":"IncreaseUsdgAmount","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"bytes32","name":"key","type":"bytes32"},{"indexed":false,"internalType":"address","name":"account","type":"address"},{"indexed":false,"internalType":"address","name":"collateralToken","type":"address"},{"indexed":false,"internalType":"address","name":"indexToken","type":"address"},{"indexed":false,"internalType":"bool","name":"isLong","type":"bool"},{"indexed":false,"internalType":"uint256","name":"size","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"collateral","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"reserveAmount","type":"uint256"},{"indexed":false,"internalType":"int256","name":"realisedPnl","type":"int256"},{"indexed":false,"internalType":"uint256","name":"markPrice","type":"uint256"}],"name":"LiquidatePosition","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"address","name":"account","type":"address"},{"indexed":false,"internalType":"address","name":"token","type":"address"},{"indexed":false,"internalType":"uint256","name":"usdgAmount","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"tokenAmount","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"feeBasisPoints","type":"uint256"}],"name":"SellUSDG","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"address","name":"account","type":"address"},{"indexed":false,"internalType":"address","name":"tokenIn","type":"address"},{"indexed":false,"internalType":"address","name":"tokenOut","type":"address"},{"indexed":false,"internalType":"uint256","name":"amountIn","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amountOut","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amountOutAfterFees","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"feeBasisPoints","type":"uint256"}],"name":"Swap","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"address","name":"token","type":"address"},{"indexed":false,"internalType":"uint256","name":"fundingRate","type":"uint256"}],"name":"UpdateFundingRate","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"bytes32","name":"key","type":"bytes32"},{"indexed":false,"internalType":"bool","name":"hasProfit","type":"bool"},{"indexed":false,"internalType":"uint256","name":"delta","type":"uint256"}],"name":"UpdatePnl","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"bytes32","name":"key","type":"bytes32"},{"indexed":false,"internalType":"uint256","name":"size","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"collateral","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"averagePrice","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"entryFundingRate","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"reserveAmount","type":"uint256"},{"indexed":false,"internalType":"int256","name":"realisedPnl","type":"int256"},{"indexed":false,"internalType":"uint256","name":"markPrice","type":"uint256"}],"name":"UpdatePosition","type":"event"},{"inputs":[],"name":"BASIS_POINTS_DIVISOR","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"FUNDING_RATE_PRECISION","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"MAX_FEE_BASIS_POINTS","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"MAX_FUNDING_RATE_FACTOR","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"MAX_LIQUIDATION_FEE_USD","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"MIN_FUNDING_RATE_INTERVAL","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"MIN_LEVERAGE","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"PRICE_PRECISION","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"USDG_DECIMALS","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_router","type":"address"}],"name":"addRouter","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"_amount","type":"uint256"},{"internalType":"address","name":"_tokenDiv","type":"address"},{"internalType":"address","name":"_tokenMul","type":"address"}],"name":"adjustForDecimals","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"","type":"uint256"}],"name":"allWhitelistedTokens","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"allWhitelistedTokensLength","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"},{"internalType":"address","name":"","type":"address"}],"name":"approvedRouters","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"bufferAmounts","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_token","type":"address"},{"internalType":"address","name":"_receiver","type":"address"}],"name":"buyUSDG","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_token","type":"address"}],"name":"clearTokenConfig","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"cumulativeFundingRates","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_account","type":"address"},{"internalType":"address","name":"_collateralToken","type":"address"},{"internalType":"address","name":"_indexToken","type":"address"},{"internalType":"uint256","name":"_collateralDelta","type":"uint256"},{"internalType":"uint256","name":"_sizeDelta","type":"uint256"},{"internalType":"bool","name":"_isLong","type":"bool"},{"internalType":"address","name":"_receiver","type":"address"}],"name":"decreasePosition","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_token","type":"address"}],"name":"directPoolDeposit","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"errorController","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"","type":"uint256"}],"name":"errors","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"feeReserves","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"fundingInterval","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"fundingRateFactor","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_indexToken","type":"address"},{"internalType":"uint256","name":"_size","type":"uint256"},{"internalType":"uint256","name":"_averagePrice","type":"uint256"},{"internalType":"bool","name":"_isLong","type":"bool"},{"internalType":"uint256","name":"_lastIncreasedTime","type":"uint256"}],"name":"getDelta","outputs":[{"internalType":"bool","name":"","type":"bool"},{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_collateralToken","type":"address"},{"internalType":"address","name":"_indexToken","type":"address"},{"internalType":"bool","name":"_isLong","type":"bool"}],"name":"getEntryFundingRate","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_token","type":"address"},{"internalType":"uint256","name":"_usdgDelta","type":"uint256"},{"internalType":"uint256","name":"_feeBasisPoints","type":"uint256"},{"internalType":"uint256","name":"_taxBasisPoints","type":"uint256"},{"internalType":"bool","name":"_increment","type":"bool"}],"name":"getFeeBasisPoints","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_account","type":"address"},{"internalType":"address","name":"_collateralToken","type":"address"},{"internalType":"address","name":"_indexToken","type":"address"},{"internalType":"bool","name":"_isLong","type":"bool"},{"internalType":"uint256","name":"_size","type":"uint256"},{"internalType":"uint256","name":"_entryFundingRate","type":"uint256"}],"name":"getFundingFee","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_token","type":"address"}],"name":"getGlobalShortDelta","outputs":[{"internalType":"bool","name":"","type":"bool"},{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_token","type":"address"}],"name":"getMaxPrice","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_token","type":"address"}],"name":"getMinPrice","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_indexToken","type":"address"},{"internalType":"uint256","name":"_size","type":"uint256"},{"internalType":"uint256","name":"_averagePrice","type":"uint256"},{"internalType":"bool","name":"_isLong","type":"bool"},{"internalType":"uint256","name":"_nextPrice","type":"uint256"},{"internalType":"uint256","name":"_sizeDelta","type":"uint256"},{"internalType":"uint256","name":"_lastIncreasedTime","type":"uint256"}],"name":"getNextAveragePrice","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_token","type":"address"}],"name":"getNextFundingRate","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_indexToken","type":"address"},{"internalType":"uint256","name":"_nextPrice","type":"uint256"},{"internalType":"uint256","name":"_sizeDelta","type":"uint256"}],"name":"getNextGlobalShortAveragePrice","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_account","type":"address"},{"internalType":"address","name":"_collateralToken","type":"address"},{"internalType":"address","name":"_indexToken","type":"address"},{"internalType":"bool","name":"_isLong","type":"bool"}],"name":"getPosition","outputs":[{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"bool","name":"","type":"bool"},{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_account","type":"address"},{"internalType":"address","name":"_collateralToken","type":"address"},{"internalType":"address","name":"_indexToken","type":"address"},{"internalType":"bool","name":"_isLong","type":"bool"}],"name":"getPositionDelta","outputs":[{"internalType":"bool","name":"","type":"bool"},{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_account","type":"address"},{"internalType":"address","name":"_collateralToken","type":"address"},{"internalType":"address","name":"_indexToken","type":"address"},{"internalType":"bool","name":"_isLong","type":"bool"},{"internalType":"uint256","name":"_sizeDelta","type":"uint256"}],"name":"getPositionFee","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_account","type":"address"},{"internalType":"address","name":"_collateralToken","type":"address"},{"internalType":"address","name":"_indexToken","type":"address"},{"internalType":"bool","name":"_isLong","type":"bool"}],"name":"getPositionKey","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"stateMutability":"pure","type":"function"},{"inputs":[{"internalType":"address","name":"_account","type":"address"},{"internalType":"address","name":"_collateralToken","type":"address"},{"internalType":"address","name":"_indexToken","type":"address"},{"internalType":"bool","name":"_isLong","type":"bool"}],"name":"getPositionLeverage","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_token","type":"address"},{"internalType":"uint256","name":"_usdgAmount","type":"uint256"}],"name":"getRedemptionAmount","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_token","type":"address"}],"name":"getRedemptionCollateral","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_token","type":"address"}],"name":"getRedemptionCollateralUsd","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_token","type":"address"}],"name":"getTargetUsdgAmount","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_token","type":"address"}],"name":"getUtilisation","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"globalShortAveragePrices","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"globalShortSizes","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"gov","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"guaranteedUsd","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"hasDynamicFees","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"inManagerMode","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"inPrivateLiquidationMode","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"includeAmmPrice","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_account","type":"address"},{"internalType":"address","name":"_collateralToken","type":"address"},{"internalType":"address","name":"_indexToken","type":"address"},{"internalType":"uint256","name":"_sizeDelta","type":"uint256"},{"internalType":"bool","name":"_isLong","type":"bool"}],"name":"increasePosition","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_router","type":"address"},{"internalType":"address","name":"_usdg","type":"address"},{"internalType":"address","name":"_priceFeed","type":"address"},{"internalType":"uint256","name":"_liquidationFeeUsd","type":"uint256"},{"internalType":"uint256","name":"_fundingRateFactor","type":"uint256"},{"internalType":"uint256","name":"_stableFundingRateFactor","type":"uint256"}],"name":"initialize","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"isInitialized","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"isLeverageEnabled","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"isLiquidator","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"isManager","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"isSwapEnabled","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"lastFundingTimes","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_account","type":"address"},{"internalType":"address","name":"_collateralToken","type":"address"},{"internalType":"address","name":"_indexToken","type":"address"},{"internalType":"bool","name":"_isLong","type":"bool"},{"internalType":"address","name":"_feeReceiver","type":"address"}],"name":"liquidatePosition","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"liquidationFeeUsd","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"marginFeeBasisPoints","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"maxGasPrice","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"maxGlobalShortSizes","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"maxLeverage","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"maxUsdgAmounts","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"minProfitBasisPoints","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"minProfitTime","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"mintBurnFeeBasisPoints","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"poolAmounts","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"name":"positions","outputs":[{"internalType":"uint256","name":"size","type":"uint256"},{"internalType":"uint256","name":"collateral","type":"uint256"},{"internalType":"uint256","name":"averagePrice","type":"uint256"},{"internalType":"uint256","name":"entryFundingRate","type":"uint256"},{"internalType":"uint256","name":"reserveAmount","type":"uint256"},{"internalType":"int256","name":"realisedPnl","type":"int256"},{"internalType":"uint256","name":"lastIncreasedTime","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"priceFeed","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_router","type":"address"}],"name":"removeRouter","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"reservedAmounts","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"router","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_token","type":"address"},{"internalType":"address","name":"_receiver","type":"address"}],"name":"sellUSDG","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_token","type":"address"},{"internalType":"uint256","name":"_amount","type":"uint256"}],"name":"setBufferAmount","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"_errorCode","type":"uint256"},{"internalType":"string","name":"_error","type":"string"}],"name":"setError","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_errorController","type":"address"}],"name":"setErrorController","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"_taxBasisPoints","type":"uint256"},{"internalType":"uint256","name":"_stableTaxBasisPoints","type":"uint256"},{"internalType":"uint256","name":"_mintBurnFeeBasisPoints","type":"uint256"},{"internalType":"uint256","name":"_swapFeeBasisPoints","type":"uint256"},{"internalType":"uint256","name":"_stableSwapFeeBasisPoints","type":"uint256"},{"internalType":"uint256","name":"_marginFeeBasisPoints","type":"uint256"},{"internalType":"uint256","name":"_liquidationFeeUsd","type":"uint256"},{"internalType":"uint256","name":"_minProfitTime","type":"uint256"},{"internalType":"bool","name":"_hasDynamicFees","type":"bool"}],"name":"setFees","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"_fundingInterval","type":"uint256"},{"internalType":"uint256","name":"_fundingRateFactor","type":"uint256"},{"internalType":"uint256","name":"_stableFundingRateFactor","type":"uint256"}],"name":"setFundingRate","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_gov","type":"address"}],"name":"setGov","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"bool","name":"_inManagerMode","type":"bool"}],"name":"setInManagerMode","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"bool","name":"_inPrivateLiquidationMode","type":"bool"}],"name":"setInPrivateLiquidationMode","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"bool","name":"_isLeverageEnabled","type":"bool"}],"name":"setIsLeverageEnabled","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"bool","name":"_isSwapEnabled","type":"bool"}],"name":"setIsSwapEnabled","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_liquidator","type":"address"},{"internalType":"bool","name":"_isActive","type":"bool"}],"name":"setLiquidator","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_manager","type":"address"},{"internalType":"bool","name":"_isManager","type":"bool"}],"name":"setManager","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"_maxGasPrice","type":"uint256"}],"name":"setMaxGasPrice","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_token","type":"address"},{"internalType":"uint256","name":"_amount","type":"uint256"}],"name":"setMaxGlobalShortSize","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"_maxLeverage","type":"uint256"}],"name":"setMaxLeverage","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_priceFeed","type":"address"}],"name":"setPriceFeed","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_token","type":"address"},{"internalType":"uint256","name":"_tokenDecimals","type":"uint256"},{"internalType":"uint256","name":"_tokenWeight","type":"uint256"},{"internalType":"uint256","name":"_minProfitBps","type":"uint256"},{"internalType":"uint256","name":"_maxUsdgAmount","type":"uint256"},{"internalType":"bool","name":"_isStable","type":"bool"},{"internalType":"bool","name":"_isShortable","type":"bool"}],"name":"setTokenConfig","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_token","type":"address"},{"internalType":"uint256","name":"_amount","type":"uint256"}],"name":"setUsdgAmount","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"contract IVaultUtils","name":"_vaultUtils","type":"address"}],"name":"setVaultUtils","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"shortableTokens","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"stableFundingRateFactor","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"stableSwapFeeBasisPoints","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"stableTaxBasisPoints","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"stableTokens","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_tokenIn","type":"address"},{"internalType":"address","name":"_tokenOut","type":"address"},{"internalType":"address","name":"_receiver","type":"address"}],"name":"swap","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"swapFeeBasisPoints","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"taxBasisPoints","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"tokenBalances","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"tokenDecimals","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_token","type":"address"},{"internalType":"uint256","name":"_tokenAmount","type":"uint256"}],"name":"tokenToUsdMin","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"tokenWeights","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"totalTokenWeights","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_collateralToken","type":"address"},{"internalType":"address","name":"_indexToken","type":"address"}],"name":"updateCumulativeFundingRate","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_newVault","type":"address"},{"internalType":"address","name":"_token","type":"address"},{"internalType":"uint256","name":"_amount","type":"uint256"}],"name":"upgradeVault","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_token","type":"address"},{"internalType":"uint256","name":"_usdAmount","type":"uint256"},{"internalType":"uint256","name":"_price","type":"uint256"}],"name":"usdToToken","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_token","type":"address"},{"internalType":"uint256","name":"_usdAmount","type":"uint256"}],"name":"usdToTokenMax","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_token","type":"address"},{"internalType":"uint256","name":"_usdAmount","type":"uint256"}],"name":"usdToTokenMin","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"usdg","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"usdgAmounts","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"useSwapPricing","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_account","type":"address"},{"internalType":"address","name":"_collateralToken","type":"address"},{"internalType":"address","name":"_indexToken","type":"address"},{"internalType":"bool","name":"_isLong","type":"bool"},{"internalType":"bool","name":"_raise","type":"bool"}],"name":"validateLiquidation","outputs":[{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"vaultUtils","outputs":[{"internalType":"contract IVaultUtils","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"whitelistedTokenCount","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"whitelistedTokens","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_token","type":"address"},{"internalType":"address","name":"_receiver","type":"address"}],"name":"withdrawFees","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"nonpayable","type":"function"}]"""
    GLPManagerAdd = "0xe1ae4d4b06A5Fe1fc288f6B4CD72f9F8323B107F"
    GLPManagerABI = """[{"inputs":[{"internalType":"address","name":"_vault","type":"address"},{"internalType":"address","name":"_usdg","type":"address"},{"internalType":"address","name":"_glp","type":"address"},{"internalType":"uint256","name":"_cooldownDuration","type":"uint256"}],"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"address","name":"account","type":"address"},{"indexed":false,"internalType":"address","name":"token","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"aumInUsdg","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"glpSupply","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"usdgAmount","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"mintAmount","type":"uint256"}],"name":"AddLiquidity","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"address","name":"account","type":"address"},{"indexed":false,"internalType":"address","name":"token","type":"address"},{"indexed":false,"internalType":"uint256","name":"glpAmount","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"aumInUsdg","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"glpSupply","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"usdgAmount","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amountOut","type":"uint256"}],"name":"RemoveLiquidity","type":"event"},{"inputs":[],"name":"MAX_COOLDOWN_DURATION","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"PRICE_PRECISION","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"USDG_DECIMALS","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_token","type":"address"},{"internalType":"uint256","name":"_amount","type":"uint256"},{"internalType":"uint256","name":"_minUsdg","type":"uint256"},{"internalType":"uint256","name":"_minGlp","type":"uint256"}],"name":"addLiquidity","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_fundingAccount","type":"address"},{"internalType":"address","name":"_account","type":"address"},{"internalType":"address","name":"_token","type":"address"},{"internalType":"uint256","name":"_amount","type":"uint256"},{"internalType":"uint256","name":"_minUsdg","type":"uint256"},{"internalType":"uint256","name":"_minGlp","type":"uint256"}],"name":"addLiquidityForAccount","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"aumAddition","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"aumDeduction","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"cooldownDuration","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"bool","name":"maximise","type":"bool"}],"name":"getAum","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"bool","name":"maximise","type":"bool"}],"name":"getAumInUsdg","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"getAums","outputs":[{"internalType":"uint256[]","name":"","type":"uint256[]"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"glp","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"gov","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"inPrivateMode","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"isHandler","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"lastAddedAt","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_tokenOut","type":"address"},{"internalType":"uint256","name":"_glpAmount","type":"uint256"},{"internalType":"uint256","name":"_minOut","type":"uint256"},{"internalType":"address","name":"_receiver","type":"address"}],"name":"removeLiquidity","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_account","type":"address"},{"internalType":"address","name":"_tokenOut","type":"address"},{"internalType":"uint256","name":"_glpAmount","type":"uint256"},{"internalType":"uint256","name":"_minOut","type":"uint256"},{"internalType":"address","name":"_receiver","type":"address"}],"name":"removeLiquidityForAccount","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"_aumAddition","type":"uint256"},{"internalType":"uint256","name":"_aumDeduction","type":"uint256"}],"name":"setAumAdjustment","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"_cooldownDuration","type":"uint256"}],"name":"setCooldownDuration","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_gov","type":"address"}],"name":"setGov","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_handler","type":"address"},{"internalType":"bool","name":"_isActive","type":"bool"}],"name":"setHandler","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"bool","name":"_inPrivateMode","type":"bool"}],"name":"setInPrivateMode","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"usdg","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"vault","outputs":[{"internalType":"contract IVault","name":"","type":"address"}],"stateMutability":"view","type":"function"}]"""
    RewardTrackerAdd = "0xd2D1162512F927a7e282Ef43a362659E4F2a728F"
    RewardTrackerABI = """[{"inputs":[{"internalType":"string","name":"_name","type":"string"},{"internalType":"string","name":"_symbol","type":"string"}],"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"address","name":"spender","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"address","name":"receiver","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount","type":"uint256"}],"name":"Claim","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Transfer","type":"event"},{"inputs":[],"name":"BASIS_POINTS_DIVISOR","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"PRECISION","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_owner","type":"address"},{"internalType":"address","name":"_spender","type":"address"}],"name":"allowance","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"},{"internalType":"address","name":"","type":"address"}],"name":"allowances","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_spender","type":"address"},{"internalType":"uint256","name":"_amount","type":"uint256"}],"name":"approve","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"averageStakedAmounts","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_account","type":"address"}],"name":"balanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"balances","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_receiver","type":"address"}],"name":"claim","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_account","type":"address"},{"internalType":"address","name":"_receiver","type":"address"}],"name":"claimForAccount","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_account","type":"address"}],"name":"claimable","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"claimableReward","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"cumulativeRewardPerToken","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"cumulativeRewards","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"decimals","outputs":[{"internalType":"uint8","name":"","type":"uint8"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"},{"internalType":"address","name":"","type":"address"}],"name":"depositBalances","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"distributor","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"gov","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"inPrivateClaimingMode","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"inPrivateStakingMode","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"inPrivateTransferMode","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address[]","name":"_depositTokens","type":"address[]"},{"internalType":"address","name":"_distributor","type":"address"}],"name":"initialize","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"isDepositToken","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"isHandler","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"isInitialized","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"name","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"previousCumulatedRewardPerToken","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"rewardToken","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_depositToken","type":"address"},{"internalType":"bool","name":"_isDepositToken","type":"bool"}],"name":"setDepositToken","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_gov","type":"address"}],"name":"setGov","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_handler","type":"address"},{"internalType":"bool","name":"_isActive","type":"bool"}],"name":"setHandler","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"bool","name":"_inPrivateClaimingMode","type":"bool"}],"name":"setInPrivateClaimingMode","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"bool","name":"_inPrivateStakingMode","type":"bool"}],"name":"setInPrivateStakingMode","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"bool","name":"_inPrivateTransferMode","type":"bool"}],"name":"setInPrivateTransferMode","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_depositToken","type":"address"},{"internalType":"uint256","name":"_amount","type":"uint256"}],"name":"stake","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_fundingAccount","type":"address"},{"internalType":"address","name":"_account","type":"address"},{"internalType":"address","name":"_depositToken","type":"address"},{"internalType":"uint256","name":"_amount","type":"uint256"}],"name":"stakeForAccount","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"stakedAmounts","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"symbol","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"tokensPerInterval","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"totalDepositSupply","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"totalSupply","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_recipient","type":"address"},{"internalType":"uint256","name":"_amount","type":"uint256"}],"name":"transfer","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_sender","type":"address"},{"internalType":"address","name":"_recipient","type":"address"},{"internalType":"uint256","name":"_amount","type":"uint256"}],"name":"transferFrom","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_depositToken","type":"address"},{"internalType":"uint256","name":"_amount","type":"uint256"}],"name":"unstake","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_account","type":"address"},{"internalType":"address","name":"_depositToken","type":"address"},{"internalType":"uint256","name":"_amount","type":"uint256"},{"internalType":"address","name":"_receiver","type":"address"}],"name":"unstakeForAccount","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"updateRewards","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_token","type":"address"},{"internalType":"address","name":"_account","type":"address"},{"internalType":"uint256","name":"_amount","type":"uint256"}],"name":"withdrawToken","outputs":[],"stateMutability":"nonpayable","type":"function"}]"""
    esGMXRewardAdd = "0x9e295B5B976a184B14aD8cd72413aD846C299660"
    esGMXRewardABI = """[{"inputs":[{"internalType":"string","name":"_name","type":"string"},{"internalType":"string","name":"_symbol","type":"string"}],"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"address","name":"spender","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"address","name":"receiver","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount","type":"uint256"}],"name":"Claim","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Transfer","type":"event"},{"inputs":[],"name":"BASIS_POINTS_DIVISOR","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"PRECISION","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_owner","type":"address"},{"internalType":"address","name":"_spender","type":"address"}],"name":"allowance","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"},{"internalType":"address","name":"","type":"address"}],"name":"allowances","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_spender","type":"address"},{"internalType":"uint256","name":"_amount","type":"uint256"}],"name":"approve","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"averageStakedAmounts","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_account","type":"address"}],"name":"balanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"balances","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_receiver","type":"address"}],"name":"claim","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_account","type":"address"},{"internalType":"address","name":"_receiver","type":"address"}],"name":"claimForAccount","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_account","type":"address"}],"name":"claimable","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"claimableReward","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"cumulativeRewardPerToken","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"cumulativeRewards","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"decimals","outputs":[{"internalType":"uint8","name":"","type":"uint8"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"},{"internalType":"address","name":"","type":"address"}],"name":"depositBalances","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"distributor","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"gov","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"inPrivateClaimingMode","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"inPrivateStakingMode","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"inPrivateTransferMode","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address[]","name":"_depositTokens","type":"address[]"},{"internalType":"address","name":"_distributor","type":"address"}],"name":"initialize","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"isDepositToken","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"isHandler","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"isInitialized","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"name","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"previousCumulatedRewardPerToken","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"rewardToken","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_depositToken","type":"address"},{"internalType":"bool","name":"_isDepositToken","type":"bool"}],"name":"setDepositToken","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_gov","type":"address"}],"name":"setGov","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_handler","type":"address"},{"internalType":"bool","name":"_isActive","type":"bool"}],"name":"setHandler","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"bool","name":"_inPrivateClaimingMode","type":"bool"}],"name":"setInPrivateClaimingMode","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"bool","name":"_inPrivateStakingMode","type":"bool"}],"name":"setInPrivateStakingMode","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"bool","name":"_inPrivateTransferMode","type":"bool"}],"name":"setInPrivateTransferMode","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_depositToken","type":"address"},{"internalType":"uint256","name":"_amount","type":"uint256"}],"name":"stake","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_fundingAccount","type":"address"},{"internalType":"address","name":"_account","type":"address"},{"internalType":"address","name":"_depositToken","type":"address"},{"internalType":"uint256","name":"_amount","type":"uint256"}],"name":"stakeForAccount","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"stakedAmounts","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"symbol","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"tokensPerInterval","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"totalDepositSupply","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"totalSupply","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_recipient","type":"address"},{"internalType":"uint256","name":"_amount","type":"uint256"}],"name":"transfer","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_sender","type":"address"},{"internalType":"address","name":"_recipient","type":"address"},{"internalType":"uint256","name":"_amount","type":"uint256"}],"name":"transferFrom","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_depositToken","type":"address"},{"internalType":"uint256","name":"_amount","type":"uint256"}],"name":"unstake","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_account","type":"address"},{"internalType":"address","name":"_depositToken","type":"address"},{"internalType":"uint256","name":"_amount","type":"uint256"},{"internalType":"address","name":"_receiver","type":"address"}],"name":"unstakeForAccount","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"updateRewards","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_token","type":"address"},{"internalType":"address","name":"_account","type":"address"},{"internalType":"uint256","name":"_amount","type":"uint256"}],"name":"withdrawToken","outputs":[],"stateMutability":"nonpayable","type":"function"}]"""

    w3 = Web3(Web3.HTTPProvider("https://api.avax.network/ext/bc/C/rpc",request_kwargs={'timeout': 5}))
    vault = w3.eth.contract(abi=VaultABI, address=VaultAdd).functions
    glp = w3.eth.contract(abi=GLPABI, address=GLPAdd).functions
    glp_manager = w3.eth.contract(abi=GLPManagerABI, address=GLPManagerAdd).functions
    reward_tracker = w3.eth.contract(abi=RewardTrackerABI, address=RewardTrackerAdd).functions
    esGMXReward = w3.eth.contract(abi=esGMXRewardABI, address=esGMXRewardAdd).functions

    static = {'MIM': {'address': "0x130966628846BFd36ff31a822705796e8cb8C18D",      'decimal': 1e18, 'volatile': False, 'normalized_symbol': 'MIM/USD:USD'},
              'WETH': {'address': "0x49D5c2BdFfac6CE2BFdB6640F4F80f226bc10bAB",     'decimal': 1e18, 'volatile': True, 'normalized_symbol': 'ETH/USD:USD'},
              'WBTC': {'address': "0x50b7545627a5162F82A992c33b87aDc75187B218",     'decimal': 1e8, 'volatile': True, 'normalized_symbol': 'BTC/USD:USD'},
              'WAVAX': {'address': "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",    'decimal': 1e18, 'volatile': True, 'normalized_symbol': 'AVAX/USD:USD'},
              'USDC': {'address': "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",     'decimal': 1e6, 'volatile': False, 'normalized_symbol': 'USDC/USD:USD'},
              'USDC_E': {'address': "0xA7D7079b0FEaD91F3e65f86E8915Cb59c1a4C664",   'decimal': 1e6, 'volatile': False, 'normalized_symbol': 'USDC/USD:USD'},
              'BTC_E': {'address': "0x152b9d0FdC40C096757F570A51E494bd4b943E50",    'decimal': 1e8, 'volatile': True, 'normalized_symbol': 'BTC/USD:USD'}}
    reward_static = {}
    check_tolerance = 1e-4
    fee = 0.001
    tx_cost = 0.005
    safe_gather_limit = 9

    # class Position:
    #     def __init__(self, collateral, entryFundingRate, size, lastIncreasedTime):
    #         self.collateral = collateral
    #         self.entryFundingRate = entryFundingRate
    #         self.size = size
    #         self.lastIncreasedTime = lastIncreasedTime

    class State:
        def __init__(self, parameters):
            self.wallet = parameters['wallet']

            self.poolAmounts = {key: 0 for key in GmxAPI.static}
            self.tokenBalances = {key: 0 for key in GmxAPI.static}
            self.usdgAmounts = {key: 0 for key in GmxAPI.static}
            self.pricesUp = {key: None for key in GmxAPI.static}
            self.pricesDown = {key: None for key in GmxAPI.static}

            self.guaranteedUsd = {key: 0 for key, data in GmxAPI.static.items() if data['volatile']}
            self.reservedAmounts = {key: 0 for key, data in GmxAPI.static.items() if data['volatile']}
            self.globalShortSizes = {key: 0 for key, data in GmxAPI.static.items() if data['volatile']}
            self.globalShortAveragePrices = {key: None for key, data in GmxAPI.static.items() if data['volatile']}
            self.feeReserves = {key: 0 for key in GmxAPI.static}

            self.totalSupply = {'total': None}
            self.actualAum = {'total': None}
            self.rewards = {'WAVAX': None}
            self.check = {key: dict() for key in GmxAPI.static}

        def valuation(self, key=None) -> float:
            '''unstakeAndRedeemGlpETH: 0xe5004b114abd13b32267514808e663c456d1803ace40c0a4ae7421571155fdd3
            total is key is None'''
            if key is None:
                return sum(self.valuation(key) for key in GmxAPI.static)
            else:
                aum = self.poolAmounts[key] * self.pricesDown[key]
                if GmxAPI.static[key]['volatile']:
                    aum += self.guaranteedUsd[key] - self.reservedAmounts[key] * self.pricesDown[key]
                    # add pnl from shorts
                    aum += (self.pricesDown[key] / self.globalShortAveragePrices[key] - 1) * self.globalShortSizes[key]
                return aum / self.totalSupply['total']

        def partial_delta(self, key: str, normalized: bool=False) -> float:
            ''' delta in token
            delta =  poolAmount - reservedAmounts + globalShortSizes/globalShortAveragePrices
            ~ what's ours + (collateral - N)^{longs}'''
            if normalized:  # this uses normalized input
                result = 0
                for token, data in GmxAPI.static.items():
                    if data['normalized_symbol'] == key:
                        result += self.poolAmounts[token]
                        if data['volatile']:
                            result += (- self.reservedAmounts[token] + self.globalShortSizes[token] / self.globalShortAveragePrices[token])
                return result / self.totalSupply['total']
            else:
                result = self.poolAmounts[key]
                if GmxAPI.static[key]['volatile']:
                    result += (- self.reservedAmounts[key] + self.globalShortSizes[key] / self.globalShortAveragePrices[key])
                if key == 'WAVAX':
                    result += self.rewards['WAVAX']
                return result / self.totalSupply['total']
        # def add_weights(self) -> None:
        #     weight_contents = urllib.request.urlopen("https://gmx-avax-server.uc.r.appspot.com/tokens").read()
        #     weight_json = json.loads(weight_contents)
        #     sillyMapping = {'MIM': 'MIM',
        #                     'BTC.b': 'BTC_E',
        #                     'ETH': 'WETH',
        #                     'BTC': 'WBTC',
        #                     'USDC.e': 'USDC_E',
        #                     'AVAX': 'WAVAX',
        #                     'USDC': 'USDC'}
        #
        #     for cur_contents in weight_json:
        #         token = sillyMapping[cur_contents['data']['symbol']]
        #         self.check[token]['weight'] = float(cur_contents["data"]["usdgAmount"]) / float(
        #             cur_contents["data"]["maxPrice"])
        #         for attribute in ['fundingRate', 'poolAmounts', 'reservedAmount', 'redemptionAmount', 'globalShortSize',
        #                           'guaranteedUsd']:
        #             self.check[token][attribute] = float(cur_contents['data'][attribute])

        def add_weights(self) -> None:
            weight_contents = urllib.request.urlopen("https://gmx-avax-server.uc.r.appspot.com/tokens").read()
            weight_json = json.loads(weight_contents)
            sillyMapping = {'MIM': 'MIM',
                            'BTC.b': 'BTC_E',
                            'ETH': 'WETH',
                            'BTC': 'WBTC',
                            'USDC.e': 'USDC_E',
                            'AVAX': 'WAVAX',
                            'USDC': 'USDC'}

            for cur_contents in weight_json:
                token = sillyMapping[cur_contents['data']['symbol']]
                self.check[token]['weight'] = float(cur_contents["data"]["usdgAmount"]) / float(
                    cur_contents["data"]["maxPrice"])
                for attribute in ['fundingRate', 'poolAmounts', 'reservedAmount', 'redemptionAmount', 'globalShortSize',
                                  'guaranteedUsd']:
                    self.check[token][attribute] = float(cur_contents['data'][attribute])

        def sanity_check(self):
            return abs(self.valuation() * self.totalSupply['total'] / self.actualAum['total'] - 1) < GmxAPI.check_tolerance

        async def reconcile(self) -> None:
            def read_contracts(address, decimal, volatile):
                coros = []
                coros.append(
                    async_wrap(lambda x: float(GmxAPI.vault.tokenBalances(address).call()) / decimal)(
                        None))
                coros.append(
                    async_wrap(lambda x: float(GmxAPI.vault.poolAmounts(address).call()) / decimal)(
                        None))
                coros.append(
                    async_wrap(lambda x: float(GmxAPI.vault.usdgAmounts(address).call()) / decimal)(
                        None))
                coros.append(
                    async_wrap(lambda x: float(GmxAPI.vault.getMaxPrice(address).call() / 1e30))(None))
                coros.append(
                    async_wrap(lambda x: float(GmxAPI.vault.getMinPrice(address).call() / 1e30))(None))
                coros.append(async_wrap(
                    lambda x: (
                        float(GmxAPI.vault.guaranteedUsd(address).call()) / 1e30 if volatile else None))(
                    None))
                coros.append(async_wrap(lambda x: (
                    float(
                        GmxAPI.vault.reservedAmounts(address).call()) / decimal if volatile else None))(
                    None))
                coros.append(async_wrap(
                    lambda x: (
                        float(GmxAPI.vault.globalShortSizes(
                            address).call()) / 1e30 if volatile else None))(
                    None))
                coros.append(async_wrap(lambda x: (
                    float(GmxAPI.vault.globalShortAveragePrices(
                        address).call()) / 1e30 if volatile else None))(
                    None))
                coros.append(async_wrap(
                    lambda x: (
                        float(
                            GmxAPI.vault.feeReserves(address).call()) / decimal if volatile else None))(
                    None))
                return coros

            coros = []
            for key, data in GmxAPI.static.items():
                coros += read_contracts(data['address'], data['decimal'], data['volatile'])

            # result.positions = GLPState.contract_vault.functions.positions().call()
            coros.append(async_wrap(lambda x: GmxAPI.glp.totalSupply().call() / 1e18)(None))
            coros.append(async_wrap(lambda x: GmxAPI.glp_manager.getAumInUsdg(
                False).call() / GmxAPI.glp.totalSupply().call())(None))

            results_values = await safe_gather(coros, semaphore=asyncio.Semaphore(GmxAPI.safe_gather_limit))  # IT'S CRUCIAL TO MAINTAIN ORDER....

            functions_list = ['tokenBalances', 'poolAmounts', 'usdgAmounts', 'pricesUp', 'pricesDown', 'guaranteedUsd',
                              'reservedAmounts', 'globalShortSizes', 'globalShortAveragePrices', 'feeReserves']

            results = {(function, token): results_values[i] for i, (token, function) in
                       enumerate(itertools.product(GmxAPI.static.keys(), functions_list))}
            results |= {('totalSupply', 'total'): results_values[-2],
                        ('actualAum', 'total'): results_values[-1],
                        ('rewards', 'WAVAX'): GmxAPI.reward_tracker.claimable(self.wallet).call()/1e18,
                        ('rewards', 'esGMX'): GmxAPI.esGMXReward.claimableReward(self.wallet).call()/1e18}

            for function in functions_list:
                setattr(self, function, {key: results[(function, key)] for key in GmxAPI.static})
            self.totalSupply = {'total': results[('totalSupply', 'total')]}
            self.actualAum = {'total': results[('actualAum', 'total')]}
            self.rewards = {'WAVAX': results[('rewards', 'WAVAX')],
                            'esGMX': results[('rewards', 'esGMX')]}

            if False:
                self.add_weights()

        def depositBalances(self):
            feeGlp = GmxAPI.reward_tracker.stakedAmounts(self.wallet).call()/1e18
            #feeGmx = GmxAPI.reward_tracker.depositBalances(self.wallet,GmxAPI.feeGmxTracker).call() / 1e18
            return feeGlp

    def __init__(self, parameters):
        super().__init__(parameters)
        self.timestamp = None
        self.state = GmxAPI.State(parameters)

    def get_id(self):
        return "gmx"

    async def reconcile(self) -> None:
        await self.state.reconcile()
        self.timestamp = myUtcNow()

    def serialize(self) -> dict:
        result = {'timestamp': self.timestamp} | {key: getattr(self.state, key)
                  for key in ['tokenBalances', 'poolAmounts', 'usdgAmounts', 'pricesUp', 'pricesDown',
                              'guaranteedUsd',
                              'reservedAmounts', 'globalShortSizes', 'globalShortAveragePrices', 'feeReserves',
                              'actualAum', 'totalSupply','rewards']}
        return result
'''
the below is only for documentation
'''
#
# def mintAndStakeGlpETH(self,_token,tokenAmount) -> None:
#     '''mintAndStakeGlp: 0x006ac9fb77641150b1e4333025cb92d0993a878839bb22008c0f354dfdcaf5e7'''
#     usdgAmount = tokenAmount*self.pricesDown[_token]
#     fee = vaultUtils.getBuyUsdgFeeBasisPoints(_token, usdgAmount)*usdgAmount
#     self.feeReserves[_token] += fee
#     self.usdgAmount[_token] += (usdgAmount - fee) * self.pricesDown[_token]
#     self.poolAmount[_token] += (usdgAmount - fee)
#
# def increasePosition(self, collateral:float, _indexToken: str, _collateralToken: str, _sizeDelta: float, _isLong: bool):
#     '''
#     for longs only:
#       poolAmounts = coll - fee
#       guaranteedUsd = _sizeDelta + fee - collateralUsd
#     createIncreasePositionETH (short): 0x0fe07013cca821bcea7bae4d013ab8fd288dbc3a26ed9dfbd10334561d3ffa91
#     setPricesWithBitsAndExecute: 0x8782436fd0f365aeef20fc8fbf9fa01524401ea35a8d37ad7c70c96332ce912b
#     '''
#     fee = GLPState.fee
#     #self.positions += GLPSimulator.Position(collateral - fee, price, _sizeDelta, myUtcNow())
#     self.reservedAmounts[_collateralToken] += _sizeDelta / self.pricesDown[_collateralToken]
#     if _isLong:
#         self.guaranteedUsd[_collateralToken] += _sizeDelta + fee - collateral * self.pricesDown[_collateralToken]
#         self.poolAmount[_collateralToken] += collateral - fee / self.pricesDown[_collateralToken]
#     else:
#         self.globalShortAveragePrices[_indexToken] = (self.pricesDown[_indexToken] * _sizeDelta + self.globalShortAveragePrices[_indexToken] * self.globalShortSizes) / (_sizeDelta + self.globalShortSizes[_collateralToken])
#         self.globalShortSizes += _sizeDelta
#
# def decreasePosition(self, _collateralToken: str, collateral: float, token: str, _sizeDelta: float, _isLong: bool):
#     self.reservedAmounts[token] = 0
#     self.poolAmount[token] -= pnl
#     if _isLong:
#         self.guaranteedUsd[token] -= _sizeDelta - collateral * originalPx
#         self.poolAmount[token] -= collateral

'''
relevant topics
'''

# - called everywhere
# emit IncreasePoolAmount(address _token, uint256 _amount) 0x976177fbe09a15e5e43f848844963a42b41ef919ef17ff21a17a5421de8f4737 --> poolAmounts(_token)
# emit DecreasePoolAmount(_token, _amount) 0x112726233fbeaeed0f5b1dba5cb0b2b81883dee49fb35ff99fd98ed9f6d31eb0 --> poolAmounts(_token)
#
# - only called on position increase
# emit IncreaseReservedAmount(_token, _amount); 0xaa5649d82f5462be9d19b0f2b31a59b2259950a6076550bac9f3a1c07db9f66d --> reservedAmounts(_token)
# emit DecreaseReservedAmount(_token, _amount); 0x533cb5ed32be6a90284e96b5747a1bfc2d38fdb5768a6b5f67ff7d62144ed67b --> reservedAmounts(_token)
#
# - globalShortSize has no emit, so need following
# emit IncreasePosition(key, _account, _collateralToken, _indexToken, _collateralDelta, _sizeDelta, _isLong, price, usdOut.sub(usdOutAfterFee)); 0x2fe68525253654c21998f35787a8d0f361905ef647c854092430ab65f2f15022
# emit DecreasePosition(key, _account, _collateralToken, _indexToken, _collateralDelta, _sizeDelta, _isLong, price, usdOut.sub(usdOutAfterFee)); 0x93d75d64d1f84fc6f430a64fc578bdd4c1e090e90ea2d51773e626d19de56d30
#
# - and for tracking usd value:
# emit IncreaseGuaranteedUsd(_token, _usdAmount); 0xd9d4761f75e0d0103b5cbeab941eeb443d7a56a35b5baf2a0787c03f03f4e474
# emit DecreaseGuaranteedUsd(_token, _usdAmount); 0x34e07158b9db50df5613e591c44ea2ebc82834eff4a4dc3a46e000e608261d68