import asyncio
import collections
import functools
from datetime import timezone, datetime
import dateutil

import numpy as np
import pandas as pd

import ccxtpro
from utils.async_utils import safe_gather
from utils.ccxt_utilities import api_params, calc_basis
from utils.config_loader import configLoader
from utils.ftx_utils import getUnderlyingType
from utils.io_utils import myUtcNow

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
        if self.strategy.lock['reconciling'].locked():
            self.strategy.logger.warning(f'message during reconciliation{args[2]}')
            self.message_missed.append(args[2])
        else:
            return wrapped(*args, **kwargs)
    return _wrapper


class PegRule:
    Actions = set(['repeg','stopout'])
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

class VenueAPI(ccxtpro.ftx):
    '''VenueAPI implements rest calls and websocket loops to observe raw market data / order events and place orders
    send events for Strategy to action
    send events to SignalEngine for further processing'''

    class Static(dict):
        _cache = dict()  # {function_name: result}

        def __init__(self):
            super().__init__()

        @staticmethod
        async def build(exchange, symbols):
            result = VenueAPI.Static()
            trading_fees = await exchange.fetch_trading_fees()
            for symbol in symbols:
                result[symbol] = \
                    {
                        'priceIncrement': float(exchange.markets[symbol]['info']['priceIncrement']),
                        'sizeIncrement': float(exchange.markets[symbol]['info']['minProvideSize']),
                        'taker_fee': trading_fees[symbol]['taker'],
                        'maker_fee': trading_fees[symbol]['maker'],
                        'takerVsMakerFee': trading_fees[symbol]['taker'] - trading_fees[symbol]['maker']
                    }
            return result

       ### get all static fields TODO: could just append coindetails if it wasn't for index,imf factor,positionLimitWeight
        @staticmethod
        async def fetch_futures(exchange):
            if 'fetch_futures' in VenueAPI.Static._cache:
                return VenueAPI.Static._cache['fetch_futures']

            includeExpired = True
            includeIndex = False

            response = await exchange.publicGetFutures()
            fetched = await exchange.fetch_markets()
            expired = await exchange.publicGetExpiredFutures() if includeExpired == True else []
            coin_details = await VenueAPI.Static.fetch_coin_details(exchange)

            otc_file = configLoader.get_static_params_used()

            #### for IM calc
            account_leverage = (await exchange.privateGetAccount())['result']
            if float(account_leverage['leverage']) >= 50: print("margin rules not implemented for leverage >=50")

            markets = exchange.safe_value(response, 'result', []) + exchange.safe_value(expired, 'result', [])

            perp_list = [f['name'] for f in markets if f['type'] == 'perpetual' and f['enabled']]
            funding_rates = await safe_gather([exchange.publicGetFuturesFutureNameStats({'future_name': f})
                                               for f in perp_list])
            funding_rates = {name: float(rate['result']['nextFundingRate']) * 24 * 365.325 for name, rate in
                             zip(perp_list, funding_rates)}

            result = []
            for i in range(0, len(markets)):
                market = markets[i]
                underlying = exchange.safe_string(market, 'underlying')
                ## eg ADA has no coin details
                if (underlying in ['ROSE', 'SCRT', 'AMC']) \
                        or (exchange.safe_string(market, 'tokenizedEquity') == True) \
                        or (exchange.safe_string(market, 'type') in ['move', 'prediction']) \
                        or (exchange.safe_string(market, 'enabled') == False):
                    continue
                if not underlying in coin_details.index:
                    if not includeIndex: continue
                try:  ## eg DMG-PERP doesn't exist (IncludeIndex = True)
                    symbol = exchange.market(exchange.safe_string(market, 'name'))['symbol']
                except Exception as e:
                    continue

                mark = exchange.safe_number(market, 'mark')
                imfFactor = exchange.safe_number(market, 'imfFactor')
                expiryTime = dateutil.parser.isoparse(exchange.safe_string(market, 'expiry')).replace(
                    tzinfo=timezone.utc) if exchange.safe_string(market, 'type') == 'future' else np.NaN
                if exchange.safe_string(market, 'type') == 'future':
                    future_carry = calc_basis(mark, market['index'], expiryTime,
                                              datetime.utcnow().replace(tzinfo=timezone.utc))
                elif market['name'] in perp_list:
                    future_carry = funding_rates[exchange.safe_string(market, 'name')]
                else:
                    future_carry = 0

                result.append({
                    'ask': exchange.safe_number(market, 'ask'),
                    'bid': exchange.safe_number(market, 'bid'),
                    'change1h': exchange.safe_number(market, 'change1h'),
                    'change24h': exchange.safe_number(market, 'change24h'),
                    'changeBod': exchange.safe_number(market, 'changeBod'),
                    'volumeUsd24h': exchange.safe_number(market, 'volumeUsd24h'),
                    'volume': exchange.safe_number(market, 'volume'),
                    'symbol': exchange.safe_string(market, 'name'),
                    "enabled": exchange.safe_value(market, 'enabled'),
                    "expired": exchange.safe_value(market, 'expired'),
                    "expiry": exchange.safe_string(market, 'expiry') if exchange.safe_string(market,
                                                                                             'expiry') else 'None',
                    'index': exchange.safe_number(market, 'index'),
                    'imfFactor': exchange.safe_number(market, 'imfFactor'),
                    'last': exchange.safe_number(market, 'last'),
                    'lowerBound': exchange.safe_number(market, 'lowerBound'),
                    'mark': exchange.safe_number(market, 'mark'),
                    'name': exchange.safe_string(market, 'name'),
                    "perpetual": exchange.safe_value(market, 'perpetual'),
                    # 'positionLimitWeight': exchange.safe_value(market, 'positionLimitWeight'),
                    # "postOnly": exchange.safe_value(market, 'postOnly'),
                    'priceIncrement': exchange.safe_value(market, 'priceIncrement'),
                    'sizeIncrement': exchange.safe_value(market, 'sizeIncrement'),
                    'underlying': exchange.safe_string(market, 'underlying'),
                    'upperBound': exchange.safe_value(market, 'upperBound'),
                    'type': exchange.safe_string(market, 'type'),
                    ### additionnals
                    'new_symbol': exchange.market(exchange.safe_string(market, 'name'))['symbol'],
                    'openInterestUsd': exchange.safe_number(market, 'openInterestUsd'),
                    'account_leverage': float(account_leverage['leverage']),
                    'collateralWeight': coin_details.loc[
                        underlying, 'collateralWeight'] if underlying in coin_details.index else 'coin_details not found',
                    'underlyingType': getUnderlyingType(
                        coin_details.loc[underlying]) if underlying in coin_details.index else 'index',
                    'spot_ticker': exchange.safe_string(market, 'underlying') + '/USD',
                    'cash_borrow': coin_details.loc[underlying, 'borrow'] if underlying in coin_details.index and
                                                                             coin_details.loc[
                                                                                 underlying, 'spotMargin'] else None,
                    'future_carry': future_carry,
                    'spotMargin': 'OTC' if underlying in otc_file.index else (coin_details.loc[
                                                                                  underlying, 'spotMargin'] if underlying in coin_details.index else 'coin_details not found'),
                    'tokenizedEquity': coin_details.loc[
                        underlying, 'tokenizedEquity'] if underlying in coin_details.index else 'coin_details not found',
                    'usdFungible': coin_details.loc[
                        underlying, 'usdFungible'] if underlying in coin_details.index else 'coin_details not found',
                    'fiat': coin_details.loc[
                        underlying, 'fiat'] if underlying in coin_details.index else 'coin_details not found',
                    'expiryTime': expiryTime
                })

            return result

        @staticmethod
        async def fetch_coin_details(exchange):
            if 'fetch_coin_details' in VenueAPI.Static._cache:
                return VenueAPI.Static._cache['fetch_coin_details']

            coin_details = pd.DataFrame((await exchange.publicGetWalletCoins())['result']).astype(
                dtype={'collateralWeight': 'float', 'indexPrice': 'float'}).set_index('id')

            borrow_rates = pd.DataFrame((await exchange.private_get_spot_margin_borrow_rates())['result']).astype(
                dtype={'coin': 'str', 'estimate': 'float', 'previous': 'float'}).set_index('coin')[['estimate']]
            borrow_rates[['estimate']] *= 24 * 365.25
            borrow_rates.rename(columns={'estimate': 'borrow'}, inplace=True)

            lending_rates = pd.DataFrame((await exchange.private_get_spot_margin_lending_rates())['result']).astype(
                dtype={'coin': 'str', 'estimate': 'float', 'previous': 'float'}).set_index('coin')[['estimate']]
            lending_rates[['estimate']] *= 24 * 365.25
            lending_rates.rename(columns={'estimate': 'lend'}, inplace=True)

            borrow_volumes = pd.DataFrame((await exchange.public_get_spot_margin_borrow_summary())['result']).astype(
                dtype={'coin': 'str', 'size': 'float'}).set_index('coin')
            borrow_volumes.rename(columns={'size': 'borrow_open_interest'}, inplace=True)

            all = pd.concat([coin_details, borrow_rates, lending_rates, borrow_volumes], join='outer', axis=1)
            all = all.loc[coin_details.index]  # borrow summary has beed seen containing provisional underlyings
            all.loc[coin_details['spotMargin'] == False, 'borrow'] = None  ### hope this throws an error...
            all.loc[coin_details['spotMargin'] == False, 'lend'] = 0

            return all

    def __init__(self, parameters, private_endpoints=True):
        config = {
            'enableRateLimit': True,
            'newUpdates': True}
        if private_endpoints:  ## David personnal
            config |= {'apiKey': 'ZUWyqADqpXYFBjzzCQeUTSsxBZaMHeufPFgWYgQU',
            'secret': api_params['ftx']['key']}
        super().__init__(config=config)
        self.parameters = parameters
        self.strategy = None
        self.static = None

        self.message_missed = collections.deque(maxlen=parameters['cache_size'])
        self.options['tradesLimit'] = parameters['cache_size'] # TODO: shoud be in signalengine with a different name. inherited from ccxt....

        self.peg_rules: dict[str, PegRule] = dict()

    @staticmethod
    async def build(parameters):
        if parameters['exchange'] not in ['ftx']:
            raise Exception(f'exchange not implemented')
        else:
            exchange = VenueAPI(parameters)
            exchange.verbose = False
            if 'subaccount' in parameters: exchange.headers = {'FTX-SUBACCOUNT': parameters['subaccount']}
            exchange.authenticate()
            await exchange.load_markets()
            symbols = parameters['symbols'] if 'symbols' in parameters else list(exchange.markets.keys())
            exchange.static = await VenueAPI.Static.build(exchange,symbols)

        return exchange

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
        if not self.strategy.lock['reconciling'].locked():
            for fill in fills:
                if hasattr(self.strategy, 'process_fill'):
                    await getattr(self.strategy, 'process_fill')(fill)

    # ---------------------------------- orders

    @loop
    async def monitor_orders(self,symbol):
        '''maintains orders, pending_new, event_records'''
        orders = await self.watch_orders(symbol=symbol)
        if not self.strategy.lock['reconciling'].locked():
            for order in orders:
                if hasattr(self.strategy, 'process_order'):
                    await getattr(self.strategy, 'process_order')(order | {'comment': 'websocket_acknowledgment'})

    # ---------------------------------- misc

    async def watch_ticker(self, symbol, params={}):
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

    async def peg_or_stopout(self, symbol, size, edit_trigger_depth=None, edit_price_depth=None, stop_depth=None):
        size = self.round_to_increment(self.static[symbol]['sizeIncrement'], size)
        if abs(size) == 0:
            return

        #TODO: https://help.ftx.com/hc/en-us/articles/360052595091-Ratelimits-on-FTX
        opposite_side = self.tickers[symbol]['ask' if size>0 else 'bid']
        mid = self.tickers[symbol]['mid']

        priceIncrement = self.static[symbol]['priceIncrement']
        sizeIncrement = self.static[symbol]['sizeIncrement']

        if stop_depth is not None:
            stop_trigger = float(self.price_to_precision(symbol,stop_depth))

        #TODO: use orderbook to place before cliff; volume matters too.
        isTaker = edit_price_depth in ['rush_in', 'rush_out', 'taker_hedge']
        if not isTaker:
            edit_price = float(self.price_to_precision(symbol, opposite_side - (1 if size > 0 else -1) * edit_price_depth))
            edit_trigger = float(self.price_to_precision(symbol, edit_trigger_depth))
        else:
            edit_price = self.sweep_price_atomic(symbol, size * mid)
            edit_trigger = None
            self.strategy.logger.warning(f'{edit_price_depth} {size} {symbol}')

        # remove open order dupes is any (shouldn't happen)
        event_histories = self.strategy.order_manager.filter_order_histories([symbol], self.strategy.order_manager.openStates)
        if len(event_histories) > 1:
            first_pending_new = np.argmin(np.array([data[0]['timestamp'] for data in event_histories]))
            for i,event_history in enumerate(self.strategy.order_manager.filter_order_histories([symbol], self.strategy.order_manager.cancelableStates)):
                if i != first_pending_new:
                    await self.cancel_order(event_history[-1]['clientOrderId'],'duplicates')
                    self.strategy.logger.info('canceled duplicate {} order {}'.format(symbol,event_history[-1]['clientOrderId']))

        # skip if there is inflight on the spread
        # if self.pending_new_histories(coin) != []:#TODO: rather incorporate orders_pending_new in risk, rather than block
        #     if self.pending_new_histories(coin,symbol) != []:
        #         self.strategy.logger.info('orders {} should not be in flight'.format([order['clientOrderId'] for order in self.pending_new_histories(coin,symbol)[-1]]))
        #     else:
        #         # this happens mostly between pending_new and create_order on the other leg. not a big deal...
        #         self.strategy.logger.info('orders {} still in flight. holding off {}'.format(
        #             [order['clientOrderId'] for order in self.pending_new_histories(coin)[-1]],symbol))
        #     return
        pending_new_histories = self.strategy.order_manager.filter_order_histories(self.parameters['symbols'],
                                                                          ['pending_new'])
        if pending_new_histories != []:
            self.strategy.logger.info('orders {} should not be in flight'.format([order[-1]['clientOrderId'] for order in pending_new_histories]))
            return

        # if no open order, create an order
        order_side = 'buy' if size>0 else 'sell'
        if len(event_histories)==0:
            await self.create_order(symbol, 'limit', order_side, abs(size), price=edit_price,
                                                  params={'postOnly': not isTaker,
                                                          'ioc': isTaker,
                                                          'comment':edit_price_depth if isTaker else 'new'})
        # if only one and it's editable, stopout or peg or wait
        elif len(event_histories)==1 \
                and (self.strategy.order_manager.latest_value(event_histories[0][-1]['clientOrderId'], 'remaining') >= sizeIncrement) \
                and event_histories[0][-1]['state'] in self.strategy.order_manager.acknowledgedStates:
            order = event_histories[0][-1]
            order_distance = (1 if order['side'] == 'buy' else -1) * (opposite_side - order['price'])
            repeg_gap = (1 if order['side'] == 'buy' else -1) * (edit_price - order['price'])

            # panic stop. we could rather place a trailing stop: more robust to latency, but less generic.
            if (stop_depth and order_distance > stop_trigger) \
                    or isTaker:
                size = self.strategy.order_manager.latest_value(order['clientOrderId'], 'remaining')
                price = self.sweep_price_atomic(symbol, size * mid)
                await self.create_order(symbol, 'limit', order_side, abs(size),
                                                 price = price,
                                                 params={'postOnly':False,
                                                         'ioc':True,
                                                         'comment':edit_price_depth if isTaker else 'stop'},
                                                 previous_clientOrderId = order['clientOrderId'])
            # peg limit order
            elif order_distance > edit_trigger and repeg_gap >= priceIncrement:
                await self.create_order(symbol, 'limit', order_side, abs(size),
                                                price=edit_price,
                                                params={'postOnly': True,
                                                        'ioc':False,
                                                        'comment':'chase'},
                                                previous_clientOrderId = order['clientOrderId'])

    async def peg_to_level(self, symbol, size, target, edit_trigger_depth=None):
        size = self.round_to_increment(self.static[symbol]['sizeIncrement'], size)
        if abs(size) == 0:
            return

        #TODO: use orderbook to place before cliff; volume matters too.
        pending_new_histories = self.strategy.order_manager.filter_order_histories(self.parameters['symbols'],
                                                                          ['pending_new'])
        if pending_new_histories != []:
            self.strategy.logger.info('orders {} should not be in flight'.format([order[-1]['clientOrderId'] for order in pending_new_histories]))
            return

        event_histories = self.strategy.order_manager.filter_order_histories([symbol], self.strategy.order_manager.openStates)
        # if no open order, create an order
        if len(event_histories)==0:
            await self.create_mkt_or_limit(symbol, size, target, 'new', edit_trigger_depth=edit_trigger_depth)
        # editables: stopout or peg
        for order in [history[-1] for history in event_histories
                      if self.strategy.order_manager.latest_value(history[-1]['clientOrderId'], 'remaining') >= self.static[symbol]['sizeIncrement']
                         and history[-1]['state'] in self.strategy.order_manager.cancelableStates
                         and history[-1]['clientOrderId'] in self.peg_rules]:
            if self.peg_rules[order['clientOrderId']](target) == 'repeg':
                if await self.cancel_order(order['clientOrderId'], 'edit'):
                    size = self.strategy.order_manager.latest_value(order['clientOrderId'], 'remaining')
                    await self.create_mkt_or_limit(symbol, size * (1 if order['side'] == 'buy' else -1), target, 'chase', edit_trigger_depth=edit_trigger_depth)

    # ---------------------------------- low level

    async def create_mkt_or_limit(self,symbol, size, target, comment, edit_trigger_depth=None):
        '''mkt if price good enough, incl slippage and fees
        limit otherwise. Mind not crossing.'''
        order_side = 'buy' if size > 0 else 'sell'
        opposite_side = self.tickers[symbol]['ask' if size > 0 else 'bid']
        sweep_price = self.sweep_price_atomic(symbol, size * self.tickers[symbol]['mid'], include_taker_vs_maker_fee=True)
        sweep_it = (sweep_price < target) if (size > 0) else (target < sweep_price)
        if sweep_it:
            await self.create_order(symbol, 'limit', order_side, abs(size), price=sweep_price,
                                     params={'postOnly': False,
                                             'ioc': True,
                                             'comment': comment})
        else:
            price = target if size * (target - opposite_side) <= 0 else opposite_side
            if size > 0:
                peg_rule = Chase(price - self.static[symbol]['priceIncrement'], price + edit_trigger_depth)
            else:
                peg_rule = Chase(price - edit_trigger_depth, price + self.static[symbol]['priceIncrement'])
            await self.create_order(symbol, 'limit', order_side, abs(size), price=price,
                                    params={'postOnly': True,
                                            'ioc': False,
                                            'comment': comment},
                                    peg_rule=peg_rule)

    async def create_taker_hedge(self,symbol, size, comment='taker_hedge'):
        '''trade and cancel. trade may be partial and cancel may have failed !!'''
        sweep_price = self.sweep_price_atomic(symbol, size * self.mid(symbol))
        coro = [self.create_order(symbol, 'limit', ('buy' if size>0 else 'sell'), abs(size), price=sweep_price,
                                                        params={'postOnly': False,
                                                                'ioc': False,
                                                                'comment': comment})]
        cancelable_orders = self.strategy.order_manager.filter_order_histories([symbol],
                                                                         self.strategy.order_manager.cancelableStates)
        coro += [self.cancel_order(order[-1]['clientOrderId'], 'cancel_symbol')
                               for order in cancelable_orders]
        await asyncio.gather(*coro)

    async def create_order(self, symbol, type, side, amount, price=None, params={},previous_clientOrderId=None,peg_rule: PegRule=None):
        '''if not new, cancel previous first
        if acknowledged, place order. otherwise just reconcile
        orders_pending_new is blocking'''
        if previous_clientOrderId is not None:
            await self.cancel_order(previous_clientOrderId, 'edit')

        trimmed_size = self.strategy.position_manager.trim_to_margin({symbol:amount * (1 if side == 'buy' else -1)})[symbol]
        rounded_amount = self.round_to_increment(self.static[symbol]['sizeIncrement'], trimmed_size)
        if rounded_amount < self.static[symbol]['sizeIncrement']:
            return
        # set pending_new -> send rest -> if success, leave pending_new and give id. Pls note it may have been caught by handle_order by then.
        clientOrderId = self.strategy.order_manager.pending_new({'symbol': symbol,
                                                    'type': type,
                                                    'side': side,
                                                    'amount': rounded_amount,
                                                    'remaining': rounded_amount,
                                                    'price': price,
                                                    'comment': params['comment']})
        try:
            # REST request
            order = await super().create_order(symbol, type, side, rounded_amount, price, {'clientOrderId':clientOrderId} | params)
        except Exception as e:
            order = {'clientOrderId':clientOrderId,
                     'timestamp':myUtcNow(),
                     'state':'rejected',
                     'comment':'create/'+str(e)}
            self.strategy.order_manager.cancel_or_reject(order)
            if isinstance(e,ccxtpro.InsufficientFunds):
                self.strategy.logger.info(f'{clientOrderId} too big: {rounded_amount*self.mid(symbol)}')
            elif isinstance(e,ccxtpro.RateLimitExceeded):
                throttle = 200.0
                self.strategy.logger.info(f'{str(e)}: waiting {throttle} ms)')
                await asyncio.sleep(throttle / 1000)
            else:
                raise e
        else:
            self.strategy.order_manager.sent(order)
            if peg_rule is not None:
                self.peg_rules[clientOrderId] = peg_rule

    async def cancel_order(self, clientOrderId, trigger):
        '''set in flight, send cancel, set as pending cancel, set as canceled or insist'''
        symbol = clientOrderId.split('_')[1]
        self.strategy.order_manager.pending_cancel({'comment':trigger}
                                          | {key: [order[key] for order in self.strategy.order_manager[clientOrderId] if key in order][-1]
                                        for key in ['clientOrderId','symbol','side','amount','remaining','price']})  # may be needed

        try:
            status = await super().cancel_order(None,params={'clientOrderId':clientOrderId})
            self.strategy.order_manager.cancel_sent({'clientOrderId':clientOrderId,
                                        'symbol':symbol,
                                        'status':status,
                                        'comment':trigger})
        except ccxtpro.CancelPending as e:
            self.strategy.order_manager.cancel_sent({'clientOrderId': clientOrderId,
                                        'symbol': symbol,
                                        'status': str(e),
                                        'comment': trigger})
            return True
        except ccxtpro.InvalidOrder as e: # could be in flight, or unknown
            self.strategy.order_manager.cancel_or_reject({'clientOrderId':clientOrderId,
                                             'status':str(e),
                                             'state':'canceled',
                                             'comment':trigger})
            return False
        except Exception as e:
            self.strategy.logger.info(f'{clientOrderId} failed to cancel: {str(e)} --> retrying')
            await asyncio.sleep(.1)
            return await self.cancel_order(clientOrderId, trigger+'+')
        else:
            return True

    async def close_dust(self):
        data = await safe_gather([self.fetch_balance(),self.fetch_markets()],semaphore=self.strategy.rest_semaphor)
        balances = data[0]
        markets = data[1]
        coros = []
        for coin, balance in balances.items():
            if coin in self.currencies.keys() \
                    and coin != 'USD' \
                    and balance['total'] != 0.0 \
                    and abs(balance['total']) < self.static[coin+'/USD']['sizeIncrement']:
                size = balance['total']
                mid = float(self.markets[coin+'/USD']['info']['last'])
                if size > 0:
                    request = {'fromCoin':coin,'toCoin':'USD','size':abs(size)}
                else:
                    request = {'fromCoin':'USD','toCoin':coin,'size':abs(size)*self.mid(f'{coin}/USD')}
                coros += [self.privatePostOtcQuotes(request)]

        quoteId_list = await safe_gather(coros,semaphore=self.strategy.rest_semaphor)
        await safe_gather([self.privatePostOtcQuotesQuoteIdAccept({'quoteId': int(quoteId['result']['quoteId'])},semaphore=self.strategy.rest_semaphor)
        for quoteId in quoteId_list
        if quoteId['success']],semaphore=self.strategy.rest_semaphor)
