import asyncio
import collections
import functools

import numpy as np
from ccxt import async_support

import ccxtpro
from utils.async_utils import safe_gather
from utils.ccxt_utilities import api_params
from utils.ftx_utils import Static, sweep_price_atomic
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

class VenueAPI(ccxtpro.ftx):
    '''VenueAPI implements rest calls and websocket loops to observe raw market data / order events and place orders
    send events for Strategy to action
    send events to SignalEngine for further processing'''
    class Static(dict):
        def __init__(self):
            super().__init__()

    def __init__(self, parameters):
        super().__init__(config={  ## David personnal
                'enableRateLimit': True,
                'apiKey': 'ZUWyqADqpXYFBjzzCQeUTSsxBZaMHeufPFgWYgQU',
                'secret': api_params['ftx']['key'],
                'newUpdates': True})
        self.parameters = parameters
        self.strategy = None
        self.static = None

        self.message_missed = collections.deque(maxlen=parameters['cache_size'])
        self.options['tradesLimit'] = parameters['cache_size'] # TODO: shoud be in signalengine with a different name. inherited from ccxt....

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
            exchange.static = await Static.build(exchange,symbols)

        return exchange

    # --------------------------------------------------------------------------------------------
    # ---------------------------------- various helpers -----------------------------------------
    # --------------------------------------------------------------------------------------------

    def mid(self,symbol):
        if symbol == 'USD/USD': return 1.0
        data = self.tickers[symbol] if symbol in self.tickers else self.markets[symbol]['info']
        return 0.5*(float(data['bid'])+float(data['ask']))

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
        self.strategy.process_order_book_update(symbol,orderbook)

    def populate_ticker(self,symbol,orderbook):
        timestamp = orderbook['timestamp'] * 1000
        mid = 0.5 * (orderbook['bids'][0][0] + orderbook['asks'][0][0])
        self.tickers[symbol] = {'symbol': symbol,
                                'timestamp': timestamp,
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
                self.strategy.position_manager.process_fill(fill)
                self.strategy.order_manager.process_fill(fill)

    # ---------------------------------- orders

    @loop
    async def monitor_orders(self,symbol):
        '''maintains orders, pending_new, event_records'''
        orders = await self.watch_orders(symbol=symbol)
        if not self.strategy.lock['reconciling'].locked():
            for order in orders:
                self.strategy.order_manager.acknowledgment(order | {'comment': 'websocket_acknowledgment'})

    # ---------------------------------- misc

    async def watch_ticker(self, symbol, params={}):
        '''watch_order_book is faster than watch_tickers so we DON'T LISTEN TO TICKERS. Dirty...'''
        raise Exception("watch_order_book is faster than watch_tickers so we DON'T LISTEN TO TICKERS. Dirty...")

    @loop
    async def monitor_trades(self,symbol):
        '''maintains risk_state, event_records, logger.info
            #     await self.reconcile_state() is safer but slower. we have monitor_risk to reconcile'''
        trades = await self.watch_trades(symbol=symbol)
        self.strategy.process_trades(trades)

    # ---------------------------------- just implemented so we hold messages while reconciling
    @intercept_message_during_reconciliation
    def handle_my_trade(self, client, message):
        '''just implemented so we hold messages while reconciling'''
        super().handle_my_trade(client, message)

    @intercept_message_during_reconciliation
    def handle_order(self, client, message):
        '''just implemented so we hold messages while reconciling'''
        super().handle_order(client, message)

    #@intercept_message_during_reconciliation
    # def handle_trade(self, client, message):
    '''just implemented so we don't block messages while reconciling'''
    #     super().handle_trade(client, message)

    #@intercept_message_during_reconciliation
    def handle_order_book_update(self, client, message):
        '''just implemented so we don't block messages while reconciling'''
        super().handle_order_book_update(client, message)
        self.strategy.signal_engine.process_order_book_update(message)

    def round_to_increment(self, sizeIncrement, amount):
        if amount >= 0:
            return np.floor(amount/sizeIncrement) * sizeIncrement
        else:
            return -np.floor(-amount / sizeIncrement) * sizeIncrement

    def peg_or_stopout(self,symbol,size,orderbook,edit_trigger_depth,edit_price_depth,stop_depth=None):
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
            edit_price = sweep_price_atomic(orderbook, size * mid)
            edit_trigger = None
            self.strategy.logger.warning(f'{edit_price_depth} {size} {symbol}')

        # remove open order dupes is any (shouldn't happen)
        event_histories = self.strategy.order_manager.filter_order_histories([symbol], self.strategy.order_manager.openStates)
        if len(event_histories) > 1:
            first_pending_new = np.argmin(np.array([data[0]['timestamp'] for data in event_histories]))
            for i,event_history in enumerate(self.strategy.order_manager.filter_order_histories([symbol], self.strategy.order_manager.cancelableStates)):
                if i != first_pending_new:
                    asyncio.create_task(self.cancel_order(event_history[-1]['clientOrderId'],'duplicates'))
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
            asyncio.create_task(self.create_order(symbol, 'limit', order_side, abs(size), price=edit_price,
                                                  params={'postOnly': not isTaker,
                                                          'ioc': isTaker,
                                                          'comment':edit_price_depth if isTaker else 'new'}))
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
                price = sweep_price_atomic(orderbook, size * mid)
                asyncio.create_task(self.edit_order(symbol, 'limit', order_side, abs(size),
                                                     price = price,
                                                     params={'postOnly':False,
                                                             'ioc':True,
                                                             'comment':edit_price_depth if isTaker else 'stop'},
                                                     previous_clientOrderId = order['clientOrderId']))
            # peg limit order
            elif order_distance > edit_trigger and repeg_gap >= priceIncrement:
                asyncio.create_task(self.edit_order(symbol, 'limit', order_side, abs(size),
                                                    price=edit_price,
                                                    params={'postOnly': True,
                                                            'ioc':False,
                                                            'comment':'chase'},
                                                    previous_clientOrderId = order['clientOrderId']))

    # ---------------------------------- low level

    async def edit_order(self,*args,**kwargs):
        if await self.cancel_order(kwargs.pop('previous_clientOrderId'), 'edit'):
            return await self.create_order(*args,**kwargs)

    async def create_order(self, symbol, type, side, amount, price=None, params={}):
        '''if acknowledged, place order. otherwise just reconcile
        orders_pending_new is blocking'''
        mid = self.tickers[symbol]['mid']
        trimmed_size = self.strategy.position_manager.trim_to_margin(mid, amount, symbol)
        rounded_amount = self.round_to_increment(self.static[symbol]['sizeIncrement'], trimmed_size)
        if rounded_amount == 0:
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
            order = await super().create_order(symbol, type, side, rounded_amount, price, params | {'clientOrderId':clientOrderId})
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
            return True
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
            return await self.strategy.order_manager.cancel_order(clientOrderId, trigger+'+')

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
