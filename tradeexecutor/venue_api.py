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

    @abstractmethod
    def mid(self,symbol):
        raise NotImplementedError

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