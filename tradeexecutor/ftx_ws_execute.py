import asyncio
import logging
import os.path
import time as t

import numpy as np
import pandas as pd

from utils.MyLogger import ExecutionLogger
from utils.ftx_utils import *
from utils.config_loader import *
from utils.ccxt_utilities import *
from histfeed.ftx_history import fetch_trades_history, vwap_from_list
from riskpnl.ftx_risk_pnl import diff_portoflio
from riskpnl.ftx_margin import MarginCalculator
from utils.api_utils import api,build_logging
from utils.async_utils import *
from utils.io_utils import async_read_csv,async_to_csv,NpEncoder,myUtcNow

import ccxtpro,collections,copy

# parameters guide
# 'time_budget': scaling aggressiveness to 0 at time_budget (in seconds)
# 'global_beta': other coin weight in the global risk
# 'cache_size': missed messages cache
# 'entry_tolerance': green light if basket better than quantile(entry_tolerance)
# 'rush_in_tolerance': mkt enter on both legs if basket better than quantile(rush_tolerance)
# 'rush_out_tolerance': mkt close on both legs if basket worse than quantile(rush_tolerance)
# 'stdev_window': stdev horizon for scaling parameters. in sec.
# 'edit_price_tolerance': place on edit_price_tolerance (in minutes) *  stdev
# 'aggressive_edit_price_tolerance': in priceIncrement
# 'edit_trigger_tolerance': chase at edit_trigger_tolerance (in minutes) *  stdev
# 'stop_tolerance': stop at stop_tolerance (in minutes) *  stdev
# 'volume_share': % of mkt volume * edit_price_tolerance. so farther is bigger.
# 'check_frequency': risk recon frequency. in seconds
# 'delta_limit': in % of pv

# qd tu ecoutes un channel ws c'est un while true loop
# c'est while symbol est encore running inventory_target


def symbol_locked(wrapped):
    # pour eviter que 2 ws agissent sur l'OMS en mm tps
    # ne fait RIEN pour le moment
    '''decorates self.order_lifecycle.xxx(lientOrderId) to prevent race condition on state'''
    @functools.wraps(wrapped)
    def _wrapper(*args, **kwargs):
        return wrapped(*args, **kwargs) # TODO:temporary disabled

        self=args[0]
        symbol = args[1]['clientOrderId'].split('_')[1]
        while self.lock[symbol].count:
            self.logger.info(f'{self.lock[symbol].count}th race conditions on {symbol} blocking {wrapped.__name__}')
            #await asyncio.sleep(0)
        with self.lock[symbol]:
            try:
                return wrapped(*args, **kwargs)
            except KeyError as e:
                pass
            except Exception as e:
                pass
    return _wrapper

def loop(func):
    @functools.wraps(func)
    async def wrapper_loop(*args, **kwargs):
        self=args[0]
        while len(args)==1 or (args[1] in args[0].strategy):
            try:
                value = await func(*args, **kwargs)
            except ccxt.NetworkError as e:
                self.logger.info(str(e))
                self.logger.info('reconciling after '+func.__name__+' dropped off')
                if self.position_manager and self. order_manager:
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
        if args[0].lock['reconciling'].locked():
            self.logger.warning(f'message during reconciliation{args[2]}')
            self.message_missed.append(args[2])
        else:
            return wrapped(*args, **kwargs)
    return _wrapper

class OrderManager(dict):
    '''OrderManager manages and records order state transitions.
    structure is {clientOrderId:[{...state dictionnary...},...]}
    able to reconcile to an exchange'''
    allStates = set(['pending_new', 'pending_cancel', 'sent', 'cancel_sent', 'pending_replace', 'acknowledged', 'partially_filled', 'filled', 'canceled', 'rejected'])
    openStates = set(['pending_new', 'sent', 'pending_cancel', 'pending_replace', 'acknowledged', 'partially_filled'])
    acknowledgedStates = set(['acknowledged', 'partially_filled'])
    cancelableStates = set(['sent', 'acknowledged', 'partially_filled'])

    def __init__(self,parameters,strategy):
        super().__init__()
        self.parameters = parameters
        self.strategy = strategy
        
        self.latest_order_reconcile_timestamp = parameters['timestamp']
        self.latest_fill_reconcile_timestamp = parameters['timestamp']
        self.logger = logging.getLogger('tradeexecutor')

    @staticmethod
    def build(parameters):
        if not parameters:
            return None
        else:
            return OrderManager({'timestamp': myUtcNow()} | parameters['order_manager'])
        
    def to_dict(self):
        return dict(self)

    '''various helpers'''

    def filter_order_histories(self,symbols=[],state_set=[]):
        '''returns all blockchains for symbol (all symbols if None), in a set of current states'''
        return [data
            for data in self.values()
            if ((data[0]['symbol'] in symbols) or (symbols == []))
            and ((data[-1]['state'] in state_set) or (state_set == []))]

    def latest_value(self,clientOrderId,key):
        for previous_state in reversed(self[clientOrderId]):
            if key in previous_state:
                return previous_state[key]
        raise f'{key} not found for {clientOrderId}'

    def find_clientID_from_fill(self,fill):
        '''find order by id, even if still in flight
        all layers events must carry id if known !! '''
        if 'clientOrderId' in fill:
            found = fill['clientOrderId']
            assert (found in self),f'{found} unknown'
        else:
            try:
                found = next(clientID for clientID, events in self.items() if
                             any('id' in event and event['id'] == fill['order'] for event in events))
            except StopIteration as e:# could still be in flight --> lookup
                try:
                    found = next(clientID for clientID,events in self.items() if
                                 any(event['price'] == fill['price']
                                     and event['amount'] == fill['amount']
                                     and event['symbol'] == fill['symbol']
                                     #and x['type'] == fill['type'] # sometimes None in fill
                                     and event['side'] == fill['side'] for event in events))
                except StopIteration as e:
                    raise Exception("fill {} not found".format(fill))
        return found

    '''state transitions'''

    # pour creer un ordre
    def pending_new(self, order_event):
        '''self = {clientId:[{key:data}]}
        order_event:trigger,symbol'''
        #1) resolve clientID
        nowtime = myUtcNow()
        clientOrderId = order_event['comment'] + '_' + order_event['symbol'] + '_' + str(int(nowtime))

        #2) validate block
        pass

        #3) make new block

        ## order details
        symbol = order_event['symbol']

        eventID = clientOrderId + '_' + str(int(nowtime))
        current = {'clientOrderId':clientOrderId,
                   'eventID': eventID,
                   'state': 'pending_new',
                   'remote_timestamp': None,
                   'timestamp': nowtime,
                   'id': None} | order_event


        ## risk details
        risk_data = self.strategy.position_manager
        current |= {'risk_timestamp':risk_data[symbol]['delta_timestamp'],
                    'delta':risk_data[symbol]['delta'],
                    'netDelta': risk_data.coin_delta(symbol),
                    'pv(wrong timestamp)':risk_data.pv,
                    'margin_headroom':risk_data.margin.actual_IM,
                    'IM_discrepancy': risk_data.margin.estimate('IM') - risk_data.margin.actual_IM}

        ## mkt details
        if symbol in self.strategy.venue_api.tickers:
            mkt_data = self.strategy.venue_api.tickers[symbol]
            timestamp = mkt_data['timestamp']
        else:
            mkt_data = self.strategy.venue_api.markets[symbol]['info']|{'bidVolume':0,'askVolume':0}#TODO: should have all risk group
            timestamp = self.strategy.parameters['timestamp']
        current |= {'mkt_timestamp': timestamp} \
                   | {key: mkt_data[key] for key in ['bid', 'bidVolume', 'ask', 'askVolume']}

        #4) mine genesis block
        self[clientOrderId] = [current]

        return clientOrderId

    @symbol_locked
    def sent(self, order_event):
        '''order_event:clientOrderId,timestamp,remaining,status'''
        # 1) resolve clientID
        clientOrderId = order_event['clientOrderId']

        # 2) validate block
        past = self[clientOrderId][-1]
        if past['state'] not in ['pending_new','acknowledged','partially_filled']:
            self.logger.warning('order {} was {} before sent'.format(past['clientOrderId'],past['state']))
            return

        # 3) new block
        nowtime = myUtcNow()
        eventID = clientOrderId + '_' + str(int(nowtime))
        current = order_event | {'eventID':eventID,
                                 'state':'sent',
                                 'remote_timestamp':order_event['timestamp']}
        current['timestamp'] = nowtime

        self[clientOrderId] += [current]
        if order_event['status'] == 'closed':
            if order_event['remaining'] == 0:
                current['order'] = order_event['id']
                current['id'] = None
                assert 'amount' in current,"'amount' in current"
                #self.fill(current)
            else:
                current['state'] = 'rejected'
                self.cancel_or_reject(current)

    @symbol_locked
    def pending_cancel(self, order_event):
        '''this is first called with result={}, then with the rest response. could be two states...
        order_event:clientOrderId,status,trigger'''
        # 1) resolve clientID
        clientOrderId = order_event['clientOrderId']

        # 2) validate block
        past = self[clientOrderId][-1]
        if past['state'] not in self.openStates:
            self.logger.warning('order {} re-canceled'.format(past['clientOrderId']))
            return

        # 3) new block
        nowtime = myUtcNow()
        eventID = clientOrderId + '_' + str(int(nowtime))
        current = order_event | {'eventID': eventID,
                                 'remote_timestamp': None,
                                 'state': 'pending_cancel'}
        current['timestamp'] = nowtime

        # 4) mine
        self[clientOrderId] += [current]

    @symbol_locked
    def cancel_sent(self, order_event):
        '''this is first called with result={}, then with the rest response. could be two states...
        order_event:clientOrderId,status'''
        # 1) resolve clientID
        clientOrderId = order_event['clientOrderId']

        # 2) validate block
        past = self[clientOrderId][-1]
        if not past['state'] in self.openStates:
            self.logger.warning('order {} re-canceled'.format(past['clientOrderId']))
            return

        # 3) new block
        nowtime = myUtcNow()
        eventID = clientOrderId + '_' + str(int(nowtime))
        current = order_event | {'eventID': eventID,
                                 'state': 'cancel_sent',
                                 'remote_timestamp': None,
                                 'timestamp': nowtime}

        # 4) mine
        self[clientOrderId] += [current]

    # reponse de websocket qui dit que c'est acknowledged
    @symbol_locked
    def acknowledgment(self, order_event):
        '''order_event: clientOrderId, trigger,timestamp,status'''
        # 1) resolve clientID
        clientOrderId = order_event['clientOrderId']

        # 2) validate block...is this needed?
        pass

        # 3) new block
        nowtime = datetime.utcnow().replace(tzinfo=timezone.utc).timestamp()*1000
        eventID = clientOrderId + '_' + str(int(nowtime))
        current = order_event | {'eventID': eventID,
                                 'remote_timestamp': order_event['timestamp']}
        # overwrite some fields
        current['timestamp'] = nowtime

        if order_event['status'] in ['new', 'open', 'triggered'] \
            and order_event['filled'] != 0:
                current['order'] = order_event['id']
                current['id'] = None
                #self.fill(current)
        elif order_event['status'] in ['closed']:
            #TODO: order event. not necessarily rejected ?
            if order_event['remaining'] == 0:
                current['order'] = order_event['id']
                current['id'] = None
                #self.fill(current)
            else:
                current['state'] = 'rejected'
                self.cancel_or_reject(current)
        elif order_event['status'] in ['canceled']:
            current['state'] = 'canceled'
            self.cancel_or_reject(current)
        # elif order_event['filled'] !=0:
        #     raise Exception('filled ?')#TODO: code that ?
        #     current['order'] = order_event['id']
        #     current['id'] = None
        #     assert 'amount' in current,"assert 'amount' in current"
        #     self.fill(current)
        else:
            current['state'] = 'acknowledged'
            self[clientOrderId] += [current]
            self.strategy.position_manager.margin.add_open_order(order_event)

    @symbol_locked
    def fill(self,order_event):
        '''order_event: id, trigger,timestamp,amount,order,symbol'''
        self.fill_flag = True
        # 1) resolve clientID
        clientOrderId = self.find_clientID_from_fill(order_event)
        symbol = order_event['symbol']

        nowtime = datetime.utcnow().replace(tzinfo=timezone.utc).timestamp()*1000
        eventID = clientOrderId + '_' + str(int(nowtime))
        current = order_event | {'eventID':eventID,
                                 'fillId': order_event['id'],
                                 'remote_timestamp': order_event['timestamp']}
        # overwrite some fields
        current['timestamp'] = nowtime
        current['remaining'] = max(0, self.latest_value(clientOrderId,'remaining') - order_event['amount'])
        current['id'] = order_event['order']

        if current['remaining'] == 0:
            self[clientOrderId] += [current|{'state':'filled'}]
        else:
            self[clientOrderId] += [current | {'state': 'partially_filled'}]

    @symbol_locked
    def cancel_or_reject(self, order_event):
        '''order_event: id, trigger,timestamp,amount,order,symbol'''
        # 1) resolve clientID
        clientOrderId = order_event['clientOrderId']

        # 2) validate block
        pass

        # 3) new block
        # if isinstance(order_event['timestamp']+.1, float):
        #     nowtime = order_event['timestamp']
        # else:
        #     nowtime = order_event['timestamp'][0]
        nowtime = myUtcNow()
        eventID = clientOrderId + '_' + str(int(nowtime))
        current = order_event | {'eventID':eventID,
                                 'state': order_event['state'],
                                 'remote_timestamp': order_event['timestamp'] if 'timestamp' in order_event else None}
        current['timestamp'] = nowtime

        # 4) mine
        self[clientOrderId] += [current]

    '''reconciliations to an exchange'''
    async def reconcile(self):
        await asyncio.gather(self.reconcile_orders(),self.reconcile_fills())

    async def reconcile_fills(self):
        '''fetch fills, to recover missed messages'''
        sincetime = max(self.strategy.parameters['timestamp'], self.latest_fill_reconcile_timestamp - 1000)
        fetched_fills = sum(await asyncio.gather(*[self.strategy.venue_api.fetch_my_trades(since=sincetime,symbol=symbol) for symbol in self.strategy.parameters['symbols']]), [])
        for fill in fetched_fills:
            fill['comment'] = 'reconciled'
            fill['clientOrderId'] = self.find_clientID_from_fill(fill)
            if fill['id'] not in [block['fillId']
                                     for block in self[fill['clientOrderId']]
                                     if 'fillId' in block]:
                self.fill(fill)
        self.latest_fill_reconcile_timestamp = myUtcNow()

    async def reconcile_orders(self):
        self.latest_order_reconcile_timestamp = myUtcNow()

        # list internal orders
        internal_order_internal_status = {clientOrderId:data[-1] for clientOrderId,data in self.items()
                                          if data[-1]['state'] in OrderManager.openStates}

        # add missing orders (we missed orders from the exchange)
        external_orders = sum(await asyncio.gather(*[self.strategy.venue_api.fetch_open_orders(symbol=symbol) for symbol in self.strategy.parameters['symbols']]), [])
        for order in external_orders:
            if order['clientOrderId'] not in internal_order_internal_status.keys():
                self.acknowledgment(order | {'comment':'reconciled_missing'})
                self.logger.warning('{} was missing'.format(order['clientOrderId']))
                found = order['clientOrderId']
                assert (found in self), f'{found} unknown'

        # remove zombie orders (we beleive they are live but they are not open)
        # should not happen so we put it in the logs
        internal_order_external_status = await safe_gather([self.strategy.venue_api.fetch_order(id=None, params={'clientOrderId':clientOrderId})
                                                   for clientOrderId in internal_order_internal_status.keys()],
                                                           semaphore=self.strategy.rest_semaphor,
                                                           return_exceptions=True)
        for clientOrderId,external_status in zip(internal_order_internal_status.keys(),internal_order_external_status):
            if isinstance(external_status, Exception):



                self.logger.warning(f'reconcile_orders {clientOrderId} : {external_status}')
                continue
            if external_status['status'] != 'open':
                self.cancel_or_reject(external_status | {'state': 'canceled', 'comment':'reconciled_zombie'})
                self.logger.warning('{} was a {} zombie'.format(clientOrderId,internal_order_internal_status[clientOrderId]['state']))

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

    def __init__(self,parameters,strategy):
        super().__init__()
        self.parameters = parameters
        self.strategy = strategy
        self.pv = None
        self.margin = None
        self.limit = None
        self.logger = logging.getLogger('tradeexecutor')
        self.risk_reconciliations = []
        self.markets = None

    def to_dict(self):
        return self.risk_reconciliations

    @staticmethod
    async def build(strategy,parameters):
        if not parameters:
            return None
        else:
            result = PositionManager({'timestamp': myUtcNow()} | parameters['position_manager'])
            result.limit = PositionManager.LimitBreached(parameters['position_manager']['check_frequency'], parameters['position_manager']['delta_limit'])
    
            for symbol in strategy.parameters['symbols']:
                result[symbol] = {'delta': 0,
                                  'delta_timestamp': myUtcNow(),
                                  'delta_id': 0}
    
            # Initializes a margin calculator that can understand the margin of an order
            # reconcile the margin of an exchange with the margin we calculate
            # pls note it needs the exchange pv, which is known after reconcile
            result.margin = await MarginCalculator.margin_calculator_factory(strategy.market_data_api)
            result.markets = strategy.market_data_api.markets
            
            return result

    async def reconcile(self,market_data_api):
        # recompute risks, margins
        previous_delta = {symbol:{'delta':data['delta']} for symbol,data in self.items()}
        previous_pv = self.pv

        state = await syncronized_state(market_data_api)
        risk_timestamp = await market_data_api.myUtcNow()

        # delta is noisy for perps, so override to delta 1.
        self.pv = 0
        for coin, balance in state['balances']['total'].items():
            if coin != 'USD':
                symbol = coin + '/USD'
                mid = state['markets']['price'][market_data_api.market_id(symbol)]
                delta = balance * mid
                if symbol not in self:
                    self[symbol] = {'delta_id': 0}
                self[symbol]['delta'] = delta
                self[symbol]['delta_timestamp'] = risk_timestamp
                self.pv += delta

        self.pv += state['balances']['total']['USD']  # doesn't contribute to delta, only pv !

        for name, position in state['positions']['netSize'].items():
            symbol = market_data_api.market(name)['symbol']
            delta = position * market_data_api.mid(symbol)

            if symbol not in self:
                self[symbol] = {'delta_id': 0}
            self[symbol]['delta'] = delta
            self[symbol]['delta_timestamp'] = risk_timestamp

        # update IM
        await self.margin.refresh(market_data_api, balances=state['balances']['total'], positions=state['positions']['netSize'],
                                  im_buffer=0.01 * self.pv)

        delta_error = {symbol: self[symbol]['delta'] - (previous_delta[symbol]['delta'] if symbol in previous_delta else 0)
                       for symbol in self}
        self.risk_reconciliations += [{'symbol': symbol_,
                                       'delta_timestamp': self[symbol_]['delta_timestamp'],
                                       'delta': self[symbol_]['delta'],
                                       'netDelta': self.coin_delta(symbol_),
                                       'pv': self.pv,
                                       'estimated_IM': self.margin.estimate('IM'),
                                       'actual_IM': self.margin.actual_IM,
                                       'pv_error': self.pv - (previous_pv or 0),
                                       'total_delta_error': sum(delta_error.values())}
                                      for symbol_ in self]
    def check_limit(self):
        absolute_risk = dict()
        for symbol in self:
            coin = self.markets[symbol]['base']
            if coin not in absolute_risk:
                absolute_risk[coin] = abs(self.coin_delta(symbol))
        if sum(absolute_risk.values()) > self.pv * self.limit.delta_limit:
            self.logger.info(
                f'absolute_risk {absolute_risk} > {self.pv * self.limit.delta_limit}')
        if self.margin.actual_IM < self.pv / 100:
            self.logger.info(f'IM {self.margin.actual_IM}  < 1%')

    def trim_to_margin(self, mid, size, symbol):
        '''trim size to margin allocation (equal for all running symbols)'''
        marginal_IM = self.margin.order_marginal_cost(symbol, size, mid, 'IM')
        estimated_IM = self.margin.estimate('IM')
        actual_IM = self.margin.actual_IM
        # TODO: headroom estimate is wrong ....
        if self.margin.actual_IM <= 0:
            self.logger.info(
                f'estimated_IM {estimated_IM} / actual_IM {actual_IM} / marginal_IM {marginal_IM}')
            # TODO: check before skipping?
            # await self.margin_calculator.update_actual(self)
            # headroom_estimate = self.margin_calculator.actual_IM
            # if headroom_estimate <= 0: return
        marginal_IM = marginal_IM if abs(marginal_IM) > 1e-9 else np.sign(marginal_IM) * 1e-9
        if actual_IM + marginal_IM < self.margin.IM_buffer:
            trim_factor = np.clip((self.margin.IM_buffer - actual_IM) / marginal_IM,a_min=0,a_max=1)
        else:
            trim_factor = 1.0
        trimmed_size = size * trim_factor
        if trim_factor < 1:
            self.logger.info(f'trimmed {size} {symbol} by {trim_factor}')
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

class Strategy(dict):
    '''Strategy leverages other managers to implement quoter (ie generate orders from mkt change or order feed)'''
    class ReadyToShutdown(Exception):
        def __init__(self, text):
            super().__init__(text)
    def __init__(self,parameters,position_manager,order_manager,venue_api,signal_engine):
        super().__init__()
        self.parameters = parameters['strategy']
        self.position_manager = position_manager
        self.order_manager = order_manager
        self.venue_api = venue_api
        self.signal_engine = signal_engine
        self.venue_api = venue_api

        self.logger = logging.getLogger('tradeexecutor')
        self.data_logger = ExecutionLogger(exchange_name=self.id)
        self.fill_flag = False

        self.rest_semaphor = asyncio.Semaphore(safe_gather_limit)
        self.lock |= {symbol: CustomRLock() for symbol in self.strategy}
        self.lock = {'reconciling':threading.Lock()}

    def to_json(self):
        return {'parameters': self.parameters} \
               | {key: getattr(self, key).to_json()
                  for key in ['strategy', 'order_manager', 'position_manager']}

    @staticmethod
    async def build(parameters):
        position_manager = await PositionManager.build(parameters['position_manager'])
        order_manager = OrderManager(parameters['order_manager'])
        market_data_api = await VenueAPI.build(parameters['market_data_api'])
        signal_engine = await SignalEngine.build(parameters['signal_engine'])
        venue_api = await OrderManager.build(parameters['venue_api'])

        result = ExecutionStrategy({'timestamp':myUtcNow()} |
                                   parameters['signal_manager'],
                                   position_manager,
                                   order_manager,
                                   market_data_api,
                                   signal_engine,
                                   venue_api)

        await result.reconcile()
        (spot, targets, timestamp) = await result.order_feed.update_order()
        result.set_target(spot, targets, timestamp)

        return result

    def run(self):
        self.logger.warning('cancelling orders')
        await asyncio.gather(*[self.venue_api.cancel_all_orders(symbol) for symbol in self.parameters['symbols']])

        coros = [self.venue_api.monitor_fills(), self.venue_api.monitor_risk()] + \
                sum([[self.venue_api.monitor_orders(symbol),
                      self.venue_api.monitor_order_book(symbol),
                      self.venue_api.monitor_trades(symbol)]
                     for symbol in self.parameters['symbols']], [])

        await asyncio.gather(*coros)


    async def build_listener(self, order, parameters):
        '''initialize all state and does some filtering (weeds out slow underlyings; should be in strategy)
            target_sub_portfolios = {coin:{rush_level_increment,
            symbol1:{'spot_price','diff','target'}]}]'''
        nowtime = datetime.utcnow().replace(tzinfo=timezone.utc)

        self.strategy = await Strategy.build(self, nowtime, order, parameters['strategy'])
        self.signal_engine = await SignalEngine.build(self, parameters['signal_engine'])

    async def reconcile(self):
        '''update risk using rest
        all symbols not present when state is built are ignored !
        if some tickers are not initialized, it just uses markets
        trigger interception of all incoming messages until done'''

        # if already running, skip reconciliation
        if self.lock['reconciling'].locked():
            return

        # or reconcile, and lock until done. We don't want to place orders while recon is running
        with self.lock['reconciling']:
            await self.order_manager.reconcile(self)
            await self.position_manager.reconcile(self)
            await self.strategy.reconcile(self)
            self.replay_missed_messages()

        # critical job is done, release lock and print data
        if self.order_manager.fill_flag:
            await self.data_logger.write_history()
            self.order_manager.fill_flag = False

    def replay_missed_messages(self):
        # replay missed _messages.
        while self.message_missed:
            message = self.message_missed.popleft()
            data = self.safe_value(message, 'data')
            channel = message['channel']
            self.logger.warning(f'replaying {channel} after recon')
            if channel == 'fills':
                fill = self.parse_trade(data)
                self.process_fill(fill | {'orderTrigger': 'replayed'})
            elif channel == 'orders':
                order = self.parse_order(data)
                if order['symbol'] in self.strategy:
                    self.process_order(order | {'orderTrigger': 'replayed'})
            # we don't interecpt the mktdata anyway...
            elif channel == 'orderbook':
                pass
                #self.populate_ticker(message['symbol'], message)
            elif channel == 'trades':
                pass
                #self.signal_engine.trades_cache[message['symbol']].append(message)

    def set_target(self, spot, targets,timestamp):
        for symbol, target in targets.items():
            if symbol not in self:
                self[symbol] = {'symbol':symbol, 'target': target, 'spot_price': spot}
        for symbol, target in targets.items():
            if self[symbol]['target'] != target:
                self.logger.info('{} weight changed from {} to {}'.format(
                    symbol, self[symbol]['target'] * spot, target * spot))
                self[symbol]['symbol'] = symbol
                self[symbol]['target'] = target
                self[symbol]['spot_price'] = spot
                self[symbol]['timestamp'] = timestamp

        self.data_logger.history += [dict(self)]

    def process_order(self,order):
        assert order['clientOrderId'],"assert order['clientOrderId']"
        self.order_manager.acknowledgment(order | {'comment': 'websocket_acknowledgment'}) # status new, triggered, open or canceled

    def process_fill(self, fill):
        symbol = fill['symbol']

        # update risk_state
        if symbol not in self.position_manager:
            self.position_manager[symbol] = {'delta': 0, 'delta_id': 0, 'delta_timestamp': myUtcNow()}
        data = self.position_manager[symbol]
        fill_size = fill['amount'] * (1 if fill['side'] == 'buy' else -1) * fill['price']
        data['delta'] += fill_size
        data['delta_id'] = max(data['delta_id'], int(fill['order']))
        data['delta_timestamp'] = fill['timestamp']

        # update margin
        self.position_manager.margin.add_instrument(symbol, fill_size)

        # only log trades being run by this process
        fill['clientOrderId'] = fill['info']['clientOrderId']
        fill['comment'] = 'websocket_fill'
        self.fill(fill)

        # logger.info
        self.logger.warning('{} filled after {}: {} {} at {}'.format(fill['clientOrderId'],
                                                                     fill['timestamp'] - int(
                                                                         fill['clientOrderId'].split('_')[-1]),
                                                                     fill['side'], fill['amount'],
                                                                     fill['price']))

        current = self.position_manager[symbol]['delta']
        target = self.strategy[symbol]['target'] * fill['price']
        diff = (self.strategy[symbol]['target'] - self.position_manager[symbol]['delta']) * fill['price']
        initial = self.strategy[symbol]['target'] * fill['price'] - diff
        self.logger.warning('{} risk at {} ms: {}% done [current {}, initial {}, target {}]'.format(
            symbol,
            self.position_manager[symbol]['delta_timestamp'],
            (current - initial) / diff * 100,
            current,
            initial,
            target))

class ExecutionStrategy(Strategy):
    async def reconcile(self, exchange):
        '''specialized to execute externally generated client orders'''
        if all(abs(exchange.position_manager[symbol]['delta']) < exchange.parameters[
            'significance_threshold'] * exchange.position_manager.pv for symbol in self):
            raise Strategy.ReadyToShutdown(
                f'no {self.keys()} delta and no {self.order_feed} order --> shutting down bot')
        if os.path.isfile(self.order_feed):
            (spot, targets, timestamp) = await self.order_feed.update_order()
            self.set_target(spot, targets, myUtcNow())
        else:
            targets = {key: 0 for key in self}
            spot = exchange.mid(next(key for key in self if exchange.market(key)['type'] == 'spot'))
            self.set_target(spot, targets, myUtcNow())

        self.signal_engine.compile_vwap(frequency=timedelta(minutes=1))

        def basket_vwap_quantile(series_list, diff_list, quantiles):
            series = pd.concat([serie * coef for serie, coef in zip(series_list, diff_list)], join='inner', axis=1)
            return [-1e18 if quantile<=0 else 1e18 if quantile>=1 else series.sum(axis=1).quantile(quantile) for quantile in quantiles]

        scaler = 1 # max(0,(time_limit-nowtime)/(time_limit-self.inventory_target.timestamp))
        for symbol, data in self.items():
            data['update_time_delta'] = data['target'] - exchange.position_manager[symbol]['delta'] / exchange.mid(symbol)

        for symbol, data in self.items():
            # entry_level = what level on basket is too bad to even place orders
            # rush_in_level = what level on basket is so good that you go in at market on both legs
            # rush_out_level = what level on basket is so bad that you go out at market on both legs
            quantiles = basket_vwap_quantile(
                [exchange.signal_engine.vwap[symbol]['vwap'] for symbol in self],
                [data['update_time_delta'] for symbol,data in self.items()],
                [exchange.parameters['entry_tolerance'],exchange.parameters['rush_in_tolerance'],exchange.parameters['rush_out_tolerance']])

            data['entry_level'] = quantiles[0]
            data['rush_in_level'] = quantiles[1]
            data['rush_out_level'] = quantiles[2]

            stdev = exchange.signal_engine.vwap[symbol]['vwap'].std().squeeze()
            if not (stdev>0): stdev = 1e-16

            # edit_price_depth = how far to put limit on risk increasing orders
            data['edit_price_depth'] = stdev * np.sqrt(exchange.parameters['edit_price_tolerance']) * scaler
            # edit_trigger_depth = how far to let the mkt go before re-pegging limit orders
            data['edit_trigger_depth'] = stdev * np.sqrt(exchange.parameters['edit_trigger_tolerance']) * scaler

            # aggressive version understand tolerance as price increment
            if isinstance(exchange.parameters['aggressive_edit_price_tolerance'],float):
                data['aggressive_edit_price_depth'] = max(1,exchange.parameters['aggressive_edit_price_tolerance']) * self.venue_api.static[symbol]['priceIncrement']
                data['aggressive_edit_trigger_depth'] = max(1,exchange.parameters['aggressive_edit_price_tolerance']) * self.venue_api.static[symbol]['priceIncrement']
            else:
                data['aggressive_edit_price_depth'] = exchange.parameters['aggressive_edit_price_tolerance']
                data['aggressive_edit_trigger_depth'] = None # takers don't peg

                # stop_depth = how far to set the stop on risk reducing orders
            data['stop_depth'] = stdev * np.sqrt(exchange.parameters['stop_tolerance']) * scaler

            # slice_size: cap to expected time to trade consistent with edit_price_tolerance
            volume_share = exchange.parameters['volume_share'] * exchange.parameters['edit_price_tolerance'] * \
                           exchange.signal_engine.vwap[symbol]['volume'].mean()
            data['slice_size'] = volume_share

    def quoter(self, symbol):
        '''
            leverages orderbook and risk to issue an order
            Critical loop, needs to go quick
            all executes in one go, no async
        '''
        mid = self.venue_api.tickers[symbol]['mid']
        params = self[symbol]

        # size to do:
        original_size = params['target'] - self.strategy.position_manager[symbol]['delta'] / mid

        # risk
        globalDelta = self.strategy.position_manager.delta_bounds(symbol)['global_delta']
        delta_limit = self.strategy.position_manager.limit.delta_limit * self.strategy.position_manager.pv
        marginal_risk = np.abs(globalDelta / mid + original_size) - np.abs(globalDelta / mid)

        # if increases risk but not out of limit, trim and go passive.
        if np.sign(original_size) == np.sign(globalDelta):
            # if (global_delta_plus / mid + original_size) > delta_limit:
            #     trimmed_size = delta_limit - global_delta_plus / mid
            #     self.strategy.logger.debug(
            #         f'{original_size * mid} {symbol} would increase risk over {self.strategy.limit.delta_limit * 100}% of {self.strategy.risk_state.pv} --> trimming to {trimmed_size * mid}')
            # elif (global_delta_minus / mid + original_size) < -delta_limit:
            #     trimmed_size = -delta_limit - global_delta_minus / mid
            #     self.strategy.logger.debug(
            #         f'{original_size * mid} {symbol} would increase risk over {self.strategy.limit.delta_limit * 100}% of {self.strategy.risk_state.pv} --> trimming to {trimmed_size * mid}')
            # else:
            #     trimmed_size = original_size
            # if np.sign(trimmed_size) != np.sign(original_size):
            #     self.strategy.logger.debug(f'skipping (we don t flip orders)')
            #     return
            if abs(globalDelta / mid + original_size) > delta_limit:
                if (globalDelta / mid + original_size) > delta_limit:
                    trimmed_size = delta_limit - globalDelta / mid
                elif (globalDelta / mid + original_size) < -delta_limit:
                    trimmed_size = -delta_limit - globalDelta / mid
                else:
                    raise Exception('what??')
                if np.sign(trimmed_size) != np.sign(original_size):
                    self.strategy.logger.debug(
                        f'{original_size * mid} {symbol} would increase risk over {self.strategy.position_manager.limit.delta_limit * 100}% of {self.strategy.position_manager.pv} --> skipping (we don t flip orders)')
                    return
                else:
                    self.strategy.logger.debug(
                        f'{original_size * mid} {symbol} would increase risk over {self.strategy.position_manager.limit.delta_limit * 100}% of {self.strategy.position_manager.pv} --> trimming to {trimmed_size * mid}')
            else:
                trimmed_size = original_size

            size = np.clip(trimmed_size, a_min=-params['slice_size'], a_max=params['slice_size'])

            current_basket_price = sum(self.venue_api.mid(_symbol) * self[_symbol]['update_time_delta']
                                       for _symbol in self.keys())
            # mkt order if target reached.
            # TODO: pray for the other coin to hit the same condition...
            if current_basket_price + 2 * abs(params['update_time_delta']) * params['takerVsMakerFee'] * mid < \
                    self[symbol]['rush_in_level']:
                # TODO: could replace current_basket_price by two way sweep after if
                self.venue_api.peg_or_stopout(symbol, size, self.venue_api.orderbook,
                                    edit_trigger_depth=params['edit_trigger_depth'],
                                    edit_price_depth='rush_in', stop_depth=None)
                return
            elif current_basket_price - 2 * abs(params['update_time_delta']) * params['takerVsMakerFee'] * mid > \
                    self[symbol]['rush_out_level']:
                # go all in as this decreases margin
                size = - self.position_manager[symbol]['delta'] / mid
                if abs(size) > 0:
                    self.venue_api.peg_or_stopout(symbol, size, self.venue_api.orderbook,
                                        edit_trigger_depth=params['edit_trigger_depth'],
                                        edit_price_depth='rush_out', stop_depth=None)
                return
            # limit order if level is acceptable (saves margin compared to faraway order)
            elif current_basket_price < self[symbol]['entry_level']:
                edit_trigger_depth = params['edit_trigger_depth']
                edit_price_depth = params['edit_price_depth']
                stop_depth = None
            # hold off to save margin, if level is bad-ish
            else:
                return
        # if decrease risk, go aggressive without flipping delta
        else:
            size = np.sign(original_size) * min(abs(- globalDelta / mid), abs(original_size))
            edit_trigger_depth = params['aggressive_edit_trigger_depth']
            edit_price_depth = params['aggressive_edit_price_depth']
            stop_depth = params['stop_depth']
        self.venue_api.peg_or_stopout(symbol, size, self.venue_api.orderbook, edit_trigger_depth=edit_trigger_depth,
                            edit_price_depth=edit_price_depth, stop_depth=stop_depth)

class AlgoStrategy(Strategy):
    async def reconcile(self):
        '''updates quoter inputs
        reads file if present. If not: if no risk then shutdown bot, else unwind.
        '''
        if all(abs(self.position_manager[symbol]['delta']) < self.parameters[
            'significance_threshold'] * self.position_manager.pv for symbol in self):
            raise Strategy.ReadyToShutdown(
                f'no {self.keys()} delta and no {self.order_feed} order --> shutting down bot')
        if os.path.isfile(self.order_feed):
            (spot, targets, timestamp) = await self.order_feed.update_order()
            self.set_target(spot, targets, myUtcNow())
        else:
            targets = {key: 0 for key in self}
            spot = self.venue_api.mid(next(key for key in self if self.venue_api.market(key)['type'] == 'spot'))
            self.set_target(spot, targets, myUtcNow())

        nowtime = myUtcNow()
        quantiles = self.signal_engine.compile_spread_distribution()

        for symbol, data in self.items():
            # entry_level = what level on basket is too bad to even place orders
            # rush_in_level = what level on basket is so good that you go in at market on both legs
            # rush_out_level = what level on basket is so bad that you go out at market on both legs
            is_long_carry = data['target'] * (-1 if ':USD' in symbol else 1) > 0
            data['rush_in_level'] = quantiles[1 if is_long_carry else 0] * self.venue_api.mid(symbol) / 365.25
            data['rush_out_level'] = quantiles[0 if is_long_carry else 1] * self.venue_api.mid(symbol) / 365.25

            stdev = self.signal_engine.vwap[symbol]['vwap'].std().squeeze()
            if not (stdev > 0): stdev = 1e-16

            # edit_price_depth = how far to put limit on risk increasing orders
            data['edit_price_depth'] = stdev * np.sqrt(self.parameters['edit_price_tolerance'])
            # edit_trigger_depth = how far to let the mkt go before re-pegging limit orders
            data['edit_trigger_depth'] = stdev * np.sqrt(self.parameters['edit_trigger_tolerance'])

            # aggressive version understand tolerance as price increment
            if isinstance(self.parameters['aggressive_edit_price_tolerance'], float):
                data['aggressive_edit_price_depth'] = max(1, self.parameters[
                    'aggressive_edit_price_tolerance']) * self.venue_api.static[symbol]['priceIncrement']
                data['aggressive_edit_trigger_depth'] = max(1, self.parameters[
                    'aggressive_edit_price_tolerance']) * self.venue_api.static[symbol]['priceIncrement']
            else:
                data['aggressive_edit_price_depth'] = self.parameters['aggressive_edit_price_tolerance']
                data['aggressive_edit_trigger_depth'] = None  # takers don't peg

                # stop_depth = how far to set the stop on risk reducing orders
            data['stop_depth'] = stdev * np.sqrt(self.parameters['stop_tolerance'])

            # slice_size: cap to expected time to trade consistent with edit_price_tolerance
            volume_share = self.parameters['volume_share'] * self.parameters[
                'edit_price_tolerance'] * \
                           self.signal_engine.vwap[symbol]['volume'].mean()
            data['slice_size'] = volume_share

    def quoter(self, symbol):
        '''
            leverages orderbook and risk to issue an order
            Critical loop, needs to go quick
            all executes in one go, no async
        '''
        mid = self.venue_api.tickers[symbol]['mid']
        params = self[symbol]

        # size to do:
        original_size = params['target'] - self.position_manager[symbol]['delta'] / mid

        # risk
        globalDelta = self.position_manager.delta_bounds(symbol)['global_delta']
        delta_limit = self.position_manager.limit.delta_limit * self.position_manager.pv
        marginal_risk = np.abs(globalDelta / mid + original_size) - np.abs(globalDelta / mid)

        # if increases risk but not out of limit, trim and go passive.
        if np.sign(original_size) == np.sign(globalDelta):
            # if (global_delta_plus / mid + original_size) > delta_limit:
            #     trimmed_size = delta_limit - global_delta_plus / mid
            #     self.strategy.logger.debug(
            #         f'{original_size * mid} {symbol} would increase risk over {self.strategy.limit.delta_limit * 100}% of {self.strategy.risk_state.pv} --> trimming to {trimmed_size * mid}')
            # elif (global_delta_minus / mid + original_size) < -delta_limit:
            #     trimmed_size = -delta_limit - global_delta_minus / mid
            #     self.strategy.logger.debug(
            #         f'{original_size * mid} {symbol} would increase risk over {self.strategy.limit.delta_limit * 100}% of {self.strategy.risk_state.pv} --> trimming to {trimmed_size * mid}')
            # else:
            #     trimmed_size = original_size
            # if np.sign(trimmed_size) != np.sign(original_size):
            #     self.strategy.logger.debug(f'skipping (we don t flip orders)')
            #     return
            if abs(globalDelta / mid + original_size) > delta_limit:
                if (globalDelta / mid + original_size) > delta_limit:
                    trimmed_size = delta_limit - globalDelta / mid
                elif (globalDelta / mid + original_size) < -delta_limit:
                    trimmed_size = -delta_limit - globalDelta / mid
                else:
                    raise Exception('what??')
                if np.sign(trimmed_size) != np.sign(original_size):
                    self.logger.debug(
                        f'{original_size * mid} {symbol} would increase risk over {self.position_manager.limit.delta_limit * 100}% of {self.position_manager.pv} --> skipping (we don t flip orders)')
                    return
                else:
                    self.logger.debug(
                        f'{original_size * mid} {symbol} would increase risk over {self.position_manager.limit.delta_limit * 100}% of {self.position_manager.pv} --> trimming to {trimmed_size * mid}')
            else:
                trimmed_size = original_size

            size = np.clip(trimmed_size, a_min=-params['slice_size'], a_max=params['slice_size'])

            current_basket_price = sum(self.venue_api.mid(_symbol) * self[_symbol]['update_time_delta']
                                       for _symbol in self.keys())
            # mkt order if target reached.
            # TODO: pray for the other coin to hit the same condition...
            if current_basket_price + 2 * abs(params['update_time_delta']) * params['takerVsMakerFee'] * mid < \
                    self[symbol]['rush_in_level']:
                # TODO: could replace current_basket_price by two way sweep after if
                self.venue_api.peg_or_stopout(symbol, size, self.venue_api.orderbook,
                                                      edit_trigger_depth=params['edit_trigger_depth'],
                                                      edit_price_depth='rush_in', stop_depth=None)
                return
            elif current_basket_price - 2 * abs(params['update_time_delta']) * params['takerVsMakerFee'] * mid > \
                    self[symbol]['rush_out_level']:
                # go all in as this decreases margin
                size = - self.position_manager[symbol]['delta'] / mid
                if abs(size) > 0:
                    self.venue_api.peg_or_stopout(symbol, size, self.venue_api.orderbook,
                                                          edit_trigger_depth=params['edit_trigger_depth'],
                                                          edit_price_depth='rush_out', stop_depth=None)
                return
            # limit order if level is acceptable (saves margin compared to faraway order)
            elif current_basket_price < self[symbol]['entry_level']:
                edit_trigger_depth = params['edit_trigger_depth']
                edit_price_depth = params['edit_price_depth']
                stop_depth = None
            # hold off to save margin, if level is bad-ish
            else:
                return
        # if decrease risk, go aggressive without flipping delta
        else:
            size = np.sign(original_size) * min(abs(- globalDelta / mid), abs(original_size))
            edit_trigger_depth = params['aggressive_edit_trigger_depth']
            edit_price_depth = params['aggressive_edit_price_depth']
            stop_depth = params['stop_depth']
        self.venue_api.peg_or_stopout(symbol, size, self.venue_api.orderbook,
                                              edit_trigger_depth=edit_trigger_depth,
                                              edit_price_depth=edit_price_depth, stop_depth=stop_depth)

class SignalEngine():
    '''SignalEngine computes derived data from venue_api and/or externally generated client orders'''
    def __init__(self, strategy, parameters):
        self.parameters = parameters
        self.strategy = strategy

        self.orderbook = {symbol: collections.deque(maxlen=parameters['cache_size'])
                          for symbol in parameters['symbols']}
        self.trades_cache = {symbol: collections.deque(maxlen=parameters['cache_size'])
                             for symbol in parameters['symbols']}

    @staticmethod
    async def build(strategy, parameters):
        if not parameters:
            return None
        elif parameters['signal'] == 'vwap':
            return ExternalSignal(strategy, parameters)
        elif parameters['signal'] == 'spread_distribution':
            return SpreadTradeSignal(strategy, parameters)

    async def to_json(self):
        coin = list(self.orderbook.keys())[0].replace(':USD', '').replace('/USD', '')

        for member_data in self.parameters['record_set']:
            if hasattr(self,member_data):
                if hasattr(self,f'compile_{member_data}'):
                    getattr(self,f'compile_{member_data}')()

                filename = os.path.join(os.sep, configLoader.get_mktdata_folder_for_exchange('ftx_tickdata'),
                                        f'{member_data}_{coin}.json')

                async with aiofiles.open(filename,'w+') as fp:
                    if isinstance(getattr(self, member_data), list):
                        await fp.write(json.dumps(getattr(self, member_data), cls=NpEncoder))
                    elif isinstance(getattr(self, member_data), dict):
                        for data in getattr(self, member_data).values():
                            await fp.write(json.dumps(data, cls=NpEncoder))

    def process_order_book_update(self, message):
        api = self.strategy.market_data_api
        marketId = api.safe_string(message, 'market')
        if marketId in api.markets_by_id:
            symbol = api.market_data_api.markets_by_id[marketId]['symbol']
            item = {'timestamp': api.market_data_api.orderbooks[symbol]['timestamp']}
            if 'mid' in self.parameters['orderbook_granularity']:
                item |= {'mid': 0.5 * (api.market_data_api.orderbooks[symbol]['bids'][0][0] +
                                       api.market_data_api.orderbooks[symbol]['asks'][0][0])}
            if 'full' in self.parameters['orderbook_granularity']:
                item |= {key: api.orderbooks[symbol][key] for key in ['timestamp', 'bids', 'asks']}

            data = next(data for data in self.parameters['orderbook_granularity'] if
                        isinstance(data, dict) and ('depth' in data))
            if data:
                depth = data['depth']
                side_px = sweep_price_atomic(api.orderbooks[symbol], depth)
                opposite_side_px = sweep_price_atomic(api.orderbooks[symbol], -depth)
                item |= {'depth': depth, 'bid_at_depth': min(side_px, opposite_side_px),
                         'ask_at_depth': max(side_px, opposite_side_px)}
            self.orderbook[symbol].append(item)

    def process_trades(self, trades):
        for trade in trades:
            self.trades_cache[trade['symbol']].append(trade)

class ExternalSignal(SignalEngine):
    def __init__(self, strategy, parameters):
        super().__init__(strategy, parameters)

        self.vwap = dict()

    async def update_order(self):
        weights = await async_read_csv(self.parameters['filename'], index_col=0)
        cash_symbol = weights['spot_ticker'].unique()[0]

        spot = weights['spot'].unique()[0]
        targets = (-weights.set_index('new_symbol')['optimalWeight'] / spot).to_dict()
        targets[cash_symbol] = -sum(targets.values())
        return (spot, targets, datetime.utcnow().timestamp() * 1000)

    async def initialize_vwap(self,market_data_api):
        # initialize vwap_history
        nowtime = myUtcNow()
        frequency = timedelta(minutes=1)
        start = nowtime - timedelta(seconds=self.parameters['stdev_window'])
        vwap_history_list = await safe_gather([fetch_trades_history(
            market_data_api.market(symbol)['id'], market_data_api, start, nowtime, frequency=frequency)
            for symbol in self.parameters['symbols']], semaphore=self.strategy.rest_semaphor)
        self.vwap = {symbol: data for symbol, data in
                       zip(self.parameters['symbols'], vwap_history_list)}

    def compile_vwap(self, frequency, purge=True):
        # get times series of target baskets, compute quantile of increments and add to last price
        for symbol in self.trades_cache:
            if len(self.trades_cache[symbol]) == 0: continue
            data = pd.DataFrame(self.trades_cache[symbol])
            data = data[(data['timestamp'] > self.vwap[symbol]['vwap'].index.max().timestamp() * 1000)]
            if data.empty: continue

            # compute vwaps
            data['liquidation'] = data['info'].apply(lambda x: x['liquidation'])
            data.rename(columns={'datetime': 'time', 'amount': 'size'}, inplace=True)
            data['size'] = data.apply(lambda x: x['size'] if x['side'] == 'buy' else -x['size'], axis=1)
            data = data[['size', 'price', 'liquidation', 'time']]
            vwap = vwap_from_list(frequency=frequency, trades=data)
            # append vwaps
            for key in self.vwap[symbol]:
                updated_data = pd.concat([self.vwap[symbol][key],vwap[key]],axis=0)
                self.vwap[symbol][key] = updated_data[~updated_data.index.duplicated()].sort_index().ffill()

            if purge:
                self.trades_cache[symbol].clear()

class SpreadTradeSignal(SignalEngine):

    def __init__(self,strategy,parameters):
        super().__init__(strategy,parameters)

        self.spread_trades = []
        self.spread_vwap = []
        self.spread_distribution = []

    def compile_spread_trades(self, purge=True):
        '''compute maker/taker spread trades and vwap'''
        for symbol in self.trades_cache:
            other_leg = symbol.replace(':USD', '') if ':USD' in symbol else f'{symbol}:USD'
            for trade in self.trades_cache[symbol]:
                if len(self.orderbook[other_leg]) == 0: continue
                current_orderbook = min(self.orderbook[other_leg],
                                       key=lambda orderbook: abs(orderbook['timestamp'] - trade['timestamp']))
                side = (-1 if trade['side'] == 'buy' else 1) # if taker bought spot -> we sold the carry
                if ':USD' in symbol: side = -side # opposite if he bought future
                opposite_side_px = sweep_price_atomic(current_orderbook,
                                                      trade['amount'] * trade['price'] * (1 if trade['side'] == 'buy' else -1)) # if taker bought, we hedge on the ask of the other leg
                price = (opposite_side_px/trade['price']-1)*365.25 # it's actually an annualized rate
                if ':USD' in symbol: price = -price
                self.spread_trades.append({'time': trade['datetime'], 'size': trade['amount'] * side, 'price': price,
                                   'liquidation': trade['info']['liquidation'],'taker_symbol':symbol})
            if purge:
                self.orderbook[symbol].clear()
                self.trades_cache[symbol].clear()

    def compile_spread_vwap(self, frequency, purge=True):
        vwap = vwap_from_list(frequency=frequency, trades=pd.DataFrame(self.spread_trades))
        for key in self.spread_vwap:
            updated_data = pd.concat([self.spread_vwap[key], vwap[key]], axis=0)
            self.spread_vwap[key] = updated_data[~updated_data.index.duplicated()].sort_index().ffill()

        if purge:
            self.spread_trades.clear()

    def compile_spread_distribution(self, purge=True):
        avg_prices= []
        sizes = np.array([trade['size'] for trade in self.spread_trades])
        costs = np.array([trade['size']*trade['price'] for trade in self.spread_trades])
        random_mktshare = np.random.rand(self.parameters['n_paths']*len(sizes),len(sizes))
        avg_prices.append(np.dot(random_mktshare, costs)/np.dot(random_mktshare, sizes))

        if len(avg_prices) == 0: return

        quantiles = np.quantile(avg_prices, q=self.parameters['quantiles'], method='normal_unbiased')
        self.spread_distribution.append({datetime.utcnow().replace(tzinfo=timezone.utc): quantiles})
        if purge:
            self.spread_trades.clear()

        return quantiles

class VenueAPI(ccxtpro.ftx):
    '''VenueAPI implements rest calls and websocket loops to observe raw market data / order events and place orders
    send events for Strategy to action
    send events to SignalEngine for further processing'''
    class Static(dict):
        def __init__(self):
            super().__init__()

    @staticmethod
    async def build(strategy,parameters):
        if parameters['exchange_name'] not in ['ftx']:
            raise Exception(f'exchange not implemented')
        else:
            exchange = ccxtpro.ftx(parameters, config={  ## David personnal
                'enableRateLimit': True,
                'apiKey': 'ZUWyqADqpXYFBjzzCQeUTSsxBZaMHeufPFgWYgQU',
                'secret': api_params['ftx']['key'],
                'newUpdates': True})
            exchange.verbose = False
            if 'subaccount' in parameters: exchange.headers = {'FTX-SUBACCOUNT': parameters['subaccount']}
            exchange.authenticate()
            await exchange.load_markets()

        exchange.strategy = strategy
        trading_fees = await exchange.fetch_trading_fees()
        exchange.static = Static()
        for symbol in parameters['symbols']:
            exchange.static[symbol] = \
                {
                    'priceIncrement': float(exchange.markets[symbol]['info']['priceIncrement']),
                    'sizeIncrement': float(exchange.markets[symbol]['info']['minProvideSize']),
                    'takerVsMakerFee': trading_fees[symbol]['taker'] - trading_fees[symbol]['maker']
                }

        return exchange

    def __init__(self, strategy, static, parameters):
        super().__init__(config=parameters)
        self.parameters = {'timestamp':myUtcNow()}|parameters
        self.strategy = strategy
        self.static = static

        self.message_missed = collections.deque(maxlen=parameters['cache_size'])
        self.options['tradesLimit'] = parameters['cache_size'] # TODO: shoud be in signalengine with a different name. inherited from ccxt....

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
        if not self.strategy.lock['reconciling'].locked():
            self.strategy.process_order_book(symbol, orderbook)

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

    def process_order_book(self, symbol, orderbook):
        with self.strategy.lock[symbol]:
            previous_mid = self.mid(symbol)  # based on ticker, in general
            self.populate_ticker(symbol, orderbook)

            # don't waste time on deep updates or nothing to do
            # size to do:
            mid = self.mid(symbol)
            original_size = self.strategy[symbol]['target'] - self.strategy.position_manager[symbol]['delta'] / mid
            if abs(original_size) < self.strategy.parameters['significance_threshold'] * self.strategy.position_manager.pv / mid \
                    or abs(original_size) < self.static[symbol]['sizeIncrement'] \
                    or self.mid(symbol) == previous_mid:
                return
            else:
                self.strategy.quoter(symbol, orderbook)

    # ---------------------------------- fills

    @loop
    async def monitor_fills(self):
        '''maintains risk_state, event_records, logger.info
            #     await self.reconcile_state() is safer but slower. we have monitor_risk to reconcile'''
        fills = await self.watch_my_trades()
        if not self.strategy.lock['reconciling'].locked():
            for fill in fills:
                self.strategy.process_fill(fill)

    # ---------------------------------- orders

    @loop
    async def monitor_orders(self,symbol):
        '''maintains orders, pending_new, event_records'''
        orders = await self.watch_orders(symbol=symbol)
        if not self.strategy.lock['reconciling'].locked():
            for order in orders:
                self.strategy.process_order(order)

    # ---------------------------------- misc

    @loop
    async def monitor_risk(self):
        '''redundant minutely risk check'''#TODO: would be cool if this could interupt other threads and restart it when margin is ok.
        await asyncio.sleep(self.strategy.position_manager.limit.check_frequency)
        await self.strategy.position_manager.reconcile()
        self.strategy.position_manager.check_limit()

    async def watch_ticker(self, symbol, params={}):
        '''watch_order_book is faster than watch_tickers so we DON'T LISTEN TO TICKERS. Dirty...'''
        raise Exception("watch_order_book is faster than watch_tickers so we DON'T LISTEN TO TICKERS. Dirty...")

    @loop
    async def monitor_trades(self,symbol):
        '''maintains risk_state, event_records, logger.info
            #     await self.reconcile_state() is safer but slower. we have monitor_risk to reconcile'''
        trades = await self.watch_trades(symbol=symbol)
        self.strategy.signal_engine.process_trades(trades)

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
        self.signal_engine.process_order_book_update(message)

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
        event_histories = self.strategy.order_manager.filter_order_histories([symbol], OrderManager.openStates)
        if len(event_histories) > 1:
            first_pending_new = np.argmin(np.array([data[0]['timestamp'] for data in event_histories]))
            for i,event_history in enumerate(self.strategy.order_manager.filter_order_histories([symbol], OrderManager.cancelableStates)):
                if i != first_pending_new:
                    asyncio.create_task(self.cancel_order(event_history[-1]['clientOrderId'],'duplicates'))
                    self.strategy.logger.info('canceled duplicate {} order {}'.format(symbol,event_history[-1]['clientOrderId']))

        # skip if there is inflight on the spread
        # if self.pending_new_histories(coin) != []:#TODO: rather incorporate orders_pending_new in risk, rather than block
        #     if self.pending_new_histories(coin,symbol) != []:
        #         self.logger.info('orders {} should not be in flight'.format([order['clientOrderId'] for order in self.pending_new_histories(coin,symbol)[-1]]))
        #     else:
        #         # this happens mostly between pending_new and create_order on the other leg. not a big deal...
        #         self.logger.info('orders {} still in flight. holding off {}'.format(
        #             [order['clientOrderId'] for order in self.pending_new_histories(coin)[-1]],symbol))
        #     return
        pending_new_histories = self.strategy.order_manager.filter_order_histories(self.strategy.parameters['symbols'],
                                                                          ['pending_new'])
        if pending_new_histories != []:
            self.logger.info('orders {} should not be in flight'.format([order[-1]['clientOrderId'] for order in pending_new_histories]))
            return

        # if no open order, create an order
        order_side = 'buy' if size>0 else 'sell'
        if len(event_histories)==0:
            trimmed_size = self.position_manager.trim_to_margin(mid, size, symbol)
            asyncio.create_task(self.create_order(symbol, 'limit', order_side, abs(trimmed_size), price=edit_price,
                                                  params={'postOnly': not isTaker,
                                                          'ioc': isTaker,
                                                          'comment':edit_price_depth if isTaker else 'new'}))
        # if only one and it's editable, stopout or peg or wait
        elif len(event_histories)==1 \
                and (self.strategy.order_manager.latest_value(event_histories[0][-1]['clientOrderId'], 'remaining') >= sizeIncrement) \
                and event_histories[0][-1]['state'] in OrderManager.acknowledgedStates:
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
        amount = self.round_to_increment(self.static[symbol]['sizeIncrement'], amount)
        if amount == 0:
            return
        # set pending_new -> send rest -> if success, leave pending_new and give id. Pls note it may have been caught by handle_order by then.
        clientOrderId = self.strategy.order_manager.pending_new({'symbol': symbol,
                                                    'type': type,
                                                    'side': side,
                                                    'amount': amount,
                                                    'remaining': amount,
                                                    'price': price,
                                                    'comment': params['comment']})
        try:
            # REST request
            order = await self.create_order(symbol, type, side, amount, price, params | {'clientOrderId':clientOrderId})
        except Exception as e:
            order = {'clientOrderId':clientOrderId,
                     'timestamp':myUtcNow(),
                     'state':'rejected',
                     'comment':'create/'+str(e)}
            self.strategy.order_manager.cancel_or_reject(order)
            if isinstance(e,ccxt.InsufficientFunds):
                self.strategy.logger.info(f'{clientOrderId} too big: {amount*self.mid(symbol)}')
            elif isinstance(e,ccxt.RateLimitExceeded):
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
            status = await self.cancel_order(None,params={'clientOrderId':clientOrderId})
            self.strategy.order_manager.cancel_sent({'clientOrderId':clientOrderId,
                                        'symbol':symbol,
                                        'status':status,
                                        'comment':trigger})
            return True
        except ccxt.CancelPending as e:
            self.strategy.order_manager.cancel_sent({'clientOrderId': clientOrderId,
                                        'symbol': symbol,
                                        'status': str(e),
                                        'comment': trigger})
            return True
        except ccxt.InvalidOrder as e: # could be in flight, or unknown
            self.strategy.order_manager.cancel_or_reject({'clientOrderId':clientOrderId,
                                             'status':str(e),
                                             'state':'canceled',
                                             'comment':trigger})
            return False
        except Exception as e:
            self.strategy.logger.info(f'{clientOrderId} failed to cancel: {str(e)}')
            await asyncio.sleep(.1)
            return await self.strategy.order_manager.cancel_order(clientOrderId, trigger+'+')

    async def close_dust(self):
        data = await asyncio.gather(*[self.fetch_balance(),self.fetch_markets()])
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

        quoteId_list = await asyncio.gather(*coros)
        await asyncio.gather(*[self.privatePostOtcQuotesQuoteIdAccept({'quoteId': int(quoteId['result']['quoteId'])})
        for quoteId in quoteId_list
        if quoteId['success']])

async def get_exec_request(exchange_obj, order_name, **kwargs):
    dirname = os.path.join(os.sep, configLoader.get_config_folder_path(config_name=kwargs['config']), "pfoptimizer")
    exchange_name = exchange_obj.id
    subaccount = exchange_obj.headers['FTX-SUBACCOUNT']

    if order_name == 'spread':
        coin = kwargs['coin']
        cash_name = coin + '/USD'
        future_name = coin + '-PERP'
        cash_price = float(exchange_obj.market(cash_name)['info']['price'])
        future_price = float(exchange_obj.market(future_name)['info']['price'])
        target_portfolio = pd.DataFrame(columns=['coin', 'name', 'optimalCoin', 'currentCoin', 'spot_price'], data=[
            [coin, cash_name, float(kwargs['cash_size']) / cash_price, 0, cash_price],
            [coin, future_name, -float(kwargs['cash_size']) / future_price, 0, future_price]])

    elif order_name == 'flatten':  # only works for basket with 2 symbols
        future_weights = pd.DataFrame(columns=['name', 'optimalWeight'])
        diff = await diff_portoflio(exchange_obj, future_weights)
        smallest_risk = diff.groupby(by='coin')['currentCoin'].agg(
            lambda series: series.apply(abs).min() if series.shape[0] > 1 else 0)
        target_portfolio = diff
        target_portfolio['optimalCoin'] = diff.apply(lambda f: smallest_risk[f['coin']] * np.sign(f['currentCoin']),
                                                     axis=1)

    elif order_name == 'unwind':
        future_weights = pd.DataFrame(columns=['name', 'optimalWeight'])
        target_portfolio = await diff_portoflio(exchange_obj, future_weights)
    else:
        raise Exception("unknown command")

    target_portfolio['exchange'] = exchange_name
    target_portfolio['subaccount'] = subaccount
    target_portfolio.rename(columns={'spot_price':'spot','optimalUSD':'optimalWeight'},inplace=True)
    target_portfolio['spot_ticker'] = target_portfolio['coin'].apply(lambda c: f'{c}/USD')
    target_portfolio['new_symbol'] = target_portfolio['name'].apply(lambda f: exchange_obj.market(f)['symbol'])

    dfs = []
    filenames = []
    for coin, df in target_portfolio.groupby(by='coin'):
        temp_filename = os.path.join(os.sep, dirname, f'{order_name}_{exchange_name}_{subaccount}_{coin}.csv')
        filenames += [temp_filename]
        df.to_csv(temp_filename)
    return filenames

async def risk_reduction_routine(order_name, **kwargs):
    config = configLoader.get_executor_params(order=order_name, dirname=kwargs['config'])
    exchange = await VenueAPI.build(kwargs['exchange'], config, subaccount=kwargs['subaccount'])
    orders = await get_exec_request(exchange_obj=exchange, order_name=order_name, **kwargs)
    exchange.logger.critical(f'generated {orders}, now them all')
    await exchange.close()
    await asyncio.gather(*[single_coin_routine(order, **kwargs) for order in orders])

async def single_coin_routine(order_name, **kwargs):
    try:
        parameters = configLoader.get_executor_params(order=order_name,dirname=kwargs['config'])
        order = os.path.join(os.sep, configLoader.get_config_folder_path(config_name=kwargs['config']), "pfoptimizer", order_name)
        parameters["signal_engine"]['filename'] = order

        strategy = await Strategy.build(parameters)

    except Exception as e:
        logger = logging.getLogger('tradeexecutor')
        logger.critical(str(e), exc_info=True)
        await asyncio.gather(*[strategy.venue_api.cancel_all_orders(symbol) for symbol in strategy.parameters['symbols']])
        logger.warning(f'cancelled orders')
        # await strategy.close_dust()  # Commenting out until bug fixed
        await strategy.venue_api.close()
        if not isinstance(e, Strategy.ReadyToShutdown):
            raise e

async def listen(order_name,**kwargs):

    config = configLoader.get_executor_params(order=order_name, dirname=kwargs['config'])

    exchange = await VenueAPI.build(kwargs['exchange'], config, subaccount='')

    coin = order_name.split('listen_')[1]
    temp_order = pd.DataFrame()
    temp_order['new_symbol'] = [symbol for symbol,data in exchange.markets.items() if data['base'] == coin and data['quote'] == 'USD']
    temp_order['spot'] = 1
    temp_order['spot_ticker'] = f'{coin}/USD'
    temp_order['optimalWeight'] = 0
    temp_order.to_csv(order_name)

    await exchange.build_listener(order_name, config | {'comment': order_name})

    coros = [exchange.broadcast_analytics()] + \
            sum([[exchange.monitor_order_book(symbol),
                  exchange.monitor_trades(symbol)]
                 for symbol in self.strategy.parameters['symbols']], [])
    await asyncio.gather(*coros)

@api
def main(*args,**kwargs):
    '''
        examples:
            tradeexecutor unwind exchange=ftx subaccount=debug config=prod
            tradeexecutor /home/david/config/pfoptimizer/weight_shard_0.csv config=prod
        args:
           order = 'unwind', 'flatten', or filename -> /home/david/config/pfoptimizer/weight_shard_0.csv
        kwargs:
           config =  inserts a subdirectory for config ~/config/config_dir/tradeexecutor_params.json (optional)
           exchange = 'ftx' (mandatory for 'unwind', 'flatten')
           subaccount = 'SysPerp' (mandatory for 'unwind', 'flatten')
           nb_runs = 1
   '''

    order_name = args[0]
    #config_name = kwargs.pop('config') if 'config' in kwargs else None
    logger = kwargs.pop('__logger')
    if 'config' not in kwargs:
        kwargs['config'] = None

    if 'listen' in order_name:
        asyncio.run(listen(order_name, **kwargs))
    elif order_name in ['unwind','flatten']:
        asyncio.run(risk_reduction_routine(order_name, **kwargs))
    else:
        asyncio.run(single_coin_routine(order_name, **kwargs)) # --> I am filled or I timed out and I have flattened position

 