import logging
import os.path
import time as t

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
from utils.io_utils import async_read_csv,async_to_csv,NpEncoder

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
        while len(args)==1 or (args[1] in args[0].inventory_manager):
            try:
                value = await func(*args, **kwargs)
            except ccxt.NetworkError as e:
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
        if args[0].lock['reconciling'].locked():
            self.logger.warning(f'message during reconciliation{args[2]}')
            self.message_missed.append(args[2])
        else:
            return wrapped(*args, **kwargs)
    return _wrapper

class OrderManager(dict):
    allStates = set(['pending_new', 'pending_cancel', 'sent', 'cancel_sent', 'pending_replace', 'acknowledged', 'partially_filled', 'filled', 'canceled', 'rejected'])
    openStates = set(['pending_new', 'sent', 'pending_cancel', 'pending_replace', 'acknowledged', 'partially_filled'])
    acknowledgedStates = set(['acknowledged', 'partially_filled'])
    cancelableStates = set(['sent', 'acknowledged', 'partially_filled'])

    def __init__(self,timestamp,parameters):
        super().__init__()
        self.parameters = parameters
        self.latest_order_reconcile_timestamp = timestamp
        self.latest_fill_reconcile_timestamp = timestamp
        self.logger = logging.getLogger('tradeexecutor')
        self.fill_flag = False

    def to_dict(self):
        return {'parameters':self.parameters,'values':dict(self)}

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
    def pending_new(self, order_event, exchange):
        '''self = {clientId:[{key:data}]}
        order_event:trigger,symbol'''
        #1) resolve clientID
        nowtime = datetime.utcnow().replace(tzinfo=timezone.utc).timestamp() * 1000
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
        risk_data = exchange.position_manager
        current |= {'risk_timestamp':risk_data[symbol]['delta_timestamp'],
                    'delta':risk_data[symbol]['delta'],
                    'netDelta': exchange.position_manager.coin_delta(symbol),
                    'pv(wrong timestamp)':exchange.position_manager.pv,
                    'margin_headroom':exchange.position_manager.margin.actual_IM,
                    'IM_discrepancy': exchange.position_manager.margin.estimate('IM') - exchange.position_manager.margin.actual_IM}

        ## mkt details
        if symbol in exchange.tickers:
            mkt_data = exchange.tickers[symbol]
            timestamp = mkt_data['timestamp']
        else:
            mkt_data = exchange.markets[symbol]['info']|{'bidVolume':0,'askVolume':0}#TODO: should have all risk group
            timestamp = exchange.inventory_manager.timestamp
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
        nowtime = datetime.utcnow().replace(tzinfo=timezone.utc).timestamp() * 1000
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
        nowtime = datetime.utcnow().replace(tzinfo=timezone.utc).timestamp() * 1000
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
        nowtime = datetime.utcnow().replace(tzinfo=timezone.utc).timestamp() * 1000
        eventID = clientOrderId + '_' + str(int(nowtime))
        current = order_event | {'eventID': eventID,
                                 'state': 'cancel_sent',
                                 'remote_timestamp': None,
                                 'timestamp': nowtime}

        # 4) mine
        self[clientOrderId] += [current]

    # reponse de websocket qui dit que c'est acknowledged
    @symbol_locked
    def acknowledgment(self, order_event, exchange):
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
            exchange.position_manager.margin.add_open_order(order_event)

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
        nowtime = datetime.utcnow().replace(tzinfo=timezone.utc).timestamp() * 1000
        eventID = clientOrderId + '_' + str(int(nowtime))
        current = order_event | {'eventID':eventID,
                                 'state': order_event['state'],
                                 'remote_timestamp': order_event['timestamp'] if 'timestamp' in order_event else None}
        current['timestamp'] = nowtime

        # 4) mine
        self[clientOrderId] += [current]

    '''reconciliations to an exchange'''

    async def reconcile_fills(self,exchange):
        '''fetch fills, to recover missed messages'''
        sincetime = max(exchange.inventory_manager.timestamp, self.latest_fill_reconcile_timestamp - 1000)
        fetched_fills = sum(await asyncio.gather(*[exchange.fetch_my_trades(since=sincetime,symbol=symbol) for symbol in exchange.inventory_manager]), [])
        for fill in fetched_fills:
            fill['comment'] = 'reconciled'
            fill['clientOrderId'] = self.find_clientID_from_fill(fill)
            if fill['id'] not in [block['fillId']
                                     for block in self[fill['clientOrderId']]
                                     if 'fillId' in block]:
                self.fill(fill)
        self.latest_fill_reconcile_timestamp = datetime.now(timezone.utc).timestamp()*1000

    async def reconcile_orders(self,exchange):
        self.latest_order_reconcile_timestamp = datetime.now(timezone.utc).timestamp() * 1000

        # list internal orders
        internal_order_internal_status = {clientOrderId:data[-1] for clientOrderId,data in self.items()
                                          if data[-1]['state'] in OrderManager.openStates}

        # add missing orders (we missed orders from the exchange)
        external_orders = sum(await asyncio.gather(*[exchange.fetch_open_orders(symbol=symbol) for symbol in exchange.inventory_manager]), [])
        for order in external_orders:
            if order['clientOrderId'] not in internal_order_internal_status.keys():
                self.acknowledgment(order | {'comment':'reconciled_missing'},exchange)
                self.logger.warning('{} was missing'.format(order['clientOrderId']))
                found = order['clientOrderId']
                assert (found in self), f'{found} unknown'

        # remove zombie orders (we beleive they are live but they are not open)
        # should not happen so we put it in the logs
        internal_order_external_status = await safe_gather([exchange.fetch_order(id=None, params={'clientOrderId':clientOrderId})
                                                   for clientOrderId in internal_order_internal_status.keys()],
                                                           semaphore=exchange.rest_semaphor,
                                                           return_exceptions=True)
        for clientOrderId,external_status in zip(internal_order_internal_status.keys(),internal_order_external_status):
            if isinstance(external_status, Exception):
                self.logger.warning(f'reconcile_orders {clientOrderId} : {external_status}')
                continue
            if external_status['status'] != 'open':
                self.cancel_or_reject(external_status | {'state': 'canceled', 'comment':'reconciled_zombie'})
                self.logger.warning('{} was a {} zombie'.format(clientOrderId,internal_order_internal_status[clientOrderId]['state']))

class PositionManager(dict):
    class LimitBreached(Exception):
        def __init__(self,check_frequency,limit):
            super().__init__()
            self.delta_limit = limit
            self.check_frequency = check_frequency

    def __init__(self,timestamp,parameters):
        super().__init__()
        self.pv = None
        self.margin = None
        self.limit = None
        self.timestamp = timestamp
        self.logger = logging.getLogger('tradeexecutor')
        self.risk_reconciliations = []
        self.parameters = parameters
        self.markets = None

    def to_dict(self):
        return {'parameters':self.parameters,'values':self.risk_reconciliations}

    @staticmethod
    async def build(exchange,nowtime,parameters):
        result = PositionManager(nowtime, parameters)
        result.limit = PositionManager.LimitBreached(parameters['check_frequency'], parameters['delta_limit'])

        for symbol, data in exchange.inventory_manager.items():
            result[symbol] = {'delta': 0,
                              'delta_timestamp':nowtime.timestamp()*1000,
                              'delta_id': 0}

        # Initializes a margin calculator that can understand the margin of an order
        # reconcile the margin of an exchange with the margin we calculate
        # pls note it needs the exchange pv, which is known after reconcile
        result.margin = await MarginCalculator.margin_calculator_factory(exchange)
        result.markets = exchange.markets
        
        return result

    async def reconcile(self,exchange):
        # recompute risks, margins
        previous_delta = {symbol:{'delta':data['delta']} for symbol,data in self.items()}
        previous_pv = self.pv

        state = await syncronized_state(exchange)
        risk_timestamp = datetime.utcnow().replace(tzinfo=timezone.utc).timestamp() * 1000

        # delta is noisy for perps, so override to delta 1.
        self.pv = 0
        for coin, balance in state['balances']['total'].items():
            if coin != 'USD':
                symbol = coin + '/USD'
                mid = state['markets']['price'][exchange.market_id(symbol)]
                delta = balance * mid
                if symbol not in self:
                    self[symbol] = {'delta_id': 0}
                self[symbol]['delta'] = delta
                self[symbol]['delta_timestamp'] = risk_timestamp
                self.pv += delta

        self.pv += state['balances']['total']['USD']  # doesn't contribute to delta, only pv !

        for name, position in state['positions']['netSize'].items():
            symbol = exchange.market(name)['symbol']
            delta = position * exchange.mid(symbol)

            if symbol not in self:
                self[symbol] = {'delta_id': 0}
            self[symbol]['delta'] = delta
            self[symbol]['delta_timestamp'] = risk_timestamp

        # update IM
        await self.margin.refresh(exchange, balances=state['balances']['total'], positions=state['positions']['netSize'],
                                  im_buffer=0.01 * self.pv)

        delta_error = {symbol: self[symbol]['delta'] - (previous_delta[symbol]['delta'] if symbol in previous_delta else 0)
                       for symbol in self}
        self.risk_reconciliations += [{'state': 'remote_risk',
                                       'symbol': symbol_,
                                       'delta_timestamp': self[symbol_]['delta_timestamp'],
                                       'delta': self[symbol_]['delta'],
                                       'netDelta': self.coin_delta(symbol_),
                                       'pv': self.pv,
                                       'estimated_IM': self.margin.estimate('IM'),
                                       'actual_IM': self.margin.actual_IM,
                                       'pv_error': self.pv - (previous_pv or 0),
                                       'total_delta_error': sum(delta_error.values())}
                                      for symbol_ in self]

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

class InventoryManager(dict):
    class ReadyToShutdown(Exception):
        def __init__(self, text):
            super().__init__(text)
    def __init__(self,timestamp,filename):
        super().__init__()
        self.timestamp = timestamp
        self.filename = filename
        self.logger = logging.getLogger('tradeexecutor')

    def to_dict(self):
        return {'filename':self.filename,'order_received_timestamp':self.timestamp,'values':dict(self)}

    @staticmethod
    async def build(exchange,nowtime,order):
        result = InventoryManager(nowtime.timestamp() * 1000, order)
        await result.set_from_source()

        trading_fees = await exchange.fetch_trading_fees()
        for symbol in result:
            result[symbol] |= {'priceIncrement': float(exchange.markets[symbol]['info']['priceIncrement']),
                               'sizeIncrement': float(
                                   exchange.markets[symbol]['info']['minProvideSize']),
                               'takerVsMakerFee': trading_fees[symbol]['taker'] - trading_fees[symbol]['maker']
                               }

        return result

    async def set_from_source(self):
        weights = await async_read_csv(self.filename)
        cash_symbol = weights['spot_ticker'].unique()[0]

        spot = weights['spot'].unique()[0]
        targets = (-weights.set_index('new_symbol')['optimalWeight'] / spot).to_dict()
        targets[cash_symbol] = -sum(targets.values())
        #assert targets.keys() == self.keys(), f'{targets.keys()}!={self.keys()}'

        self.set_target(spot,targets)

    def set_target(self, spot, targets):
        for symbol, target in targets.items():
            if symbol not in self:
                self[symbol] = {'target': target, 'spot_price': spot}
        for symbol, target in targets.items():
            if self[symbol]['target'] != target:
                self.logger.info('{} weight changed from {} to {}'.format(
                    symbol, self[symbol]['target'] * spot, target * spot))
                self[symbol]['target'] = target
                self[symbol]['spot_price'] = spot
                self.timestamp = datetime.now().timestamp() * 1000

    async def update_quoter_params(self, exchange):
        '''updates quoter inputs
        reads file if present. If not: if no risk then shutdown bot, else unwind.
        '''
        if os.path.isfile(self.filename):
            await self.set_from_source()
        elif all(abs(exchange.position_manager[symbol]['delta'])<exchange.parameters['significance_threshold'] * exchange.position_manager.pv for symbol in self):
            raise InventoryManager.ReadyToShutdown(f'no {self.keys()} delta and no {self.filename} order --> shutting down bot')
        else:
            targets = {key:0 for key in self}
            spot = exchange.mid(next(key for key in self if exchange.market(key)['type']=='spot'))
            self.set_target(spot,targets)

        nowtime = datetime.utcnow().replace(tzinfo=timezone.utc).timestamp() * 1000
        exchange.analytics_manager.compile_vwap_history(frequency=timedelta(minutes=1))

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
                [exchange.analytics_manager.vwap_history[symbol]['vwap'] for symbol in self],
                [data['update_time_delta'] for symbol,data in self.items()],
                [exchange.parameters['entry_tolerance'],exchange.parameters['rush_in_tolerance'],exchange.parameters['rush_out_tolerance']])

            data['entry_level'] = quantiles[0]
            data['rush_in_level'] = quantiles[1]
            data['rush_out_level'] = quantiles[2]

            stdev = exchange.analytics_manager.vwap_history[symbol]['vwap'].std().squeeze()
            if not (stdev>0): stdev = 1e-16

            # edit_price_depth = how far to put limit on risk increasing orders
            data['edit_price_depth'] = stdev * np.sqrt(exchange.parameters['edit_price_tolerance']) * scaler
            # edit_trigger_depth = how far to let the mkt go before re-pegging limit orders
            data['edit_trigger_depth'] = stdev * np.sqrt(exchange.parameters['edit_trigger_tolerance']) * scaler

            # aggressive version understand tolerance as price increment
            if isinstance(exchange.parameters['aggressive_edit_price_tolerance'],float):
                data['aggressive_edit_price_depth'] = max(1,exchange.parameters['aggressive_edit_price_tolerance']) * data['priceIncrement']
                data['aggressive_edit_trigger_depth'] = max(1,exchange.parameters['aggressive_edit_price_tolerance']) * data['priceIncrement']
            else:
                data['aggressive_edit_price_depth'] = exchange.parameters['aggressive_edit_price_tolerance']
                data['aggressive_edit_trigger_depth'] = exchange.parameters['aggressive_edit_price_tolerance']

                # stop_depth = how far to set the stop on risk reducing orders
            data['stop_depth'] = stdev * np.sqrt(exchange.parameters['stop_tolerance']) * scaler

            # slice_size: cap to expected time to trade consistent with edit_price_tolerance
            volume_share = exchange.parameters['volume_share'] * exchange.parameters['edit_price_tolerance'] * \
                           exchange.analytics_manager.vwap_history[symbol]['volume'].mean()
            data['slice_size'] = volume_share

        self.timestamp = nowtime
        self.logger.info(f'scaled params by {scaler} at {nowtime}')

class AnalyticsManager():
    def __init__(self,parameters):
        self.parameters = parameters
        self.trades_cache = dict()
        self.vwap_history = dict()
        self.orderbook_history = dict()
        self.spread_vwap_history = dict()

    @staticmethod
    async def build(exchange,parameters):
        # get times series of target baskets, compute quantile of increments and add to last price
        initialized = AnalyticsManager(parameters)
        initialized.orderbook_history = {symbol: collections.deque(maxlen=initialized.parameters['cache_size'])
                                         for symbol in exchange.inventory_manager}
        initialized.trades_cache = {symbol: collections.deque(maxlen=initialized.parameters['cache_size'])
                                         for symbol in exchange.inventory_manager}

        # initialize vwap_history
        nowtime = datetime.utcnow().replace(tzinfo=timezone.utc)
        frequency = timedelta(minutes=1)
        start = nowtime - timedelta(seconds=initialized.parameters['stdev_window'])
        vwap_history_list = await safe_gather([fetch_trades_history(
            exchange.market(symbol)['id'], exchange, start, nowtime, frequency=frequency)
            for symbol in exchange.inventory_manager], semaphore=exchange.rest_semaphor)
        initialized.vwap_history = {exchange.market(symbol)['symbol']: pd.DataFrame(data=data) for symbol, data in
                                    zip(exchange.inventory_manager.keys(), vwap_history_list)}

        return initialized

    def compile_vwap_history(self, frequency):
        # get times series of target baskets, compute quantile of increments and add to last price
        for symbol in self.trades_cache:
            if len(self.trades_cache[symbol]) == 0: continue
            data = pd.DataFrame(self.trades_cache[symbol])
            data = data[(data['timestamp'] > self.vwap_history[symbol]['vwap'].index.max().timestamp() * 1000)]
            if data.empty: continue

            # compute vwaps
            data['liquidation'] = data['info'].apply(lambda x: x['liquidation'])
            data.rename(columns={'datetime': 'time', 'amount': 'size'}, inplace=True)
            data['size'] = data.apply(lambda x: x['size'] if x['side'] == 'buy' else -x['size'], axis=1)
            data = data[['size', 'price', 'liquidation', 'time']]
            vwap = vwap_from_list(frequency=frequency, trades=data)
            # append vwaps
            extended = pd.concat([self.vwap_history[symbol], vwap])
            self.vwap_history[symbol] = extended[~extended.index.duplicated()].sort_index().ffill()

            if 'spread_vwap' in self.parameters['options']:
                self.compile_spread_vwap(frequency)

            self.trades_cache[symbol].clear()

    def compile_spread_vwap(self, frequency):
        # compute maker/taker spread trades and vwap
        for symbol in self.trades_cache:
            spread_trades = []
            other_leg = symbol.replace(':USD', '') if ':USD' in symbol else f'{symbol}:USD'
            for trade in self.trades_cache[symbol]:
                if len(self.orderbook_history[other_leg]) == 0: continue
                curent_orderbook = min(self.orderbook_history[other_leg],
                                       key=lambda orderbook: abs(orderbook['timestamp'] - trade['timestamp']))
                side = (1 if trade['side'] == 'buy' else -1)
                if ':USD' in symbol: side = -side
                opposite_side_px = sweep_price_atomic(curent_orderbook,
                                                      trade['amount'] * (-1 if trade['side'] == 'buy' else 1))
                price = opposite_side_px - trade['price']
                if ':USD' in symbol: price = -price
                spread_trades += [{'time': trade['datetime'], 'size': trade['amount'] * side, 'price': price,
                                   'liquidation': trade['info']['liquidation']}]
            if len(spread_trades) == 0: continue
            vwap = vwap_from_list(frequency=frequency, trades=pd.DataFrame(spread_trades))
            # append vwaps
            if symbol not in self.spread_vwap_history:
                self.spread_vwap_history[symbol] = pd.DataFrame()
            extended = pd.concat([self.spread_vwap_history[symbol], vwap])
            self.spread_vwap_history[symbol] = extended[~extended.index.duplicated()].sort_index().ffill()

    async def broadcast_analytics(self):
        self.compile_spread_vwap(timedelta(minutes=1))
        coin = list(self.spread_vwap_history.keys())[0].replace(':USD','').replace('/USD','')
        filename = os.path.join(os.sep, configLoader.get_mktdata_folder_for_exchange('ftx_tickdata'),f'spread_vwap_{coin}.csv')
        prev_df = pd.read_csv(filename,index_col=0) if os.path.isfile(filename) else pd.DataFrame()
        pd.concat([prev_df] + [data for data in self.spread_vwap_history.values()]).to_csv(filename)

class myFtx(ccxtpro.ftx):
    def __init__(self, parameters, config = dict()):
        super().__init__(config=config)
        self.parameters = parameters

        self.lock = {'reconciling':threading.Lock()}
        self.rest_semaphor = asyncio.Semaphore(safe_gather_limit)

        self.message_missed = collections.deque(maxlen=parameters['cache_size'])
        self.options['tradesLimit'] = parameters['analytics_manager']['cache_size']

        self.inventory_manager = None
        self.order_manager = None
        self.position_manager = None
        self.analytics_manager = None

        self.logger = logging.getLogger('tradeexecutor')
        self.data_logger = ExecutionLogger(exchange_name=self.id)

    def to_dict(self):
        return {'parameters':self.parameters} \
               |{key:getattr(self,key).to_dict()
                 for key in ['inventory_manager','order_manager','position_manager']}

    @staticmethod
    async def open_exec_exchange(exchange_name, config, subaccount=None):
        '''TODO unused for now'''
        if exchange_name not in ['ftx']:
            raise Exception(f'exchange {exchange_name} not implemented')
        else:
            exchange = myFtx(config, config={  ## David personnal
                'enableRateLimit': True,
                'apiKey': 'ZUWyqADqpXYFBjzzCQeUTSsxBZaMHeufPFgWYgQU',
                'secret': api_params['ftx']['key'],
                'newUpdates': True})
            exchange.verbose = False
            if subaccount: exchange.headers = {'FTX-SUBACCOUNT': subaccount}
            exchange.authenticate()
            await exchange.load_markets()

        return exchange

    # --------------------------------------------------------------------------------------------
    # ---------------------------------- various helpers -----------------------------------------
    # --------------------------------------------------------------------------------------------

    def mid(self,symbol):
        if symbol == 'USD/USD': return 1.0
        data = self.tickers[symbol] if symbol in self.tickers else self.markets[symbol]['info']
        return 0.5*(float(data['bid'])+float(data['ask']))

    def round_to_increment(self, sizeIncrement, amount):
        if amount >= 0:
            return np.floor(amount/sizeIncrement) * sizeIncrement
        else:
            return -np.floor(-amount / sizeIncrement) * sizeIncrement
                    
    # ---------------------------------------------------------------------------------------------
    # ---------------------------------- PMS -----------------------------------------
    # ---------------------------------------------------------------------------------------------

    async def build(self, order, parameters):
        '''initialize all state and does some filtering (weeds out slow underlyings; should be in strategy)
            target_sub_portfolios = {coin:{rush_level_increment,
            symbol1:{'spot_price','diff','target'}]}]'''
        nowtime = datetime.utcnow().replace(tzinfo=timezone.utc)

        self.inventory_manager = await InventoryManager.build(self, nowtime, order)
        self.position_manager = await PositionManager.build(self, nowtime, parameters['position_manager'])
        self.order_manager = OrderManager(self.inventory_manager.timestamp, parameters['order_manager'])
        self.analytics_manager = await AnalyticsManager.build(self, parameters['analytics_manager'])

        self.lock |= {symbol: CustomRLock() for symbol in self.inventory_manager}
        await self.reconcile()

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
            # fills, orders, refresh exec parameters
            await self.order_manager.reconcile_fills(self)   # goes on exchange, check the fills (fetch my trades, REST request at the exchange), puts them in the OMS
            # there is a state in the OMS that knows that it is coming from reconcile
            await self.order_manager.reconcile_orders(self)   # ask all orders vs your own open order state.
            # await self.update_inventory_target()   # refresh the parameters of the exec that are quantitative aggressiveness, where I put my limit vs top of book,
            # when do I think levels are good I shoot market on both legs

            await self.position_manager.reconcile(self)

            # update_inventory_target
            await self.inventory_manager.update_quoter_params(self)

            # replay missed _messages.
            self.logger.warning(f'replaying {len(self.message_missed)} messages after recon')
            while self.message_missed:
                message = self.message_missed.popleft()
                data = self.safe_value(message, 'data')
                channel = message['channel']
                if channel == 'fills':
                    fill = self.parse_trade(data)
                    self.process_fill(fill | {'orderTrigger':'replayed'})
                elif channel == 'orders':
                    order = self.parse_order(data)
                    if order['symbol'] in self.inventory_manager:
                        self.process_order(order | {'orderTrigger':'replayed'})
                # we don't interecpt the mktdata anyway...
                elif channel == 'orderbook':
                    pass
                    self.populate_ticker(message['symbol'], message)
                elif channel == 'trades':
                    pass
                    self.analytics_manager.trades_cache[message['symbol']].append(message)

        # critical job is done, release lock and print data
        if self.order_manager.fill_flag:
            async with aiofiles.open(f'{self.data_logger.json_filename}.json', mode='a') as file:
                await file.write(json.dumps(self.to_dict(), cls=NpEncoder))
            self.order_manager.fill_flag = False


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
        if not self.lock['reconciling'].locked():
            self.process_order_book(symbol, orderbook)

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
        with self.lock[symbol]:
            previous_mid = self.mid(symbol)  # based on ticker, in general
            self.populate_ticker(symbol, orderbook)

            # don't waste time on deep updates
            if self.mid(symbol) != previous_mid:
                self.quoter(symbol, orderbook)

    # ---------------------------------- fills

    @loop
    async def monitor_fills(self):
        '''maintains risk_state, event_records, logger.info
            #     await self.reconcile_state() is safer but slower. we have monitor_risk to reconcile'''
        fills = await self.watch_my_trades()
        if not self.lock['reconciling'].locked():
            for fill in fills:
                self.process_fill(fill)

    # Qd tu as un fill tu update ton risk interne + ton OMS
    def process_fill(self, fill):
        symbol = fill['symbol']

        # update risk_state
        if symbol not in self.position_manager:
            self.position_manager[symbol] = {'delta':0, 'delta_id':0}
        data = self.position_manager[symbol]
        fill_size = fill['amount'] * (1 if fill['side'] == 'buy' else -1) * fill['price']
        data['delta'] += fill_size
        data['delta_timestamp'] = fill['timestamp']
        latest_delta = data['delta_id']
        data['delta_id'] = max(latest_delta, int(fill['order']))

        #update margin
        self.position_manager.margin.add_instrument(symbol, fill_size)

        # only log trades being run by this process
        fill['clientOrderId'] = fill['info']['clientOrderId']
        fill['comment'] = 'websocket_fill'
        if symbol in self.inventory_manager:
            self.order_manager.fill(fill)

            # logger.info
            self.logger.warning('{} filled after {}: {} {} at {}'.format(fill['clientOrderId'],
                                                                      fill['timestamp'] - int(fill['clientOrderId'].split('_')[-1]),
                                                                      fill['side'], fill['amount'],
                                                                      fill['price']))

            current = self.position_manager[symbol]['delta']
            target = self.inventory_manager[symbol]['target'] * fill['price']
            diff = (self.inventory_manager[symbol]['target'] - self.position_manager[symbol]['delta']) * fill['price']
            initial = self.inventory_manager[symbol]['target'] * fill['price'] - diff
            self.logger.warning('{} risk at {} ms: {}% done [current {}, initial {}, target {}]'.format(
                symbol,
                self.position_manager[symbol]['delta_timestamp'],
                (current - initial) / diff * 100,
                current,
                initial,
                target))

    # ---------------------------------- orders

    @loop
    async def monitor_orders(self,symbol):
        '''maintains orders, pending_new, event_records'''
        orders = await self.watch_orders(symbol=symbol)
        if not self.lock['reconciling'].locked():
            for order in orders:
                self.process_order(order)

    def process_order(self,order):
        assert order['clientOrderId'],"assert order['clientOrderId']"
        self.order_manager.acknowledgment(order | {'comment': 'websocket_acknowledgment'}, self) # status new, triggered, open or canceled

    # ---------------------------------- misc

    @loop
    async def monitor_risk(self):
        '''redundant minutely risk check'''#TODO: would be cool if this could interupt other threads and restart it when margin is ok.
        await asyncio.sleep(self.position_manager.limit.check_frequency)
        await self.reconcile()
        await self.check_limit()

    async def check_limit(self):
        absolute_risk = dict()
        for symbol in self.position_manager:
            coin = self.markets[symbol]['base']
            if coin not in absolute_risk:
                absolute_risk[coin] = abs(self.position_manager.coin_delta(symbol))
        if sum(absolute_risk.values()) > self.position_manager.pv * self.position_manager.limit.delta_limit:
            self.logger.info(
                f'absolute_risk {absolute_risk} > {self.position_manager.pv * self.position_manager.limit.delta_limit}')
        if self.position_manager.margin.actual_IM < self.position_manager.pv / 100:
            self.logger.info(f'IM {self.position_manager.margin.actual_IM}  < 1%')

    async def watch_ticker(self, symbol, params={}):
        '''watch_order_book is faster than watch_tickers so we DON'T LISTEN TO TICKERS. Dirty...'''
        raise Exception("watch_order_book is faster than watch_tickers so we DON'T LISTEN TO TICKERS. Dirty...")

    @loop
    async def monitor_trades(self,symbol):
        '''maintains risk_state, event_records, logger.info
            #     await self.reconcile_state() is safer but slower. we have monitor_risk to reconcile'''
        trades = await self.watch_trades(symbol=symbol)
        for trade in trades:
            self.analytics_manager.trades_cache[symbol].append(trade)

    @loop
    async def broadcast_analytics(self):
        '''redundant minutely risk check'''#TODO: would be cool if this could interupt other threads and restart it when margin is ok.
        await asyncio.sleep(self.position_manager.limit.check_frequency)
        await self.analytics_manager.broadcast_analytics()
        
    # ---------------------------------- just to record messages while reconciling
    @intercept_message_during_reconciliation
    def handle_my_trade(self, client, message):
        super().handle_my_trade(client, message)

    @intercept_message_during_reconciliation
    def handle_order(self, client, message):
        super().handle_order(client, message)

    #@intercept_message_during_reconciliation
    def handle_trade(self, client, message):
        super().handle_trade(client, message)

    #@intercept_message_during_reconciliation
    def handle_order_book_update(self, client, message):
        '''self.orderbook_history[symbol] = dict {'timestamp','bids','asks','mid_at_depth'}'''
        super().handle_order_book_update(client, message)

        marketId = self.safe_string(message, 'market')
        if marketId in self.markets_by_id:
            symbol = self.markets_by_id[marketId]['symbol']
            item = {'timestamp':self.orderbooks[symbol]['timestamp']}
            if 'mid' in self.analytics_manager.parameters['options']:
                item |= {'mid':0.5*(self.orderbooks[symbol]['bids'][0][0]+self.orderbooks[symbol]['asks'][0][0])}
            if 'full' in self.analytics_manager.parameters['options']:
                item |= {key:self.orderbooks[symbol][key] for key in ['timestamp','bids','asks']}
            if 'depth' in self.analytics_manager.parameters['options']:
                depth = (self.inventory_manager[symbol]['target'] - self.position_manager[symbol]['delta'] / self.mid(
                    symbol)) * self.mid(symbol)
                side_px = sweep_price_atomic(self.orderbooks[symbol], depth)
                opposite_side_px = sweep_price_atomic(self.orderbooks[symbol], -depth)
                item |= {'depth':depth,'bid_at_depth':min(side_px,opposite_side_px),'ask_at_depth':max(side_px,opposite_side_px)}
            self.analytics_manager.orderbook_history[symbol].append(item)

    # --------------------------------------------------------------------------------------------
    # ---------------------------------- order placement -----------------------------------------
    # --------------------------------------------------------------------------------------------

    # ---------------------------------- high level

    def quoter(self, symbol, orderbook):
        '''
            leverages orderbook and risk to issue an order
            Critical loop, needs to go quick
            all executes in one go, no async
        '''
        coin = self.markets[symbol]['base']
        mid = self.tickers[symbol]['mid']
        params = self.inventory_manager[symbol]

        # size to do:
        original_size = params['target'] - self.position_manager[symbol]['delta'] / mid
        if np.abs(original_size * mid) < self.parameters['significance_threshold'] * self.position_manager.pv: #self.inventory_target[symbol]['sizeIncrement']:
            return

        #risk
        globalDelta = self.position_manager.delta_bounds(symbol)['global_delta']
        marginal_risk = np.abs(globalDelta/mid + original_size)-np.abs(globalDelta/mid)
        delta_limit = self.position_manager.limit.delta_limit * self.position_manager.pv

        # if increases risk but not out of limit, trim and go passive.
        if marginal_risk>0:
            # if (global_delta_plus / mid + original_size) > delta_limit:
            #     trimmed_size = delta_limit - global_delta_plus / mid
            #     self.logger.debug(
            #         f'{original_size * mid} {symbol} would increase risk over {self.limit.delta_limit * 100}% of {self.risk_state.pv} --> trimming to {trimmed_size * mid}')
            # elif (global_delta_minus / mid + original_size) < -delta_limit:
            #     trimmed_size = -delta_limit - global_delta_minus / mid
            #     self.logger.debug(
            #         f'{original_size * mid} {symbol} would increase risk over {self.limit.delta_limit * 100}% of {self.risk_state.pv} --> trimming to {trimmed_size * mid}')
            # else:
            #     trimmed_size = original_size
            # if np.sign(trimmed_size) != np.sign(original_size):
            #     self.logger.debug(f'skipping (we don t flip orders)')
            #     return
            if np.abs(globalDelta / mid + original_size) > delta_limit:
                if (globalDelta / mid + original_size) > delta_limit:
                    trimmed_size = delta_limit - globalDelta / mid
                elif (globalDelta / mid + original_size) < -delta_limit:
                    trimmed_size = -delta_limit - globalDelta / mid
                else:
                    raise Exception('what??')
                if np.sign(trimmed_size) != np.sign(original_size):
                    self.logger.debug(f'{original_size*mid} {symbol} would increase risk over {self.position_manager.limit.delta_limit * 100}% of {self.position_manager.pv} --> skipping (we don t flip orders)')
                    return
                else:
                    self.logger.debug(f'{original_size*mid} {symbol} would increase risk over {self.position_manager.limit.delta_limit * 100}% of {self.position_manager.pv} --> trimming to {trimmed_size * mid}')
            else:
                trimmed_size = original_size

            size = np.clip(trimmed_size, a_min=-params['slice_size'],a_max=params['slice_size'])

            current_basket_price = sum(self.mid(_symbol) * self.inventory_manager[_symbol]['update_time_delta']
                                       for _symbol in self.inventory_manager.keys() if _symbol in self.markets)
            # mkt order if target reached.
            # TODO: pray for the other coin to hit the same condition...
            if current_basket_price + 2 * np.abs(params['update_time_delta']) * params['takerVsMakerFee'] * mid < self.inventory_manager[symbol]['rush_in_level']:
                # TODO: could replace current_basket_price by two way sweep after if
                self.peg_or_stopout(symbol, size, orderbook,
                                    edit_trigger_depth=params['edit_trigger_depth'],
                                    edit_price_depth='rush_in', stop_depth=None)
                self.logger.warning(f'rushing into {coin}')
                return
            elif current_basket_price - 2 * np.abs(params['update_time_delta']) * params['takerVsMakerFee'] * mid > self.inventory_manager[symbol]['rush_out_level']:
                # go all in as this decreases margin
                size = - self.position_manager[symbol]['delta'] / mid
                if abs(size) > 0:
                    self.logger.warning(f'rushing out of {size} {coin}')
                    self.peg_or_stopout(symbol, size, orderbook,
                                        edit_trigger_depth=params['edit_trigger_depth'],
                                        edit_price_depth='rush_out', stop_depth=None)
                return
            # limit order if level is acceptable (saves margin compared to faraway order)
            elif current_basket_price < self.inventory_manager[symbol]['entry_level']:
                edit_trigger_depth=params['edit_trigger_depth']
                edit_price_depth = params['edit_price_depth']
                stop_depth = None
            # hold off to save margin, if level is bad-ish
            else:
                return
        # if decrease risk, go aggressive
        else:
            size = original_size
            edit_trigger_depth = params['aggressive_edit_trigger_depth']
            edit_price_depth = params['aggressive_edit_price_depth']
            stop_depth = params['stop_depth']
        self.peg_or_stopout(symbol,size,orderbook,edit_trigger_depth=edit_trigger_depth,edit_price_depth=edit_price_depth,stop_depth=stop_depth)

    def peg_or_stopout(self,symbol,size,orderbook,edit_trigger_depth,edit_price_depth,stop_depth=None):
        '''places an order after checking OMS + margin
        creates or edits orders, pegging to orderbook
        goes taker when edit_price_depth is str
        size in coin, already filtered
        skips if any pending_new, cancels duplicates
        '''
        if abs(size) == 0:
            return

        #TODO: https://help.ftx.com/hc/en-us/articles/360052595091-Ratelimits-on-FTX
        opposite_side = self.tickers[symbol]['ask' if size>0 else 'bid']
        mid = self.tickers[symbol]['mid']

        priceIncrement = self.inventory_manager[symbol]['priceIncrement']
        sizeIncrement = self.inventory_manager[symbol]['sizeIncrement']

        if stop_depth is not None:
            stop_trigger = float(self.price_to_precision(symbol,stop_depth))
        edit_trigger = float(self.price_to_precision(symbol,edit_trigger_depth))
        #TODO: use orderbook to place before cliff; volume matters too.
        if edit_price_depth not in ['rush_in', 'rush_out']:
            edit_price = float(self.price_to_precision(symbol, opposite_side - (1 if size > 0 else -1) * edit_price_depth))
        else:
            edit_price = sweep_price_atomic(orderbook, size * mid)

        # remove open order dupes is any (shouldn't happen)
        event_histories = self.order_manager.filter_order_histories([symbol], OrderManager.openStates)
        if len(event_histories) > 1:
            first_pending_new = np.argmin(np.array([data[0]['timestamp'] for data in event_histories]))
            for i,event_history in enumerate(self.order_manager.filter_order_histories([symbol], OrderManager.cancelableStates)):
                if i != first_pending_new:
                    asyncio.create_task(self.cancel_order(event_history[-1]['clientOrderId'],'duplicates'))
                    self.logger.info('canceled duplicate {} order {}'.format(symbol,event_history[-1]['clientOrderId']))

        # skip if there is inflight on the spread
        # if self.pending_new_histories(coin) != []:#TODO: rather incorporate orders_pending_new in risk, rather than block
        #     if self.pending_new_histories(coin,symbol) != []:
        #         self.logger.info('orders {} should not be in flight'.format([order['clientOrderId'] for order in self.pending_new_histories(coin,symbol)[-1]]))
        #     else:
        #         # this happens mostly between pending_new and create_order on the other leg. not a big deal...
        #         self.logger.info('orders {} still in flight. holding off {}'.format(
        #             [order['clientOrderId'] for order in self.pending_new_histories(coin)[-1]],symbol))
        #     return
        pending_new_histories = self.order_manager.filter_order_histories([_symbol
                                                                           for _symbol in self.inventory_manager
                                                                           if _symbol in self.markets],
                                                                          ['pending_new'])
        if pending_new_histories != []:
            self.logger.info('orders {} should not be in flight'.format([order[-1]['clientOrderId'] for order in pending_new_histories]))
            return

        # if no open order, create an order
        order_side = 'buy' if size>0 else 'sell'
        if len(event_histories)==0:
            trimmed_size = self.position_manager.trim_to_margin(mid, size, symbol)
            asyncio.create_task(self.create_order(symbol, 'limit', order_side, abs(trimmed_size), price=edit_price,
                                                  params={'postOnly': edit_price_depth not in ['rush_in', 'rush_out'],
                                                          'ioc': edit_price_depth in ['rush_in', 'rush_out'],
                                                          'comment':edit_price_depth if edit_price_depth in ['rush_in','rush_out'] else 'new'}))
        # if only one and it's editable, stopout or peg or wait
        elif len(event_histories)==1 \
                and (self.order_manager.latest_value(event_histories[0][-1]['clientOrderId'], 'remaining') >= sizeIncrement) \
                and event_histories[0][-1]['state'] in OrderManager.acknowledgedStates:
            order = event_histories[0][-1]
            order_distance = (1 if order['side'] == 'buy' else -1) * (opposite_side - order['price'])
            repeg_gap = (1 if order['side'] == 'buy' else -1) * (edit_price - order['price'])

            # panic stop. we could rather place a trailing stop: more robust to latency, but less generic.
            if (stop_depth and order_distance > stop_trigger) \
                    or edit_price_depth in ['rush_in','rush_out']:
                size = self.order_manager.latest_value(order['clientOrderId'], 'remaining')
                price = sweep_price_atomic(orderbook, size * mid)
                asyncio.create_task(self.edit_order(symbol, 'limit', order_side, abs(size),
                                                     price = price,
                                                     params={'postOnly':False,
                                                             'ioc':True,
                                                             'comment':edit_price_depth if edit_price_depth in ['rush_in','rush_out'] else 'stop'},
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
        amount = self.round_to_increment(self.inventory_manager[symbol]['sizeIncrement'], amount)
        if amount == 0:
            return
        # set pending_new -> send rest -> if success, leave pending_new and give id. Pls note it may have been caught by handle_order by then.
        clientOrderId = self.order_manager.pending_new({'symbol': symbol,
                                                    'type': type,
                                                    'side': side,
                                                    'amount': amount,
                                                    'remaining': amount,
                                                    'price': price,
                                                    'comment': params['comment']},
                                                       self)
        try:
            # REST request
            order = await super().create_order(symbol, type, side, amount, price, params | {'clientOrderId':clientOrderId})
        except Exception as e:
            order = {'clientOrderId':clientOrderId,
                     'timestamp':datetime.utcnow().replace(tzinfo=timezone.utc).timestamp() * 1000,
                     'state':'rejected',
                     'comment':'create/'+str(e)}
            self.order_manager.cancel_or_reject(order)
            if isinstance(e,ccxt.InsufficientFunds):
                self.logger.info(f'{clientOrderId} too big: {amount*self.mid(symbol)}')
            elif isinstance(e,ccxt.RateLimitExceeded):
                throttle = 200.0
                self.logger.info(f'{str(e)}: waiting {throttle} ms)')
                await asyncio.sleep(throttle / 1000)
            else:
                raise e
        else:
            self.order_manager.sent(order)

    async def cancel_order(self, clientOrderId, trigger):
        '''set in flight, send cancel, set as pending cancel, set as canceled or insist'''
        symbol = clientOrderId.split('_')[1]
        self.order_manager.pending_cancel({'comment':trigger}
                                          | {key: [order[key] for order in self.order_manager[clientOrderId] if key in order][-1]
                                        for key in ['clientOrderId','symbol','side','amount','remaining','price']})  # may be needed

        try:
            status = await super().cancel_order(None,params={'clientOrderId':clientOrderId})
            self.order_manager.cancel_sent({'clientOrderId':clientOrderId,
                                        'symbol':symbol,
                                        'status':status,
                                        'comment':trigger})
            return True
        except ccxt.CancelPending as e:
            self.order_manager.cancel_sent({'clientOrderId': clientOrderId,
                                        'symbol': symbol,
                                        'status': str(e),
                                        'comment': trigger})
            return True
        except ccxt.InvalidOrder as e: # could be in flight, or unknown
            self.order_manager.cancel_or_reject({'clientOrderId':clientOrderId,
                                             'status':str(e),
                                             'state':'canceled',
                                             'comment':trigger})
            return False
        except Exception as e:
            self.logger.info(f'{clientOrderId} failed to cancel: {str(e)}')
            await asyncio.sleep(.1)
            return await self.cancel_order(clientOrderId, trigger+'+')

    async def close_dust(self):
        data = await asyncio.gather(*[self.fetch_balance(),self.fetch_markets()])
        balances = data[0]
        markets = data[1]
        coros = []
        for coin, balance in balances.items():
            if coin in self.currencies.keys() \
                    and coin != 'USD' \
                    and balance['total'] != 0.0 \
                    and np.abs(balance['total']) < float(self.markets[coin+'/USD']['info']['sizeIncrement']):
                size = balance['total']
                mid = float(self.markets[coin+'/USD']['info']['last'])
                if size > 0:
                    request = {'fromCoin':coin,'toCoin':'USD','size':np.abs(size)}
                else:
                    request = {'fromCoin':'USD','toCoin':coin,'size':np.abs(size)*self.mid(f'{coin}/USD')}
                coros += [self.privatePostOtcQuotes(request)]

        quoteId_list = await asyncio.gather(*coros)
        await asyncio.gather(*[self.privatePostOtcQuotesQuoteIdAccept({'quoteId': int(quoteId['result']['quoteId'])})
        for quoteId in quoteId_list
        if quoteId['success']])

async def get_exec_request(exchange,order_name,**kwargs):
    dirname = os.path.join(os.sep, configLoader.get_config_folder_path(config_name=kwargs['config']), "pfoptimizer")
    exchange_name = exchange.id
    subaccount = exchange.headers['FTX-SUBACCOUNT']

    if order_name == 'spread':
        coin = kwargs['coin']
        cash_name = coin + '/USD'
        future_name = coin + '-PERP'
        cash_price = float(exchange.market(cash_name)['info']['price'])
        future_price = float(exchange.market(future_name)['info']['price'])
        target_portfolio = pd.DataFrame(columns=['coin', 'name', 'optimalCoin', 'currentCoin', 'spot_price'], data=[
            [coin, cash_name, float(kwargs['cash_size']) / cash_price, 0, cash_price],
            [coin, future_name, -float(kwargs['cash_size']) / future_price, 0, future_price]])

    elif order_name == 'flatten':  # only works for basket with 2 symbols
        future_weights = pd.DataFrame(columns=['name', 'optimalWeight'])
        diff = await diff_portoflio(exchange, future_weights)
        smallest_risk = diff.groupby(by='coin')['currentCoin'].agg(
            lambda series: series.apply(np.abs).min() if series.shape[0] > 1 else 0)
        target_portfolio = diff
        target_portfolio['optimalCoin'] = diff.apply(lambda f: smallest_risk[f['coin']] * np.sign(f['currentCoin']),
                                                     axis=1)

    elif order_name == 'unwind':
        future_weights = pd.DataFrame(columns=['name', 'optimalWeight'])
        target_portfolio = await diff_portoflio(exchange, future_weights)
    else:
        raise Exception("unknown command")

    target_portfolio['exchange'] = exchange_name
    target_portfolio['subaccount'] = subaccount
    target_portfolio.rename(columns={'spot_price':'spot','optimalUSD':'optimalWeight'},inplace=True)
    target_portfolio['spot_ticker'] = target_portfolio['coin'].apply(lambda c: f'{c}/USD')
    target_portfolio['new_symbol'] = target_portfolio['name'].apply(lambda f: exchange.market(f)['symbol'])

    dfs = []
    filenames = []
    for coin, df in target_portfolio.groupby(by='coin'):
        temp_filename = os.path.join(os.sep, dirname, f'{order_name}_{exchange_name}_{subaccount}_{coin}.csv')
        filenames += [temp_filename]
        df.to_csv(temp_filename)
    return filenames

async def risk_reduction_routine(order_name, **kwargs):
    config = configLoader.get_executor_params(order=order_name, dirname=kwargs['config'])
    exchange = await myFtx.open_exec_exchange(kwargs['exchange'], config, subaccount=kwargs['subaccount'])
    orders = await get_exec_request(exchange, order_name, kwargs['config'])
    exchange.logger.critical(f'generated {orders}, now them all')
    await exchange.close()
    await asyncio.gather(*[single_coin_routine(order, kwargs['config'], **kwargs) for order in orders])

async def single_coin_routine(order_name, **kwargs):
    try:
        config = configLoader.get_executor_params(order=order_name,dirname=kwargs['config'])
        order = os.path.join(os.sep, configLoader.get_config_folder_path(config_name=kwargs['config']), "pfoptimizer", order_name)

        future_weights = pd.read_csv(order)
        exchange_name = future_weights['exchange'].unique()[0]
        subaccount = future_weights['subaccount'].unique()[0]

        exchange = await myFtx.open_exec_exchange(exchange_name, config, subaccount=subaccount)
        await exchange.build(order, config | {'comment': order})  # i
        exchange.logger.warning('cancelling orders')
        await asyncio.gather(*[exchange.cancel_all_orders(symbol) for symbol in exchange.inventory_manager])

        coros = [exchange.monitor_fills(), exchange.monitor_risk()] + \
                sum([[exchange.monitor_orders(symbol),
                      exchange.monitor_order_book(symbol),
                      exchange.monitor_trades(symbol)]
                     for symbol in exchange.inventory_manager], [])

        await asyncio.gather(*coros)

    except Exception as e:
        logger = logging.getLogger('tradeexecutor')
        logger.critical(str(e), exc_info=True)
        await asyncio.gather(*[exchange.cancel_all_orders(symbol) for symbol in exchange.inventory_manager])
        logger.warning(f'cancelled {exchange.inventory_manager} orders')
        # await exchange.close_dust()  # Commenting out until bug fixed
        await exchange.close()
        if not isinstance(e,InventoryManager.ReadyToShutdown):
            raise e

async def listen(order_name,**kwargs):

    config = configLoader.get_executor_params(order=order_name, dirname=kwargs['config'])
    order = os.path.join(os.sep, configLoader.get_config_folder_path(config_name=kwargs['config']), "pfoptimizer",
                         order_name)

    future_weights = pd.read_csv(order)
    exchange_name = future_weights['exchange'].unique()[0]
    subaccount = future_weights['subaccount'].unique()[0]

    exchange = await myFtx.open_exec_exchange(exchange_name, config, subaccount=subaccount)
    config['analytics_manager']['options'] = ["full","spread_vwap"]
    await exchange.build(order, config | {'comment': order})

    coros = [exchange.broadcast_analytics()] + \
            sum([[exchange.monitor_order_book(symbol),
                  exchange.monitor_trades(symbol)]
                 for symbol in exchange.inventory_manager], [])
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

    if 'listen' in kwargs and kwargs.pop('listen') == "True":
        asyncio.run(listen(order_name, **kwargs))
    elif order_name in ['unwind','flatten']:
        asyncio.run(risk_reduction_routine(order_name, **kwargs))
    else:
        asyncio.run(single_coin_routine(order_name, **kwargs)) # --> I am filled or I timed out and I have flattened position

 