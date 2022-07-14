import os.path
import time as t

from utils.MyLogger import ExecutionLogger

from utils.ftx_utils import *
from utils.config_loader import *
from utils.ccxt_utilities import *
from histfeed.ftx_history import fetch_trades_history, vwap_from_list
from riskpnl.ftx_risk_pnl import diff_portoflio
from riskpnl.ftx_margin import MarginCalculator
from utils.api_utils import MyModules,api
from utils.async_utils import *

import ccxtpro,collections

# parameters guide
# 'max_nb_coins': 'sharding'
# 'time_budget': scaling aggressiveness to 0 at time_budget (in seconds)
# 'global_beta': other coin weight in the global risk
# 'max_cache_size': mkt data cache
# 'message_cache_size': missed messages cache
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
# c'est while symbol est encore running running_symbols


def symbol_locked(wrapped):
    # pour eviter que 2 ws agissent sur l'OMS en mm tps
    # ne fait RIEN pour le moment
    '''decorates self.lifecycle_xxx(lientOrderId) to prevent race condition on state'''
    @functools.wraps(wrapped)
    def _wrapper(*args, **kwargs):
        return wrapped(*args, **kwargs) # TODO:temporary disabled

        self=args[0]
        symbol = args[1]['clientOrderId'].split('_')[1]
        while self.lock[symbol].count:
            self.myLogger.logger.info(f'{self.lock[symbol].count}th race conditions on {symbol} blocking {wrapped.__name__}')
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
        while len(args)==1 or (args[1] in args[0].running_symbols):
            try:
                value = await func(*args, **kwargs)
            except ccxt.NetworkError as e:
                self.myLogger.logger.info(str(e))
                self.myLogger.logger.info('reconciling after '+func.__name__+' dropped off')
                await self.reconcile() # implicitement redemarre la socket, ccxt fait ca comme ca
            except Exception as e:
                self.myLogger.logger.info(e, exc_info=True)
                raise e
    return wrapper_loop

def intercept_message_during_reconciliation(wrapped):
    '''decorates self.watch_xxx(message) to block incoming messages during reconciliation'''
    @functools.wraps(wrapped)
    def _wrapper(*args, **kwargs):
        self=args[0]
        if args[0].lock['reconciling'].locked():
            self.myLogger.logger.warning(f'message during reconciliation{args[2]}')
            self.message_missed.append(args[2])
        else:
            return wrapped(*args, **kwargs)
    return _wrapper

class myFtx(ccxtpro.ftx):
    def __init__(self, parameters, logger=None, config={}):
        super().__init__(config=config)
        self.parameters = parameters
        self.lock = {'reconciling':threading.Lock()}
        self.message_missed = collections.deque(maxlen=parameters['message_cache_size'])
        self.options['tradesLimit'] = parameters['message_cache_size']
        self.orderbook_history = None
        self.trades_history = None
        # self.trades is in ccxtpro
        if __debug__: self.all_messages = collections.deque(maxlen=parameters['message_cache_size']*10)
        self.rest_semaphor = asyncio.Semaphore(safe_gather_limit)

        self.orders_lifecycle = dict()
        self.latest_order_reconcile_timestamp = None
        self.latest_fill_reconcile_timestamp = None
        self.latest_exec_parameters_reconcile_timestamp = None

        self.limit = myFtx.LimitBreached(parameters['check_frequency'])
        self.exec_parameters = {}
        self.running_symbols =[]

        self.risk_reconciliations = []
        self.risk_state = {}
        self.pv = None
        self.total_delta = None # in USD
        self.margin = None

        self.myLogger = logger if logger is not None else build_logging('tradeexecutor')

    # --------------------------------------------------------------------------------------------
    # ---------------------------------- various helpers -----------------------------------------
    # --------------------------------------------------------------------------------------------

    class DoneDeal(Exception):
        def __init__(self,status=None):
            super().__init__(status)

    class NothingToDo(DoneDeal):
        def __init__(self, status=None):
            super().__init__(status)

    class TimeBudgetExpired(Exception):
        def __init__(self,status=None):
            super().__init__(status)
    class LimitBreached(Exception):
        def __init__(self,check_frequency,limit=None):
            super().__init__()
            self.delta_limit = limit
            self.check_frequency = check_frequency

    def find_clientID_from_fill(self,fill):
        '''find order by id, even if still in flight
        all layers events must carry id if known !! '''
        if 'clientOrderId' in fill:
            found = fill['clientOrderId']
            assert (found in self.orders_lifecycle),f'{found} unknown'
        else:
            try:
                found = next(clientID for clientID, events in self.orders_lifecycle.items() if
                             any('id' in event and event['id'] == fill['order'] for event in events))
            except StopIteration as e:# could still be in flight --> lookup
                try:
                    found = next(clientID for clientID,events in self.orders_lifecycle.items() if
                                 any(event['price'] == fill['price']
                                     and event['amount'] == fill['amount']
                                     and event['symbol'] == fill['symbol']
                                     #and x['type'] == fill['type'] # sometimes None in fill
                                     and event['side'] == fill['side'] for event in events))
                except StopIteration as e:
                    raise Exception("fill {} not found".format(fill))
        return found

    def mid(self,symbol):
        if symbol == 'USD/USD': return 1.0
        data = self.tickers[symbol] if symbol in self.tickers else self.markets[symbol]['info']
        return 0.5*(float(data['bid'])+float(data['ask']))

    def round_to_increment(self, sizeIncrement, amount):
        if amount >= 0:
            return np.floor(amount/sizeIncrement) * sizeIncrement
        else:
            return -np.floor(-amount / sizeIncrement) * sizeIncrement

    def trim_to_margin(self, mid, size, symbol):
        '''trim size to margin allocation (equal for all running symbols)'''
        marginal_IM = self.margin.order_marginal_cost(symbol, size, mid, 'IM',
                                                      self.markets[symbol]['type'])
        estimated_IM = self.margin.estimate(self, 'IM')
        actual_IM = self.margin.actual_IM
        # TODO: headroom estimate is wrong ....
        if self.margin.actual_IM <= 0:
            self.myLogger.logger.info(
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
            self.myLogger.logger.info(f'trimmed {size} {symbol} by {trim_factor}')
        return trimmed_size

    # --------------------------------------------------------------------------------------------
    # ---------------------------------- OMS             -----------------------------------------
    # --------------------------------------------------------------------------------------------

    # orders_lifecycle is a dictionary of blockchains, one per order intended.
    # each block has a lifecyle_state in ['pending_new','sent','pending_cancel','acknowledged','partially_filled','canceled','rejected','filled']
    # lifecycle_xxx do:
    # 1) resolve clientID
    # 2) validate block, notably maintaining orders_pending_new (which is orders_lifecycle[x][-1]['lifecycle_state']=='pending_new')
    # 3) build a new block from messages received
    # 4) mines it to the blockchain
    #
    allStates = set(['pending_new', 'pending_cancel', 'sent', 'cancel_sent', 'pending_replace', 'acknowledged', 'partially_filled', 'filled', 'canceled', 'rejected'])
    openStates = set(['pending_new', 'sent', 'pending_cancel', 'pending_replace', 'acknowledged', 'partially_filled'])
    acknowledgedStates = set(['acknowledged', 'partially_filled'])
    cancelableStates = set(['sent', 'acknowledged', 'partially_filled'])
    # --------------------------------------------------------------------------------------------

    def filter_order_histories(self,symbols=[],state_set=[]):
        '''returns all blockchains for symbol (all symbols if None), in a set of current states'''
        return [data
            for data in self.orders_lifecycle.values()
            if ((data[0]['symbol'] in symbols) or (symbols == []))
            and ((data[-1]['lifecycle_state'] in state_set) or (state_set == []))]

    def latest_value(self,clientOrderId,key):
        for previous_state in reversed(self.orders_lifecycle[clientOrderId]):
            if key in previous_state:
                return previous_state[key]
        raise f'{key} not found for {clientOrderId}'

    # pour creer un ordre
    def lifecycle_pending_new(self, order_event):
        '''self.orders_lifecycle = {clientId:[{key:data}]}
        order_event:trigger,symbol'''
        #1) resolve clientID
        nowtime = datetime.utcnow().replace(tzinfo=timezone.utc).timestamp() * 1000
        clientOrderId = order_event['comment'] + '_' + order_event['symbol'] + '_' + str(int(nowtime))

        #2) validate block
        pass

        #3) make new block

        ## order details
        symbol = order_event['symbol']
        coin = self.markets[symbol]['base']

        eventID = clientOrderId + '_' + str(int(nowtime))
        current = {'clientOrderId':clientOrderId,
                   'eventID': eventID,
                   'lifecycle_state': 'pending_new',
                   'remote_timestamp': None,
                   'timestamp': nowtime,
                   'id': None} | order_event

        ## risk details
        risk_data = self.risk_state[coin]
        current |= {'risk_timestamp':risk_data[symbol]['delta_timestamp'],
                    'delta':risk_data[symbol]['delta'],
                    'netDelta': risk_data['netDelta'],
                    'pv(wrong timestamp)':self.pv,
                    'margin_headroom':self.margin.actual_IM,
                    'IM_discrepancy': self.margin.estimate(self,'IM') - self.margin.actual_IM}

        ## mkt details
        if symbol in self.tickers:
            mkt_data = self.tickers[symbol]
            timestamp = mkt_data['timestamp']
        else:
            mkt_data = self.markets[symbol]['info']|{'bidVolume':0,'askVolume':0}#TODO: should have all risk group
            timestamp = self.exec_parameters['timestamp']
        current |= {'mkt_timestamp': timestamp} \
                   | {key: mkt_data[key] for key in ['bid', 'bidVolume', 'ask', 'askVolume']}

        #4) mine genesis block
        self.orders_lifecycle[clientOrderId] = [current]

        return clientOrderId

    @symbol_locked
    def lifecycle_sent(self, order_event):
        '''order_event:clientOrderId,timestamp,remaining,status'''
        # 1) resolve clientID
        clientOrderId = order_event['clientOrderId']

        # 2) validate block
        past = self.orders_lifecycle[clientOrderId][-1]
        if past['lifecycle_state'] not in ['pending_new','acknowledged','partially_filled']:
            self.myLogger.logger.info('order {} was {} before sent'.format(past['clientOrderId'],past['lifecycle_state']))
            return

        # 3) new block
        nowtime = datetime.utcnow().replace(tzinfo=timezone.utc).timestamp() * 1000
        eventID = clientOrderId + '_' + str(int(nowtime))
        current = order_event | {'eventID':eventID,
                                 'lifecycle_state':'sent',
                                 'remote_timestamp':order_event['timestamp']}
        current['timestamp'] = nowtime

        self.orders_lifecycle[clientOrderId] += [current]
        if order_event['status'] == 'closed':
            if order_event['remaining'] == 0:
                current['order'] = order_event['id']
                current['id'] = None
                assert 'amount' in current,"'amount' in current"
                #self.lifecycle_fill(current)
            else:
                current['lifecycle_state'] = 'rejected'
                self.lifecycle_cancel_or_reject(current)

    @symbol_locked
    def lifecycle_pending_cancel(self, order_event):
        '''this is first called with result={}, then with the rest response. could be two states...
        order_event:clientOrderId,status,trigger'''
        # 1) resolve clientID
        clientOrderId = order_event['clientOrderId']

        # 2) validate block
        past = self.orders_lifecycle[clientOrderId][-1]
        if past['lifecycle_state'] not in self.openStates:
            self.myLogger.logger.info('order {} re-canceled'.format(past['clientOrderId']))
            return

        # 3) new block
        nowtime = datetime.utcnow().replace(tzinfo=timezone.utc).timestamp() * 1000
        eventID = clientOrderId + '_' + str(int(nowtime))
        current = order_event | {'eventID': eventID,
                                 'remote_timestamp': None,
                                 'lifecycle_state': 'pending_cancel'}
        current['timestamp'] = nowtime

        # 4) mine
        self.orders_lifecycle[clientOrderId] += [current]

    @symbol_locked
    def lifecycle_cancel_sent(self, order_event):
        '''this is first called with result={}, then with the rest response. could be two states...
        order_event:clientOrderId,status'''
        # 1) resolve clientID
        clientOrderId = order_event['clientOrderId']

        # 2) validate block
        past = self.orders_lifecycle[clientOrderId][-1]
        if not past['lifecycle_state'] in self.openStates:
            self.myLogger.logger.info('order {} re-canceled'.format(past['clientOrderId']))
            return

        # 3) new block
        nowtime = datetime.utcnow().replace(tzinfo=timezone.utc).timestamp() * 1000
        eventID = clientOrderId + '_' + str(int(nowtime))
        current = order_event | {'eventID': eventID,
                                 'lifecycle_state': 'cancel_sent',
                                 'remote_timestamp': None,
                                 'timestamp': nowtime}

        # 4) mine
        self.orders_lifecycle[clientOrderId] += [current]

    # reponse de websocket qui dit que c'est acknowledged
    @symbol_locked
    def lifecycle_acknowledgment(self, order_event):
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
                #self.lifecycle_fill(current)
        elif order_event['status'] in ['closed']:
            #TODO: order event. not necessarily rejected ?
            if order_event['remaining'] == 0:
                current['order'] = order_event['id']
                current['id'] = None
                #self.lifecycle_fill(current)
            else:
                current['lifecycle_state'] = 'rejected'
                self.lifecycle_cancel_or_reject(current)
        elif order_event['status'] in ['canceled']:
            current['lifecycle_state'] = 'canceled'
            self.lifecycle_cancel_or_reject(current)
        # elif order_event['filled'] !=0:
        #     raise Exception('filled ?')#TODO: code that ?
        #     current['order'] = order_event['id']
        #     current['id'] = None
        #     assert 'amount' in current,"assert 'amount' in current"
        #     self.lifecycle_fill(current)
        else:
            current['lifecycle_state'] = 'acknowledged'
            self.orders_lifecycle[clientOrderId] += [current]
            self.margin.add_open_order(order_event, self.markets)

    @symbol_locked
    def lifecycle_fill(self,order_event):
        '''order_event: id, trigger,timestamp,amount,order,symbol'''
        # 1) resolve clientID
        clientOrderId = self.find_clientID_from_fill(order_event)
        symbol = order_event['symbol']
        coin = self.markets[symbol]['base']
        size_threshold = self.exec_parameters[coin][symbol]['sizeIncrement'] / 2

        nowtime = datetime.utcnow().replace(tzinfo=timezone.utc).timestamp()*1000
        eventID = clientOrderId + '_' + str(int(nowtime))
        current = order_event | {'eventID':eventID,
                                 'fillId': order_event['id'],
                                 'remote_timestamp': order_event['timestamp']}
        # overwrite some fields
        current['timestamp'] = nowtime
        current['remaining'] = max(0, self.latest_value(clientOrderId,'remaining') - order_event['amount'])
        current['id'] = order_event['order']

        if current['remaining'] < size_threshold:
            self.orders_lifecycle[clientOrderId] += [current|{'lifecycle_state':'filled'}]
        else:
            self.orders_lifecycle[clientOrderId] += [current | {'lifecycle_state': 'partially_filled'}]

    @symbol_locked
    def lifecycle_cancel_or_reject(self, order_event):
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
                                 'lifecycle_state': order_event['lifecycle_state'],
                                 'remote_timestamp': order_event['timestamp'] if 'timestamp' in order_event else None}
        current['timestamp'] = nowtime

        # 4) mine
        self.orders_lifecycle[clientOrderId] += [current]

    async def reconcile_fills(self):
        '''fetch fills, to recover missed messages'''
        sincetime = max(self.exec_parameters['timestamp'],self.latest_fill_reconcile_timestamp-1000)
        fetched_fills = sum(await asyncio.gather(*[self.fetch_my_trades(since=sincetime,symbol=symbol) for symbol in self.running_symbols]),[])
        for fill in fetched_fills:
            fill['comment'] = 'reconciled'
            fill['clientOrderId'] = self.find_clientID_from_fill(fill)
            if fill['id'] not in [block['fillId']
                                     for block in self.orders_lifecycle[fill['clientOrderId']]
                                     if 'fillId' in block]:
                self.lifecycle_fill(fill)
        self.latest_fill_reconcile_timestamp = datetime.now(timezone.utc).timestamp()*1000

    async def reconcile_orders(self):
        self.latest_order_reconcile_timestamp = datetime.now(timezone.utc).timestamp() * 1000

        # list internal orders
        internal_order_internal_status = {clientOrderId:data[-1] for clientOrderId,data in self.orders_lifecycle.items()
                         if data[-1]['lifecycle_state'] in myFtx.openStates}

        # add missing orders (we missed orders from the exchange)
        external_orders = sum(await asyncio.gather(*[self.fetch_open_orders(symbol=symbol) for symbol in self.running_symbols]),[])
        for order in external_orders:
            if order['clientOrderId'] not in internal_order_internal_status.keys():
                self.lifecycle_acknowledgment(order | {'comment':'reconciled_missing'})
                self.myLogger.logger.info('{} was missing'.format(order['clientOrderId']))
                found = order['clientOrderId']
                assert (found in self.orders_lifecycle), f'{found} unknown'

        # remove zombie orders (we beleive they are live but they are not open)
        # should not happen so we put it in the logs
        internal_order_external_status = await safe_gather([self.fetch_order(id=None, params={'clientOrderId':clientOrderId})
                                                   for clientOrderId in internal_order_internal_status.keys()],
                                                           semaphore=self.rest_semaphor,
                                                           return_exceptions=True)
        for clientOrderId,external_status in zip(internal_order_internal_status.keys(),internal_order_external_status):
            if isinstance(external_status, Exception):
                self.myLogger.logger.info(f'reconcile_orders {clientOrderId} : {external_status}')
                continue
            if external_status['status'] != 'open':
                self.lifecycle_cancel_or_reject(external_status | {'lifecycle_state': 'canceled', 'comment':'reconciled_zombie'})
                self.myLogger.logger.info('{} was a {} zombie'.format(clientOrderId,internal_order_internal_status[clientOrderId]['lifecycle_state']))


    # ---------------------------------------------------------------------------------------------
    # ---------------------------------- PMS -----------------------------------------
    # ---------------------------------------------------------------------------------------------

    async def build_state(self, weights,parameters):
        '''initialize all state and does some filtering (weeds out slow underlyings; should be in strategy)
            target_sub_portfolios = {coin:{rush_level_increment,
            symbol1:{'spot_price','diff','target'}]}]'''
        self.limit.delta_limit = self.parameters['delta_limit']

        frequency = timedelta(minutes=1)
        end = datetime.now(timezone.utc)
        start = end - timedelta(seconds=self.parameters['stdev_window'])

        trades_history_list = await safe_gather([fetch_trades_history(
            self.market(symbol)['id'], self, start, end, frequency=frequency)
            for symbol in weights['name']],semaphore=self.rest_semaphor)
        self.trades_history = {self.market(symbol)['symbol']: data for symbol,data in zip(weights['name'],trades_history_list)}
        trading_fees = await self.fetch_trading_fees()

        weights['diffCoin'] = weights['optimalCoin'] - weights['currentCoin']
        weights['diffUSD'] = weights['diffCoin'] * weights['spot_price']
        weights['name'] = weights['name'].apply(lambda s: self.market(s)['symbol'])
        weights.set_index('name', inplace=True)
        coin_list = weights['coin'].unique()

        # {coin:{symbol1:{data1,data2...},sumbol2:...}}
        full_dict = {}
        for symbol in self.trades_history:
            coin = self.market(symbol)['base']
            if coin in full_dict:
                full_dict[coin] |={symbol:
                              {'diff': weights.loc[symbol, 'diffCoin'],
                               'target': weights.loc[symbol, 'optimalCoin'],
                               'spot_price': weights.loc[symbol, 'spot_price']}}
            else:
                full_dict[coin] = {symbol:
                                        {'diff': weights.loc[symbol, 'diffCoin'],
                                         'target': weights.loc[symbol, 'optimalCoin'],
                                         'spot_price': weights.loc[symbol, 'spot_price']}}
        # exclude coins too slow or symbol diff too small, limit to max_nb_coins
        diff_threshold = sorted(
            [np.abs(data['diff']*data['spot_price'])
                  for coin_data in full_dict.values() for data in coin_data.values()])[-min(self.parameters['max_nb_coins'],len(coin_list)-1)]
        equity = float((await self.privateGetAccount())['result']['totalAccountValue'])
        diff_threshold = max(diff_threshold,self.parameters['significance_threshold'] * equity)

        data_dict = {coin: coin_data
                     for coin, coin_data in full_dict.items()
                     if (parameters['comment'] != 'sysperp'
                         or all(self.trades_history[symbol]['volume'].mean() * self.parameters['time_budget'] * self.parameters['volume_share'] > np.abs(data['diff']) for symbol,data in coin_data.items()))
                     and any(np.abs(data['diff']) >= max(
                         diff_threshold/data['spot_price'],
                         float(self.markets[symbol]['info']['minProvideSize']))
                             for symbol, data in coin_data.items())}
        if data_dict =={}:
            self.exec_parameters = {'timestamp': end.timestamp() * 1000}
            raise myFtx.DoneDeal('nothing to do')

        self.running_symbols = [symbol
                                    for coin_data in data_dict.values()
                                    for symbol in coin_data.keys()]

        # get times series of target baskets, compute quantile of increments and add to last price
        # remove series
        self.exec_parameters = {'timestamp':end.timestamp()*1000} \
                               |{sys.intern(coin):
                                     {sys.intern(symbol):
                                         {
                                             sys.intern('diff'): data['diff'], # how much coin I should do
                                             sys.intern('target'): data['target'], # how much coin I must have at the end
                                             sys.intern('priceIncrement'): float(self.markets[symbol]['info']['priceIncrement']),
                                             sys.intern('sizeIncrement'): float(self.markets[symbol]['info']['minProvideSize']),
                                             sys.intern('takerVsMakerFee'): trading_fees[symbol]['taker']-trading_fees[symbol]['maker'],
                                             sys.intern('spot'): self.mid(symbol)
                                         }
                                         for symbol, data in coin_data.items()}
                                 for coin, coin_data in data_dict.items()}

        self.lock |= {symbol: CustomRLock() for symbol in self.running_symbols}
        self.orderbook_history = {symbol: collections.deque(maxlen=parameters['message_cache_size']*10)
                                  for symbol in self.running_symbols}
        self.trades_history = {symbol: self.trades_history[symbol]
                               for symbol in self.running_symbols}

        # initialize the risk of the system <=> of what is running. What is the risk that we think we have
        self.risk_state = {sys.intern(coin):
                               {sys.intern('netDelta'):0}
                               | {sys.intern(symbol):
                                   {
                                       sys.intern('delta'): 0,
                                       sys.intern('delta_timestamp'):end.timestamp()*1000,
                                       sys.intern('delta_id'): 0
                                   }
                                   for symbol, data in coin_data.items()}
                           for coin, coin_data in data_dict.items()}

        # Initializes a margin calculator that can understand the margin of an order
        # reconcile the margin of an exchange with the margin we calculate
        # pls note it needs the exchange pv, which is known after reconcile
        self.margin = await MarginCalculator.margin_calculator_factory(self)

        # populates risk, pv and IM
        self.latest_order_reconcile_timestamp = self.exec_parameters['timestamp']
        self.latest_fill_reconcile_timestamp = self.exec_parameters['timestamp']
        self.latest_exec_parameters_reconcile_timestamp = self.exec_parameters['timestamp']
        await self.reconcile()

        # Dumps the order that it received from current_weight.xlsx. only logs
        await self.myLogger.data_to_json({symbol:data
                       for coin,coin_data in self.exec_parameters.items() if coin in self.currencies
                       for symbol,data in coin_data.items() if symbol in self.markets}
                      | {'parameters': {'inception_time':self.exec_parameters['timestamp']} | self.parameters},
                                   'request')

    async def update_exec_parameters(self):
        '''scales order placement params with time'''
        frequency = timedelta(minutes=1)

        # get times series of target baskets, compute quantile of increments and add to last price
        for symbol in self.running_symbols:
            if symbol in self.trades:
                data = pd.DataFrame(self.trades[symbol])
                data = data[(data['timestamp']>self.trades_history[symbol]['vwap'].index.max().timestamp()*1000)]
                if data.empty:
                    continue
                else:
                    # compute vwaps
                    data['liquidation'] = data['info'].apply(lambda x: x['liquidation'])
                    data.rename(columns={'datetime':'time','amount':'size'},inplace=True)
                    data['size'] = data.apply(lambda x: x['size'] if x['side']=='buy' else -x['size'], axis=1)
                    data = data[['size', 'price', 'liquidation', 'time']]
                    vwap = vwap_from_list(frequency=frequency,trades=data)
                    # append vwaps
                    for key in self.trades_history[symbol]:
                        extended = pd.concat([self.trades_history[symbol][key],vwap[key]])
                        self.trades_history[symbol][key] = extended[~extended.index.duplicated()].sort_index().ffill()
                    #TODO: empty cache
                    #self.trades[symbol].clear()

        nowtime = datetime.utcnow().replace(tzinfo=timezone.utc).timestamp() * 1000
        time_limit = self.exec_parameters['timestamp'] + self.parameters['time_budget']*1000

        if nowtime > time_limit:
            self.myLogger.logger.warning('time budget expired')
            raise myFtx.TimeBudgetExpired('')

        def basket_vwap_quantile(series_list, diff_list, quantiles):
            series = pd.concat([serie * coef for serie, coef in zip(series_list, diff_list)], join='inner', axis=1)
            return [-1e18 if quantile<=0 else 1e18 if quantile>=1 else series.sum(axis=1).quantile(quantile) for quantile in quantiles]

        scaler = max(0,(time_limit-nowtime)/(time_limit-self.exec_parameters['timestamp']))
        for coin,coin_data in self.exec_parameters.items():
            if coin not in self.currencies:
                continue

            # used to get proper rush live levels. in coin.
            for symbol, data in coin_data.items():
                if symbol in self.running_symbols:
                    data['update_time_delta'] = data['target'] - self.risk_state[coin][symbol]['delta'] / self.mid(symbol)

            # entry_level = what level on basket is too bad to even place orders
            # rush_in_level = what level on basket is so good that you go in at market on both legs
            # rush_out_level = what level on basket is so bad that you go out at market on both legs
            quantiles = basket_vwap_quantile(
                [self.trades_history[symbol]['vwap'] for symbol in coin_data if symbol in self.markets],
                [data['update_time_delta'] * self.mid(symbol) for symbol,data in coin_data.items() if symbol in self.markets],
                [self.parameters['entry_tolerance'],self.parameters['rush_in_tolerance'],self.parameters['rush_out_tolerance']])

            coin_data['entry_level'] = quantiles[0]
            coin_data['rush_in_level'] = quantiles[1]
            coin_data['rush_out_level'] = quantiles[2]

            for symbol, data in coin_data.items():
                if symbol not in self.running_symbols:
                    continue
                stdev = self.trades_history[symbol]['vwap'].std().squeeze()
                if not (stdev>0): stdev = 1e-16

                # edit_price_depth = how far to put limit on risk increasing orders
                data['edit_price_depth'] = stdev * np.sqrt(self.parameters['edit_price_tolerance']) * scaler

                # aggressive_edit_price_depth = how far to put limit on risk reducing orders
                data['aggressive_edit_price_depth'] = self.parameters['aggressive_edit_price_tolerance'] * data['priceIncrement']

                # edit_trigger_depth = how far to let the mkt go before re-pegging limit orders
                data['edit_trigger_depth'] = stdev * np.sqrt(self.parameters['edit_trigger_tolerance']) * scaler

                # stop_depth = how far to set the stop on risk reducing orders
                data['stop_depth'] = stdev * np.sqrt(self.parameters['stop_tolerance']) * scaler

                # slice_size: cap to expected time to trade consistent with edit_price_tolerance
                volume_share = self.parameters['volume_share'] * self.parameters['edit_price_tolerance'] * \
                               self.trades_history[symbol]['volume'].mean()
                data['slice_size'] = volume_share

        self.latest_exec_parameters_reconcile_timestamp = nowtime
        self.myLogger.logger.warning(f'scaled params by {scaler} at {nowtime}')

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
            await self.reconcile_fills()   # goes on exchange, check the fills (fetch my trades, REST request at the exchange), puts them in the OMS
            # there is a state in the OMS that knows that it is coming from reconcile
            await self.reconcile_orders()   # ask all orders vs your own open order state.
            # await self.update_exec_parameters()   # refresh the parameters of the exec that are quantitative aggressiveness, where I put my limit vs top of book,
            # when do I think levels are good I shoot market on both legs

            # now recompute risks
            previous_total_delta = self.total_delta
            previous_pv = self.pv

            state = await syncronized_state(self)
            risk_timestamp = datetime.utcnow().replace(tzinfo=timezone.utc).timestamp() * 1000

            # delta is noisy for perps, so override to delta 1.
            self.pv = 0
            for coin, balance in state['balances']['total'].items():
                if coin != 'USD':
                    symbol = coin+'/USD'
                    mid = state['markets']['price'][self.market_id(symbol)]
                    delta = balance * mid
                    if coin not in self.risk_state:
                        self.risk_state[coin] = {'netDelta': 0}
                    if symbol not in self.risk_state[coin]:
                        self.risk_state[coin][symbol] = {'delta_id':0}
                    self.risk_state[coin][symbol]['delta'] = delta
                    self.risk_state[coin][symbol]['delta_timestamp'] = risk_timestamp
                    self.pv += delta

            self.total_delta = self.pv
            self.pv += state['balances']['total']['USD'] #doesn't contribute to delta, only pv !

            for name,position in state['positions']['netSize'].items():
                symbol = self.market(name)['symbol']
                coin = self.markets[symbol]['base']
                delta = position * self.mid(symbol)

                if coin not in self.risk_state:
                    self.risk_state[coin] = {'netDelta':0}
                if symbol not in self.risk_state[coin]:
                    self.risk_state[coin][symbol] = {'delta_id':0}

                self.risk_state[coin][symbol]['delta'] = delta
                self.risk_state[coin][symbol]['delta_timestamp']=risk_timestamp
                self.total_delta += delta

            for coin,coin_data in self.risk_state.items():
                coin_data['netDelta']= sum([data['delta']
                                            for symbol,data in coin_data.items()
                                            if symbol in self.markets and 'delta' in data.keys()])

            # update IM
            await self.margin.refresh(self, balances=state['balances']['total'], positions=state['positions']['netSize'],im_buffer= 0.01 * self.pv)

            # update_exec_parameters
            await self.update_exec_parameters()

            # replay missed _messages.
            self.myLogger.logger.warning(f'replaying {len(self.message_missed)} messages after recon')
            while self.message_missed:
                message = self.message_missed.popleft()
                data = self.safe_value(message, 'data')
                channel = message['channel']
                if channel == 'fills':
                    fill = self.parse_trade(data)
                    self.process_fill(fill | {'orderTrigger':'replayed'})
                elif channel == 'orders':
                    order = self.parse_order(data)
                    if order['symbol'] in self.running_symbols:
                        self.process_order(order | {'orderTrigger':'replayed'})
                # we record the orderbook but don't launch quoter (don't want to react after the fact)
                elif channel == 'orderbook':
                    pass
                    #orderbook = self.parse_order_book(data,symbol,data['time'])
                    #self.process_order_book(symbol, orderbook | {'orderTrigger':'replayed'})

        # critical job is done, release lock
        await self.myLogger.data_to_json(self.orders_lifecycle,'events')

        self.risk_reconciliations += [{'lifecycle_state': 'remote_risk', 'symbol':symbol_, 'delta_timestamp':self.risk_state[coin][symbol_]['delta_timestamp'],
                                       'delta':self.risk_state[coin][symbol_]['delta'],'netDelta':self.risk_state[coin]['netDelta'],
                                       'pv':self.pv,'estimated_IM':self.margin.estimate(self,'IM'), 'actual_IM':self.margin.actual_IM,
                                       'pv error': self.pv-(previous_pv or 0),'total_delta error': self.total_delta-(previous_total_delta or 0)}
                                      for coin,coindata in self.risk_state.items()
                                      for symbol_ in coindata.keys() if symbol_ in self.markets]
        await self.myLogger.data_to_json(self.risk_reconciliations,'risk_reconciliations')


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
        self.orderbook_history[symbol].append([timestamp, mid])

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
        coin = self.markets[symbol]['base']

        # update risk_state
        if coin not in self.risk_state:
            self.risk_state[coin] = {'netDelta':0}
        if symbol not in self.risk_state[coin]:
            self.risk_state[coin][symbol] = {'delta':0,'delta_id':0}
        data = self.risk_state[coin][symbol]
        fill_size = fill['amount'] * (1 if fill['side'] == 'buy' else -1) * fill['price']
        data['delta'] += fill_size
        data['delta_timestamp'] = fill['timestamp']
        latest_delta = data['delta_id']
        data['delta_id'] = max(latest_delta, int(fill['order']))
        self.risk_state[coin]['netDelta'] += fill_size
        self.total_delta += fill_size

        #update margin
        self.margin.add_instrument(symbol, fill_size, self.markets[symbol]['type'])

        # only log trades being run by this process
        fill['clientOrderId'] = fill['info']['clientOrderId']
        fill['comment'] = 'websocket_fill'
        if symbol in self.running_symbols:
            self.lifecycle_fill(fill)

            # logger.info
            self.myLogger.logger.warning('{} filled after {}: {} {} at {}'.format(fill['clientOrderId'],
                                                                      fill['timestamp'] - int(fill['clientOrderId'].split('_')[-1]),
                                                                      fill['side'], fill['amount'],
                                                                      fill['price']))

            current = self.risk_state[coin][symbol]['delta']
            target = self.exec_parameters[coin][symbol]['target'] * fill['price']
            diff = self.exec_parameters[coin][symbol]['diff'] * fill['price']
            initial = self.exec_parameters[coin][symbol]['target'] * fill['price'] - diff
            self.myLogger.logger.warning('{} risk at {} ms: {}% done [current {}, initial {}, target {}]'.format(
                symbol,
                self.risk_state[coin][symbol]['delta_timestamp'],
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
        self.lifecycle_acknowledgment(order| {'comment': 'websocket_acknowledgment'}) # status new, triggered, open or canceled

    # ---------------------------------- misc

    @loop
    async def monitor_risk(self):
        '''redundant minutely risk check'''#TODO: would be cool if this could interupt other threads and restart it when margin is ok.
        await asyncio.sleep(self.limit.check_frequency)
        await self.reconcile()

        absolute_risk = sum(abs(data['netDelta']) for data in self.risk_state.values())
        if absolute_risk > self.pv * self.limit.delta_limit:
            self.myLogger.logger.info(f'absolute_risk {absolute_risk} > {self.pv * self.limit.delta_limit}')
        if self.margin.actual_IM < self.pv/100:
            self.myLogger.logger.info(f'IM {self.margin.actual_IM}  < 1%')

    async def watch_ticker(self, symbol, params={}):
        '''watch_order_book is faster than watch_tickers so we DON'T LISTEN TO TICKERS. Dirty...'''
        raise Exception("watch_order_book is faster than watch_tickers so we DON'T LISTEN TO TICKERS. Dirty...")

    @loop
    async def monitor_trades(self,symbol):
        '''maintains risk_state, event_records, logger.info
            #     await self.reconcile_state() is safer but slower. we have monitor_risk to reconcile'''
        trades = await self.watch_trades(symbol=symbol)

    # ---------------------------------- just to record messages while reconciling
    @intercept_message_during_reconciliation
    def handle_my_trade(self, client, message):
        super().handle_my_trade(client, message)

    @intercept_message_during_reconciliation
    def handle_order(self, client, message):
        super().handle_order(client, message)

    @intercept_message_during_reconciliation
    def handle_trade(self, client, message):
        super().handle_trade(client, message)

    #@intercept_message_during_reconciliation
    def handle_order_book_update(self, client, message):
        '''self.orderbook_history[symbol] = dict {'timestamp','bids','asks','mid_at_depth'}'''
        super().handle_order_book_update(client, message)
        marketId = self.safe_string(message, 'market')
        if marketId in self.markets_by_id:
            symbol = self.markets_by_id[marketId]['symbol']
            coin = self.markets[symbol]['base']
            depth = self.exec_parameters[coin][symbol]['diff'] * self.mid(symbol)
            side_px = sweep_price_atomic(self.orderbooks[symbol], depth)
            opposite_side_px = sweep_price_atomic(self.orderbooks[symbol], -depth)
            mid_at_depth = 0.5*(side_px+opposite_side_px)
            self.orderbook_history[symbol].append({key:self.orderbooks[symbol][key] for key in ['timestamp','bids','asks']}
                                                  |{'mid_at_depth':mid_at_depth})

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
        params = self.exec_parameters[coin][symbol]

        # size to do:
        original_size = params['target'] - self.risk_state[coin][symbol]['delta']/mid
        if np.abs(original_size * mid) < self.parameters['significance_threshold'] * self.pv: #self.exec_parameters[coin][symbol]['sizeIncrement']:
            self.running_symbols.remove(symbol)
            self.myLogger.logger.warning(f'{symbol} done, {self.running_symbols} left to do')
            if self.running_symbols == []:
                raise myFtx.DoneDeal('all done')
            else: return

        #risk
        delta_timestamp = self.risk_state[coin][symbol]['delta_timestamp']
        delta_plus = self.risk_state[coin]['netDelta'] + sum(self.margin.open_orders[symbol]['longs']
                                                             for symbol in self.margin.open_orders
                                                             if self.markets[symbol]['base']==coin
                                                             and symbol in self.margin.open_orders)
        delta_minus = self.risk_state[coin]['netDelta'] + sum(self.margin.open_orders[symbol]['shorts']
                                                             for symbol in self.margin.open_orders
                                                              if self.markets[symbol]['base']==coin
                                                             and symbol in self.margin.open_orders)
        total_delta_plus = sum(data['delta'] + self.margin.open_orders[symbol]['longs'] if 'longs' in self.margin.open_orders[symbol] else 0
                               for coin,coin_data in self.risk_state.items() if coin in self.currencies
                               for symbol,data in coin_data.items() if symbol in self.markets)
        total_delta_minus = sum(data['delta'] + self.margin.open_orders[symbol]['shorts'] if 'shorts' in self.margin.open_orders[symbol] else 0
                               for coin,coin_data in self.risk_state.items() if coin in self.currencies
                               for symbol,data in coin_data.items() if symbol in self.markets)
        global_delta_plus = delta_plus + self.parameters['global_beta'] * (total_delta_plus - delta_plus)
        global_delta_minus = delta_minus + self.parameters['global_beta'] * (total_delta_minus - delta_minus)

        globalDelta = self.risk_state[coin]['netDelta'] + self.parameters['global_beta'] * (self.total_delta - self.risk_state[coin]['netDelta'])
        marginal_risk = np.abs(globalDelta/mid + original_size)-np.abs(globalDelta/mid)
        delta_limit = self.limit.delta_limit * self.pv

        # if increases risk but not out of limit, trim and go passive.
        if marginal_risk>0:
            # if (global_delta_plus / mid + original_size) > delta_limit:
            #     trimmed_size = delta_limit - global_delta_plus / mid
            #     self.myLogger.logger.debug(
            #         f'{original_size * mid} {symbol} would increase risk over {self.limit.delta_limit * 100}% of {self.pv} --> trimming to {trimmed_size * mid}')
            # elif (global_delta_minus / mid + original_size) < -delta_limit:
            #     trimmed_size = -delta_limit - global_delta_minus / mid
            #     self.myLogger.logger.debug(
            #         f'{original_size * mid} {symbol} would increase risk over {self.limit.delta_limit * 100}% of {self.pv} --> trimming to {trimmed_size * mid}')
            # else:
            #     trimmed_size = original_size
            # if np.sign(trimmed_size) != np.sign(original_size):
            #     self.myLogger.logger.debug(f'skipping (we don t flip orders)')
            #     return
            if np.abs(globalDelta / mid + original_size) > delta_limit:
                if (globalDelta / mid + original_size) > delta_limit:
                    trimmed_size = delta_limit - globalDelta / mid
                elif (globalDelta / mid + original_size) < -delta_limit:
                    trimmed_size = -delta_limit - globalDelta / mid
                else:
                    raise Exception('what??')
                if np.sign(trimmed_size) != np.sign(original_size):
                    self.myLogger.logger.debug(f'{original_size*mid} {symbol} would increase risk over {self.limit.delta_limit * 100}% of {self.pv} --> skipping (we don t flip orders)')
                    return
                else:
                    self.myLogger.logger.debug(f'{original_size*mid} {symbol} would increase risk over {self.limit.delta_limit * 100}% of {self.pv} --> trimming to {trimmed_size*mid}')
            else:
                trimmed_size = original_size

            size = np.clip(trimmed_size, a_min=-params['slice_size'],a_max=params['slice_size'])

            current_basket_price = sum(self.mid(_symbol) * self.exec_parameters[coin][_symbol]['update_time_delta']
                                       for _symbol in self.exec_parameters[coin].keys() if _symbol in self.markets)
            # mkt order if target reached.
            # TODO: pray for the other coin to hit the same condition...
            if current_basket_price + 2 * np.abs(params['update_time_delta']) * params['takerVsMakerFee'] * mid < self.exec_parameters[coin]['rush_in_level']:
                # TODO: could replace current_basket_price by two way sweep after if
                self.peg_or_stopout(symbol, size, orderbook,
                                    edit_trigger_depth=params['edit_trigger_depth'],
                                    edit_price_depth='rush_in', stop_depth=None)
                self.myLogger.logger.warning(f'rushing into {coin}')
                return
            elif current_basket_price - 2 * np.abs(params['update_time_delta']) * params['takerVsMakerFee'] * mid > self.exec_parameters[coin]['rush_out_level']:
                # go all in as this decreases margin
                size = - self.risk_state[coin][symbol]['delta'] / mid
                self.peg_or_stopout(symbol, size, orderbook,
                                    edit_trigger_depth=params['edit_trigger_depth'],
                                    edit_price_depth='rush_out', stop_depth=None)
                self.myLogger.logger.warning(f'rushing out of {coin}')
                return
            # limit order if level is acceptable (saves margin compared to faraway order)
            elif current_basket_price < self.exec_parameters[coin]['entry_level']:
                edit_price_depth = params['edit_price_depth']
                stop_depth = None
            # hold off to save margin, if level is bad-ish
            else:
                return
        # if decrease risk, go aggressive
        else:
            size = original_size
            edit_price_depth = params['aggressive_edit_price_depth']
            stop_depth = params['stop_depth']
        self.peg_or_stopout(symbol,size,orderbook,edit_trigger_depth=params['edit_trigger_depth'],edit_price_depth=edit_price_depth,stop_depth=stop_depth)

    def peg_or_stopout(self,symbol,size,orderbook,edit_trigger_depth,edit_price_depth,stop_depth=None):
        '''places an order after checking OMS + margin
        creates or edits orders, pegging to orderbook
        goes taker when edit_price_depth is str
        size in coin, already filtered
        skips if any pending_new, cancels duplicates
        '''
        if abs(size) == 0:
            return
        coin = self.markets[symbol]['base']

        #TODO: https://help.ftx.com/hc/en-us/articles/360052595091-Ratelimits-on-FTX
        opposite_side = self.tickers[symbol]['ask' if size>0 else 'bid']
        mid = self.tickers[symbol]['mid']

        priceIncrement = self.exec_parameters[coin][symbol]['priceIncrement']
        sizeIncrement = self.exec_parameters[coin][symbol]['sizeIncrement']

        if stop_depth is not None:
            stop_trigger = float(self.price_to_precision(symbol,stop_depth))
        edit_trigger = float(self.price_to_precision(symbol,edit_trigger_depth))
        #TODO: use orderbook to place before cliff; volume matters too.
        if edit_price_depth not in ['rush_in', 'rush_out']:
            edit_price = float(self.price_to_precision(symbol, opposite_side - (1 if size > 0 else -1) * edit_price_depth))
        else:
            edit_price = sweep_price_atomic(orderbook, size * mid)

        # remove open order dupes is any (shouldn't happen)
        event_histories = self.filter_order_histories([symbol],myFtx.openStates)
        if len(event_histories) > 1:
            first_pending_new = np.argmin(np.array([data[0]['timestamp'] for data in event_histories]))
            for i,event_history in enumerate(self.filter_order_histories([symbol],myFtx.cancelableStates)):
                if i != first_pending_new:
                    asyncio.create_task(self.cancel_order(event_history[-1]['clientOrderId'],'duplicates'))
                    self.myLogger.logger.info('canceled duplicate {} order {}'.format(symbol,event_history[-1]['clientOrderId']))

        # skip if there is inflight on the spread
        # if self.pending_new_histories(coin) != []:#TODO: rather incorporate orders_pending_new in risk, rather than block
        #     if self.pending_new_histories(coin,symbol) != []:
        #         self.myLogger.logger.info('orders {} should not be in flight'.format([order['clientOrderId'] for order in self.pending_new_histories(coin,symbol)[-1]]))
        #     else:
        #         # this happens mostly between pending_new and create_order on the other leg. not a big deal...
        #         self.myLogger.logger.info('orders {} still in flight. holding off {}'.format(
        #             [order['clientOrderId'] for order in self.pending_new_histories(coin)[-1]],symbol))
        #     return
        pending_new_histories = self.filter_order_histories([_symbol
                                                             for _symbol in self.exec_parameters[coin]
                                                             if _symbol in self.markets],
                                                            ['pending_new'])
        if pending_new_histories != []:
            self.myLogger.logger.info('orders {} should not be in flight'.format([order[-1]['clientOrderId'] for order in pending_new_histories]))
            return

        # if no open order, create an order
        order_side = 'buy' if size>0 else 'sell'
        if len(event_histories)==0:
            trimmed_size = self.trim_to_margin(mid, size, symbol)
            asyncio.create_task(self.create_order(symbol, 'limit', order_side, abs(trimmed_size), price=edit_price,
                                                  params={'postOnly': edit_price_depth not in ['rush_in', 'rush_out'],
                                                          'ioc': edit_price_depth in ['rush_in', 'rush_out'],
                                                          'comment':edit_price_depth if edit_price_depth in ['rush_in','rush_out'] else 'new'}))
        # if only one and it's editable, stopout or peg or wait
        elif len(event_histories)==1 \
                and (self.latest_value(event_histories[0][-1]['clientOrderId'],'remaining') >= sizeIncrement) \
                and event_histories[0][-1]['lifecycle_state'] in self.acknowledgedStates:
            order = event_histories[0][-1]
            order_distance = (1 if order['side'] == 'buy' else -1) * (opposite_side - order['price'])
            repeg_gap = (1 if order['side'] == 'buy' else -1) * (edit_price - order['price'])

            # panic stop. we could rather place a trailing stop: more robust to latency, but less generic.
            if (stop_depth and order_distance > stop_trigger) \
                    or edit_price_depth in ['rush_in','rush_out']:
                size = self.latest_value(order['clientOrderId'],'remaining')
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
        amount = self.round_to_increment(self.exec_parameters[self.markets[symbol]['base']][symbol]['sizeIncrement'],amount)
        if amount == 0:
            return
        # set pending_new -> send rest -> if success, leave pending_new and give id. Pls note it may have been caught by handle_order by then.
        clientOrderId = self.lifecycle_pending_new({'symbol': symbol,
                                                    'type': type,
                                                    'side': side,
                                                    'amount': amount,
                                                    'remaining': amount,
                                                    'price': price,
                                                    'comment': params['comment']})
        try:
            # REST request
            order = await super().create_order(symbol, type, side, amount, price, params | {'clientOrderId':clientOrderId})
        except Exception as e:
            order = {'clientOrderId':clientOrderId,
                     'timestamp':datetime.utcnow().replace(tzinfo=timezone.utc).timestamp() * 1000,
                     'lifecycle_state':'rejected',
                     'comment':'create/'+str(e)}
            self.lifecycle_cancel_or_reject(order)
            if isinstance(e,ccxt.InsufficientFunds):
                self.myLogger.logger.info(f'{clientOrderId} too big: {amount*self.mid(symbol)}')
            elif isinstance(e,ccxt.RateLimitExceeded):
                throttle = 200.0
                self.myLogger.logger.info(f'{str(e)}: waiting {throttle} ms)')
                await asyncio.sleep(throttle / 1000)
            else:
                raise e
        else:
            self.lifecycle_sent(order)

    async def cancel_order(self, clientOrderId, trigger):
        '''set in flight, send cancel, set as pending cancel, set as canceled or insist'''
        symbol = clientOrderId.split('_')[1]
        self.lifecycle_pending_cancel({'comment':trigger}
                                      |{key: [order[key] for order in self.orders_lifecycle[clientOrderId] if key in order][-1]
                                        for key in ['clientOrderId','symbol','side','amount','remaining','price']})  # may be needed

        try:
            status = await super().cancel_order(None,params={'clientOrderId':clientOrderId})
            self.lifecycle_cancel_sent({'clientOrderId':clientOrderId,
                                        'symbol':symbol,
                                        'status':status,
                                        'comment':trigger})
            return True
        except ccxt.CancelPending as e:
            self.lifecycle_cancel_sent({'clientOrderId': clientOrderId,
                                        'symbol': symbol,
                                        'status': str(e),
                                        'comment': trigger})
            return True
        except ccxt.InvalidOrder as e: # could be in flight, or unknown
            self.lifecycle_cancel_or_reject({'clientOrderId':clientOrderId,
                                             'status':str(e),
                                             'lifecycle_state':'canceled',
                                             'comment':trigger})
            return False
        except Exception as e:
            self.myLogger.logger.info(f'{clientOrderId} failed to cancel: {str(e)}')
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

async def open_exec_exchange(exchange_name,subaccount,config,logger):
    '''TODO unused for now'''
    if exchange_name not in ['ftx']:
        raise Exception(f'exchange {exchange_name} not implemented')
    else:
        exchange = myFtx(config, logger, config={  ## David personnal
            'enableRateLimit': True,
            'apiKey': 'ZUWyqADqpXYFBjzzCQeUTSsxBZaMHeufPFgWYgQU',
            'secret': api_params['ftx']['key'],
            'newUpdates': True})
        exchange.verbose = False
        exchange.headers = {'FTX-SUBACCOUNT': subaccount}
        exchange.authenticate()
        await exchange.load_markets()

    return exchange

async def get_exec_request(*argv,subaccount):
    '''TODO unused for now'''
    exchange = await open_exec_exchange(subaccount)

    if argv[0] == 'sysperp':
        future_weights = configLoader.get_current_weights()
        target_portfolio = await diff_portoflio(exchange, future_weights)  # still a Dataframe

    elif argv[0] == 'spread':
        coin = argv[3]
        cash_name = coin + '/USD'
        future_name = coin + '-PERP'
        cash_price = float(exchange.market(cash_name)['info']['price'])
        future_price = float(exchange.market(future_name)['info']['price'])
        target_portfolio = pd.DataFrame(columns=['coin', 'name', 'optimalCoin', 'currentCoin', 'spot_price'], data=[
            [coin, cash_name, float(argv[4]) / cash_price, 0, cash_price],
            [coin, future_name, -float(argv[4]) / future_price, 0, future_price]])

    elif argv[0] == 'flatten':  # only works for basket with 2 symbols
        future_weights = pd.DataFrame(columns=['name', 'optimalWeight'])
        diff = await diff_portoflio(exchange, future_weights)
        smallest_risk = diff.groupby(by='coin')['currentCoin'].agg(
            lambda series: series.apply(np.abs).min() if series.shape[0] > 1 else 0)
        target_portfolio = diff
        target_portfolio['optimalCoin'] = diff.apply(lambda f: smallest_risk[f['coin']] * np.sign(f['currentCoin']),
                                                     axis=1)

    elif argv[0] == 'unwind':
        future_weights = pd.DataFrame(columns=['name', 'optimalWeight'])
        target_portfolio = await diff_portoflio(exchange, future_weights)

    else:
        exchange.myLogger.exception(f'unknown command {argv[0]}', exc_info=True)
        raise Exception(f'unknown command {argv[0]}', exc_info=True)

    await exchange.close()

    return target_portfolio

async def ftx_ws_spread_main_wrapper(order,config_name,logger,**kwargs):
    try:
        # open exchange
        config = configLoader.get_executor_params(order=order,dirname=config_name)
        if order not in ['unwind','flatten']:
            future_weights = configLoader.get_current_weights(filename=order,dirname=config_name)
            exchange_name = future_weights['exchange'].unique()[0]
            subaccount = future_weights['subaccount'].unique()[0]
        else:
            if 'exchange' not in kwargs: raise Exception('exchange compulsory')
            else: exchange_name = kwargs['exchange']
            if 'subaccount' not in kwargs: raise Exception('subaccount compulsory')
            else: subaccount = kwargs['subaccount']

        exchange = await open_exec_exchange(exchange_name,subaccount,config,logger)

        # build order
        if order not in ['unwind','flatten']:
            target_portfolio = await diff_portoflio(exchange, future_weights)  # still a Dataframe
            if target_portfolio.empty: raise myFtx.NothingToDo()

        elif order=='flatten': # only works for basket with 2 symbols
            diff = await diff_portoflio(exchange)
            smallest_risk = diff.groupby(by='coin')['currentCoin'].agg(lambda series: series.apply(np.abs).min() if series.shape[0]>1 else 0)
            target_portfolio=diff
            target_portfolio['optimalCoin'] = diff.apply(lambda f: smallest_risk[f['coin']]*np.sign(f['currentCoin']),axis=1)
            if target_portfolio.empty: raise myFtx.NothingToDo()

        elif order=='unwind':
            target_portfolio = await diff_portoflio(exchange)
            if target_portfolio.empty: raise myFtx.NothingToDo()

        else:
            exchange.myLogger.logger.exception(f'unknown command {order}',exc_info=True)
            raise Exception(f'unknown command {order}', exc_info=True)

        await exchange.build_state(target_portfolio, config | {'comment': order})   # i

        exchange.myLogger.logger.warning('cancelling running_symbols orders')
        await asyncio.gather(*[exchange.cancel_all_orders(symbol) for symbol in exchange.running_symbols])

        coros = [exchange.monitor_fills(),exchange.monitor_risk()] + \
                sum([[exchange.monitor_orders(symbol),
                      exchange.monitor_order_book(symbol),
                      exchange.monitor_trades(symbol)]
                     for symbol in exchange.running_symbols],[])

        await asyncio.gather(*coros)
    except Exception as e:
        logging.getLogger('tradeexecutor').critical(str(e), exc_info=True)
        try:
            await asyncio.gather(*[exchange.cancel_all_orders(symbol) for symbol in exchange.running_symbols])
            logging.getLogger('tradeexecutor').warning(f'cancelled {exchange.running_symbols} orders')
            # await exchange.close_dust()  # Commenting out until bug fixed
            # await exchange.close()
        except Exception:
            logging.getLogger('tradeexecutor').critical(str(e), exc_info=True)
        await exchange.close()
        raise e

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

    order = args[0]
    config_name = kwargs.pop('config') if 'config' in kwargs else None
    config = configLoader.get_executor_params(order=order,dirname=config_name)
    logger = ExecutionLogger(order,config,log_mapping={logging.INFO: 'exec_info.log', logging.WARNING: 'oms_warning.log', logging.CRITICAL: 'program_flow.log'})
    logger.logger = kwargs.pop('__logger')

    for i in range(int(kwargs.pop('nb_runs')) if 'nb_runs' in kwargs else 1):
        try:
            asyncio.run(ftx_ws_spread_main_wrapper(order,config_name,logger,**kwargs)) # --> I am filled or I timed out and I have flattened position
        except myFtx.DoneDeal as e:
            # Wait for 5 minutes and start over
            if i>0:
                logger.logger.critical(f'{str(e)} --> SLEEPING for 5 minutes...')
                t.sleep(60 * 5)
            if not isinstance(e,myFtx.NothingToDo):
                ExecutionLogger.batch_summarize_exec_logs()

        except myFtx.TimeBudgetExpired as e:
            logger.logger.critical(f'{str(e)} --> FLATTEN UNTIL FINISHED')
            while datetime.utcnow().replace(tzinfo=timezone.utc).minute < 50:
                try:
                    # Force flattens until it returns FILLED, give up 5 mins before funding time
                    if datetime.utcnow().replace(tzinfo=timezone.utc).minute >= 55:
                        logger.logger.critical(f'{str(e)}  --> ouch, too close to hour to flatten')
                        t.sleep(60 * 20)
                    asyncio.run(ftx_ws_spread_main_wrapper('flatten',config,logger,**kwargs))
                except myFtx.DoneDeal as e:
                    logger.logger.critical(f'{str(e)} TIMEOUT --> FLATTEN UNTIL FINISHED')
                    break
