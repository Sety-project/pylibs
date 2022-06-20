import time as t

from utils.io_utils import *
from utils.ftx_utils import *
from utils.config_loader import *
from utils.ccxt_utilities import *
from histfeed.ftx_history import fetch_trades_history
from riskpnl.ftx_portfolio import diff_portoflio, MarginCalculator
from riskpnl.post_trade import log_reader

import ccxtpro

# parameters guide
# 'max_nb_coins': 'sharding'
# 'time_budget': scaling aggressiveness to 0 at time_budget (in seconds)
# 'global_beta': other coin weight in the global risk
# 'max_cache_size': mkt data cache
# 'message_cache_size': missed messages cache
# 'entry_tolerance': green light if basket better than quantile(entry_tolerance)
# 'rush_tolerance': mkt on both legs if basket better than quantile(rush_tolerance)
# 'stdev_window': stdev horizon for scaling parameters. in sec.
# 'edit_price_tolerance': place on edit_price_tolerance (in minutes) *  stdev
# 'aggressive_edit_price_tolerance': in priceIncrement
# 'edit_trigger_tolerance': chase at edit_trigger_tolerance (in minutes) *  stdev
# 'stop_tolerance': stop at stop_tolerance (in minutes) *  stdev
# 'slice_factor': % of mkt volume * edit_price_tolerance. so farther is bigger.
# 'check_frequency': risk recon frequency. in seconds
# 'delta_limit': in % of pv

class CustomRLock(threading._PyRLock):
    @property
    def count(self):
        return self._count

# qd tu ecoutes un channel ws c'est un while true loop
# c'est while symbol est encore running running_symbols
def loop(func):
    @functools.wraps(func)
    async def wrapper_loop(*args, **kwargs):
        self=args[0]
        while len(args)==1 or (args[1] in args[0].running_symbols):
            try:
                value = await func(*args, **kwargs)
            except ccxt.NetworkError as e:
                self.myLogger.warning(str(e))
                self.myLogger.warning('reconciling after '+func.__name__+' dropped off')
                await self.reconcile() # implicitement redemarre la socket, ccxt fait ca comme ca
            except Exception as e:
                self.myLogger.warning(e, exc_info=True)
                raise e
    return wrapper_loop

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
            self.myLogger.warning(f'{self.lock[symbol].count}th race conditions on {symbol} blocking {wrapped.__name__}')
            #await asyncio.sleep(0)
        with self.lock[symbol]:
            try:
                return wrapped(*args, **kwargs)
            except KeyError as e:
                pass
            except Exception as e:
                pass
    return _wrapper

def intercept_message_during_reconciliation(wrapped):
    '''decorates self.watch_xxx(message) to block incoming messages during reconciliation'''
    @functools.wraps(wrapped)
    def _wrapper(*args, **kwargs):
        self=args[0]
        if args[0].lock['reconciling'].locked():
            self.myLogger.info(f'message during reconciliation{args[2]}')
            self.message_missed.append(args[2])
        else:
            return wrapped(*args, **kwargs)
    return _wrapper

class myFtx(ccxtpro.ftx):
    def __init__(self, parameters, config={}):
        super().__init__(config=config)
        self.parameters = parameters
        self.lock = {'reconciling':threading.Lock()}
        self.message_missed = collections.deque(maxlen=parameters['message_cache_size'])
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
        self.usd_balance = None # it's handy for marginal calcs
        self.total_delta = None # in USD
        self.margin_headroom = None
        self.margin_calculator = None
        self.calculated_IM = None

        self.myLogger = logging.getLogger(__name__)

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
        data = self.tickers[symbol] if symbol in self.tickers else self.markets[symbol]['info']
        return 0.5*(float(data['bid'])+float(data['ask']))

    def amount_to_precision(self, symbol, amount):
        market = self.market(symbol)
        return self.decimal_to_precision(amount, ccxt.ROUND, market['precision']['amount'], self.precisionMode, self.paddingMode)

    def build_logging(self):
        '''3 handlers: >=debug, ==info and >=warning'''
        class MyFilter(object):
            '''this is to restrict info logger to info only'''
            def __init__(self, level):
                self.__level = level

            def filter(self, logRecord):
                return logRecord.levelno <= self.__level

        # mkdir log repos if does not exist
        log_path = os.path.join(os.sep, "tmp", "tradeexecutor")
        if not os.path.exists(log_path):
            os.umask(0)
            os.makedirs(log_path, mode=0o777)

        # logs
        handler_warning = logging.FileHandler(os.path.join(log_path, 'oms_warning.log'), mode='w')
        handler_warning.setLevel(logging.WARNING)
        handler_warning.setFormatter(logging.Formatter(f"%(levelname)s: %(message)s"))
        self.myLogger.addHandler(handler_warning)

        handler_info = logging.FileHandler(os.path.join(log_path, 'exec_info.log'), mode='w')
        handler_info.setLevel(logging.INFO)
        handler_info.setFormatter(logging.Formatter(f"%(levelname)s: %(message)s"))
        handler_info.addFilter(MyFilter(logging.INFO))
        self.myLogger.addHandler(handler_info)

        # handler_alert = logging.handlers.SMTPHandler(mailhost='smtp.google.com',
        #                                              fromaddr='david@pronoia.link',
        #                                              toaddrs=['david@pronoia.link'],
        #                                              subject='auto alert',
        #                                              credentials=('david@pronoia.link', ''),
        #                                              secure=None)
        # handler_alert.setLevel(logging.CRITICAL)
        # handler_alert.setFormatter(logging.Formatter(f"%(levelname)s: %(message)s"))
        # self.myLogger.addHandler(handler_alert)

        self.myLogger.setLevel(logging.DEBUG)

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

    # ce sont tous les new orders qui sont pas sent, pas ack. very early stage
    def pending_new_histories(self, coin,symbol=None):
        '''returns all blockchains for risk group, which current state open_order_histories not filled or canceled'''
        if symbol:
            return [data
                    for clientID,data in self.orders_lifecycle.items()
                    if data[0]['symbol'] == symbol
                    and data[-1]['lifecycle_state'] == 'pending_new']
        else:
            return [data
                    for clientID,data in self.orders_lifecycle.items()
                    if data[0]['symbol'] in self.exec_parameters[coin].keys()
                    and data[-1]['lifecycle_state'] == 'pending_new']

    def filter_order_histories(self,symbol=None,state_set=None):
        '''returns all blockchains for symbol (all symbols if None), in a set of current states'''
        return [data
                for data in self.orders_lifecycle.values()
                if (data[0]['symbol'] == symbol or symbol is None)
                and ((data[-1]['lifecycle_state'] in state_set) or state_set is None)]

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
        nowtime = datetime.now().timestamp() * 1000
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
                    'margin_headroom':self.margin_headroom,
                    'IM_discrepancy':self.calculated_IM - self.margin_headroom}

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
        self.orders_lifecycle[clientOrderId] = [current]   # c'est l'order book

        return clientOrderId

    @symbol_locked
    def lifecycle_sent(self, order_event):
        '''order_event:clientOrderId,timestamp,remaining,status'''
        # 1) resolve clientID
        clientOrderId = order_event['clientOrderId']

        # 2) validate block
        past = self.orders_lifecycle[clientOrderId][-1]
        if past['lifecycle_state'] not in ['pending_new','acknowledged','partially_filled']:
            self.myLogger.warning('order {} was {} before sent'.format(past['clientOrderId'],past['lifecycle_state']))
            return

        # 3) new block
        nowtime = datetime.now().timestamp() * 1000
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
                self.lifecycle_fill(current)
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
            self.myLogger.warning('order {} re-canceled'.format(past['clientOrderId']))
            return

        # 3) new block
        nowtime = datetime.now().timestamp() * 1000
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
            self.myLogger.warning('order {} re-canceled'.format(past['clientOrderId']))
            return

        # 3) new block
        nowtime = datetime.now().timestamp() * 1000
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
        nowtime = datetime.now().timestamp()*1000
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

    @symbol_locked
    def lifecycle_fill(self,order_event):
        '''order_event: id, trigger,timestamp,amount,order,symbol'''
        # 1) resolve clientID
        clientOrderId = self.find_clientID_from_fill(order_event)
        symbol = order_event['symbol']
        coin = self.markets[symbol]['base']
        size_threshold = self.exec_parameters[coin][symbol]['sizeIncrement'] / 2

        nowtime = datetime.now().timestamp()*1000
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
        nowtime = datetime.now().timestamp() * 1000
        eventID = clientOrderId + '_' + str(int(nowtime))
        current = order_event | {'eventID':eventID,
                                 'lifecycle_state': order_event['lifecycle_state'],
                                 'remote_timestamp': order_event['timestamp'] if 'timestamp' in order_event else None}
        current['timestamp'] = nowtime

        # 4) mine
        self.orders_lifecycle[clientOrderId] += [current]

    async def lifecycle_to_json(self,filename = os.path.join(os.sep, "tmp", "tradeexecutor", 'latest_events.json')):
        async with aiofiles.open(filename,mode='w') as file:
            await file.write(json.dumps(self.orders_lifecycle, cls=NpEncoder))

        # File copy
        # Mkdir log repos if does not exist
        log_path = os.path.join(os.sep, 'tmp', 'tradeexecutor', 'archive')
        if not os.path.exists(log_path):
            os.umask(0)
            os.makedirs(log_path, mode=0o777)
        shutil.copy2(filename, os.path.join(log_path,
                                            datetime.utcfromtimestamp(self.exec_parameters['timestamp'] / 1000).replace(
                                                tzinfo=timezone.utc).strftime("%Y%m%d_%H%M%S") + '_events.json'))

    async def risk_reconciliation_to_json(self,filename = os.path.join(os.sep, "tmp", "tradeexecutor", 'latest_risk_reconciliations.json')):
        async with aiofiles.open(filename,mode='w') as file:
            await file.write(json.dumps(self.risk_reconciliations, cls=NpEncoder))

        # File copy
        # Mkdir log repos if does not exist
        log_path = os.path.join(os.sep, 'tmp', 'tradeexecutor', 'archive')
        if not os.path.exists(log_path):
            os.umask(0)
            os.makedirs(log_path, mode=0o777)
        shutil.copy2(filename, os.path.join(log_path, datetime.utcfromtimestamp(self.exec_parameters['timestamp']/1000).replace(tzinfo=timezone.utc).strftime("%Y%m%d_%H%M%S")+'_risk_reconciliations.json'))

    #@synchronized
    async def reconcile_fills(self):
        '''fetch fills, to recover missed messages'''
        sincetime = max(self.exec_parameters['timestamp'],self.latest_fill_reconcile_timestamp-1000)
        fetched_fills = await self.fetch_my_trades(since=sincetime)
        for fill in fetched_fills:
            fill['comment'] = 'reconciled'
            fill['clientOrderId'] = self.find_clientID_from_fill(fill)
            if fill['id'] not in [block['fillId']
                                     for block in self.orders_lifecycle[fill['clientOrderId']]
                                     if 'fillId' in block]:
                self.lifecycle_fill(fill)
        self.latest_fill_reconcile_timestamp = datetime.now(timezone.utc).timestamp()*1000
        await self.lifecycle_to_json()

    async def reconcile_orders(self):
        self.latest_order_reconcile_timestamp = datetime.now(timezone.utc).timestamp() * 1000

        # list internal orders
        internal_order_internal_status = {clientOrderId:data[-1] for clientOrderId,data in self.orders_lifecycle.items()
                         if data[-1]['lifecycle_state'] in myFtx.openStates}

        # add missing orders (we missed orders from the exchange)
        external_orders = await self.fetch_open_orders()
        for order in external_orders:
            if order['clientOrderId'] not in internal_order_internal_status.keys():
                self.lifecycle_acknowledgment(order | {'comment':'reconciled_missing'})
                self.myLogger.warning('{} was missing'.format(order['clientOrderId']))
                found = order['clientOrderId']
                assert (found in self.orders_lifecycle), f'{found} unknown'

        # remove zombie orders (we beleive they are live but they are not open)
        # should not happen so we put it in the logs
        internal_order_external_status = await safe_gather([self.fetch_order(id=None, params={'clientOrderId':clientOrderId})
                                                   for clientOrderId in internal_order_internal_status.keys()],
                                                           semaphore=self.rest_semaphor,
                                                           return_exceptions=True)
        for clientOrderId,external_status in zip(internal_order_internal_status.keys(),internal_order_external_status):
            if isinstance(external_status, ccxt.OrderNotFound):
                self.myLogger.warning(f'zombie {clientOrderId} not found')
                continue
            if external_status['status'] != 'open':
                self.lifecycle_cancel_or_reject(external_status | {'lifecycle_state': 'canceled', 'comment':'reconciled_zombie'})
                self.myLogger.warning('{} was a {} zombie'.format(clientOrderId,internal_order_internal_status[clientOrderId]['lifecycle_state']))

        await self.lifecycle_to_json()

    # ---------------------------------------------------------------------------------------------
    # ---------------------------------- PMS -----------------------------------------
    # ---------------------------------------------------------------------------------------------

    # le rush c'est pour avoir un meilleur niveau d'entree

    async def build_state(self, weights,parameters): # cut in 10):
        '''initialize all state and does some filtering (weeds out slow underlyings; should be in strategy)
            target_sub_portfolios = {coin:{rush_level_increment,
            symbol1:{'spot_price','diff','target'}]}]'''
        self.build_logging()

        self.limit.delta_limit = self.parameters['delta_limit']

        frequency = timedelta(minutes=1)
        end = datetime.now(timezone.utc)
        start = end - timedelta(seconds=self.parameters['stdev_window'])

        trades_history_list = await safe_gather([fetch_trades_history(
            self.market(symbol)['id'], self, start, end, frequency=frequency)
            for symbol in weights['name']],semaphore=self.rest_semaphor)
        trading_fees = await self.fetch_trading_fees()

        weights['diffCoin'] = weights['optimalCoin'] - weights['currentCoin']
        weights['diffUSD'] = weights['diffCoin'] * weights['spot_price']
        weights['name'] = weights['name'].apply(lambda s: self.market(s)['symbol'])
        weights.set_index('name', inplace=True)
        coin_list = weights['coin'].unique()

        # {coin:{symbol1:{data1,data2...},sumbol2:...}}
        full_dict = {coin:
                         {data['symbol']:
                              {'diff': weights.loc[data['symbol'], 'diffCoin'],
                               'target': weights.loc[data['symbol'], 'optimalCoin'],
                               'spot_price': weights.loc[data['symbol'], 'spot_price'],
                               'volume': data['volume'].mean().squeeze() / frequency.total_seconds(),# in coin per second
                               'series': data['vwap']}
                          for data in trades_history_list if data['coin']==coin}
                     for coin in coin_list}

        # exclude coins too slow or symbol diff too small, limit to max_nb_coins
        diff_threshold = sorted(
            [max([np.abs(weights.loc[data['symbol'], 'diffUSD'])
                  for data in trades_history_list if data['coin']==coin])
             for coin in coin_list])[-min(self.parameters['max_nb_coins'],len(coin_list))]

        data_dict = {coin: coin_data
                     for coin, coin_data in full_dict.items() if
                     all(data['volume'] * self.parameters['time_budget'] > np.abs(data['diff']) for data in coin_data.values())
                     and any(np.abs(data['diff']) >= max(diff_threshold/data['spot_price'], float(self.markets[symbol]['info']['minProvideSize'])) for symbol, data in coin_data.items())}
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
                                             sys.intern('spot'): self.mid(symbol),
                                             sys.intern('history'): collections.deque(maxlen=self.parameters['max_cache_size'])   #time series builder dans le timesocket mais vide au debut
                                         }
                                         for symbol, data in coin_data.items()}
                                 for coin, coin_data in data_dict.items()}

        self.lock |= {symbol: CustomRLock() for symbol in self.running_symbols}

        # initialize the risk of the system <=> of what is running. What is the risk that we think we have
        self.risk_state = {sys.intern(coin):
                               {sys.intern('netDelta'):0}
                               | {sys.intern(symbol):
                                   {
                                       sys.intern('delta'): 0,
                                       sys.intern('delta_timestamp'):end.timestamp()*1000,
                                       sys.intern('delta_id'): None
                                   }
                                   for symbol, data in coin_data.items()}
                           for coin, coin_data in data_dict.items()}

        # compute IM
        # Initializes a margin calculator that can understand the margin of an order
        # reconcile the margin of an exchange with the margin we calculate
        self.margin_calculator = await MarginCalculator.build_margin_calculator(self)

        # populates risk, pv and IM
        self.latest_order_reconcile_timestamp = self.exec_parameters['timestamp']
        self.latest_fill_reconcile_timestamp = self.exec_parameters['timestamp']
        self.latest_exec_parameters_reconcile_timestamp = self.exec_parameters['timestamp']
        await self.reconcile()

        # Dumps the order that it received from current_weight.xlsx. only logs
        with open(os.path.join(os.sep, "tmp", "tradeexecutor", 'latest_request.json'), 'w') as file:
            json.dump({symbol:data
                       for coin,coin_data in self.exec_parameters.items() if coin in self.currencies
                       for symbol,data in coin_data.items() if symbol in self.markets}
                      | {'parameters': {'inception_time':self.exec_parameters['timestamp']} | self.parameters},
                      file,
                      cls=NpEncoder)
        shutil.copy2(os.path.join(os.sep, "tmp", "tradeexecutor", 'latest_request.json'),
                     os.path.join(os.sep, "tmp", "tradeexecutor",'archive',
                                  datetime.utcfromtimestamp(self.exec_parameters['timestamp'] / 1000).replace(
                                      tzinfo=timezone.utc).strftime(
                                      "%Y%m%d_%H%M%S") + '_request.json'))

    async def update_exec_parameters(self): # cut in 10):   # le process minutely fait ca en gros
        '''scales order placement params with time'''
        frequency = timedelta(minutes=1)
        end = datetime.now(timezone.utc)
        start = end - timedelta(seconds=self.parameters['stdev_window'])

        # TODO: rather use cached history except for first run
        #if any(len(self.exec_parameters[self.markets[symbol]['base']][symbol]['history']) < 100 for symbol in self.running_symbols):
        trades_history_list = await safe_gather([fetch_trades_history(
            self.market(symbol)['id'], self, start, end, frequency=frequency)
            for symbol in self.running_symbols],semaphore=self.rest_semaphor)
        trades_history_dict = dict(zip(self.running_symbols, trades_history_list))

        coin_list = set([self.markets[symbol]['base'] for symbol in self.running_symbols])
        trades_history_dict = {coin:
                                   {symbol: trades_history_dict[symbol]
                                    for symbol in self.running_symbols
                                    if self.markets[symbol]['base'] == coin}
                               for coin in coin_list}

        # get times series of target baskets, compute quantile of increments and add to last price
        nowtime = datetime.now().timestamp() * 1000
        time_limit = self.exec_parameters['timestamp'] + self.parameters['time_budget']*1000

        if nowtime > time_limit:
            self.myLogger.info('time budget expired')
            raise myFtx.TimeBudgetExpired('')

        def basket_vwap_quantile(series_list, diff_list, quantile):
            series = pd.concat([serie * coef for serie, coef in zip(series_list, diff_list)], join='inner', axis=1)
            return series.sum(axis=1).quantile(quantile)

        scaler = max(0,(time_limit-nowtime)/(time_limit-self.exec_parameters['timestamp']))
        for coin,coin_data in trades_history_dict.items():
            # entry_level = what level on basket is too bad to even place orders
            self.exec_parameters[coin]['entry_level'] = basket_vwap_quantile(
                [data['vwap'] for data in coin_data.values()],
                [self.exec_parameters[coin][symbol]['diff'] for symbol in coin_data.keys()],
                self.parameters['entry_tolerance'])
            # rush_level = what level on basket is so good good that you go market on both legs
            self.exec_parameters[coin]['rush_level'] = basket_vwap_quantile(
                [data['vwap'] for data in coin_data.values()],
                [self.exec_parameters[coin][symbol]['diff'] for symbol in coin_data.keys()],
                self.parameters['rush_tolerance'])
            for symbol, data in coin_data.items():
                stdev = data['vwap'].std().squeeze()
                if not (stdev>0): stdev = 1e-16
                # edit_price_depth = how far to put limit on risk increasing orders
                self.exec_parameters[coin][symbol]['edit_price_depth'] = stdev * np.sqrt(self.parameters['edit_price_tolerance']) * scaler
                # aggressive_edit_price_depth = how far to put limit on risk reducing orders
                self.exec_parameters[coin][symbol]['aggressive_edit_price_depth'] = self.parameters[
                                                                                        'aggressive_edit_price_tolerance'] * \
                                                                                    self.exec_parameters[coin][symbol][
                                                                                        'priceIncrement']
                # edit_trigger_depth = how far to let the mkt go before re-pegging limit orders
                self.exec_parameters[coin][symbol]['edit_trigger_depth'] = stdev * np.sqrt(
                    self.parameters['edit_trigger_tolerance']) * scaler
                # stop_depth = how far to set the stop on risk reducing orders
                self.exec_parameters[coin][symbol]['stop_depth'] = stdev * np.sqrt(self.parameters['stop_tolerance']) * scaler
                # slice_size: cap IM impact to:
                # - an equal share of margin headroom
                # - an expected time to trade consistent with edit_price_tolerance
                # - risk
                minProvideSize = self.exec_parameters[coin][symbol]['sizeIncrement']
                incremental_margin_usd = self.margin_calculator.margin_cost(symbol,
                                                          self.mid(symbol),
                                                          self.exec_parameters[coin][symbol]['diff'],
                                                          self.usd_balance)
                margin_share = self.margin_headroom * np.abs(self.exec_parameters[coin][symbol]['diff']/ incremental_margin_usd ) / len(self.running_symbols)
                volume_share = self.parameters['slice_factor'] * self.parameters['edit_price_tolerance'] * \
                               data['volume'].mean()
                self.exec_parameters[coin][symbol]['slice_size'] = max(
                    [minProvideSize, min([margin_share, volume_share])])

        self.latest_exec_parameters_reconcile_timestamp = nowtime
        self.myLogger.info(f'scaled params by {scaler} at {nowtime}')

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

            #TODO: apply the gather trick
            risks = await safe_gather([self.fetch_positions(),self.fetch_balance(),self.fetch_markets()],semaphore=self.rest_semaphor)
            positions = risks[0]
            balances = risks[1]
            markets = risks[2]
            risk_timestamp = datetime.now().timestamp() * 1000

            # delta is noisy for perps, so override to delta 1.
            self.pv = 0
            for coin, balance in balances.items():
                if coin in self.currencies.keys() and coin != 'USD' and balance['total']!=0:
                    symbol = coin+'/USD'
                    mid = next(0.5*(float(market['info']['bid'])+float(market['info']['ask']))
                               for market in markets if market['symbol'] == symbol)
                    delta = balance['total'] * mid
                    if coin in self.risk_state and symbol in self.risk_state[coin]:
                        self.risk_state[coin][symbol]['delta'] = delta
                        self.risk_state[coin][symbol]['delta_timestamp'] = risk_timestamp
                    self.pv += delta

            self.total_delta = self.pv
            self.usd_balance = balances['USD']['total']
            self.pv += self.usd_balance #doesn't contribute to delta, only pv !

            for position in positions:
                if float(position['info']['size'])!=0:
                    symbol = position['symbol']
                    coin = self.markets[symbol]['base']
                    if self.markets[symbol]['type'] == 'future':
                        delta  = position['notional']*(1 if position['side'] == 'long' else -1)
                    else:
                        delta = float(position['info']['size']) * self.mid(symbol)*(1 if position['side'] == 'long' else -1)

                    if coin in self.risk_state and symbol in self.risk_state[coin]:
                        self.risk_state[coin][symbol]['delta'] = delta
                        self.risk_state[coin][symbol]['delta_timestamp']=risk_timestamp
                    self.total_delta += delta

            for coin,coin_data in self.risk_state.items():
                coin_data['netDelta']= sum([data['delta']
                                            for symbol,data in coin_data.items()
                                            if symbol in self.markets and 'delta' in data.keys()])

            #compute IM
            await self.update_margin_data(balances, positions)

            # update_exec_parameters
            await self.update_exec_parameters()

            # replay missed _messages.
            self.myLogger.info(f'replaying {len(self.message_missed)} messages after recon')
            while self.message_missed:
                message = self.message_missed.popleft()
                data = self.safe_value(message, 'data')
                channel = message['channel']
                if channel == 'fills':
                    fill = self.parse_trade(data)
                    self.process_fill(fill | {'orderTrigger':'replayed'})
                elif channel == 'orders':
                    order = self.parse_order(data)
                    self.process_order(order | {'orderTrigger':'replayed'})
                # we record the orderbook but don't launch quoter (don't want to react after the fact)
                elif channel == 'orderbook':
                    pass
                    #orderbook = self.parse_order_book(data,symbol,data['time'])
                    #self.process_order_book(symbol, orderbook | {'orderTrigger':'replayed'})

        # critical job is done, release lock
        # log risk
        self.risk_reconciliations += [{'lifecycle_state': 'remote_risk', 'symbol':symbol_, 'delta_timestamp':self.risk_state[coin][symbol_]['delta_timestamp'],
                                       'delta':self.risk_state[coin][symbol_]['delta'],'netDelta':self.risk_state[coin]['netDelta'],
                                       'pv':self.pv,'calculated_IM':self.calculated_IM,'margin_headroom':self.margin_headroom,
                                       'pv error': self.pv-(previous_pv or 0),'total_delta error': self.total_delta-(previous_total_delta or 0)}
                                      for coin,coindata in self.risk_state.items()
                                      for symbol_ in coindata.keys() if symbol_ in self.markets]
        await self.risk_reconciliation_to_json()

    async def update_margin_data(self, balances, positions):
        spot_weight = {}
        future_weight = {}
        for position in positions:
            if float(position['info']['netSize']) != 0:
                symbol = position['symbol']
                mid = self.mid(symbol)
                future_weight |= {symbol: {'weight': float(position['info']['netSize']) * mid, 'mark': mid}}
        for coin, balance in balances.items():
            if coin != 'USD' and coin in self.currencies and balance['total'] != 0:
                mid = self.mid(coin + '/USD')
                spot_weight |= {(coin): {'weight': balance['total'] * mid, 'mark': mid}}
        (spot_weight, future_weight) = MarginCalculator.add_pending_orders(self, spot_weight, future_weight)

        self.margin_calculator.estimate(self.usd_balance, spot_weight, future_weight)
        self.calculated_IM = sum(value for value in self.margin_calculator.estimated_IM.values())
        # fetch IM
        account_info = (await self.privateGetAccount())['result']
        self.margin_calculator.update_actual(account_info)
        self.margin_headroom = self.margin_calculator.actual_IM if float(
            account_info['totalPositionSize']) else self.pv

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
        coin = self.markets[symbol]['base']
        timestamp = orderbook['timestamp'] * 1000
        mid = 0.5 * (orderbook['bids'][0][0] + orderbook['asks'][0][0])
        self.tickers[symbol] = {'symbol': symbol,
                                'timestamp': timestamp,
                                'bid': orderbook['bids'][0][0],
                                'ask': orderbook['asks'][0][0],
                                'mid': 0.5 * (orderbook['bids'][0][0] + orderbook['asks'][0][0]),
                                'bidVolume': orderbook['bids'][0][1],
                                'askVolume': orderbook['asks'][0][1]}
        self.exec_parameters[coin][symbol]['history'].append([timestamp, mid])

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
        data = self.risk_state[coin][symbol]
        fill_size = fill['amount'] * (1 if fill['side'] == 'buy' else -1) * fill['price']
        data['delta'] += fill_size
        data['delta_timestamp'] = fill['timestamp']
        latest_delta = data['delta_id']
        data['delta_id'] = max(latest_delta or 0, int(fill['order']))
        self.risk_state[coin]['netDelta'] += fill_size
        self.total_delta += fill_size

        # log event
        fill['clientOrderId'] = fill['info']['clientOrderId']
        fill['comment'] = 'websocket_fill'
        self.lifecycle_fill(fill)

        # logger.info
        self.myLogger.info('{} fill at {}: {} {} {} at {}'.format(symbol,
                                                                  fill['timestamp'],
                                                                  fill['side'], fill['amount'], symbol,
                                                                  fill['price']))

        current = self.risk_state[coin][symbol]['delta']
        initial = self.exec_parameters[coin][symbol]['target'] * fill['price'] - \
                  self.exec_parameters[coin][symbol][
                      'diff'] * fill['price']
        target = self.exec_parameters[coin][symbol]['target'] * fill['price']
        self.myLogger.info('{} risk at {} ms: {}% done [current {}, initial {}, target {}]'.format(
            symbol,
            self.risk_state[coin][symbol]['delta_timestamp'],
            (current - initial) / (target - initial) * 100,
            current,
            initial,
            target))

    # ---------------------------------- orders

    @loop
    async def monitor_orders(self):
        '''maintains orders, pending_new, event_records'''
        orders = await self.watch_orders()
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
            self.myLogger.warning(f'absolute_risk {absolute_risk} > {self.pv * self.limit.delta_limit}')
        if self.margin_headroom < self.pv/100:
            self.myLogger.warning(f'IM {self.margin_headroom}  < 1%')

    async def watch_ticker(self, symbol, params={}):
        '''watch_order_book is faster than watch_tickers so we DON'T LISTEN TO TICKERS. Dirty...'''
        raise Exception("watch_order_book is faster than watch_tickers so we DON'T LISTEN TO TICKERS. Dirty...")

    # ---------------------------------- just to record messages while reconciling
    @intercept_message_during_reconciliation
    def handle_my_trade(self, client, message):
        super().handle_my_trade(client, message)

    @intercept_message_during_reconciliation
    def handle_order(self, client, message):
        super().handle_order(client, message)

    #@intercept_message_during_reconciliation
    def handle_order_book_update(self, client, message):
        super().handle_order_book_update(client, message)

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
        if (np.abs(original_size) < self.exec_parameters[coin][symbol]['sizeIncrement']):
            self.running_symbols.remove(symbol)
            self.myLogger.info(f'{symbol} done, {self.running_symbols} left to do')
            if self.running_symbols == []:
                raise myFtx.DoneDeal('all done')
            else: return

        #risk
        delta_timestamp = self.risk_state[coin][symbol]['delta_timestamp']
        globalDelta = self.risk_state[coin]['netDelta'] + self.parameters['global_beta'] * (self.total_delta - self.risk_state[coin]['netDelta'])
        marginal_risk = np.abs(globalDelta/mid + original_size)-np.abs(globalDelta/mid)

        # if increases risk, go passive. logique de comment on place les ordres.
        # tu regardes le prix et le risk et tu decides comment tu places les ordres
        if marginal_risk>0:
            stop_depth = None
            current_basket_price = sum(self.mid(_symbol)*self.exec_parameters[coin][_symbol]['diff']
                                       for _symbol in self.exec_parameters[coin].keys() if _symbol in self.markets)
            # mkt order if target reached.
            if current_basket_price + 2 * np.abs(params['diff']) * params['takerVsMakerFee']< self.exec_parameters[coin]['rush_level']:
                # (np.abs(netDelta+size)-np.abs(netDelta))/np.abs(size)*params['edit_price_depth'] # equate: profit if done ~ marginal risk * stdev
                size = np.sign(original_size) * min([np.abs(original_size), params['slice_size']])
                edit_price_depth = 0
                for _symbol in self.exec_parameters[coin]:
                    if _symbol in self.markets:
                        self.exec_parameters[coin][_symbol]['edit_price_depth'] = 0
            # limit order if level is acceptable
            elif current_basket_price < self.exec_parameters[coin]['entry_level']:
                size = np.sign(original_size) * min([np.abs(original_size), params['slice_size']])
                edit_price_depth = params['edit_price_depth']
            # hold off if level is bad
            else:
                return
        # if decrease risk, go aggressive
        else:
            size = np.sign(original_size) * min([np.abs(original_size), np.abs(globalDelta/mid)])
            edit_price_depth = params['aggressive_edit_price_depth']
            stop_depth = params['stop_depth']
        self.peg_or_stopout(symbol,size,orderbook,edit_trigger_depth=params['edit_trigger_depth'],edit_price_depth=edit_price_depth,stop_depth=stop_depth)

    def peg_or_stopout(self,symbol,size,orderbook,edit_trigger_depth,edit_price_depth,stop_depth=None):
        # il a tous les niveaux qu'il faut faire
        '''places an order after checking OMS
        creates or edits orders, pegging to orderbook
        goes taker when depth<increment
        size in coin, already filtered
        skips if any pending_new, cancels duplicates
        extensive exception handling
        '''
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
        edit_price = float(self.price_to_precision(symbol,opposite_side - (1 if size>0 else -1)*edit_price_depth))

        try:
            # remove open order dupes is any (shouldn't happen)
            event_histories = self.filter_order_histories(symbol,myFtx.openStates)
            if len(event_histories) > 1:
                first_pending_new = np.argmin(np.array([data[0]['timestamp'] for data in event_histories]))
                for i,event_history in enumerate(self.filter_order_histories(symbol,myFtx.cancelableStates)):
                    if i != first_pending_new:
                        order = event_histories[i][-1]
                        asyncio.create_task(self.cancel_order(event_history[-1]['clientOrderId'],'duplicates'))
                        self.myLogger.warning('canceled duplicate {} order {}'.format(symbol,event_history[-1]['clientOrderId']))


            # if no open order, create an order
            order_side = 'buy' if size>0 else 'sell'
            if len(event_histories)==0:
                (postOnly, ioc) = (True, False) if edit_price_depth > priceIncrement else (False, True)
                asyncio.create_task(self.create_order(symbol, 'limit', order_side, np.abs(size), price=edit_price,
                                                      params={'postOnly': postOnly,'ioc': ioc, 'comment':'new'}))
            # if only one and it's editable, stopout or peg or wait
            elif (self.latest_value(event_histories[0][-1]['clientOrderId'],'remaining') >= sizeIncrement) \
                    and event_histories[0][-1]['lifecycle_state'] in self.acknowledgedStates:
                order = event_histories[0][-1]
                order_distance = (1 if order['side'] == 'buy' else -1) * (opposite_side - order['price'])

                # panic stop. we could rather place a trailing stop: more robust to latency, but less generic.
                if (stop_depth and order_distance > stop_trigger)\
                        or edit_price_depth <= 0.0:
                    size = self.latest_value(order['clientOrderId'],'remaining')
                    price = sweep_price(orderbook, size)
                    asyncio.create_task( self.edit_order(symbol, 'limit', order_side, size, price = price,
                                                         params={'ioc':True,'comment':'stop'},previous_clientOrderId = order['clientOrderId']))
                # peg limit order
                elif order_distance > edit_trigger and (np.abs(edit_price-order['price']) >= priceIncrement):
                    asyncio.create_task(self.edit_order(symbol, 'limit', order_side, np.abs(size),price=edit_price,
                                                        params={'postOnly': True,'comment':'chase'},previous_clientOrderId = order['clientOrderId']))

        # raise exception if it cannot create order
        ### see error_hierarchy in DerivativeArbitrage/venv/Lib/site-packages/ccxt/base/errors.py
        except ccxt.InsufficientFunds as e: # is ExchangeError
            cost = self.margin_calculator.margin_cost(symbol, mid, size, self.usd_balance)
            self.myLogger.warning(f'marginal cost {cost}, vs margin_headroom {self.margin_headroom} and calculated_IM {self.calculated_IM}')
        except ccxt.InvalidOrder as e: # is ExchangeError
            if "Order not found" in str(e):
                self.myLogger.warning(str(e) + str(order))
            elif ("Size too small for provide" or "Size too small") in str(e):
                # usually because filled btw remaining checked and order modify
                self.myLogger.warning('{}: {} too small {}...or {} < {}'.format(order,np.abs(size),sizeIncrement,order['clientOrderId'].split('_')[2],order['timestamp']))
            else:
                self.myLogger.warning(str(e) + str(order))
        except ccxt.ExchangeError as e:  # is base error
            if "Must modify either price or size" in str(e):
                self.myLogger.warning(str(e) + str(order))
            else:
                self.myLogger.warning(str(e), exc_info=True)

    # ---------------------------------- low level

    async def edit_order(self,*args,**kwargs):
        if await self.cancel_order(kwargs.pop('previous_clientOrderId'), 'edit'):
            return await self.create_order(*args,**kwargs)

    async def create_order(self, symbol, type, side, amount, price=None, params={}):
        '''if acknowledged, place order. otherwise just reconcile
        orders_pending_new is blocking'''
        coin = self.markets[symbol]['base']
        # check that there is no inflight on the spread
        if self.pending_new_histories(coin) != []:#TODO: rather incorporate orders_pending_new in risk, rather than block
            if self.pending_new_histories(coin,symbol) != []:
                self.myLogger.warning('orders {} should not be in flight'.format([order['clientOrderId'] for order in self.pending_new_histories(coin,symbol)[-1]]))
            else:
                # this happens mostly between pending_new and create_order on the other leg. not a big deal...
                self.myLogger.warning('orders {} still in flight. holding off {}'.format(
                    [order['clientOrderId'] for order in self.pending_new_histories(coin)[-1]],symbol))
            await asyncio.sleep(1)
            #await self.reconcile()
        else:
            # si ya pas d'order inflight, je veux envoyer un ordre.
            # L'OMS c'est un dico de liste d'etats.
            # il cree un nouvel clOrderId et le met dans le state pending new
            # set pending_new -> send rest -> if success, leave pending_new and give id. Pls note it may have been caught by handle_order by then.
            clientOrderId = self.lifecycle_pending_new({'symbol': symbol,
                                                        'type': type,
                                                        'side': side,
                                                        'amount': amount,
                                                        'remaining': amount,
                                                        'price': price,
                                                        'comment': params['comment']})
            try:
                # REST request pour envoyer l'ordre
                order = await super().create_order(symbol, type, side, amount, price, params | {'clientOrderId':clientOrderId})
            except Exception as e:
                order = {'clientOrderId':clientOrderId,
                         'timestamp':datetime.now().timestamp() * 1000,
                         'lifecycle_state':'rejected',
                         'comment':'create/'+str(e)}
                self.lifecycle_cancel_or_reject(order)
                if isinstance(e,ccxt.InsufficientFunds):
                    risks = await safe_gather([self.fetch_balance(), self.fetch_positions()])
                    await self.update_margin_data(*risks)
                    cost = self.margin_calculator.margin_cost(symbol,
                                                              self.mid(symbol),
                                                              amount * (1 if side == 'buy' else -1),
                                                              self.usd_balance)
                    self.myLogger.warning(
                        f'marginal cost {cost}, vs margin_headroom {self.margin_headroom} and calculated_IM {self.calculated_IM}')
                    #self.exec_parameters[coin][symbol]['slice_size'] = max([self.exec_parameters[coin][symbol]['slice_size']/1.1,
                    #                                                        self.exec_parameters[coin][symbol]['sizeIncrement']])
                elif isinstance(e,ccxt.RateLimitExceeded):
                    throttle = 200.0
                    self.myLogger.warning(f'{str(e)}: waiting {throttle} ms)')
                    await asyncio.sleep(throttle / 1000)
            else:
                self.lifecycle_sent(order)

    async def cancel_order(self, clientOrderId, trigger):
        '''set in flight, send cancel, set as pending cancel, set as canceled or insist'''
        symbol = clientOrderId.split('_')[1]
        self.lifecycle_pending_cancel({'clientOrderId':clientOrderId,
                                       'symbol': symbol,
                                       'comment':trigger})

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
            self.myLogger.warning(f'{clientOrderId} failed to cancel: {str(e)}')
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

async def ftx_ws_spread_main_wrapper(*argv,**kwargs):

    execution_status = None

    try:
        # TODO: read complex orders
        parameters = configLoader.get_executor_params()

        exchange = myFtx(parameters, config={  ## David personnal
            'enableRateLimit': True,
            'apiKey': 'ZUWyqADqpXYFBjzzCQeUTSsxBZaMHeufPFgWYgQU',
            'secret': api_params['ftx']['key'],
            'newUpdates': True})
        exchange.verbose = False
        exchange.headers = {'FTX-SUBACCOUNT': argv[2]}
        exchange.authenticate()
        await exchange.cancel_all_orders()
        await exchange.load_markets()

        # Cancel all legacy orders (if any) before starting new batch
        await exchange.cancel_all_orders()

        if argv[0]=='sysperp':
            future_weights = configLoader.get_current_weights()
            target_portfolio = await diff_portoflio(exchange, future_weights)  # still a Dataframe
            if target_portfolio.empty: raise myFtx.NothingToDo()

        elif argv[0]=='spread':
            coin=argv[3]
            cash_name = coin+'/USD'
            future_name = coin + '-PERP'
            cash_price = float(exchange.market(cash_name)['info']['price'])
            future_price = float(exchange.market(future_name)['info']['price'])
            target_portfolio = pd.DataFrame(columns=['coin','name','optimalCoin','currentCoin','spot_price'],data=[
                [coin,cash_name,float(argv[4])/cash_price,0,cash_price],
                [coin,future_name,-float(argv[4])/future_price,0,future_price]])
            if target_portfolio.empty: raise myFtx.NothingToDo()

        elif argv[0]=='flatten': # only works for basket with 2 symbols
            future_weights = pd.DataFrame(columns=['name','optimalWeight'])
            diff = await diff_portoflio(exchange, future_weights)
            smallest_risk = diff.groupby(by='coin')['currentCoin'].agg(lambda series: series.apply(np.abs).min() if series.shape[0]>1 else 0)
            target_portfolio=diff
            target_portfolio['optimalCoin'] = diff.apply(lambda f: smallest_risk[f['coin']]*np.sign(f['currentCoin']),axis=1)
            if target_portfolio.empty: raise myFtx.NothingToDo()

        elif argv[0]=='unwind':
            future_weights = pd.DataFrame(columns=['name','optimalWeight'])
            target_portfolio = await diff_portoflio(exchange, future_weights)
            if target_portfolio.empty: raise myFtx.NothingToDo()

        else:
            exchange.logger.exception(f'unknown command {argv[0]}',exc_info=True)
            raise Exception(f'unknown command {argv[0]}', exc_info=True)

        await exchange.build_state(target_portfolio, parameters)   # i
        coros = [exchange.monitor_risk(),exchange.monitor_orders(),exchange.monitor_fills()]+ \
                [exchange.monitor_order_book(symbol)
                 for symbol in exchange.running_symbols]
        await asyncio.gather(*coros)

    except myFtx.DoneDeal as e:
        # I want to wait for the budgettime period to run out before I read new weights and try again
        execution_status = e

    except myFtx.TimeBudgetExpired as e:
        # I want to flatten my book, but it can also timeout...
        execution_status = e

    except Exception as e:
        exchange.logger.warning(e, exc_info=True)
        raise e
    finally:
        await exchange.cancel_all_orders()
        # await exchange.close_dust()  # Commenting out until bug fixed
        await exchange.close()
        exchange.logger.info('exchange closed', exc_info=True)
        return execution_status

def ftx_ws_spread_main(*argv):
    '''
        @params: run_type, exchange, subaccount, *coin, *optimalcoin -> return status filled, partial
    '''

    argv = list(argv)

    if len(argv) == 0:
        argv.extend(['sysperp'])        # Means run the sysperp strategy

    if len(argv) < 3:
        argv.extend(['ftx', 'SysPerp']) # SysPerp and debug are subaccounts

    if argv[0] in ['sysperp', 'flatten', 'unwind', 'spread']:

        logging.info(f'running {argv}')

        while True:

            execution_status = asyncio.run(ftx_ws_spread_main_wrapper(*argv)) # --> I am filled or I timed out and I have flattened position
            print(f'{datetime.now().strftime("%Y%m%d_%H%M%S")}  EXIT with execution_status={execution_status}')

            if not isinstance(execution_status, myFtx.NothingToDo):
                log_reader()

            if isinstance(execution_status, myFtx.DoneDeal):
                # Wait for 5 minutes and start over
                t.sleep(60 * 5)
                print(f'{datetime.now().strftime("%Y%m%d_%H%M%S")}  ALLDONE --> SLEEPING for 5 minutes...')

            elif isinstance(execution_status, myFtx.TimeBudgetExpired):
                # Force flattens until it returns FILLED
                while not isinstance(execution_status, myFtx.DoneDeal):
                    execution_status = asyncio.run(ftx_ws_spread_main_wrapper(*(['flatten']+argv[1:])))
                    print(f'{datetime.now().strftime("%Y%m%d_%H%M%S")}  TIMEOUT --> FLATTEN UNTIL FINISHED')

            else:
                print(f'{datetime.now().strftime("%Y%m%d_%H%M%S")}  UNCAUGHT --> EXCEPTION NOT CAUGHT')

    else:
        logging.info(f'commands: sysperp [ftx][debug], flatten [ftx][debug],unwind [ftx][debug], spread [ftx][debug][coin][cash in usd]')

def main(*args):
    '''
        Examples runs :
            - main debug
            - sysperp ftx debug
            - sysperp ftx SysPerp

            - sysperp ftx SysPerp
            - flatten ftx SysPerp
    '''
    ftx_ws_spread_main(*sys.argv[1:])
