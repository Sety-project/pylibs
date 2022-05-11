import asyncio
import collections
import datetime
import threading

import dateutil.parser
import pandas as pd

from ftx_utils import fetch_futures
from ftx_history import fetch_trades_history
from ftx_portfolio import diff_portoflio, MarginCalculator
from ccxt_utilities import *
from async_utils import *
import ccxtpro

max_nb_coins = 10  # TODO: sharding needed
max_cache_size = 1000000
check_frequency = 60

entry_tolerance = 0.5 # green light if basket better than median
edit_trigger_tolerance = np.sqrt(60/60) # chase on 1m stdev
stop_tolerance = np.sqrt(5) # stop on 5m stdev
time_budget = 15*60 # 30m. used in transaction speed screener and to scale down exec params
delta_limit = 0.2 # delta limit / pv
slice_factor = 0.25 # % of request
edit_price_tolerance = np.sqrt(60/60)#price on 10s std

class CustomRLock(threading._PyRLock):
    @property
    def count(self):
        return self._count

def log_reader(prefix='latest',dirname='Runtime/logs/ftx_ws_execute'):
    path = f'{dirname}/{prefix}'
    with open(f'{path}_events.json', 'r') as file:
        d = json.load(file)
        events = {clientId: pd.DataFrame(data) for clientId, data in d.items()}
    with open(f'{path}_risk_reconciliations.json', 'r') as file:
        d = json.load(file)
        risk = pd.DataFrame(d)
    with open(f'{path}_request.json', 'r') as file:
        d = json.load(file)
        #start_time = d.pop('inception_time')
        request = pd.DataFrame(d)
    history = from_parquet(f'{path}_tickbytick.parquet')

    with pd.ExcelWriter(f'{dirname}/latest_exec.xlsx', engine='xlsxwriter', mode="w") as writer:
        def name(symbol):
            return symbol.replace('/USD:USD','-PERP').replace('/USD','')

        data = pd.concat([data for clientOrderId, data in events.items()], axis=0)

        # find out inception and fill block for each clientOrderId
        temp_timeseries = [{'inception_event':clientOrderId_data[clientOrderId_data['lifecycle_state']=='pending_new'].iloc[0],
                            'last_fill_event' : clientOrderId_data.iloc[clientOrderId_data['filled'].argmax()]}
                           for clientOrderId, clientOrderId_data in data.groupby(by='clientOrderId') if clientOrderId_data['filled'].max()>0]
        # summary
        clientOrderId_summary = pd.DataFrame([{'symbol':name(clientOrderId_data['inception_event']['symbol']),
                                          'slice_started' : float(clientOrderId_data['inception_event']['timestamp']),
                                          'mid_at_inception' : 0.5*(clientOrderId_data['inception_event']['bid']+clientOrderId_data['inception_event']['ask']),
                                          'amount' : clientOrderId_data['inception_event']['amount']*(1 if clientOrderId_data['inception_event']['side']=='buy' else -1),

                                          'filled' : clientOrderId_data['last_fill_event']['filled']*(1 if clientOrderId_data['inception_event']['side']=='buy' else -1),
                                          'average' : clientOrderId_data['last_fill_event']['average'],
                                          'slice_ended' : dateutil.parser.isoparse(clientOrderId_data['last_fill_event']['datetime']).timestamp()*1000,
                                          'fee':0}  #TODO: fee
                                              for clientOrderId_data in temp_timeseries])
        clientOrderId_summary['cost'] = clientOrderId_summary['average'] / clientOrderId_summary['mid_at_inception'] - 1
        clientOrderId_summary['time_to_fill'] = clientOrderId_summary['slice_ended'] - clientOrderId_summary['slice_started']

        symbol_summary = pd.DataFrame([{'symbol':symbol,
                                   'slice_started' : symbol_data['slice_started'].min(),
                                   'mid_at_inception' : symbol_data.iloc[symbol_data['slice_started'].argmin()]['mid_at_inception'],
                                   'amount' : sum(symbol_data['amount']),

                                   'filled' : sum(symbol_data['filled']),
                                   'average' : sum(symbol_data['filled']*symbol_data['average'])/sum(symbol_data['filled']),
                                   'slice_ended' : symbol_data['slice_ended'].max(),
                                   'fee':sum(symbol_data['fee'])}
                                       for symbol, symbol_data in clientOrderId_summary.groupby(by='symbol')])
        symbol_summary['exec_pnl'] = -(symbol_summary['average'] / symbol_summary['mid_at_inception'] - 1)*symbol_summary['amount'].apply(np.sign)
        symbol_summary['time_to_fill'] = symbol_summary['slice_ended'] - symbol_summary['slice_started']

        # format summary for inclusion in tick timeseries
        exec_history = pd.concat([symbol_data[['slice_started','mid_at_inception','amount']].rename(columns={
            'slice_started':'datetime',
            'mid_at_inception':f'{symbol}/exec/order_level',
            'amount': f'{symbol}/exec/order_amount',})
                                     for symbol, symbol_data in clientOrderId_summary.groupby(by='symbol')]
                                 +
                                 [symbol_data[['slice_ended','average', 'filled']].rename(
                                     columns={
                                         'slice_ended': 'datetime',
                                         'average': f'{symbol}/exec/fill_level',
                                         'filled': f'{symbol}/exec/fill_amount', })
                                     for symbol, symbol_data in clientOrderId_summary.groupby(by='symbol')]
                                 )
        exec_history['datetime'] = exec_history['datetime'].apply(lambda t:datetime.fromtimestamp(t/1000.0))

        # format tick history and merge
        history.index = [datetime.fromtimestamp(t / 1000.0) for t in history.index]
        history.columns = [name(c)+'/market/twap' for c in history.columns]
        history = pd.concat([history]+[exec_history.set_index('datetime')])

        history_amount = history[[c for c in history.columns if 'amount' not in c]].resample('1s').mean()
        history_levels = history[[c for c in history.columns if 'amount' in c]].resample('1s').sum(min_count=1)
        history = history_levels.join(history_amount,how='outer')

        symbol_summary.to_excel(writer, sheet_name='summary')
        history.to_excel(writer, sheet_name='history')
        request.to_excel(writer, sheet_name='request')

        for symbol,data in pd.concat(events.values(),axis=0).groupby(by='symbol'):
            data.sort_values(by='timestamp',ascending=True).to_excel(writer, sheet_name=name(symbol))

        if not risk.empty:
            risk.sort_values(by='delta_timestamp', ascending=True).to_excel(writer, sheet_name='risk_recon')

        return symbol_summary,history,request,risk

def loop(func):
    @functools.wraps(func)
    async def wrapper_loop(*args, **kwargs):
        self=args[0]
        while len(args)==1 or (args[1] in args[0].running_symbols):
            try:
                value = await func(*args, **kwargs)
            except ccxt.NetworkError as e:
                self.myLogger.debug(str(e))
                self.myLogger.info('reconciling after '+func.__name__+' dropped off')
                await self.reconcile()
            except Exception as e:
                self.myLogger.warning(e, exc_info=True)
                raise e
    return wrapper_loop

def symbol_locked(wrapped):
    '''decorates self.lifecycle_xxx(lientOrderId) to prevent race condition on state'''
    @functools.wraps(wrapped)
    def _wrapper(*args, **kwargs):
        return wrapped(*args, **kwargs) # temporary disable

        self=args[0]
        symbol = args[1]['clientOrderId'].split('_')[1]
        if self.lock[symbol].count:
            self.myLogger.info(f'{self.lock[symbol].count} race conditions on {symbol} blocking {wrapped.__name__}')
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
    def __init__(self, config={}):
        super().__init__(config=config)
        self.lock = {'reconciling':threading.Lock()}
        self.message_missed = collections.deque(maxlen=100)
        if __debug__: self.all_messages = collections.deque(maxlen=1000)
        self.rest_semaphor = asyncio.Semaphore(safe_gather_limit)

        self.orders_lifecycle = dict()
        self.latest_order_reconcile_timestamp = None
        self.latest_fill_reconcile_timestamp = None
        self.latest_exec_parameters_reconcile_timestamp = None

        self.limit = myFtx.LimitBreached()
        self.exec_parameters = {}
        self.exec_parameters_scaler = 1.0
        self.running_symbols =[]

        self.risk_reconciliations = []
        self.risk_state = {}
        self.pv = None
        self.usd_balance = None # it's handy for marginal calcs
        self.margin_headroom = None
        self.margin_calculator = None
        self.calculated_IM = None

        self.myLogger = logging.getLogger(__name__)

    # --------------------------------------------------------------------------------------------
    # ---------------------------------- various helpers -----------------------------------------
    # --------------------------------------------------------------------------------------------

    class DoneDeal(Exception):
        def __init__(self,status):
            super().__init__(status)
    class TimeBudgetExpired(Exception):
        def __init__(self,status):
            super().__init__(status)
    class LimitBreached(Exception):
        def __init__(self,limit=None,check_frequency=check_frequency):
            super().__init__()
            self.limit = limit
            self.check_frequency = check_frequency

    def find_clientID_from_fill(self,fill):
        '''find order by id, even if still in flight
        all layers events must carry id if known !! '''
        if 'clientOrderId' in fill:
            found = fill['clientOrderId']
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
                    raise Exception("fill {} not found".format(fill['symbol']))
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

        # logs
        handler_warning = logging.FileHandler('Runtime/logs/ftx_ws_execute/warning.log', mode='w')
        handler_warning.setLevel(logging.WARNING)
        handler_warning.setFormatter(logging.Formatter(f"%(levelname)s: %(message)s"))
        self.myLogger.addHandler(handler_warning)

        handler_info = logging.FileHandler('Runtime/logs/ftx_ws_execute/info.log', mode='w')
        handler_info.setLevel(logging.INFO)
        handler_info.setFormatter(logging.Formatter(f"%(levelname)s: %(message)s"))
        handler_info.addFilter(MyFilter(logging.INFO))
        self.myLogger.addHandler(handler_info)

        handler_debug = logging.FileHandler('Runtime/logs/ftx_ws_execute/debug.log', mode='w')
        handler_debug.setLevel(logging.DEBUG)
        handler_debug.setFormatter(logging.Formatter(f"%(levelname)s: %(message)s"))
        self.myLogger.addHandler(handler_debug)

        handler_alert = logging.handlers.SMTPHandler(mailhost='smtp.google.com',
                                                     fromaddr='david@pronoia.link',
                                                     toaddrs=['david@pronoia.link'],
                                                     subject='auto alert',
                                                     credentials=('david@pronoia.link', ''),
                                                     secure=None)
        handler_alert.setLevel(logging.CRITICAL)
        handler_alert.setFormatter(logging.Formatter(f"%(levelname)s: %(message)s"))
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
    editableStates = set(['acknowledged', 'partially_filled'])
    cancelableStates = set(['sent', 'acknowledged', 'partially_filled'])
    # --------------------------------------------------------------------------------------------

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
        '''returns all blockchains for symbol (all symbols if None), and current state'''
        return [data
                for data in self.orders_lifecycle.values()
                if (data[0]['symbol'] == symbol or symbol is None)
                and ((data[-1]['lifecycle_state'] in state_set) or state_set is None)]

    def latest_value(self,clientOrderId,key):
        for previous_state in reversed(self.orders_lifecycle[clientOrderId]):
            if 'remaining' in previous_state:
                return previous_state[key]
        raise f'remaning not founc for {clientOrderId}'

    def lifecycle_pending_new(self, order_event):
        '''self.orders_lifecycle = {clientId:[{key:data}]}
        order_event:trigger,symbol'''
        #1) resolve clientID
        nowtime = self.milliseconds()
        clientOrderId = order_event['trigger'] + '_' + order_event['symbol'] + '_' + str(int(nowtime))

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
        current |= {'mkt_timestamp': self.exec_parameters['timestamp']}\
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
            self.myLogger.warning('order {} was {}'.format(past['clientOrderId'],past['lifecycle_state']))
            return

        # 3) new block
        nowtime = order_event['timestamp']
        eventID = clientOrderId + '_' + str(int(nowtime))
        current = order_event | {'eventID':eventID,
                   'lifecycle_state':'sent'}
        # overwrite some fields
        current['timestamp'] = nowtime

        if order_event['status'] == 'closed':
            if order_event['remaining'] == 0:
                current['order'] = order_event['id']
                current['id'] = None
                assert 'amount' in current,"'amount' in current"
                self.lifecycle_fill(current)
            else:
                current['lifecycle_state'] = 'rejected'
                self.lifecycle_cancel_or_reject(current)

        #4) mine
        else:
            self.orders_lifecycle[clientOrderId] += [current]

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
        nowtime = self.milliseconds()
        eventID = clientOrderId + '_' + str(int(nowtime))
        current = order_event | {'eventID': eventID,
                                 'lifecycle_state': 'pending_cancel',
                                 'timestamp':nowtime}

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
        nowtime = order_event['timestamp']
        eventID = clientOrderId + '_' + str(int(nowtime))
        current = order_event | {'eventID': eventID,
                   'lifecycle_state': 'cancel_sent',
                   'timestamp': nowtime}

        # 4) mine
        self.orders_lifecycle[clientOrderId] += [current]

    @symbol_locked
    def lifecycle_acknowledgment(self, order_event):
        '''order_event: clientOrderId, trigger,timestamp,status'''
        # 1) resolve clientID
        clientOrderId = order_event['clientOrderId']

        # 2) validate block...is this needed?
        pass

        # 3) new block
        nowtime = order_event['timestamp']
        eventID = clientOrderId + '_' + str(int(nowtime))
        current = order_event | {'eventID':eventID}
        # overwrite some fields
        current['timestamp'] = nowtime

        if order_event['status'] in ['new', 'open', 'triggered']:
            current['lifecycle_state'] = 'acknowledged'
            self.orders_lifecycle[clientOrderId] += [current]
        elif order_event['status'] in ['closed']:
            #TODO: order event. not necessarily rejected ?
            if order_event['remaining'] == 0:
                current['order'] = order_event['id']
                current['id'] = None
                assert 'amount' in current,"assert 'amount' in current"
                self.lifecycle_fill(current)
            else:
                current['lifecycle_state'] = 'rejected'
                self.lifecycle_cancel_or_reject(current)
        elif order_event['status'] in ['canceled']:
            current['lifecycle_state'] = 'canceled'
            self.lifecycle_cancel_or_reject(current)
        elif order_event['filled'] !=0:
            raise Exception('filled ?')#TODO: code that ?
            current['order'] = order_event['id']
            current['id'] = None
            assert 'amount' in current,"assert 'amount' in current"
            self.lifecycle_fill(current)
        else:
            raise Exception('unknown status{}'.format(order_event['status']))

    @symbol_locked
    def lifecycle_fill(self,order_event):
        '''order_event: id, trigger,timestamp,amount,order,symbol'''
        # 1) resolve clientID
        clientOrderId = self.find_clientID_from_fill(order_event)

        # 2) validate block
        past = self.orders_lifecycle[clientOrderId][-1]

        # reconcile often rereads filled trades, skip then.
        if order_event['id'] in [block['fillId'] for block in self.orders_lifecycle[clientOrderId] if 'fillId' in block]:
            pass
            assert order_event['trigger'] in ['reconciled','replayed'],"order_event['trigger'] == 'reconciled'"
            #return

        # 3) new block
        if not isinstance(order_event['timestamp'], tuple):
            nowtime = order_event['timestamp']
        else:
            nowtime = order_event['timestamp'][0]
        eventID = clientOrderId + '_' + str(int(nowtime))
        current = order_event | {'eventID':eventID,
                          'fillId': order_event['id']}
        # overwrite some fields
        current['timestamp'] = nowtime
        current['remaining'] = past['remaining']-order_event['amount']
        current['id'] = order_event['order']

        symbol = order_event['symbol']
        coin = self.markets[symbol]['base']
        if current['remaining'] < self.exec_parameters[coin][symbol]['sizeIncrement']/2:
            current['lifecycle_state'] = 'filled'
            prices = np.array([event['price'] for event in self.orders_lifecycle[clientOrderId] if event['lifecycle_state'] in ['partially_filled','fill']])
            amounts = np.array([event['amount'] for event in self.orders_lifecycle[clientOrderId] if event['lifecycle_state'] in ['partially_filled','fill']])
            current |= {'avgPrice': np.dot(prices,amounts)/sum(amounts)}
        else:
            current['lifecycle_state'] = 'partially_filled'

        # 4) mine
        self.orders_lifecycle[clientOrderId] += [current]

    @symbol_locked
    def lifecycle_cancel_or_reject(self, order_event):
        '''order_event: id, trigger,timestamp,amount,order,symbol'''
        # 1) resolve clientID
        clientOrderId = order_event['clientOrderId']

        # 2) validate block
        pass

        # 3) new block
        if isinstance(order_event['timestamp']+.1, float):
            nowtime = order_event['timestamp']
        else:
            nowtime = order_event['timestamp'][0]
        eventID = clientOrderId + '_' + str(int(nowtime))
        current = order_event
        current['eventID'] = eventID
        current['lifecycle_state'] = order_event['lifecycle_state']

        # 4) mine
        self.orders_lifecycle[clientOrderId] += [current]

    async def lifecycle_to_json(self,filename = 'Runtime/logs/ftx_ws_execute/latest_events.json'):
        '''asyncronous + has it's own lock'''
        lock = threading.Lock()
        with lock:
            async with aiofiles.open(filename,mode='w') as file:
                await file.write(json.dumps(self.orders_lifecycle, cls=NpEncoder))
            shutil.copy2(filename, 'Runtime/logs/ftx_ws_execute/'+datetime.fromtimestamp(self.exec_parameters['timestamp']/1000).strftime("%Y-%m-%d-%H-%M")+'_events.json')

    async def risk_reconciliation_to_json(self,filename = 'Runtime/logs/ftx_ws_execute/latest_risk_reconciliations.json'):
        '''asyncronous + has it's own lock'''
        lock = threading.Lock()
        with lock:
            async with aiofiles.open(filename,mode='w') as file:
                await file.write(json.dumps(self.risk_reconciliations, cls=NpEncoder))
            shutil.copy2(filename, 'Runtime/logs/ftx_ws_execute/'+datetime.fromtimestamp(self.exec_parameters['timestamp']/1000).strftime("%Y-%m-%d-%H-%M")+'_risk_reconciliations.json')

    #@synchronized
    async def reconcile_fills(self):
        '''fetch fills, to recover missed messages'''
        self.latest_fill_reconcile_timestamp = self.milliseconds()
        fetched_fills = await self.fetch_my_trades(since=max(self.exec_parameters['timestamp'],self.latest_fill_reconcile_timestamp-1000))
        for fill in fetched_fills:
            if any(fill['id'] == block['fillId'] for block in self.orders_lifecycle[self.find_clientID_from_fill(fill)] if 'fillId' in block):
                continue

            fill['trigger'] = 'reconciled'
            fill['clientOrderId'] = self.find_clientID_from_fill(fill)
            self.lifecycle_fill(fill)
        # hopefully using another lock instance
        await self.lifecycle_to_json()

    # @synchronized
    async def reconcile_orders(self):
        '''fetch orders, recover missed messages and discard other pending_new'''

        fetched_orders = await self.fetch_orders(since=max(self.exec_parameters['timestamp'],self.latest_order_reconcile_timestamp)-1000)
        self.latest_order_reconcile_timestamp = self.milliseconds()

        fetch_processed = []
        # forcefully acknowledge
        for order in fetched_orders:
            if any(all(order[common_key] == block[common_key] for common_key in set(block.keys())&set(order.keys())) for block in self.orders_lifecycle[order['clientOrderId']]):
                continue

            if order['status'] == 'open':
                assert order['remaining'] > 0,"assert order['remaining'] > 0"
                self.lifecycle_acknowledgment(order| {'trigger': 'reconciled'})
            elif order['status'] == 'canceled':
                self.lifecycle_cancel_or_reject(order| {'trigger':'reconciled','lifecycle_state':'canceled'})
            elif order['status'] == 'closed':
                current = order
                current['trigger'] = 'reconciled'
                if order['remaining'] == 0:
                    current['order'] = order['id']
                    current['id'] = None
                    self.lifecycle_fill(current)
                else:
                    current['trigger'] = 'reconciled'
                    current['lifecycle_state'] = 'rejected'
                    self.lifecycle_cancel_or_reject(current)
            else:
                raise Exception('unknown status {}'.format(order['status']))
            fetch_processed += [order['clientOrderId']]

        # remove fakes
        for clientOrderId,data in self.orders_lifecycle.items():
            if data[-1]['lifecycle_state'] in myFtx.openStates \
                    and data[-1]['timestamp'] > self.latest_order_reconcile_timestamp \
                    and clientOrderId not in fetch_processed:
                current = {'clientOrderId': clientOrderId,
                           'timestamp': self.milliseconds(),
                           'lifecycle_state': 'rejected',
                           'trigger': 'reconciled_not_found'}
                self.lifecycle_cancel_or_reject(current)

        await self.lifecycle_to_json()

    # ---------------------------------------------------------------------------------------------
    # ---------------------------------- PMS -----------------------------------------
    # ---------------------------------------------------------------------------------------------

    async def build_state(self, weights,
                          entry_tolerance = entry_tolerance,
                          edit_trigger_tolerance = edit_trigger_tolerance, # chase on 30s stdev
                          edit_price_tolerance = edit_price_tolerance,
                          stop_tolerance = stop_tolerance, # stop on 5min stdev
                          time_budget = time_budget,
                          delta_limit = delta_limit, # delta limit / pv
                          slice_factor = slice_factor): # cut in 10):
        '''initialize all state and does some filtering (weeds out slow underlyings; should be in strategy)
            target_sub_portfolios = {coin:{entry_level_increment,
            symbol1:{'spot_price','diff','target'}]}]'''
        self.build_logging()

        self.limit.delta_limit = delta_limit

        frequency = timedelta(minutes=1)
        end = datetime.now()
        start = end - timedelta(hours=1)

        trades_history_list = await safe_gather([fetch_trades_history(
            self.market(symbol)['id'], self, start, end, frequency=frequency)
            for symbol in weights['name']],semaphore=self.rest_semaphor)

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
                               'volume': data['vwap'].filter(like='/trades/volume').mean().values[0] / frequency.total_seconds(),# in coin per second
                               'series': data['vwap'].filter(like='/trades/vwap')}
                          for data in trades_history_list if data['coin']==coin}
                     for coin in coin_list}

        # exclude coins too slow or symbol diff too small, limit to max_nb_coins
        diff_threshold = sorted(
            [max([np.abs(weights.loc[data['symbol'], 'diffUSD'])
                  for data in trades_history_list if data['coin']==coin])
             for coin in coin_list])[-min(max_nb_coins,len(coin_list))]

        data_dict = {coin: {symbol:data
                            for symbol, data in coin_data.items()}
                     for coin, coin_data in full_dict.items() if
                     all(data['volume'] * time_budget > np.abs(data['diff']) for data in coin_data.values())
                     and any(np.abs(data['diff']) >= max(diff_threshold/data['spot_price'], float(self.markets[symbol]['info']['minProvideSize'])) for symbol, data in coin_data.items())}
        if data_dict =={}:
            self.exec_parameters = {'timestamp': end.timestamp() * 1000}
            raise myFtx.DoneDeal('nothing to do')

        def basket_vwap_quantile(series_list,diff_list,quantile):
            series = pd.concat(series_list,axis=1).dropna(axis=0)-diff_list
            return series.sum(axis=1).diff().quantile(quantile)
        def z_score(series,z_score):# stdev of 1min prices * quantile
            series = series.dropna(axis=0)
            return series.std().values[0]*z_score

        # get times series of target baskets, compute quantile of increments and add to last price
        # remove series
        self.exec_parameters = {'timestamp':end.timestamp()*1000} \
                               |{sys.intern(coin):
                                     {sys.intern('entry_level'):
                                          sum([float(self.markets[symbol]['info']['price']) * data['diff']
                                               for symbol, data in coin_data.items()])
                                          + basket_vwap_quantile([data['series'] for data in coin_data.values()],
                                                                 [data['diff'] for data in coin_data.values()],
                                                                 entry_tolerance)}
                                     | {sys.intern(symbol):
                                         {
                                             sys.intern('diff'): data['diff'],
                                             sys.intern('target'): data['target'],
                                             sys.intern('slice_size'): max([float(self.markets[symbol]['info']['minProvideSize']),
                                                                slice_factor * np.abs(data['diff'])]),  # in coin
                                             sys.intern('edit_trigger_depth'): z_score(data['series'],edit_trigger_tolerance),
                                             sys.intern('edit_price_depth'): z_score(data['series'],edit_price_tolerance), # floored at priceIncrement. Used for aggressive, overriden for passive.
                                             sys.intern('stop_depth'): z_score(data['series'],stop_tolerance),
                                             sys.intern('priceIncrement'): float(self.markets[symbol]['info']['priceIncrement']),
                                             sys.intern('sizeIncrement'): float(self.markets[symbol]['info']['minProvideSize']),
                                             sys.intern('spot'): self.mid(symbol),
                                             sys.intern('history'): collections.deque(maxlen=max_cache_size)
                                         }
                                         for symbol, data in coin_data.items()}
                                 for coin, coin_data in data_dict.items()}

        self.lock |= {symbol: CustomRLock() for coin, coin_data in data_dict.items() for symbol, data in coin_data.items()}

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

        futures = pd.DataFrame(await fetch_futures(self))
        account_leverage = float(futures.iloc[0]['account_leverage'])
        collateralWeight = futures.set_index('underlying')['collateralWeight'].to_dict()
        imfFactor = futures.set_index('new_symbol')['imfFactor'].to_dict()
        self.margin_calculator = MarginCalculator(account_leverage, collateralWeight, imfFactor)

        # populates risk, pv and IM
        self.latest_order_reconcile_timestamp = self.exec_parameters['timestamp']
        self.latest_fill_reconcile_timestamp = self.exec_parameters['timestamp']
        self.latest_exec_parameters_reconcile_timestamp = self.exec_parameters['timestamp']
        await self.reconcile()

        vwap_dataframe = pd.concat([data['vwap'].filter(like='vwap').fillna(method='ffill') for data in trades_history_list],axis=1,join='outer').fillna(method='bfill')
        vwap_dataframe=vwap_dataframe.apply(np.log).diff()
        size_dataframe = pd.concat([data['vwap'].filter(like='volume').fillna(method='ffill') for data in trades_history_list], axis=1, join='outer').fillna(method='bfill')
        to_parquet(pd.concat([vwap_dataframe,size_dataframe], axis=1, join='outer'),'Runtime/logs/ftx_ws_execute/latest_minutely.parquet')
        shutil.copy2('Runtime/logs/ftx_ws_execute/latest_minutely.parquet',
                     'Runtime/logs/ftx_ws_execute/' + datetime.fromtimestamp(self.exec_parameters['timestamp'] / 1000).strftime(
                         "%Y-%m-%d-%Hh") + '_minutely.parquet')

        with open('Runtime/logs/ftx_ws_execute/latest_request.json', 'w') as file:
            json.dump({'inception_time':self.exec_parameters['timestamp']}|
                      {symbol:data
                        for coin,coin_data in self.exec_parameters.items() if coin in self.currencies
                        for symbol,data in coin_data.items() if symbol in self.markets}, file, cls=NpEncoder)
        shutil.copy2('Runtime/logs/ftx_ws_execute/latest_request.json','Runtime/logs/ftx_ws_execute/' + datetime.fromtimestamp(self.exec_parameters['timestamp'] / 1000).strftime(
                         "%Y-%m-%d-%Hh") + '_request.json')

    def update_exec_parameters(self): # cut in 10):
        '''scales order placement params with time
        dumps history'''

        def basket_vwap_quantile(series_list,diff_list,quantile):
            series = pd.concat(series_list,axis=1).dropna(axis=0)-diff_list
            return series.sum(axis=1).diff().quantile(quantile)
        def z_score(deque,z_score):# stdev of 1min prices * quantile
            series = pd.Series(list(deque)).dropna(axis=0)
            return series.std().values[0]*z_score

        # get times series of target baskets, compute quantile of increments and add to last price
        nowtime = self.milliseconds()
        time_limit = self.exec_parameters['timestamp'] + time_budget*1000

        if nowtime > time_limit:
            self.myLogger.info('time budget expired')
            raise myFtx.TimeBudgetExpired('')

        self.exec_parameters_scaler = max(0,(time_limit-nowtime)/(time_limit-self.exec_parameters['timestamp']))
        for coin, coin_data in self.exec_parameters.items():
            if coin not in self.currencies: continue
            # TODO: update signals here

        self.latest_exec_parameters_reconcile_timestamp = nowtime
        self.myLogger.info(f'scaled params by {self.exec_parameters_scaler} at {nowtime}')

        # dump tick history and clear deque
        history_dump = pd.concat([pd.DataFrame(index=[i[0] for i in self.exec_parameters[coin][symbol]['history']],
                                               columns=[symbol],
                                               data=[i[1] for i in self.exec_parameters[coin][symbol]['history']])
                                  for coin,data in self.exec_parameters.items() if coin in self.currencies
                                  for symbol in data.keys() if symbol in self.markets])
        to_parquet(history_dump,f'Runtime/logs/ftx_ws_execute/latest_tickbytick.parquet', mode="a")
        [self.exec_parameters[coin][symbol]['history'].clear()
         for coin, data in self.exec_parameters.items() if coin in self.currencies
         for symbol in data.keys() if symbol in self.markets]

        # display running threads
        actually_running = [self.market(f.replace('orderbook:',''))['symbol'] for f in self.clients['wss://ftx.com/ws'].futures.keys() if 'orderbook' in f]
        self.myLogger.info('running: {}'.format(actually_running))
        self.myLogger.info('missing: {}'.format(set(self.running_symbols)-set(actually_running)))

    async def reconcile(self):
        '''update risk using rest
        all symbols not present when state is built are ignored !
        if some tickers are not initialized, it just uses markets
        trigger interception of all incoming messages until done'''

        # if already running, skip reconciliation
        if self.lock['reconciling'].locked():
            return

        # or reconcile, and lock until done
        with self.lock['reconciling']:

            await self.reconcile_fills()
            await self.reconcile_orders()
            self.update_exec_parameters()

            risks = await safe_gather([self.fetch_positions(),self.fetch_balance()],semaphore=self.rest_semaphor)
            positions = risks[0]
            balances = risks[1]
            risk_timestamp = self.milliseconds()

            # delta is noisy for perps, so override to delta 1.
            for position in positions:
                if float(position['info']['size'])!=0:
                    symbol = position['symbol']
                    coin = self.markets[symbol]['base']
                    if coin in self.risk_state and symbol in self.risk_state[coin]:
                        self.risk_state[coin][symbol]['delta'] = position['notional']*(1 if position['side'] == 'long' else -1) \
                            if self.markets[symbol]['type'] == 'future' \
                            else float(position['info']['size']) * self.mid(symbol)*(1 if position['side'] == 'long' else -1)
                        self.risk_state[coin][symbol]['delta_timestamp']=risk_timestamp

            for coin, balance in balances.items():
                if coin in self.currencies.keys() and coin != 'USD' and balance['total']!=0 and coin in self.risk_state and coin+'/USD' in self.risk_state[coin]:
                    symbol = coin+'/USD'
                    self.risk_state[coin][symbol]['delta'] = balance['total'] * self.mid(symbol)
                    self.risk_state[coin][symbol]['delta_timestamp'] = risk_timestamp

            for coin,coin_data in self.risk_state.items():
                coin_data['netDelta']= sum([data['delta'] for symbol,data in coin_data.items() if symbol in self.markets and 'delta' in data.keys()])

            # update pv and usd balance
            self.usd_balance = balances['USD']['total']
            self.pv = sum(coin_data[coin+'/USD']['delta'] for coin, coin_data in self.risk_state.items() if coin+'/USD' in coin_data.keys()) + self.usd_balance

            #compute IM
            spot_weight={}
            future_weight={}
            for position in positions:
                if float(position['info']['netSize'])!=0:
                    symbol = position['symbol']
                    mid = self.mid(symbol)
                    future_weight |= {symbol: {'weight': float(position['info']['netSize'])*mid, 'mark':mid}}
            for coin,balance in balances.items():
                if coin!='USD' and coin in self.currencies and balance['total']!=0:
                    mid = self.mid(coin+'/USD')
                    spot_weight |= {(coin):{'weight':balance['total']*mid,'mark':mid}}
            (spot_weight,future_weight) = MarginCalculator.add_pending_orders(self, spot_weight, future_weight)

            self.margin_calculator.estimate(self.usd_balance, spot_weight, future_weight)
            self.calculated_IM = sum(value for value in self.margin_calculator.estimated_IM.values())
            # fetch IM
            account_info = (await self.privateGetAccount())['result']
            self.margin_calculator.actual(account_info)
            self.margin_headroom = self.margin_calculator.actual_IM if float(account_info['totalPositionSize']) else self.pv

        # critical job is done, release lock

        # replay missed _messages. It's all concurrent so no pb with handle_xxx still filling the deque.
        self.myLogger.info(f'replaying {len(self.message_missed)} messages after recon')
        while self.message_missed:
            message = self.message_missed.popleft()
            channel = message['channel']
            if channel == 'orders':
                if any(all(message['data'][common_key] == block[common_key] for common_key in set(block.keys()) & set(message['data'].keys()))
                       for block in self.orders_lifecycle[message['data']['clientOrderId']]):
                    continue
                message['data'] |= {'orderTrigger':'replayed'}
                self.handle_order(self.client('wss://ftx.com/ws'),message)
            elif channel == 'fills':
                if any(message['data']['id'] == block['fillId'] for block in self.orders_lifecycle[message['data']['clientOrderId']]):
                    continue
                message['data'] |= {'orderTrigger': 'replayed'}
                self.handle_my_trade(self.client('wss://ftx.com/ws'), message)

        # log risk
        if True:
            self.risk_reconciliations += [{'lifecycle_state': 'remote_risk', 'symbol':symbol_, 'delta_timestamp':self.risk_state[coin][symbol_]['delta_timestamp'],
                                           'delta':self.risk_state[coin][symbol_]['delta'],'netDelta':self.risk_state[coin]['netDelta'],
                                           'pv':self.pv,'calculated_IM':self.calculated_IM,'margin_headroom':self.margin_headroom}
                                          for coin,coindata in self.risk_state.items()
                                          for symbol_ in coindata.keys() if symbol_ in self.markets]
            await self.risk_reconciliation_to_json()

    # --------------------------------------------------------------------------------------------
    # ---------------------------------- WS loops             -----------------------------------------
    # --------------------------------------------------------------------------------------------

    @loop
    async def monitor_fills(self):
        fills = await self.watch_my_trades()#limit=1)
        return

    @intercept_message_during_reconciliation
    def handle_my_trade(self, client, message):
        '''maintains risk_state, event_records, logger.info
        await self.reconcile_state() is safer but slower. we have monitor_risk to reconcile'''
        super().handle_my_trade(client, message)
        if __debug__: self.all_messages.append(message)
        data = self.safe_value(message, 'data')
        fill = self.parse_trade(data)
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

        # log event
        fill['clientOrderId'] = fill['info']['clientOrderId']
        fill['trigger'] = 'websocket_fill'
        self.lifecycle_fill(fill)

        # logger.info
        self.myLogger.info('{} fill at {}: {} {} {} at {}'.format(symbol,
                                                                fill['timestamp'] - self.exec_parameters['timestamp'],
                                                                fill['side'], fill['amount'], symbol, fill['price']))

        current = self.risk_state[coin][symbol]['delta']
        initial = self.exec_parameters[coin][symbol]['target'] * fill['price'] - self.exec_parameters[coin][symbol][
            'diff'] * fill['price']
        target = self.exec_parameters[coin][symbol]['target'] * fill['price']
        self.myLogger.info('{} risk at {} ms: {}% done [current {}, initial {}, target {}]'.format(
            symbol,
            self.risk_state[coin][symbol]['delta_timestamp'] - self.exec_parameters['timestamp'],
            (current - initial) / (target - initial) * 100,
            current,
            initial,
            target))

    @loop
    async def monitor_risk(self):
        '''redundant minutely risk check'''#TODO: would be cool if this could interupt other threads and restart it when margin is ok.
        await asyncio.sleep(self.limit.check_frequency)
        await self.reconcile()

        self.limit.limit = self.pv * self.limit.delta_limit
        absolute_risk = sum(abs(data['netDelta']) for data in self.risk_state.values())
        if absolute_risk > self.limit.limit:
            self.myLogger.warning(f'absolute_risk {absolute_risk} > {self.limit.limit}')
        if self.margin_headroom < self.pv/100:
            self.myLogger.warning(f'IM {self.margin_headroom}  < 1%')

    @loop
    async def monitor_orders(self):
        orders = await self.watch_orders()

    @intercept_message_during_reconciliation
    def handle_order(self, client, message):
        '''maintains orders, pending_new, event_records'''
        super().handle_order(client, message)
        if __debug__: self.all_messages.append(message)
        data = self.safe_value(message, 'data')
        order = self.parse_order(data)
        assert order['clientOrderId'],"assert order['clientOrderId']"

        self.lifecycle_acknowledgment(order| {'trigger': data['orderTrigger'] if 'orderTrigger' in data else 'websocket_acknowledgment'}) # status new, triggered, open or canceled

    async def watch_ticker(self, symbol, params={}):
        '''watch_order_book is faster than watch_tickers so we DON'T LISTEN TO TICKERS. Dirty...'''
        raise Exception("watch_order_book is faster than watch_tickers so we DON'T LISTEN TO TICKERS. Dirty...")

    @loop
    async def monitor_order_book(self, symbol):
        order_book = await self.watch_order_book(symbol)

    def handle_order_book_update(self, client, message):
        '''on each top of book update, update market_state and send orders
        tunes aggressiveness according to risk.
        populates event_records, maintains pending_new
        no interception is done, so we keep collecting mktdata'''
        super().handle_order_book_update(client, message)
        marketId = self.safe_string(message, 'market')
        symbol = self.markets_by_id[marketId]['symbol']
        coin = self.markets[symbol]['base']
        # watch_order_book is faster than watch_tickers so we DON'T LISTEN TO TICKERS. Dirty...
        #'order_book = await self.watch_order_book(symbol)
        orderbook = self.orderbooks[symbol]
        timestamp = message['data']['time'] * 1000
        mid = 0.5 * (orderbook['bids'][0][0] + orderbook['asks'][0][0])
        self.tickers[symbol]={'symbol':symbol,
                              'timestamp':timestamp,
                              'bid':orderbook['bids'][0][0],
                              'ask':orderbook['asks'][0][0],
                              'mid':0.5*(orderbook['bids'][0][0]+orderbook['asks'][0][0]),
                              'bidVolume':orderbook['bids'][0][1],
                              'askVolume':orderbook['asks'][0][1]}
        self.exec_parameters[coin][symbol]['history'].append([timestamp,mid])
        if symbol in self.running_symbols and not self.lock['reconciling'].locked():
            with self.lock[symbol]:
                self.quoter(symbol, message)

    # --------------------------------------------------------------------------------------------
    # ---------------------------------- order placement -----------------------------------------
    # --------------------------------------------------------------------------------------------

    def quoter(self, symbol, message):
        coin = self.markets[symbol]['base']
        mid = self.tickers[symbol]['mid']
        params = self.exec_parameters[coin][symbol]

        #risk
        delta = self.risk_state[coin][symbol]['delta']/mid
        delta_timestamp = self.risk_state[coin][symbol]['delta_timestamp']
        netDelta = self.risk_state[coin]['netDelta']/mid

        # size to do: aim at target, slice, round to sizeIncrement
        size = params['target'] - delta
        # size < slice_size and margin_headroom
        size = np.sign(size)*float(self.amount_to_precision(symbol, min([np.abs(size), params['slice_size']])))
        if (np.abs(size) < self.exec_parameters[coin][symbol]['sizeIncrement']):
            self.running_symbols.remove(symbol)
            if self.running_symbols == []:
                raise myFtx.DoneDeal('all done')
            else: return

        # if not enough margin, hold it# TODO: this may be slow, and depends on orders anyway
        #if self.margin_calculator.margin_cost(coin, mid, size, self.usd_balance) > self.margin_headroom:
        #    await asyncio.sleep(60) # TODO: I don't know how to stop/restart a thread..
        #    self.myLogger.info('margin {} too small for order size {}'.format(size*mid, self.margin_headroom))
        #    return None

        # if increases risk, go passive
        if np.abs(netDelta+size)-np.abs(netDelta)>0:

            # set limit at target quantile
            #current_basket_price = sum(self.mid(symbol)*self.exec_parameters[coin][symbol]['diff']
            #                           for symbol in self.exec_parameters[coin].keys() if symbol in self.markets)
            #edit_price_depth = max([0,(current_basket_price-self.exec_parameters[coin]['entry_level'])/params['diff']])#TODO: sloppy logic assuming no correlation
            edit_price_depth = (np.abs(netDelta+size)-np.abs(netDelta))/np.abs(size)*params['edit_price_depth']*self.exec_parameters_scaler # equate: profit if done ~ marginal risk * stdev
            edit_trigger_depth=params['edit_trigger_depth']*self.exec_parameters_scaler
            self.peg_or_stopout(symbol,size,orderbook_depth=0,edit_trigger_depth=edit_trigger_depth,edit_price_depth=edit_price_depth,stop_depth=None)
        # if decrease risk, go aggressive
        else:
            edit_trigger_depth=params['priceIncrement'] # priceIncrement not edit_trigger_depth !
            edit_price_depth=params['priceIncrement'] # priceIncrement not edit_price_depth !
            stop_depth=params['stop_depth']*self.exec_parameters_scaler
            self.peg_or_stopout(symbol,size,orderbook_depth=0,edit_trigger_depth=edit_trigger_depth,edit_price_depth=edit_price_depth,stop_depth=stop_depth)

    def peg_or_stopout(self,symbol,size,orderbook_depth,edit_trigger_depth,edit_price_depth,stop_depth=None):
        '''creates of edit orders, pegging to orderbook
        goes taker when depth<increment
        size in coin, already filtered
        skips if any pending_new, cancels duplicates
        extensive exception handling
        '''
        coin = self.markets[symbol]['base']

        #TODO: https://help.ftx.com/hc/en-us/articles/360052595091-Ratelimits-on-FTX

        price = self.tickers[symbol]['ask' if size<0 else 'bid']
        opposite_side = self.tickers[symbol]['ask' if size>0 else 'bid']
        mid = self.tickers[symbol]['mid']

        priceIncrement = self.exec_parameters[coin][symbol]['priceIncrement']
        sizeIncrement = self.exec_parameters[coin][symbol]['sizeIncrement']

        if stop_depth:
            stop_trigger = float(self.price_to_precision(symbol,stop_depth))
        edit_trigger = float(self.price_to_precision(symbol,edit_trigger_depth))
        edit_price = float(self.price_to_precision(symbol,opposite_side - (1 if size>0 else -1)*edit_price_depth))

        try:
            # look for open orders on the symbol. if dupes, remove latest.
            event_histories = self.filter_order_histories(symbol,self.cancelableStates)
            if len(event_histories) > 1:
                first_pending_new = np.argmin(np.array([data[0]['timestamp'] for data in event_histories]))
                for i,event_history in enumerate(event_histories):
                    if i == first_pending_new: continue
                    asyncio.create_task(self.cancel_order(event_history[-1]['clientOrderId'],'duplicates'))
                    self.myLogger.warning('canceled duplicate {} order {}'.format(symbol,event_history[-1]['clientOrderId']))
                order = event_histories[first_pending_new][-1]

            # if none, create genesis
            (postOnly,order_type) = (True,'limit') if edit_price_depth > priceIncrement else (False,'market')
            order_side = 'buy' if size>0 else 'sell'
            if len(event_histories)==0:
                asyncio.create_task(self.create_order(symbol, order_type, order_side, np.abs(size), price=edit_price,
                                                params={'postOnly': postOnly,'trigger':'new'}))
            # if only one, stopout or peg or wait
            elif (self.latest_value(event_histories[0][-1]['clientOrderId'],'remaining') >= sizeIncrement) \
                    and event_histories[0][-1]['lifecycle_state'] in self.editableStates:
                order = event_histories[0][-1]
                order_distance = (1 if order['side'] == 'buy' else -1) * (opposite_side - order['price'])

                # panic stop. we could rather place a trailing stop: more robust to latency, but less generic.
                if stop_depth and order_distance > stop_trigger:
                    asyncio.create_task( self.edit_order(symbol, 'market', order_side,self.latest_value(order['clientOrderId'],'remaining'),
                                                         params={'trigger':'stop'},previous_clientOrderId = order['clientOrderId']))
                # peg limit order
                elif order_distance > edit_trigger and (np.abs(edit_price-order['price']) >= priceIncrement):
                    asyncio.create_task(self.edit_order(symbol, order_type, order_side, np.abs(size),price=edit_price,
                                                        params={'postOnly': True,'trigger':'chase'},previous_clientOrderId = order['clientOrderId']))

        ### see error_hierarchy in DerivativeArbitrage/venv/Lib/site-packages/ccxt/base/errors.py
        except ccxt.InsufficientFunds as e: # is ExchangeError
            cost = self.margin_calculator.margin_cost(coin, mid, size, self.usd_balance)
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

    async def edit_order(self,*args,**kwargs):
        if await self.cancel_order(kwargs.pop('previous_clientOrderId'), 'edit'):
            return await self.create_order(*args,**kwargs)

    async def create_order(self, symbol, type, side, amount, price=None, params={}):
        '''if acknowledged, place order. otherwise just reconcile
        orders_pending_new is blocking'''
        order = None
        coin = self.markets[symbol]['base']
        if self.pending_new_histories(coin) != []:#TODO: rather incorporate orders_pending_new in risk, rather than block
            if self.pending_new_histories(coin,symbol) != []:
                self.myLogger.warning('orders {} should not be in flight'.format([order['clientOrderId'] for order in self.pending_new_histories(coin,symbol)[-1]]))
            else:
                self.myLogger.warning('orders {} still in flight. holding off {}'.format(
                    [order['clientOrderId'] for order in self.pending_new_histories(coin)[-1]],symbol))
            await asyncio.sleep(1)
            await self.reconcile()
        else:
            # set pending_new -> send rest -> if success, leave pending_new and give id. Pls note it may have been caught by handle_order by then.
            clientOrderId = self.lifecycle_pending_new({'symbol': symbol,
                                                        'type': type,
                                                        'side': side,
                                                        'amount': amount,
                                                        'remaining': amount,
                                                        'price': price,
                                                        'trigger': params['trigger']})
            try:
                order = await super().create_order(symbol, type, side, amount, price, params | {'clientOrderId':clientOrderId})
            except ccxt.InsufficientFunds as e:
                raise e
            except Exception as e:
                order = {'clientOrderId':clientOrderId,
                         'timestamp':self.milliseconds(),
                         'lifecycle_state':'rejected',
                         'trigger':'create/'+str(e)}
                self.lifecycle_cancel_or_reject(order)
            else:
                self.lifecycle_sent(order)

        return order

    async def cancel_order(self, clientOrderId, trigger):
        '''set in flight, send cancel, set as pending cancel, set as canceled or insist'''
        symbol = clientOrderId.split('_')[1]
        self.lifecycle_pending_cancel({'clientOrderId':clientOrderId,
                                       'symbol': symbol,
                                       'trigger':trigger})

        try:
            status = await super().cancel_order(None,params={'clientOrderId':clientOrderId})
            self.lifecycle_cancel_sent({'clientOrderId':clientOrderId,
                                        'symbol':symbol,
                                        'timestamp':self.milliseconds(),
                                        'status':status,
                                        'trigger':trigger})
            return True
        except ccxt.CancelPending as e:
            self.lifecycle_cancel_sent({'clientOrderId': clientOrderId,
                                        'symbol': symbol,
                                        'timestamp': self.milliseconds(),
                                        'status': str(e),
                                        'trigger': trigger})
            return True
        except ccxt.InvalidOrder as e: # could be in flight, or unknown
            self.lifecycle_cancel_or_reject({'clientOrderId':clientOrderId,
                                             'status':str(e),
                                             'timestamp':self.milliseconds(),
                                             'lifecycle_state':'canceled',
                                             'trigger':trigger})
            return False
        except Exception as e:
                self.myLogger.warning('cancel for {} fails with {}'.format(clientOrderId,str(e)))
                await asyncio.sleep(.1)
                return await self.cancel_order(clientOrderId, trigger+'+')

    async def close_dust(self):
        '''leaves slightly positive cash for ease of convert'''
        risks = await safe_gather([self.fetch_positions(),self.fetch_balance()],semaphore=self.rest_semaphor)
        positions = risks[0]
        balances = risks[1]

        futures_dust = [position for position in positions
                        if float(position['info']['size'])!=0
                        and np.abs(float(position['info']['size']))<2*float(self.markets[position['symbol']]['info']['minProvideSize'])]
        for coro in [super(myFtx,self).create_order(symbol=position['symbol'],
                                          type='market',
                                          side='buy' if position['side']=='short' else 'sell',
                                          amount=float(position['info']['size']),
                                          params={'clientOrderId':'dust_'+position['symbol']+'/USD'+str(int(self.exec_parameters['timestamp']))})
                     for position in futures_dust]:
            await coro
            await asyncio.sleep(.2)

        shorts_dust = {coin:float(self.markets[coin+'/USD']['info']['sizeIncrement']) for coin, balance in balances.items()
                     if coin in set(self.currencies) - set(['USD'])
                     and balance['total']<0
                     and float(balance['total'])+float(self.markets[coin+'/USD']['info']['sizeIncrement'])>0}
        for coro in [super(myFtx,self).create_order(coin+'/USD','market','buy',size,params={'clientOrderId':'dust_'+coin+'/USD_'+str(int(self.exec_parameters['timestamp']))})
                     for coin,size in shorts_dust.items()]:
            await coro
            await asyncio.sleep(.2)

async def ftx_ws_spread_main_wrapper(*argv,**kwargs):
    allDone = False
    while not allDone:
        try:
            exchange = myFtx({  ## David personnal
                'enableRateLimit': True,
                'apiKey': 'ZUWyqADqpXYFBjzzCQeUTSsxBZaMHeufPFgWYgQU',
                'secret': api_params.loc['ftx', 'value'],
                'newUpdates': True})
            exchange.verbose = False
            exchange.headers = {'FTX-SUBACCOUNT': argv[2]}
            exchange.authenticate()
            await exchange.load_markets()

            if argv[0]=='sysperp':
                future_weights = pd.read_excel('Runtime/ApprovedRuns/current_weights.xlsx')
                target_portfolio = await diff_portoflio(exchange, future_weights)
                if target_portfolio.empty: return
                #selected_coins = ['REN']#target_portfolios.sort_values(by='USDdiff', key=np.abs, ascending=False).iloc[2]['underlying']
                #target_portfolio=diff[diff['coin'].isin(selected_coins)]
                await exchange.build_state(target_portfolio,
                                           entry_tolerance=entry_tolerance,
                                           edit_trigger_tolerance = edit_trigger_tolerance,
                                           edit_price_tolerance = edit_price_tolerance,
                                           stop_tolerance = stop_tolerance,
                                           time_budget = time_budget,
                                           delta_limit = delta_limit,
                                           slice_factor = slice_factor)

            elif argv[0]=='spread':
                coin=argv[3]
                cash_name = coin+'/USD'
                future_name = coin + '-PERP'
                cash_price = float(exchange.market(cash_name)['info']['price'])
                future_price = float(exchange.market(future_name)['info']['price'])
                target_portfolio = pd.DataFrame(columns=['coin','name','optimalCoin','currentCoin','spot_price'],data=[
                    [coin,cash_name,float(argv[4])/cash_price,0,cash_price],
                    [coin,future_name,-float(argv[4])/future_price,0,future_price]])
                if target_portfolio.empty: return

                await exchange.build_state(target_portfolio,
                                           entry_tolerance=0.99,
                                           edit_trigger_tolerance = np.sqrt(5),
                                           edit_price_tolerance = 0,
                                           stop_tolerance = np.sqrt(30),
                                           time_budget = 999,
                                           delta_limit = delta_limit,
                                           slice_factor = 0.5)

            elif argv[0]=='flatten': # only works for basket with 2 symbols
                future_weights = pd.DataFrame(columns=['name','optimalWeight'])
                diff = await diff_portoflio(exchange, future_weights)
                smallest_risk = diff.groupby(by='coin')['currentCoin'].agg(lambda series: series.apply(np.abs).min() if series.shape[0]>1 else 0)
                target_portfolio=diff
                target_portfolio['optimalCoin'] = diff.apply(lambda f: smallest_risk[f['coin']]*np.sign(f['currentCoin']),axis=1)
                if target_portfolio.empty: return

                await exchange.build_state(target_portfolio,
                                           entry_tolerance=entry_tolerance,
                                           edit_trigger_tolerance = edit_trigger_tolerance,
                                           edit_price_tolerance = edit_price_tolerance,
                                           stop_tolerance = stop_tolerance,
                                           time_budget = time_budget,
                                           delta_limit = delta_limit,
                                           slice_factor = slice_factor)

            elif argv[0]=='unwind':
                future_weights = pd.DataFrame(columns=['name','optimalWeight'])
                target_portfolio = await diff_portoflio(exchange, future_weights)
                if target_portfolio.empty: return

                await exchange.build_state(target_portfolio,
                                           entry_tolerance=.99,
                                           edit_trigger_tolerance=edit_trigger_tolerance,
                                           edit_price_tolerance=edit_price_tolerance,
                                           stop_tolerance=stop_tolerance,
                                           time_budget=999999,
                                           delta_limit=delta_limit,
                                           slice_factor=slice_factor)

            else:
                exchange.logger.exception(f'unknown command {argv[0]}',exc_info=True)
                raise Exception(f'unknown command {argv[0]}',exc_info=True)

            exchange.running_symbols = [symbol
                                        for coin_data in exchange.risk_state.values()
                                        for symbol in coin_data.keys() if symbol in exchange.markets]
            await safe_gather([exchange.monitor_risk(),exchange.monitor_orders()]+ # exchange.monitor_fills(),
                                   [exchange.monitor_order_book(symbol)
                                    for symbol in exchange.running_symbols],semaphore=exchange.rest_semaphor)

        except myFtx.DoneDeal as e:
            allDone = True
        except myFtx.TimeBudgetExpired as e:
            allDone = True
        except Exception as e:
            exchange.logger.warning(e,exc_info=True)
            raise e
        finally:
            await exchange.cancel_all_orders()
            await exchange.close_dust()
            await exchange.close()
            exchange.logger.info('exchange closed',exc_info=True)
            return log_reader()

    return

def ftx_ws_spread_main(*argv):
    argv=list(argv)
    if len(argv) == 0:
        argv.extend(['sysperp'])
    if len(argv) < 3:
        argv.extend(['ftx', 'SysPerp'])
    logging.info(f'running {argv}')
    if argv[0] in ['sysperp', 'flatten','unwind','spread']:
        return asyncio.run(ftx_ws_spread_main_wrapper(*argv))
    else:
        logging.info(f'commands: sysperp [ftx][debug], flatten [ftx][debug],unwind [ftx][debug], spread [ftx][debug][coin][cash in usd]')

if __name__ == "__main__":
    ftx_ws_spread_main(*sys.argv[1:])