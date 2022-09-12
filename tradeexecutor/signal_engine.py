import collections, os
import json
from datetime import timedelta, datetime, timezone

import aiofiles
import numpy as np
import pandas as pd

from histfeed.ftx_history import fetch_trades_history, vwap_from_list
from utils.async_utils import safe_gather

from utils.config_loader import configLoader
from utils.ftx_utils import sweep_price_atomic
from utils.io_utils import NpEncoder, myUtcNow


class SignalEngine(dict):
    '''SignalEngine computes derived data from venue_api and/or externally generated client orders
    key/values hold target'''
    def __init__(self, parameters):
        self.parameters = parameters
        self.strategy = None

        # raw data caches, filled by API. Processed data is in inheriting objects.
        self.orderbook = None
        self.trades_cache = None

    @staticmethod
    async def build(parameters):
        if not parameters:
            return None
        elif parameters['signal'] == 'external':
            result = ExternalSignal(parameters)
        elif parameters['signal'] == 'spread_distribution':
            result = SpreadTradeSignal(parameters)
        elif parameters['signal'] == 'listen':
            result = SignalEngine(parameters)
            result[parameters['symbol']] = 0
        await result.reconcile()
        parameters['symbols'] = list(result.keys())

        result.orderbook = {symbol: collections.deque(maxlen=parameters['cache_size'])
                          for symbol in parameters['symbols']}
        result.trades_cache = {symbol: collections.deque(maxlen=parameters['cache_size'])
                             for symbol in parameters['symbols']}

        return result

    async def reconcile(self):
        pass

    def to_dict(self):
        return {symbol:{'timestamp':self.timestamp} | data for symbol,data in self.items()}

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
        api = self.strategy.venue_api
        marketId = api.safe_string(message, 'market')
        if marketId in api.markets_by_id:
            symbol = api.markets_by_id[marketId]['symbol']
            item = {'timestamp': api.orderbooks[symbol]['timestamp']}
            if 'mid' in self.parameters['orderbook_granularity']:
                item |= {'mid': 0.5 * (api.orderbooks[symbol]['bids'][0][0] +
                                       api.orderbooks[symbol]['asks'][0][0])}
            if 'full' in self.parameters['orderbook_granularity']:
                item |= {key: api.orderbooks[symbol][key] for key in ['timestamp', 'bids', 'asks']}

            data = next((data for data in self.parameters['orderbook_granularity'] if
                        isinstance(data, dict) and ('depth' in data)),None)
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
    def __init__(self, parameters):
        if not os.path.isfile(parameters['filename']):
            raise Exception("{} not found".format(parameters['filename']))
        super().__init__(parameters)
        self.timestamp = None
        self.vwap = None

    async def reconcile(self):
        async with aiofiles.open(self.parameters['filename'], 'r') as fp:
            content = await fp.read()
        weights = json.loads(content)

        if dict(self) != weights:
            for symbol,data in weights.items():
                self[symbol] = data
            self.timestamp = myUtcNow()

    async def initialize_vwap(self):
        # initialize vwap_history
        nowtime = myUtcNow(return_type='datetime')
        frequency = timedelta(minutes=1)
        start = nowtime - timedelta(seconds=self.parameters['stdev_window'])
        vwap_history_list = await safe_gather([fetch_trades_history(
            self.strategy.venue_api.market(symbol)['id'], self.strategy.venue_api, start, nowtime, frequency=frequency)
            for symbol in self], semaphore=self.strategy.rest_semaphor)
        self.vwap = {symbol: data for symbol, data in
                       zip(self.parameters['symbols'], vwap_history_list)}

    def compile_vwap(self, frequency, purge=True):
        # get times series of target baskets, compute quantile of increments and add to last price
        for symbol in self.trades_cache:
            if len(self.trades_cache[symbol]) > 0:
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

    def __init__(self,parameters):
        super().__init__(parameters)

        self.spread_trades = []
        self.spread_vwap = []
        self.spread_distribution = []

    async def update_quoter_analytics(self):
        '''specialized to execute externally generated client orders'''
        raise Exception('not implemented')
        if os.path.isfile(self.parameters['filename']):
            await self.signal_engine.reconcile()
        else:
            targets = {key: 0 for key in self}
            spot = self.strategy.venue_api.mid(next(key for key in self if self.strategy.venue_api.market(key)['type'] == 'spot'))
            timestamp = myUtcNow()
        self.set_target(self.signal_engine)

        if not self.vwap:
            await self.initialize_vwap()
        self.compile_vwap(frequency=timedelta(minutes=1))

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
