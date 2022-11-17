import aiofiles
import collections, json
import itertools
from datetime import timedelta

import numpy as np
import pandas as pd

from utils.async_utils import safe_gather
from utils.io_utils import myUtcNow
from tradeexecutor.signal_engine import SignalEngine

class SpreadTradeSignal(SignalEngine):
    def __init__(self,parameters):
        '''parameters has n_paths,distribution_window_trades,quantiles'''
        super().__init__(parameters)

        self.spread_trades = collections.deque(maxlen=SignalEngine.cache_size)
        self.spread_vwap = collections.deque(maxlen=SignalEngine.cache_size)
        self.spread_distribution: dict[str,dict[str, collections.deque]] = dict()

    def process_trade(self, trade, purge = False):
        super().process_trade(trade)
        symbol = trade['symbol']
        other_leg = symbol.replace(':USD', '') if ':USD' in symbol else f'{symbol}:USD'
        if other_leg in self.strategy.venue_api.orderbooks:
            opposite_side_px = self.strategy.venue_api.sweep_price_atomic(other_leg,
                                                  trade['amount'] * trade['price'] * (1 if trade['side'] == 'buy' else -1))  # if taker bought, we hedge on the ask of the other leg
            self.spread_trades.append({'time': trade['datetime'],
                                       'size': trade['amount'],
                                       'side': trade['side'],
                                       'premium': opposite_side_px - trade['price'],
                                       'liquidation': trade['info']['liquidation'],
                                       'taker_symbol':symbol})

    async def set_weights(self):
        async with aiofiles.open(self.parameters['filename'], 'r') as fp:
            content = await fp.read()
        weights = json.loads(content)

        # clumsily, require position_manager.pv...
        if self.strategy is not None and self.strategy.position_manager is not None and self.strategy.position_manager.pv is not None:
            pv = self.strategy.position_manager.pv
        else:
            pv = None
        for coin, data in weights.items():
            self.data[f'{coin}/USD'] = (data * pv if pv is not None else None)
            self.data[f'{coin}/USD:USD'] = (data * pv if pv is not None else None)
        self.timestamp = myUtcNow()

    def serialize(self) -> list[dict]:
        res = [res for data in self.spread_distribution.values() for res in data.values()]
        return res

    async def initialize_vwap(self):
        # initialize vwap_history
        nowtime = myUtcNow(return_type='datetime')
        frequency = timedelta(minutes=1)
        start = nowtime - timedelta(seconds=self.parameters['stdev_window'])
        vwap_history_list = await safe_gather([self.strategy.venue_api.fetch_trades_history(
            self.strategy.venue_api.market(symbol)['id'], start, nowtime, frequency=frequency)
            for symbol in self.data], semaphore=self.strategy.rest_semaphore)
        self.vwap = {symbol: data for symbol, data in
                     zip(self.parameters['symbols'], vwap_history_list)}

    def compile_vwap(self, frequency):
        # get times series of target baskets, compute quantile of increments and add to last price
        for symbol in self.trades:
            if len(self.trades[symbol]) > 0:
                data = pd.DataFrame(self.trades[symbol])
                data = data[(data['timestamp'] > self.vwap[symbol]['vwap'].index.max().timestamp() * 1000)]
                if data.empty: continue

                # compute vwaps
                data['liquidation'] = data['info'].apply(lambda x: x['liquidation'])
                data.rename(columns={'datetime': 'time', 'amount': 'size'}, inplace=True)
                data['size'] = data.apply(lambda x: x['size'] if x['side'] == 'buy' else -x['size'], axis=1)
                data = data[['size', 'price', 'liquidation', 'time']]
                vwap = self.strategy.venue_api.vwap_from_list(frequency=frequency, trades=data)

                # append vwaps
                for key in self.vwap[symbol]:
                    updated_data = pd.concat([self.vwap[symbol][key],vwap[key]],axis=0)
                    self.vwap[symbol][key] = updated_data[~updated_data.index.duplicated()].sort_index().ffill()

    def compile_spread_distribution(self):
        for symbol,direction in itertools.product(self.data.keys(),['buy','sell']):
            spread_trades = [spread_trade for spread_trade in self.spread_trades
                             if spread_trade['taker_symbol'] == symbol
                             and spread_trade['side'] == direction][-self.parameters['distribution_window_trades']:-1]
            if symbol not in self.spread_distribution:
                self.spread_distribution[symbol] = {'buy': collections.deque(maxlen=SignalEngine.cache_size),
                                                    'sell': collections.deque(maxlen=SignalEngine.cache_size)}
            if len(spread_trades) == self.parameters['distribution_window_trades']-1:
                quantile = self.parameters['quantile'] if direction == 'buy' else 1 - self.parameters['quantile']
                level = np.quantile(np.array([trade['premium'] for trade in spread_trades]),
                                    q=quantile,
                                    method='normal_unbiased')
                self.spread_distribution[symbol][direction].append({'timestamp': myUtcNow(), 'level': level})