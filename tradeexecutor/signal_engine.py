import aiofiles
import collections, os, json
import itertools
from datetime import timedelta
from abc import abstractmethod

import numpy as np
import pandas as pd

from histfeed.ftx_history import fetch_trades_history, vwap_from_list
from utils.async_utils import safe_gather
from utils.config_loader import configLoader
from utils.io_utils import NpEncoder, myUtcNow, nested_dict_to_tuple
from tradeexecutor.venue_api import GmxAPI
from tradeexecutor.interface.StrategyEnabler import StrategyEnabler

class SignalEngine(StrategyEnabler):
    '''SignalEngine computes derived data from venue_api and/or externally generated client orders
    key/values hold target'''
    cache_size = 100000

    def __init__(self, parameters):
        super().__init__(parameters)
        self.data: dict = dict()
        # raw data caches, filled by API. Processed data is in inheriting objects.
        self.orderbook = None
        self.trades = None
        self.vwap = None

    @staticmethod
    async def build(parameters):
        if not parameters:
            return None
        elif parameters['type'] == 'external':
            result = ExternalSignal(parameters)
        elif parameters['type'] == 'spread_distribution':
            result = SpreadTradeSignal(parameters)
        elif parameters['type'] == 'parent_strategy':
            result = GLPSignal(parameters)
        else:
            return None

        await result.set_weights()
        result.parameters['symbols'] = list(result.data.keys())

        result.orderbook = {symbol: collections.deque(maxlen=SignalEngine.cache_size)
                          for symbol in parameters['symbols']}
        result.trades = {symbol: collections.deque(maxlen=SignalEngine.cache_size)
                         for symbol in parameters['symbols']}

        return result

    @abstractmethod
    async def set_weights(self):
        raise NotImplementedError

    async def reconcile(self):
        await self.set_weights()
        for member_data in self.parameters['record_set']:
            if hasattr(self, member_data):
                if hasattr(self, f'compile_{member_data}'):
                    getattr(self, f'compile_{member_data}')()
        await self.to_json()

    async def to_json(self):
        coin = list(self.orderbook.keys())[0].replace(':USD', '').replace('/USD', '')

        for member_data in self.parameters['record_set']:
            if hasattr(self,member_data):
                if isinstance(getattr(self, member_data), list) or isinstance(getattr(self, member_data), collections.deque):
                    filename = os.path.join(os.sep, configLoader.get_mktdata_folder_for_exchange('ftx_tickdata'),
                                            f'{member_data}_{coin}.json')
                    async with aiofiles.open(filename,'w+') as fp:
                        await fp.write(json.dumps(getattr(self, member_data), cls=NpEncoder))
                elif isinstance(getattr(self, member_data), dict):
                    for key,data in getattr(self, member_data).items():
                        filename = os.path.join(os.sep,
                                                configLoader.get_mktdata_folder_for_exchange('ftx_tickdata'),
                                                f'{member_data}_{coin}.json')
                        async with aiofiles.open(filename,'w+') as fp:
                            await fp.write(json.dumps(data, cls=NpEncoder))

    def process_order_book_update(self, symbol, orderbook):
        item = {'timestamp': orderbook['timestamp']}
        if 'mid' in self.parameters['orderbook_granularity']:
            item |= {'mid': 0.5 * (orderbook['bids'][0][0] +
                                   orderbook['asks'][0][0])}
        if 'full' in self.parameters['orderbook_granularity']:
            item |= {key: orderbook[key] for key in ['timestamp', 'bids', 'asks']}

        data = next((data for data in self.parameters['orderbook_granularity'] if
                    isinstance(data, dict) and ('depth' in data)),None)
        if data:
            depth = data['depth']
            side_px = self.strategy.venue_api.sweep_price_atomic(symbol, depth)
            opposite_side_px = self.strategy.venue_api.sweep_price_atomic(symbol, -depth)
            item |= {'depth': depth, 'bid_at_depth': min(side_px, opposite_side_px),
                     'ask_at_depth': max(side_px, opposite_side_px)}
        self.orderbook[symbol].append(item)

    def process_trade(self, trade):
        self.trades[trade['symbol']].append(trade)

class ExternalSignal(SignalEngine):
    def __init__(self, parameters):
        super().__init__(parameters)

    async def set_weights(self):
        async with aiofiles.open(self.parameters['filename'], 'r') as fp:
            content = await fp.read()
        weights = json.loads(content)
        if self.data != weights:
            self.data = weights
            self.timestamp = myUtcNow()

    def serialize(self) -> list[dict]:
        return [{'symbol':symbol,'timestamp':self.timestamp} | data for symbol,data in self.data.items()]

    async def initialize_vwap(self):
        # initialize vwap_history
        nowtime = myUtcNow(return_type='datetime')
        frequency = timedelta(minutes=1)
        start = nowtime - timedelta(seconds=self.parameters['stdev_window'])
        vwap_history_list = await safe_gather([fetch_trades_history(
            self.strategy.venue_api.market(symbol)['id'], self.strategy.venue_api, start, nowtime, frequency=frequency)
            for symbol in self.data], semaphore=self.strategy.rest_semaphor)
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
                vwap = vwap_from_list(frequency=frequency, trades=data)

                # append vwaps
                for key in self.vwap[symbol]:
                    updated_data = pd.concat([self.vwap[symbol][key],vwap[key]],axis=0)
                    self.vwap[symbol][key] = updated_data[~updated_data.index.duplicated()].sort_index().ffill()

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
        vwap_history_list = await safe_gather([fetch_trades_history(
            self.strategy.venue_api.market(symbol)['id'], self.strategy.venue_api, start, nowtime, frequency=frequency)
            for symbol in self.data], semaphore=self.strategy.rest_semaphor)
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
                vwap = vwap_from_list(frequency=frequency, trades=data)

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

class GLPSignal(ExternalSignal):
    def __init__(self, parameters):
        if 'parents' not in parameters:
            raise Exception('parents needed')
        super().__init__(parameters)

        self.series = collections.deque(maxlen=SignalEngine.cache_size)
        for data in GmxAPI.static.values():
            if data['volatile']:
                self.data[data['normalized_symbol']] = None

    async def set_weights(self):
        lp_strategy = self.strategy.parents['GLP'] if self.strategy is not None else self.parameters['parents']['GLP']  # for the first time..
        gmx_state = lp_strategy.venue_api
        await gmx_state.reconcile()
        gmx_state.sanity_check()

        glp_position = lp_strategy.position_manager.data['GLP']['delta']/gmx_state.valuation()
        weights = {'ETH/USD:USD': {'target': - (lp_strategy.hedge_ratio-1) * glp_position * gmx_state.partial_delta('WETH'),
                           'benchmark': 0.5*(gmx_state.pricesDown['WETH']+gmx_state.pricesUp['WETH'])},
                 'AVAX/USD:USD': {'target': - (lp_strategy.hedge_ratio-1) * glp_position * gmx_state.partial_delta('WAVAX'),
                           'benchmark': 0.5*(gmx_state.pricesDown['WAVAX']+gmx_state.pricesUp['WAVAX'])},
                 'BTC/USD:USD': {'target': - (lp_strategy.hedge_ratio-1) * glp_position * (gmx_state.partial_delta('WBTC') + gmx_state.partial_delta('WBTC')),
                                 'benchmark': 0.5*(gmx_state.pricesDown['WBTC'] + gmx_state.pricesUp['WBTC'])}}

        if self.data != weights:
            # pls note also updates timestamp when benchmark changes :(
            self.data = weights
            self.timestamp = gmx_state.timestamp

    def serialize(self) -> list[dict]:
        return self.series

    async def to_json(self):
        filename = os.path.join(os.sep, configLoader.get_mktdata_folder_for_exchange('glp'),
                                f'history.json')
        with open(filename, "w") as f:
            json.dump([{json.dumps(k): v for k, v in nested_dict_to_tuple(item).items()} for item in self.series],
                      f, indent=1, cls=NpEncoder)
            self.strategy.logger.info('wrote to {} at {}'.format(filename, self.timestamp))

    def compile_pnlexplain(self, current_state, do_calcs=True):
        current = current_state.serialize()

        # compute risk
        if do_calcs:
            current |= {'delta': {key: current_state.partial_delta(key) for key in GmxAPI.static},
                        'valuation': {key: current_state.valuation(key) for key in GmxAPI.static}}
            current['delta']['total'] = sum(current['delta'].values())
            current['valuation']['total'] = sum(current['valuation'].values())
            # compute plex
            if len(self.series) > 0:
                previous = self.series[-1]
                # delta_pnl
                current |= {'delta_pnl': {
                    key: previous['delta'][key] * (current['pricesDown'][key] - previous['pricesDown'][key])
                    for key in GmxAPI.static}}
                current['delta_pnl']['total'] = sum(current['delta_pnl'].values())
                # other_pnl (non-delta)
                current |= {'other_pnl': {
                    key: current['valuation'][key] - previous['valuation'][key] - current['delta_pnl'][key] for key in
                    GmxAPI.static}}
                current['other_pnl']['total'] = sum(current['other_pnl'].values())
                # discrepancy btw actual and estimate
                current['discrepancy'] = {'total': (current['actualAum']['total'] - current['valuation']['total'])}

                # capital and tx cost. Don't update GLP.
                current['capital'] = {
                    key: abs(current['delta'][key]) * current['pricesDown'][key] * self.strategy.delta_buffer[key] for key
                    in GmxAPI.static}

                # delta hedge cost
                # TODO:self.strategy.hedge_strategy.venue_api.sweep_price_atomic(symbol, sizeUSD, include_taker_vs_maker_fee=True)
                current['tx_cost'] = {
                    key: -abs(current['delta'][key] - previous['delta'][key]) * self.strategy.hedge_tx_cost[key] for key in
                    GmxAPI.static}
                current['tx_cost']['total'] = sum(current['tx_cost'].values())
            else:
                # initialize capital and tx cost
                current['capital'] = {
                    key: abs(current['delta'][key]) * current['pricesDown'][key] * self.strategy.parents['GLP'].parameters['delta_buffer'] for key
                    in GmxAPI.static}
                current['capital']['total'] = current['actualAum']['total']
                # initial delta hedge cost + entry+exit of glp
                current['tx_cost'] = {key: -abs(current['delta'][key]) * self.strategy.parents['GLP'].parameters['hedge_tx_cost'] for key in
                                      GmxAPI.static}
                current['tx_cost']['total'] = sum(current['tx_cost'].values()) - 2 * GmxAPI.tx_cost * \
                                              current['actualAum']['total']
        # done. record.
        self.series.append(current)