import aiofiles, collections, os, json
from abc import abstractmethod
from tradeexecutor.utils.config_loader import configLoader
from tradeexecutor.utils.io_utils import NpEncoder
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

    @abstractmethod
    async def set_weights(self):
        raise NotImplementedError
    @abstractmethod
    def serialize(self) -> list[dict]:
        raise NotImplementedError

    async def reconcile(self):
        await self.set_weights()
        for member_data in self.parameters['record_set']:
            if hasattr(self, member_data):
                if hasattr(self, f'compile_{member_data}'):
                    getattr(self, f'compile_{member_data}')()
        await self.to_json()

    async def to_json(self):
        strategies = list(self.strategy.parents.values())
        if strategies == []: strategies = [self.strategy]
        for strategy in strategies:
            venue_id = strategy.venue_api.get_id()
            dirname = configLoader.get_mktdata_folder_for_exchange(f'{venue_id}_tickdata')
            for member_data in self.parameters['record_set']:
                filename = os.path.join(os.sep, dirname, f'{member_data}_{self.createdAt.strftime("%Y%m%d_%H%M%S")}.json')
                async with aiofiles.open(filename, 'w+') as fp:
                    if isinstance(getattr(self, member_data), list) or isinstance(getattr(self, member_data),
                                                                                  collections.deque):
                        await fp.write(json.dumps(getattr(self, member_data), cls=NpEncoder))
                    elif isinstance(getattr(self, member_data), dict):
                        for key,data in getattr(self, member_data).items():
                            filename = os.path.join(os.sep, dirname, f'{member_data}_{self.createdAt.strftime("%Y%m%d_%H%M%S")}.json')
                            async with aiofiles.open(filename, 'w+') as fp:
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