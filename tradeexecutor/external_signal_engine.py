import aiofiles, json
from datetime import timedelta
import pandas as pd

from tradeexecutor.utils.async_utils import safe_gather
from tradeexecutor.utils.io_utils import myUtcNow
from tradeexecutor.interface.signal_engine import SignalEngine

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