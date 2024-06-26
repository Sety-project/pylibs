import asyncio
import datetime
from utils.ccxt_utilities import open_exchange
from utils.async_utils import safe_gather
import pandas as pd

async def futs():
    exchange = await open_exchange('binance','')
    markets = await exchange.fetch_markets({'type': 'future'})
    ftt_futures = [market['symbol'] for market in markets if market['swap'] == True]
    list_oi = await safe_gather([exchange.fetch_open_interest_history(market, limit=500) for market in ftt_futures])
    return dict(zip(ftt_futures, [{datetime.datetime.fromtimestamp(y['timestamp']/1000) : float(y['info']['sumOpenInterestValue']) for y in x} for x in list_oi]))

d = asyncio.run(futs())
pd.concat([pd.Series(name=key,data=data).sort_index() for key,data in d.items()])
print(d)