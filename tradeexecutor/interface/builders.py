import collections
from utils.io_utils import myUtcNow

from tradeexecutor.binance.api import BinanceAPI
from tradeexecutor.gmx.api import GmxAPI
from tradeexecutor.external_signal_engine import ExternalSignal
from tradeexecutor.ftx.spreaddistribution_signal_engine import SpreadTradeSignal
from tradeexecutor.gmx.signal_engine import GLPSignal
from tradeexecutor.ftx.position_manager import FtxPositionManager
from tradeexecutor.binance.position_manager import BinancePositionManager
from tradeexecutor.gmx.position_manager import GMXPositionManager
from tradeexecutor.interface.order_manager import OrderManager

async def build_VenueAPI(parameters):
    if parameters['exchange'] == 'ftx':
        raise NotImplementedError
        # exchange = FtxAPI(parameters)
        # exchange.verbose = False
        # if 'subaccount' in parameters: {'FTX-SUBACCOUNT': parameters['subaccount'] if 'subaccount' in parameters else 'SysPerp'}
        # await exchange.authenticate()
        # await exchange.load_markets()
        # if 'symbol' in parameters:
        #     symbols = parameters['symbols']
        # else:
        #     futures = await FtxAPI.Static.fetch_futures(exchange)
        #     future_symbols = [future['new_symbol'] for future in futures]
        #     spot_symbols = [future['spot_ticker'] for future in futures if future['spot_ticker'] in exchange.markets]
        #     symbols = future_symbols + spot_symbols
        # exchange.static = await FtxAPI.Static.build(exchange, symbols)
    elif parameters['exchange'] == 'binanceusdm':
        exchange = BinanceAPI(parameters)
        exchange.verbose = False
        #if 'subaccount' in parameters: {'FTX-SUBACCOUNT': parameters['subaccount'] if 'subaccount' in parameters else 'SysPerp'}
        await exchange.authenticate()
        await exchange.load_markets()
        if 'symbol' in parameters:
            symbols = parameters['symbols']
        else:
            futures = await BinanceAPI.Static.fetch_futures(exchange)
            future_symbols = [future['symbol'] for future in futures]
            spot_symbols = [] #[future['spot_ticker'] for future in futures if future['spot_ticker'] in exchange.markets]
            symbols = future_symbols + spot_symbols
        exchange.static = await BinanceAPI.Static.build(exchange, symbols)
    elif parameters['exchange'] == 'gmx':
        exchange = GmxAPI(parameters)
    else:
        raise ValueError

    return exchange

async def build_SignalEngine(parameters):
    if not parameters:
        return None
    elif parameters['type'] == 'external':
        result = ExternalSignal(parameters)
    elif parameters['type'] == 'spread_distribution':
        result = SpreadTradeSignal(parameters)
    elif parameters['type'] == 'parent_strategy':
        result = GLPSignal(parameters)
        await parameters['parents']['GLP'].venue_api.reconcile()
    else:
        raise ValueError

    await result.set_weights()
    result.parameters['symbols'] = list(result.data.keys())

    result.orderbook = {symbol: collections.deque(maxlen=result.cache_size)
                      for symbol in parameters['symbols']}
    result.trades = {symbol: collections.deque(maxlen=result.cache_size)
                     for symbol in parameters['symbols']}

    return result

async def build_PositionManager(parameters):
    if parameters['exchange'] == 'gmx':
        result = GMXPositionManager(parameters)
    elif parameters['exchange'] == 'binanceusdm':
        result = BinancePositionManager(parameters)
    elif parameters['exchange'] == 'ftx':
        result = FtxPositionManager(parameters)
    else:
        raise ValueError

    result.limit = result.LimitBreached(parameters['check_frequency'], parameters['delta_limit'])

    for symbol in parameters['symbols']:
        result.data[symbol] = {'delta': 0,
                          'delta_timestamp': myUtcNow(),
                          'delta_id': 0}
    return result

async def build_OrderManager(parameters):
    if not parameters:
        return None
    else:
        return OrderManager(parameters)