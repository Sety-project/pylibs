from pylibs.staticdata.generator.staticdata_generator import StaticDataGenerator
# from pylibs.staticdata.generator.cex_drivers.kraken_futures import KrakenFuturesDriver
# from pylibs.staticdata.generator.cex_drivers.binance_derivs_usdt import BinanceDerivsUSDT
# from pylibs.staticdata.generator.cex_drivers.binance_derivs_coin import BinanceDerivsCoinM
# from pylibs.staticdata.generator.cex_drivers.huobi_gbl_coin import HuobiGlblCoinM
# from pylibs.staticdata.generator.cex_drivers.huobi_gbl_usd import HuobiGlblUSDM
# from pylibs.staticdata.generator.cex_drivers.huobi_futures import HuobiGlblFutures
# from pylibs.staticdata.generator.cex_drivers.poloniex_futures import PoloniexFutures
# from pylibs.staticdata.generator.cex_drivers.bitmex_sf import BitmexSF
# from pylibs.staticdata.generator.cex_drivers.hitbtc_CS import HitBTC
from pylibs.staticdata.generator.cex_drivers.ccxt_public import CcxtPublic
# from pylibs.staticdata.generator.OTC_drivers.lmax_digital import LmaxDigital
# from pylibs.staticdata.generator.OTC_drivers.lmax_fx import LmaxFX
# from pylibs.staticdata.generator.cex_drivers.bybit_inverse_swap import BybitInverseSwap
# from pylibs.staticdata.generator.cex_drivers.okex_drivers import OkexDriver
# from pylibs.staticdata.generator.cex_drivers.okex_futures import OkexFuturesDriver
# from pylibs.staticdata.generator.cex_drivers.okex_swaps import OkexSwapDriver
# from pylibs.staticdata.generator.cex_drivers.deribit_driver import DeribitDriver
from pylibs.staticdata.generator.cex_drivers.ftx_CSF import FTXDriver
from pylibs.staticdata.generator.cex_drivers.ftxus_C import FTXusDriver
# from pylibs.staticdata.generator.OTC_drivers.woorton import Woorton
# from pylibs.staticdata.generator.OTC_drivers.woorton_main import WoortonMain
# from pylibs.staticdata.generator.cex_drivers.kucoin_cash import KucoinCash
# from pylibs.staticdata.generator.cex_drivers.kucoin_derivatives import KucoinDerivatives
# from pylibs.staticdata.generator.cex_drivers.gate_io import GateIO
# from pylibs.staticdata.generator.cex_drivers.gate_io_USDM_swap import GateIOSwapUSDM
# from pylibs.staticdata.generator.cex_drivers.gate_io_BTCM_swap import  GateIOSwapBTCM
# from pylibs.staticdata.generator.cex_drivers.gate_io_BTCM_future import  GateIOFuturesBTCM
# from pylibs.staticdata.generator.cex_drivers.gate_io_USDM_future import  GateIOFuturesUSDM
# from pylibs.staticdata.generator.cex_drivers.bybit_cash import BybitCash
# from pylibs.staticdata.generator.cex_drivers.bybit_inverse_futures import BybitInverseFutures
# from pylibs.staticdata.generator.cex_drivers.bybit_linear_swap import BybitLinearSwap
# from pylibs.staticdata.generator.defi_drivers.dex.uniswap import UniswapV2, UniswapV3
# from pylibs.staticdata.generator.defi_drivers.dex.pancakeswap import PancakeSwapV2
# from pylibs.staticdata.generator.defi_drivers.dex.quickswap import QuickSwapV2
# from pylibs.staticdata.generator.cex_drivers.blockchain_cash import BlockchainCash

exchanges_list_to_update = [
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 15, 16, 17, 18,
    19, 20, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33,
    34, 35, 36, 37, 38, 39, 41, 42, 43, 44, 45, 46, 47, 48,
    49, 50, 51, 52, 53, 54, 55
    ]

def runner(exchanges_to_update):
    ccxt_map = {#1 :'bitmex',
           2: 'bitfinex2',
           3: 'coinbasepro',
           4: 'binance',
           5: 'poloniex',
           6: 'kraken',
           7: 'itbit',
           8: 'bittrex',
           9: 'bitstamp',
           10: 'gemini',
           #11: 'hitbtc',
           #12 : 'krakenrest',
           13: 'binanceus',
           #14: 'Binanceje',
           #15: 'binance',
           16: 'exx',
           17: 'huobipro',
           #18: huobi_DM,
           19: 'okcoin',
           #20: 'okex',
           #22: 'bybit',
           #21 : "coinbene",
           #23: 'ftx',
           24: 'bitcoincom',
           25: 'ascendex',
           #26: 'deribit',
           27: 'phemex',
           28: 'upbit',
           35: 'lbank'}

    gen = StaticDataGenerator()

    try:
        gen.add_driver(FTXusDriver(), exchanges_to_update)
    except:
        print("NO DATA FOR: FTXusDriver")
    try:
        gen.add_driver(FTXDriver(), exchanges_to_update)
    except:
        print("NO DATA FOR: FTXDriver")

    gen.build_static()
    return gen.get_new_static()


def main(*args):
    ''' This is the main runner to feed persist the data on a set of days for a given config
    @:param args should contain at least one date, one config number and one s3_region
    '''

    args = list(*args)

    arg_count = len(args)
    runner(exchanges_list_to_update)

# import urllib3.util.connection as urllib3_cn
#
# def allowed_gai_family():
#     '''
#     Function to use the requests package to use Ipv4 protocol
#     :return: ipv4 socket
#     '''
#     import socket
#
#     family = socket.AF_INET
#     return family
#
# urllib3_cn.allowed_gai_family = allowed_gai_family
#
# def test():

#     import requests
#     import logging
#     import json
#     import http.client
#     # http.client.HTTPConnection.debuglevel = 1
#     #
#     # # You must initialize logging, otherwise you'll not see debug output.
#     # logging.basicConfig()
#     # logging.getLogger().setLevel(logging.DEBUG)
#     # requests_log = logging.getLogger("requests.packages.urllib3")
#     # requests_log.setLevel(logging.DEBUG)
#     # requests_log.propagate = True
#
#     # a = requests.get("https://ftx.com/api/markets", timeout=0.05)
#     a = requests.get("https://ftx.com/api/markets")
#     b = json.loads(a.content.decode('utf-8'))['result']
#     print(b)
#
# def test2():
#
#     import requests
#     import time
#     import logging
#     logging.basicConfig(level=logging.DEBUG)
#     url = "https://bittrex.com/api/v1.1/public/getorderbook?market=BTC-LTC&type=both"
#     session = requests.Session()
#     session.trust_env = False
#
#     for i in range(10):
#         start = time.time()
#
#         session.get(url)
#
#         end = time.time()
#
#         print('Time: {}. index: {}'.format(end - start, i))