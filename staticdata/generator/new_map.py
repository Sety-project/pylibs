new_map = {'BitmexDerivsFutures' : 1,#https://www.bitmex.com/api/v1/instrument/activeAndIndices
'BitfinexCashDerivs' : 2,#https://api-pub.bitfinex.com/v2/' + https://api-pub.bitfinex.com/v2/conf/pub:list:pair:futures
#+https://api.bitfinex.com/v1/symbols_details
'CoinbaseCash' : 3,#https://api.pro.coinbase.com/currencies + https://api.pro.coinbase.com/products
'BinanceCash' : 4,#https://api.binance.com/api/v3/exchangeInfo
'PoloniexCash': 5,#https://poloniex.com/public
'KrakenCash' : 6,#https://api.kraken.com/0/public + https://api.kraken.com/0/public/Assets + https://api.kraken.com/0/public/AssetPairs
'ItbitCash' : 7,#https://api.itbit.com/v1/
'BittrexCash' : 8,#https://api.bittrex.com/v3/markets + https://api.bittrex.com/v3/currencies
'BitstampCash' : 9,# https://www.bitstamp.net/api/v2/trading-pairs-info/
'GeminiCash' : 10,#https://docs.gemini.com/rest-api
'HITBTC' : 11,#https://api.hitbtc.com/api/2/public/currency +  https://api.hitbtc.com/api/2/public/symbol
'KrakenRest' : 12,#off
'BinanceusCash' : 13,#https://api.binance.us/api/v1/exchangeInfo
'BinancejeCash' : 14,#off
'BinanceUSDMDerivs' : 15,#https://fapi.binance.com/fapi/v1/exchangeInfo
'EXXCash' : 16,#https://api.exx.com/data/v1/markets
'HuobiGlobalCash' :17,#https://api.huobi.pro/v1/settings/currencys?language=en-US+ https://api.huobi.pro/v1/common/symbols
'HuobiGlobalDM' : 18,#https://api.hbdm.com
'OkcoinCash' :19,#https://www.okcoin.com/api/spot/v3/instruments
'OKEXCashDerivsFutures' : 20,#https://www.okex.com/api/spot/v3/instruments + https://www.okex.com/api/futures/v3/instruments +https://www.okex.com/api/swap/v3/instruments
'CoinbeneCash': 21,#off
'BybitDerivsFutures' : 22,#https://api.bybit.com/v2/public/symbols
'FTXCashDerivs' : 23,#https://ftx.com/api/coins +https://ftx.com/api/markets
'Bitcoin.com_C' : 24,#
'AscendexCashDerivs' : 25,#https://ascendex.com/api/pro/v1/assets + https://ascendex.com/api/pro/v1/margin/assets+https://ascendex.com/api/pro/v1/cash/assets
#https://ascendex.com/api/pro/v1/products + https://ascendex.com/api/pro/v1/cash/products +https://ascendex.com/api/pro/v1/futures/contracts
'DeribitDerivsFuturesOptions' : 26,
'PhemexCashDerivs':27,#'https://api.phemex.com/v1/exchange/public/products'
#PhemexWebsocketInterface.cpp https://api.phemex.com/exchange/public/cfg/v2/products + https://api.phemex.com/exchange/public/cfg/v2/products
'HuobiGlobalDerivsSwapUSDM' : 28,#TBD
'HuobiGlobalDerivsSwapCoinM' : 29,#TBD
'PhemexCashDerivs':27,#PhemexWebsocketInterface.cpp https://api.phemex.com/exchange/public/cfg/v2/products + https://api.phemex.com/exchange/public/cfg/v2/products
'UpbitCash': 28,#https://api.upbit.com/v1/market/all    UpbitWebsocketInterface.cpp
'PoloniexFutures' : 32,#https://futures-api.poloniex.com#
'BinanceCoinM' :30,
'KrakenFutures' : 29, #https://futures.kraken.com/derivatives/api/v3'

}

active_exchange_map = [{'exchangeid': 1, 'exchange': 'Bitmex_SF', 'active': True},
                       {'exchangeid': 2, 'exchange': 'Bitfinex_CS', 'active': True},
                       {'exchangeid': 3, 'exchange': 'Coinbase_C', 'active': True},
                       {'exchangeid': 4, 'exchange': 'Binance_C', 'active': True},
                       {'exchangeid': 5, 'exchange': 'Poloniex_C', 'active': True},
                       {'exchangeid': 6, 'exchange': 'Kraken_C' , 'active': True},
                       {'exchangeid': 7, 'exchange': 'Itbit_C' , 'active': True},
                       {'exchangeid': 8, 'exchange': 'Bittrex_C', 'active': True},
                       {'exchangeid': 9, 'exchange': 'Bitstamp_C' , 'active': True},
                       {'exchangeid': 10, 'exchange': 'Gemini_C', 'active': True},
                       {'exchangeid': 11, 'exchange': 'HITBTC_CS', 'active': True},
                       #{'exchangeid': 12, 'exchange': 'KrakenRest', 'active': False},#off
                       {'exchangeid': 13, 'exchange': 'Binanceus_C' , 'active': True},
                       # {'exchangeid': 14, 'exchange': 'Binanceje_C', 'active': False},#off
                       {'exchangeid': 15, 'exchange': 'BinanceUSDM_SF', 'active': True},
                       {'exchangeid': 16, 'exchange': 'EXX_C', 'active': True},
                       {'exchangeid': 17, 'exchange': 'HuobiGbl_C', 'active': True},
                       {'exchangeid': 18, 'exchange': 'HuobiGbl_USD_M_S', 'active': True},
                       {'exchangeid': 19, 'exchange': 'Okcoin_C' , 'active': True},
                       {'exchangeid': 20, 'exchange': 'OKEX_C', 'active': True},
                       #{'exchangeid': 21, 'exchange': 'Coinbene_C', 'active': False},#off
                       {'exchangeid': 22, 'exchange': 'BybitInverse_S', 'active': True},
                       {'exchangeid': 23, 'exchange': 'FTX_CSF', 'active': True},
                       {'exchangeid': 24, 'exchange': 'Bitcoin.com_C', 'active': True},
                       {'exchangeid': 25, 'exchange': 'Ascendex_CS', 'active': True },
                       {'exchangeid': 26, 'exchange': 'Deribit_SFO', 'active': True},
                       {'exchangeid': 27, 'exchange': 'Phemex_CS', 'active': True},
                       {'exchangeid': 28, 'exchange': 'Upbit_C', 'active': True},
                       {'exchangeid': 29, 'exchange': 'Kraken_SF', 'active': True},
                       {'exchangeid': 30, 'exchange': 'BinanceCoinM_SF', 'active': True} ,
                       {'exchangeid': 31, 'exchange': 'HuobiGblCoin_S', 'active': True},
                       {'exchangeid': 32, 'exchange': 'Poloniex_S', 'active': True},
                       {'exchangeid': 33, 'exchange': 'Okex_S', 'active': True},
                       {'exchangeid': 34, 'exchange': 'Okex_F', 'active': True},
                       {'exchangeid': 35, 'exchange': 'Lbank_SFO', 'active': True},
                       {'exchangeid': 36, 'exchange': 'Lmax_digital_C', 'active': True},
                       {'exchangeid': 37, 'exchange': 'Lmax_fx_C', 'active': True},
                       {'exchangeid': 38, 'exchange': 'Huobi_F', 'active': True},
                       {'exchangeid': 39, 'exchange': 'FTXus_C', 'active': True},
                       {'exchangeid': 40, 'exchange': 'Woorton_C', 'active': True},
                       {'exchangeid': 41, 'exchange': 'Kucoin_C', 'active': True},
                       {'exchangeid': 42, 'exchange': 'Kucoin_S', 'active': True},
                       {'exchangeid': 43, 'exchange': 'GateIO_C', 'active': True},
                       {'exchangeid': 44, 'exchange': 'GateIO_USDM_S', 'active': True},
                       {'exchangeid': 45, 'exchange': 'GateIO_CoinM_S', 'active': True},
                       {'exchangeid': 46, 'exchange': 'GateIO_USDM_F', 'active': True},
                       {'exchangeid': 47, 'exchange': 'GateIO_CoinM_F', 'active': True},
                       {'exchangeid': 48, 'exchange': 'Bybit_C', 'active': True},
                       {'exchangeid': 49, 'exchange': 'BybitLinear_S', 'active': True},
                       {'exchangeid': 50, 'exchange': 'BybitFutures_F', 'active': True},
                       {'exchangeid': 51, 'exchange': 'UniswapV2', 'active': True},
                       {'exchangeid': 52, 'exchange': 'UniswapV3', 'active': True},
                       {'exchangeid': 53, 'exchange': 'PancakeSwapV2', 'active': True},
                       {'exchangeid': 54, 'exchange': 'QuickSwapV2', 'active': True},
                       {'exchangeid': 55, 'exchange': 'Blockchain_C', 'active': True},
                       ]

