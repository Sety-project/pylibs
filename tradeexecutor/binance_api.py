from datetime import timezone, datetime
import dateutil

import numpy as np
import pandas as pd

import ccxtpro
from tradeexecutor.venue_api import CeFiAPI, PegRule, VenueAPI, loop, intercept_message_during_reconciliation
from utils.async_utils import safe_gather, safe_gather_limit
from utils.ccxt_utilities import api_params, calc_basis
from utils.config_loader import configLoader

class BinanceAPI(CeFiAPI,ccxtpro.binanceusdm):
    '''VenueAPI implements rest calls and websocket loops to observe raw market data / order events and place orders
    send events for Strategy to action
    send events to SignalEngine for further processing'''

    def get_id(self):
        return "binance"

    class Static(dict):
        _cache = dict()  # {function_name: result}

        @staticmethod
        async def build(exchange, symbols):
            result = BinanceAPI.Static()
            trading_fees = await exchange.fetch_trading_fees()
            for symbol in symbols:
                market = exchange.market(symbol)
                result[symbol] = \
                    {
                        'priceIncrement': float(next(_filter for _filter in market['info']['filters']
                                           if _filter['filterType'] == 'PRICE_FILTER')['tickSize']),
                        'sizeIncrement': float(next(_filter for _filter in market['info']['filters']
                                          if _filter['filterType'] == 'LOT_SIZE')['stepSize']),
                        'taker_fee': trading_fees[symbol]['taker'],
                        'maker_fee': trading_fees[symbol]['maker'],
                        'takerVsMakerFee': trading_fees[symbol]['taker'] - trading_fees[symbol]['maker']
                    }
            return result

       ### get all static fields TODO: only works for perps
        @staticmethod
        async def fetch_futures(exchange):
            if 'fetch_futures' in BinanceAPI.Static._cache:
                return BinanceAPI.Static._cache['fetch_futures']

            includeExpired = False
            includeIndex = False
            includeInverse = False

            perp_markets = await exchange.fetch_markets({'type': 'future'})
            #future_markets = await exchange.fetch_markets({'type': 'delivery'})
            margin_markets = await exchange.fetch_markets({'type': 'margin'})

            future_list = [f for f in perp_markets
                           if (('status' in f['info'] and f['info']['status'] == 'TRADING')) # only for linear perps it seems
                           and (f['info']['underlyingType'] == 'COIN' or includeIndex)]

            otc_file = configLoader.get_static_params_used()

            perp_list = [f['id'] for f in future_list if f['expiry'] == None]
            funding_rates = await safe_gather([exchange.fetch_funding_rate(symbol=f) for f in perp_list])
            # perp_tickers = await exchange.fetch_tickers(symbols=[f['id'] for f in future_list], params={'type':'future'})
            # delivery_tickers = await exchange.fetch_tickers(symbols=[f['id'] for f in future_list], params={'type': 'delivery'})
            open_interests = await safe_gather([exchange.fapiPublicGetOpenInterest({'symbol':f}) for f in perp_list])

            result = []
            for i in range(0, len(funding_rates)):
                funding_rate = funding_rates[i]
                market = next(m for m in perp_markets if m['symbol'] == funding_rate['symbol'])
                margin_market = next((m for m in margin_markets if m['symbol'] == funding_rate['symbol']), None)
                open_interest = next((m for m in open_interests if m['symbol'] == market['id']), None)

                index = funding_rate['indexPrice']
                mark = funding_rate['markPrice']
                expiryTime = dateutil.parser.isoparse(market['expiryDatetime']).replace(
                    tzinfo=timezone.utc) if bool(market['delivery']) else np.NaN
                if bool(market['delivery']):
                    future_carry = calc_basis(mark, index, expiryTime,
                                              datetime.utcnow().replace(tzinfo=timezone.utc))
                elif bool(market['swap']):
                    future_carry = funding_rate['fundingRate']*3*365.25
                else:
                    future_carry = 0

                result.append({
                    'symbol': exchange.safe_string(market, 'symbol'),
                    'index': index,
                    'mark': mark,
                    'name': exchange.safe_string(market, 'id'),
                    'perpetual': bool(exchange.safe_value(market, 'swap')),
                    'priceIncrement': float(next(_filter for _filter in market['info']['filters']
                                                 if _filter['filterType'] == 'PRICE_FILTER')['tickSize']),
                    'sizeIncrement': float(next(_filter for _filter in market['info']['filters']
                                                if _filter['filterType'] == 'LOT_SIZE')['stepSize']),
                    'mktSizeIncrement':
                        float(next(_filter for _filter in market['info']['filters']
                             if _filter['filterType'] == 'MARKET_LOT_SIZE')['stepSize']),
                    'underlying': market['base'],
                    'quote': market['quote'],
                    'type': 'perpetual' if market['swap'] else None,
                    'underlyingType': exchange.safe_number(market, 'underlyingType'),
                    'underlyingSubType': exchange.safe_number(market, 'underlyingSubType'),
                    'spot_ticker': '{}/{}'.format(market['base'], market['quote']),
                    'spotMargin': margin_market['info']['isMarginTradingAllowed'] if margin_market else False,
                    'cash_borrow': None,
                    'future_carry': future_carry,
                    'openInterestUsd': float(open_interest['openInterest'])*mark,
                    'expiryTime': expiryTime
                })

            BinanceAPI.Static._cache['fetch_futures'] = result
            return result

    def __init__(self, parameters, private_endpoints=True):
        config = {
            'enableRateLimit': True,
            'newUpdates': True}
        if private_endpoints:  ## David personnal
            config |= {'apiKey': 'V2KfGbMd9Zd9fATONTESrbtUtkEHFcVDr6xAI4KyGBjKs7z08pQspTaPhqITwh1M',
            'secret': api_params['binance']['key']}
        super().__init__(parameters)
        super(ccxtpro.binance,self).__init__(config=config)
        self.state = CeFiAPI.State()

        self.options['tradesLimit'] = VenueAPI.cache_size # TODO: shoud be in signalengine with a different name. inherited from ccxt....

        self.peg_rules: dict[str, PegRule] = dict()

    async def reconcile(self):
        raise NotImplementedError
        # fetch mark,spot and balances as closely as possible
        # shoot rest requests
        n_requests = int(safe_gather_limit / 3)
        p = [getattr(self, coro)(params={'dummy': i})
             for i in range(n_requests)
             for coro in ['fetch_markets', 'fetch_balance', 'fetch_positions']]
        results = await safe_gather(p, semaphore=self.strategy.rest_semaphore)
        # avg to reduce impact of latency
        markets_list = []
        for result in results[0::3]:
            res = pd.DataFrame(result).set_index('id')
            res['price'] = res['info'].apply(lambda f: float(f['price']) if f['price'] else -9999999)
            markets_list.append(res[['price']])
        markets = sum(markets_list) / len(markets_list)

        balances_list = [pd.DataFrame(r['info']['result']).set_index('coin')[['total']].astype(float) for r in
                         results[1::3]]
        balances = sum(balances_list) / len(balances_list)
        var = sum([bal * bal for bal in balances_list]) / len(balances_list) - balances * balances
        balances = balances[balances['total'] != 0.0].fillna(0.0)

        positions_list = [
            pd.DataFrame([r['info'] for r in result]).set_index('future')[['netSize', 'unrealizedPnl']].astype(float)
            for
            result in results[2::3]]
        positions = sum(positions_list) / len(positions_list)
        var = sum([pos * pos for pos in positions_list]) / len(positions_list) - positions * positions
        positions = positions[positions['netSize'] != 0.0].fillna(0.0)

        self.state.markets = markets.to_dict()
        self.state.balances = balances.to_dict()
        self.state.positions = positions.to_dict()
