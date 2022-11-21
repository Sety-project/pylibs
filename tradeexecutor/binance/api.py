from datetime import timezone, datetime, timedelta

import ccxt.base.errors

from utils.io_utils import ignore_error, async_to_csv, myUtcNow
import dateutil, os, asyncio

import numpy as np
import pandas as pd

import ccxtpro
from tradeexecutor.interface.venue_api import CeFiAPI, PegRule, VenueAPI
from utils.async_utils import safe_gather
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
        async def build(self, symbols):
            result = BinanceAPI.Static()
            trading_fees = await self.fetch_trading_fees()
            for symbol in symbols:
                market = self.market(symbol)
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
        async def fetch_futures(self):
            if 'fetch_futures' in BinanceAPI.Static._cache:
                return BinanceAPI.Static._cache['fetch_futures']

            includeExpired = False
            includeIndex = False
            includeInverse = False

            perp_markets = await self.fetch_markets({'type': 'future'})
            #future_markets = await self.fetch_markets({'type': 'delivery'})
            margin_markets = await self.fetch_markets({'type': 'margin'})

            future_list = [f for f in perp_markets
                           if (('status' in f['info'] and f['info']['status'] == 'TRADING')) # only for linear perps it seems
                           and (f['info']['underlyingType'] == 'COIN' or includeIndex)]

            otc_file = configLoader.get_static_params_used()

            perp_list = [f['id'] for f in future_list if f['expiry'] == None]
            funding_rates = await safe_gather([self.fetch_funding_rate(symbol=f) for f in perp_list])
            # perp_tickers = await self.fetch_tickers(symbols=[f['id'] for f in future_list], params={'type':'future'})
            # delivery_tickers = await self.fetch_tickers(symbols=[f['id'] for f in future_list], params={'type': 'delivery'})
            open_interests = await safe_gather([self.fapiPublicGetOpenInterest({'symbol':f}) for f in perp_list])

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
                    'symbol': self.safe_string(market, 'symbol'),
                    'index': index,
                    'mark': mark,
                    'name': self.safe_string(market, 'id'),
                    'perpetual': bool(self.safe_value(market, 'swap')),
                    'priceIncrement': float(next(_filter for _filter in market['info']['filters']
                                                 if _filter['filterType'] == 'PRICE_FILTER')['tickSize']),
                    'sizeIncrement': max(float(next(_filter for _filter in market['info']['filters']
                                                    if _filter['filterType'] == 'LOT_SIZE')['minQty']),
                                         float(next(_filter for _filter in market['info']['filters']
                                                    if _filter['filterType'] == 'MIN_NOTIONAL')['notional'])/mark),
                    'mktSizeIncrement':
                        max(float(next(_filter for _filter in market['info']['filters']
                                       if _filter['filterType'] == 'MARKET_LOT_SIZE')['minQty']),
                            float(next(_filter for _filter in market['info']['filters']
                                       if _filter['filterType'] == 'MIN_NOTIONAL')['notional']) / mark),
                    'underlying': market['base'],
                    'quote': market['quote'],
                    'type': 'perpetual' if market['swap'] else None,
                    'underlyingType': self.safe_number(market, 'underlyingType'),
                    'underlyingSubType': self.safe_number(market, 'underlyingSubType'),
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
        # fetch mark,spot and balances as closely as possible
        # shoot rest requests
        p = [self.fetch_tickers(),
             self.fetch_account_positions(params={'type': 'future', 'all': True}),
             self.fetch_account_positions(params={'type': 'delivery', 'all': True})]
        results = await safe_gather(p, semaphore=self.strategy.rest_semaphore)

        # avg to reduce impact of latency
        markets_list = []
        for result in results[0::3]:
            res = pd.DataFrame(list(result.values()), columns=['symbol', 'average'])
            res['symbol'] = res['symbol'].apply(lambda x: self.market(x)['symbol'])
            res.set_index('symbol', inplace=True)
            markets_list.append(res['average'])
        markets = (sum(markets_list) / len(markets_list))

        fapi_balances = pd.Series(results[1]['assets']['total'])
        fapi_balances = fapi_balances[fapi_balances != 0.0].dropna()

        dapi_balances = pd.Series(results[2]['assets']['total'])
        dapi_balances = dapi_balances[dapi_balances != 0.0].dropna()

        balances = pd.concat([fapi_balances, dapi_balances], axis=1, join='outer').sum(axis=1).dropna()
        balances = balances[balances != 0]

        fapi_positions = pd.DataFrame(results[1]['positions'], columns=['symbol', 'contracts', 'side']).set_index('symbol')
        fapi_positions = fapi_positions[fapi_positions['side'].isin(['long','short'])]
        fapi_positions['positionAmt'] = fapi_positions.apply(lambda p: p['contracts'] if p['side'] == 'long' else -p['contracts'],axis=1)

        dapi_positions = pd.DataFrame(results[2]['positions'], columns=['symbol', 'contracts', 'side']).set_index('symbol')
        dapi_positions = dapi_positions[dapi_positions['side'].isin(['long','short'])]
        dapi_positions['positionAmt'] = dapi_positions.apply(lambda p: p['contracts'] if p['side'] == 'long' else -p['contracts'],axis=1)

        positions = pd.concat([fapi_positions,dapi_positions],axis=0)['positionAmt'].dropna()
        positions = positions[positions != 0]

        self.state.markets = markets.to_dict()
        self.state.balances = balances.to_dict()
        self.state.positions = positions.to_dict()

    # --------------------------------------------------------------------------------------------
    # ---------------------------------- various helpers -----------------------------------------
    # --------------------------------------------------------------------------------------------

    async def fetch_account_positions(self, symbols=None, params={'all':False}):
        """
        override of ccxt to yield balance and positions
        fetch account positions
        :param [str]|None symbols: list of unified market symbols
        :param dict params: extra parameters specific to the binance api endpoint
        :returns dict: data on account positions
        """
        if symbols is not None:
            if not isinstance(symbols, list):
                raise ccxt.base.errors.ArgumentsRequired(self.id + ' fetchPositions() requires an array argument for symbols')
        await self.load_markets()
        await self.load_leverage_brackets(False, params)
        method = None
        defaultType = self.safe_string(self.options, 'defaultType', 'future')
        type = self.safe_string(params, 'type', defaultType)
        query = self.omit(params, 'type')
        if type == 'future':
            method = 'fapiPrivateGetAccount'
        elif type == 'delivery':
            method = 'dapiPrivateGetAccount'
        else:
            raise ccxt.base.errors.NotSupported(self.id + ' fetchPositions() supports linear and inverse contracts only')
        account = await getattr(self, method)(query)
        positions = self.parse_account_positions(account)

        # cash_carry_legacy behaviour
        if not params['all']:
            symbols = self.market_symbols(symbols)
            return self.filter_by_array(positions, 'symbol', symbols, False)
        else:
            account['assets'] = self.parse_balance(account)
            symbols = self.market_symbols(symbols)
            account['positions'] = self.filter_by_array(positions, 'symbol', symbols, False)
            return account

    async def fetch_borrow_rate_history(self, code, since=None, limit=None, params={}):
        """ fix interface bug in ccxt
        retrieves a history of a currencies borrow interest rate at specific time slots
        :param str code: unified currency code
        :param int|None since: timestamp for the earliest borrow rate
        :param int|None limit: the maximum number of `borrow rate structures <https://docs.ccxt.com/en/latest/manual.html#borrow-rate-structure>` to retrieve
        :param dict params: extra parameters specific to the exchange api endpoint
        :returns [dict]: an array of `borrow rate structures <https://docs.ccxt.com/en/latest/manual.html#borrow-rate-structure>`
        """
        await self.load_markets()
        if limit is None:
            limit = 93
        elif limit > 93:
            # Binance API says the limit is 100, but "Illegal characters found in a parameter." is returned when limit is > 93
            raise ccxt.base.errors.BadRequest(self.id + ' fetchBorrowRateHistory() limit parameter cannot exceed 92')
        currency = self.currency(code)
        request = {
            'asset': currency['id'],
            'limit': limit,
        }
        if since is not None:
            request['startTime'] = since
            endTime = self.sum(since, limit * 86400000) - 1  # required when startTime is further than 93 days in the past
            now = self.milliseconds()
            request['endTime'] = min(endTime, now)  # cannot have an endTime later than current time
        response = await self.sapiGetMarginInterestRateHistory(self.extend(request, params))
        #
        #     [
        #         {
        #             "asset": "USDT",
        #             "timestamp": 1638230400000,
        #             "dailyInterestRate": "0.0006",
        #             "vipLevel": 0
        #         },
        #     ]
        #
        return self.parse_borrow_rate_history(response, code, since, limit)

    async def fetch_open_orders(self, symbol=None, since=None, limit=None, params={}):
        """ fix bug in ccxt
        fetch all unfilled currently open orders
        :param str|None symbol: unified market symbol
        :param int|None since: the earliest time in ms to fetch open orders for
        :param int|None limit: the maximum number of  open orders structures to retrieve
        :param dict params: extra parameters specific to the binance api endpoint
        :param str|None params['marginMode']: 'cross' or 'isolated', for spot margin trading
        :returns [dict]: a list of `order structures <https://docs.ccxt.com/en/latest/manual.html#order-structure>`
        """
        await self.load_markets()
        market = None
        request = {}
        marginMode, query = self.handle_margin_mode_and_params('fetchOpenOrders', params)
        if symbol is not None:
            market = self.market(symbol)
            request['symbol'] = market['id']
            defaultType = self.safe_string_2(self.options, 'fetchOpenOrders', 'defaultType', 'spot')
        elif self.options['warnOnFetchOpenOrdersWithoutSymbol']:
            symbols = self.symbols
            numSymbols = len(symbols)
            fetchOpenOrdersRateLimit = int(numSymbols / 2)
            raise ccxt.base.errors.ExchangeError(self.id + ' fetchOpenOrders() WARNING: fetching open orders without specifying a symbol is rate-limited to one call per ' + str(fetchOpenOrdersRateLimit) + ' seconds. Do not call self method frequently to avoid ban. Set ' + self.id + '.options["warnOnFetchOpenOrdersWithoutSymbol"] = False to suppress self warning message.')
        else:
            defaultType = self.safe_string_2(self.options, 'fetchOpenOrders', 'defaultType', 'spot')
        type = self.safe_string(query, 'type', defaultType)
        requestParams = self.omit(query, 'type')
        method = 'privateGetOpenOrders'
        if type == 'future':
            method = 'fapiPrivateGetOpenOrders'
        elif type == 'delivery':
            method = 'dapiPrivateGetOpenOrders'
        elif type == 'margin' or marginMode is not None:
            method = 'sapiGetMarginOpenOrders'
            if marginMode == 'isolated':
                request['isIsolated'] = True
                if symbol is None:
                    raise ccxt.base.errors.ArgumentsRequired(self.id + ' fetchOpenOrders() requires a symbol argument for isolated markets')
        response = await getattr(self, method)(self.extend(request, requestParams))
        return self.parse_orders(response, market, since, limit)

    def parse_trade(self, trade, market=None):
        result = super().parse_trade(trade, market)
        if 'info' in trade and 'c' in trade['info']:
            result |= {'clientOrderId': trade['info']['c']}
        return result

    def mid(self,symbol):
        if symbol == 'USDT/USDT': return 1.0
        data = self.tickers[symbol]['mid'] if symbol in self.tickers else self.state.markets[symbol]
        return data

    ### only perps, only borrow and funding, only hourly, time is fixing / payment time.
    @ignore_error
    async def borrow_history(self, coin,
                             end=(datetime.now(tz=timezone.utc).replace(minute=0, second=0, microsecond=0)),
                             start=(datetime.now(tz=timezone.utc).replace(minute=0, second=0,
                                                                          microsecond=0)) - timedelta(days=30),
                             dirname=''):
        max_funding_data = int(92)  # in days, limit is 92 only
        resolution = pd.Timedelta(self.describe()['timeframes']['1d']).total_seconds()

        e = end.timestamp()
        s = start.timestamp()
        f = max_funding_data * resolution
        start_times = [int(round(e - k * f)) for k in range(1 + int((e - s) / f)) if e - k * f > s] + [s]

        lists = await safe_gather([
            self.fetch_borrow_rate_history(coin, since=int(start_time * 1000), limit=max_funding_data)
            for start_time in start_times])
        borrow = [y for x in lists for y in x]

        data = pd.DataFrame(borrow)
        data = data.set_index('timestamp')[['rate']] * 365.25
        data.rename(columns={'rate': coin + '_rate_borrow'}, inplace=True)
        data.index = [datetime.fromtimestamp(x / 1000) for x in data.index]
        data = data[~data.index.duplicated()].sort_index()

        if dirname != '': await async_to_csv(data, os.path.join(dirname, coin + '_borrow.csv'), mode='a', header=False)

    ######### annualized funding for perps, time is fixing / payment time.
    @ignore_error
    async def funding_history(self, future,
                              start=(datetime.now(tz=timezone.utc).replace(minute=0, second=0,
                                                                           microsecond=0)) - timedelta(days=30),
                              end=(datetime.now(tz=timezone.utc).replace(minute=0, second=0, microsecond=0)),
                              dirname=''):

        max_funding_data = int(100)  # in hour. limit is 100 :(
        resolution = pd.Timedelta(self.describe()['timeframes']['1h']).total_seconds()

        e = end.timestamp()
        s = start.timestamp()
        f = max_funding_data * resolution
        start_times = [int(round(s + k * f)) for k in range(1 + int((e - s) / f)) if s + k * f < e]

        lists = await safe_gather([
            self.fetch_funding_rate_history(self.market(future['symbol'])['symbol'],
                                                params={'startTime': start_time * 1000,
                                                        'endTime': (start_time + f) * 1000})
            for start_time in start_times])
        funding = [y for x in lists for y in x]

        if len(funding) > 0:
            data = pd.DataFrame(funding)
            data['time'] = data['timestamp'].astype(dtype='int64')
            data[self.market(future['symbol'])['id'] + '_rate_funding'] = data['fundingRate'] * 365.25 * 8
            data = data[['time', self.market(future['symbol'])['id'] + '_rate_funding']].set_index('time')
            data.index = [datetime.fromtimestamp(x / 1000) for x in data.index]
            data = data[~data.index.duplicated()].sort_index()

            if dirname != '': await async_to_csv(data, os.path.join(dirname,
                                                                    self.market(future['symbol'])[
                                                                        'id'] + '_funding.csv'),
                                                 mode='a', header=False)

    #### annualized rates for futures and perp, volumes are daily
    @ignore_error
    async def rate_history( self, future,
                           end=datetime.now(tz=timezone.utc).replace(minute=0, second=0, microsecond=0),
                           start=datetime.now(tz=timezone.utc).replace(minute=0, second=0, microsecond=0) - timedelta(
                               days=30),
                           timeframe='1h',
                           dirname=''):
        symbol = self.market(future['symbol'])['symbol']

        max_mark_data = int(500)
        resolution = pd.Timedelta(self.describe()['timeframes'][timeframe]).total_seconds()

        e = end.timestamp()
        s = start.timestamp()
        f = max_mark_data * resolution
        start_times = [int(round(s + k * f)) for k in range(1 + int((e - s) / f)) if s + k * f < e]

        mark_indexes = await safe_gather([
            self.fetch_ohlcv(symbol, timeframe=timeframe, params=params)  # volume is for max_mark_data*resolution
            for start_time in start_times
            for params in [{'startTime': start_time * 1000, 'endTime': (start_time + f - resolution) * 1000},
                           {'start_time': start_time, 'end_time': start_time + f - resolution, 'price': 'index'}]])
        mark = [y for x in mark_indexes[::2] for y in x]
        indexes = [y for x in mark_indexes[1::2] for y in x]
        column_names = ['t', 'o', 'h', 'l', 'c', 'volume']

        ###### indexes
        indexes = pd.DataFrame([dict(zip(column_names, row)) for row in indexes], dtype=float).astype(
            dtype={'t': 'int64'}).set_index('t')
        indexes['volume'] = indexes['volume'] * 24 * 3600 / resolution

        ###### marks
        mark = pd.DataFrame([dict(zip(column_names, row)) for row in mark]).astype(dtype={'t': 'int64'}).set_index('t')
        mark['volume'] = mark['volume'] * 24 * 3600 / resolution

        mark.columns = ['mark_' + column for column in mark.columns]
        indexes.columns = ['indexes_' + column for column in indexes.columns]

        ##### openInterestUsd
        max_oi_data = 500
        e = end.timestamp()
        s = start.timestamp()
        f = max_oi_data * resolution
        start_times = [int(round(s + k * f)) for k in range(1 + int((e - s) / f)) if s + k * f < e]

        openInterest_list = await safe_gather([
            self.fetch_open_interest_history(symbol, timeframe='5m' if timeframe == '1m' else timeframe,
                                                 since=int(start_time * 1000), limit=max_oi_data)
            for start_time in start_times])
        openInterest_list = [y for x in openInterest_list for y in x]
        openInterest = pd.DataFrame(openInterest_list, columns=['timestamp', 'openInterestAmount']).astype(
            dtype={'timestamp': 'int64'}).set_index('timestamp')

        data = mark.join(indexes, how='inner').join(openInterest, how='inner')

        ########## rates from index to mark
        if future['type'] == 'future':
            expiry_time = dateutil.parser.isoparse(future['expiry']).timestamp()
            data['rate_T'] = data.apply(lambda t: (expiry_time - int(t.name) / 1000) / 3600 / 24 / 365.25, axis=1)

            data['rate_c'] = data.apply(
                lambda y: calc_basis(y['mark_c'],
                                     indexes.loc[y.name, 'indexes_c'], future['expiryTime'],
                                     datetime.fromtimestamp(int(y.name / 1000), tz=timezone.utc)), axis=1)
            data['rate_h'] = data.apply(
                lambda y: calc_basis(y['mark_h'], indexes.loc[y.name, 'indexes_h'], future['expiryTime'],
                                     datetime.fromtimestamp(int(y.name / 1000), tz=timezone.utc)), axis=1)
            data['rate_l'] = data.apply(
                lambda y: calc_basis(y['mark_l'], indexes.loc[y.name, 'indexes_l'], future['expiryTime'],
                                     datetime.fromtimestamp(int(y.name / 1000), tz=timezone.utc)), axis=1)
        elif future['type'] == 'perpetual':  ### 1h funding = (mark/spot-1)/24
            data['rate_T'] = None
            data['rate_c'] = (mark['mark_c'] / indexes['indexes_c'] - 1) * 365.25
            data['rate_h'] = (mark['mark_h'] / indexes['indexes_h'] - 1) * 365.25
            data['rate_l'] = (mark['mark_l'] / indexes['indexes_l'] - 1) * 365.25
        else:
            raise Exception('what is ' + future['symbol'] + ' ?')

        data.columns = [self.market(future['symbol'])['id'] + '_' + c for c in data.columns]
        data.index = [datetime.fromtimestamp(x / 1000) for x in data.index]
        data = data[~data.index.duplicated()].sort_index()

        if dirname != '': await async_to_csv(data,
                                             os.path.join(dirname,
                                                          self.market(future['symbol'])['id'] + '_futures.csv'),
                                             mode='a', header=False)

    ## populates future_price or spot_price depending on type
    @ignore_error
    async def spot_history(self, symbol,
                           end=(datetime.now(tz=timezone.utc).replace(minute=0, second=0, microsecond=0)),
                           start=(datetime.now(tz=timezone.utc).replace(minute=0, second=0, microsecond=0)) - timedelta(
                               days=30),
                           timeframe='1h',
                           dirname=''):
        max_mark_data = int(500)
        resolution = pd.Timedelta(self.describe()['timeframes'][timeframe]).total_seconds()

        e = end.timestamp()
        s = start.timestamp()
        f = max_mark_data * resolution
        start_times = [int(round(s + k * f)) for k in range(1 + int((e - s) / f)) if s + k * f < e]

        spot_lists = await safe_gather([
            self.fetch_ohlcv(symbol, timeframe=timeframe, params={'startTime': start_time * 1000,
                                                                      'endTime': (start_time + f - resolution) * 1000})
            for start_time in start_times])
        spot = [y for x in spot_lists for y in x]
        column_names = ['t', 'o', 'h', 'l', 'c', 'volume']

        ###### spot
        data = pd.DataFrame(columns=column_names, data=spot).astype(dtype={'t': 'int64', 'volume': 'float'}).set_index(
            't')
        data['volume'] = data['volume'] * 24 * 3600 / resolution
        data.columns = [symbol.replace('/', '') + '_price_' + column for column in data.columns]
        data.index = [datetime.fromtimestamp(x / 1000) for x in data.index]
        data = data[~data.index.duplicated()].sort_index()
        if dirname != '': await async_to_csv(data,
                                             os.path.join(dirname,
                                                          symbol.replace('/', '') + '_price.csv'),
                                             mode='a', header=False)

    @ignore_error
    async def fetch_trades_history(self,symbol,
                                   start=(datetime.now(tz=timezone.utc).replace(minute=0, second=0,
                                                                                microsecond=0)) - timedelta(days=30),
                                   end=(datetime.now(tz=timezone.utc).replace(minute=0, second=0, microsecond=0)),
                                   frequency=timedelta(minutes=1),
                                   dirname=''):

        max_trades_data = int(1000)  # in trades, limit is 1000 :(

        ### grab data per batch of 5000, try hourly
        trades = []
        start_time = start.timestamp()
        end_time = end.timestamp()

        while start_time < end.timestamp():
            new_trades = await self.fetch_trades(symbol,
                                                     params={'startTime': start_time, 'endTime': end_time})
            trades.extend(new_trades)

            if len(new_trades) > 0:
                last_trade_time = max(trade['timestamp'] for trade in new_trades)
                if last_trade_time > end.timestamp(): break
                if (len(new_trades) < max_trades_data) & (end_time > end.timestamp()): break
                start_time = last_trade_time if len(new_trades) == max_trades_data else end_time
            else:
                start_time = end_time
            # 1h limit: https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md#recent-trades-list
            end_time = start_time + 3600

        if len(trades) == 0:
            vwap = pd.DataFrame(columns=['size', 'volume', 'count', 'vwap', 'vwvol', 'liquidation_intensity'])
            vwap.columns = [symbol.split('/USDT')[0] + '_trades_' + column for column in vwap.columns]
            return {'symbol': self.market(symbol)['symbol'],
                    'coin': self.market(symbol)['base'],
                    'vwap': vwap[symbol.split('/USDT')[0] + '_trades_' + 'vwap'],
                    'vwvol': vwap[symbol.split('/USDT')[0] + '_trades_' + 'vwvol'],
                    'volume': vwap[symbol.split('/USDT')[0] + '_trades_' + 'volume'],
                    'liquidation_intensity': vwap[symbol.split('/USDT')[0] + '_trades_' + 'liquidation_intensity']}

        vwap = self.vwap_from_list(frequency, trades)
        vwap.columns = [symbol.split('/USDT')[0] + '_trades_' + column for column in vwap.columns]
        # data.index = [datetime.fromtimestamp(x / 1000) for x in data.index]
        vwap = vwap[~vwap.index.duplicated()].sort_index().ffill()

        if dirname != '':
            csv_filename = os.path.join(dirname, symbol.split('/USDT')[0] + "_trades.csv")
            vwap.to_csv(csv_filename)

        return {'vwap': vwap[symbol.split('/USDT')[0] + '_trades_vwap'],
                'vwvol': vwap[symbol.split('/USDT')[0] + '_trades_vwvol'],
                'volume': vwap[symbol.split('/USDT')[0] + '_trades_volume'],
                'liquidation_intensity': vwap[symbol.split('/USDT')[0] + '_trades_liquidation_intensity']}

    @staticmethod
    def vwap_from_list(frequency, trades: list[dict]) -> pd.DataFrame:
        '''needs ['size', 'price', 'liquidation', 'time' as isostring]'''
        data = pd.DataFrame(data=trades)
        data[['amount', 'price']] = data[['amount', 'price']].astype(float)
        data['volume'] = data['amount'] * data['price']
        data['square'] = data['amount'] * data['price'] * data['price']
        data['count'] = 1
        data['liquidation_volume'] = None
        data['time'] = data['datetime'].apply(lambda t: dateutil.parser.isoparse(t).replace(tzinfo=timezone.utc))
        data.set_index('time', inplace=True)
        vwap = data[['amount', 'volume', 'square', 'count', 'liquidation_volume']].resample(frequency).sum(numeric_only=True)
        vwap['vwap'] = vwap['volume'] / vwap['amount']
        vwap['vwvol'] = (vwap['square'] / vwap['amount'] - vwap['vwap'] * vwap['vwap']).apply(np.sqrt)
        vwap['liquidation_intensity'] = None
        return vwap[~vwap.index.duplicated()].sort_index().ffill()

    # --------------------------------------------------------------------------------------------
    # ---------------------------------- execution -----------------------------------------
    # --------------------------------------------------------------------------------------------

    async def peg_or_stopout(self, symbol, size, edit_trigger_depth=None, edit_price_depth=None, stop_depth=None):
        size = self.round_to_increment(self.static[symbol]['sizeIncrement'], size)
        if abs(size) == 0:
            return

        #TODO: https://help.ftx.com/hc/en-us/articles/360052595091-Ratelimits-on-FTX
        opposite_side = self.tickers[symbol]['ask' if size>0 else 'bid']
        mid = self.tickers[symbol]['mid']

        priceIncrement = self.static[symbol]['priceIncrement']
        sizeIncrement = self.static[symbol]['sizeIncrement']

        if stop_depth is not None:
            stop_trigger = float(self.price_to_precision(symbol,stop_depth))

        #TODO: use orderbook to place before cliff; volume matters too.
        isTaker = edit_price_depth in ['rush_in', 'rush_out', 'taker_hedge']
        if not isTaker:
            edit_price = float(self.price_to_precision(symbol, opposite_side - (1 if size > 0 else -1) * edit_price_depth))
            edit_trigger = float(self.price_to_precision(symbol, edit_trigger_depth))
        else:
            edit_price = self.sweep_price_atomic(symbol, size * mid)
            edit_trigger = None
            self.strategy.logger.warning(f'{edit_price_depth} {size} {symbol}')

        # remove open order dupes is any (shouldn't happen)
        event_histories = self.strategy.order_manager.filter_order_histories([symbol], self.strategy.order_manager.openStates)
        if len(event_histories) > 1:
            first_pending_new = np.argmin(np.array([data[0]['timestamp'] for data in event_histories]))
            for i,event_history in enumerate(self.strategy.order_manager.filter_order_histories([symbol], self.strategy.order_manager.cancelableStates)):
                if i != first_pending_new:
                    await self.cancel_order(event_history[-1]['clientOrderId'],'duplicates')
                    self.strategy.logger.info('canceled duplicate {} order {}'.format(symbol,event_history[-1]['clientOrderId']))

        # skip if there is inflight on the spread
        # if self.pending_new_histories(coin) != []:#TODO: rather incorporate orders_pending_new in risk, rather than block
        #     if self.pending_new_histories(coin,symbol) != []:
        #         self.strategy.logger.info('orders {} should not be in flight'.format([order['clientOrderId'] for order in self.pending_new_histories(coin,symbol)[-1]]))
        #     else:
        #         # this happens mostly between pending_new and create_order on the other leg. not a big deal...
        #         self.strategy.logger.info('orders {} still in flight. holding off {}'.format(
        #             [order['clientOrderId'] for order in self.pending_new_histories(coin)[-1]],symbol))
        #     return
        pending_new_histories = self.strategy.order_manager.filter_order_histories(self.parameters['symbols'],
                                                                          ['pending_new'])
        if pending_new_histories != []:
            self.strategy.logger.info('orders {} should not be in flight'.format([order[-1]['clientOrderId'] for order in pending_new_histories]))
            return

        # if no open order, create an order
        order_side = 'buy' if size>0 else 'sell'
        if len(event_histories)==0:
            await self.create_order(symbol, 'limit', order_side, abs(size), price=edit_price,
                                                  params={'comment':edit_price_depth if isTaker else 'new'})
        # if only one and it's editable, stopout or peg or wait
        elif len(event_histories)==1 \
                and (self.strategy.order_manager.latest_value(event_histories[0][-1]['clientOrderId'], 'remaining') >= sizeIncrement) \
                and event_histories[0][-1]['state'] in self.strategy.order_manager.acknowledgedStates:
            order = event_histories[0][-1]
            order_distance = (1 if order['side'] == 'buy' else -1) * (opposite_side - order['price'])

            # panic stop. we could rather place a trailing stop: more robust to latency, but less generic.
            if (stop_depth and order_distance > stop_trigger) \
                    or isTaker:
                size = self.strategy.order_manager.latest_value(order['clientOrderId'], 'remaining')
                price = self.sweep_price_atomic(symbol, size * mid)
                await self.create_order(symbol, 'limit', order_side, abs(size),
                                                 price = price,
                                                 params={'comment':edit_price_depth if isTaker else 'stop'},
                                                 previous_clientOrderId = order['clientOrderId'])
            # peg limit order
            elif order_distance > edit_trigger and abs(edit_price - order['price']) >= priceIncrement:
                await self.create_order(symbol, 'limit', order_side, abs(size),
                                                price=edit_price,
                                                params={'comment':'chase'},
                                                previous_clientOrderId=order['clientOrderId'])

    async def create_order(self, symbol, type, side, amount, price=None, params=dict(),previous_clientOrderId=None,peg_rule: PegRule=None):
        '''if not new, cancel previous first
        if acknowledged, place order. otherwise just reconcile
        orders_pending_new is blocking'''
        if previous_clientOrderId is not None:
            await self.cancel_order(previous_clientOrderId, 'edit')

        trimmed_size = self.strategy.position_manager.trim_to_margin({symbol:amount * (1 if side == 'buy' else -1)})[symbol]
        rounded_amount = self.round_to_increment(self.static[symbol]['sizeIncrement'], abs(trimmed_size))
        if rounded_amount < self.static[symbol]['sizeIncrement']:
            return
        # set pending_new -> send rest -> if success, leave pending_new and give id. Pls note it may have been caught by handle_order by then.
        clientOrderId = self.strategy.order_manager.pending_new({'symbol': symbol,
                                                    'type': type,
                                                    'side': side,
                                                    'amount': rounded_amount,
                                                    'remaining': rounded_amount,
                                                    'price': price,
                                                    'comment': params['comment']})
        try:
            # REST request
            order = await super().create_order(symbol, type, side, rounded_amount, price, {'clientOrderId':clientOrderId} | params)
        except Exception as e:
            order = {'clientOrderId':clientOrderId,
                     'timestamp':myUtcNow(),
                     'state':'rejected',
                     'comment':'create/'+str(e)}
            self.strategy.order_manager.cancel_or_reject(order)
            if isinstance(e,ccxtpro.InsufficientFunds):
                self.strategy.logger.info(f'{clientOrderId} too big: {rounded_amount*self.mid(symbol)}')
            elif isinstance(e,ccxtpro.RateLimitExceeded):
                throttle = 200.0
                self.strategy.logger.info(f'{str(e)}: waiting {throttle} ms)')
                await asyncio.sleep(throttle / 1000)
            else:
                raise e
        else:
            self.strategy.order_manager.sent(order)
            if peg_rule is not None:
                self.peg_rules[clientOrderId] = peg_rule

    async def cancel_order(self, clientOrderId, trigger):
        '''set in flight, send cancel, set as pending cancel, set as canceled or insist'''
        symbol = clientOrderId.split('_')[1]
        self.strategy.order_manager.pending_cancel({'comment':trigger}
                                          | {key: [order[key] for order in self.strategy.order_manager.data[clientOrderId] if key in order][-1]
                                        for key in ['clientOrderId','symbol','side','amount','remaining','price']})  # may be needed

        try:
            status = await super().cancel_order(id=None, symbol=symbol, params={'origClientOrderId': clientOrderId})
            self.strategy.order_manager.cancel_sent({'clientOrderId':clientOrderId,
                                        'symbol':symbol,
                                        'status':status,
                                        'comment':trigger})
        except ccxtpro.CancelPending as e:
            self.strategy.order_manager.cancel_sent({'clientOrderId': clientOrderId,
                                        'symbol': symbol,
                                        'status': str(e),
                                        'comment': trigger})
            return True
        except ccxtpro.InvalidOrder as e: # could be in flight, or unknown
            self.strategy.order_manager.cancel_or_reject({'clientOrderId':clientOrderId,
                                             'status':str(e),
                                             'state':'canceled',
                                             'comment':trigger})
            return False
        except Exception as e:
            self.strategy.logger.info(f'{clientOrderId} failed to cancel: {str(e)} --> retrying')
            await asyncio.sleep(0.2)
            return await self.cancel_order(clientOrderId, trigger+'+')
        else:
            return True