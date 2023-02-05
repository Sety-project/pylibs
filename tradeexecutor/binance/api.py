from datetime import timezone, datetime, timedelta

import ccxt.base.errors

from utils.io_utils import ignore_error, async_to_csv, myUtcNow
import dateutil, os, asyncio

import numpy as np
import pandas as pd

from tradeexecutor.interface.venue_api import CeFiAPI, PegRule, VenueAPI
from utils.async_utils import safe_gather
from utils.ccxt_utilities import api_params, calc_basis
from utils.config_loader import configLoader

class BinanceAPI(CeFiAPI,ccxt.pro.binanceusdm):
    '''VenueAPI implements rest calls and websocket loops to observe raw market data / order events and place orders
    send events for Strategy to action
    send events to SignalEngine for further processing'''
    funding_frequency = '8h'
    def get_id(self):
        return "binance"

    class Static:
        _cache = dict()  # {function_name: result}

        @staticmethod
        async def fetch_coin_details(exchange):
            if 'fetch_coin_details' in BinanceAPI.Static._cache:
                return BinanceAPI.Static._cache['fetch_coin_details']
            portfolio_collateral_rate = await exchange.sapiGetPortfolioCollateralRate()
            coin_details = pd.DataFrame((await exchange.fapiPublicGetExchangeInfo())['symbols']).set_index('symbol')[['maintMarginPercent', 'requiredMarginPercent', 'baseAsset']]
            coin_details['PortfolioCollateralRate'] = coin_details['baseAsset'].apply(
                lambda f: next((asset['collateralRate'] for asset in portfolio_collateral_rate if asset['asset'] == f),0.0))

            for col in coin_details.columns:
                coin_details[col] = pd.to_numeric(coin_details[col], errors='ignore')

            all = coin_details[['maintMarginPercent', 'requiredMarginPercent', 'PortfolioCollateralRate']]

            # borrow_rates = pd.DataFrame((await exchange.private_get_spot_margin_borrow_rates())['result']).astype(
            #     dtype={'coin': 'str', 'estimate': 'float', 'previous': 'float'}).set_index('coin')[['estimate']]
            # borrow_rates[['estimate']] *= 24 * 365.25
            # borrow_rates.rename(columns={'estimate': 'borrow'}, inplace=True)
            #
            # lending_rates = pd.DataFrame((await exchange.private_get_spot_margin_lending_rates())['result']).astype(
            #     dtype={'coin': 'str', 'estimate': 'float', 'previous': 'float'}).set_index('coin')[['estimate']]
            # lending_rates[['estimate']] *= 24 * 365.25
            # lending_rates.rename(columns={'estimate': 'lend'}, inplace=True)
            #
            # borrow_volumes = pd.DataFrame((await exchange.public_get_spot_margin_borrow_summary())['result']).astype(
            #     dtype={'coin': 'str', 'size': 'float'}).set_index('coin')
            # borrow_volumes.rename(columns={'size': 'borrow_open_interest'}, inplace=True)
            #
            # all = pd.concat([coin_details, borrow_rates, lending_rates, borrow_volumes], join='outer', axis=1)
            # all = all.loc[coin_details.index]  # borrow summary has beed seen containing provisional underlyings
            # all.loc[coin_details['spotMargin'] == False, 'borrow'] = None  ### hope this throws an error...
            # all.loc[coin_details['spotMargin'] == False, 'lend'] = 0

            BinanceAPI.Static._cache['fetch_coin_details'] = all
            return all

    def __init__(self, parameters, private_endpoints=True):
        config = {
            'enableRateLimit': True,
            'newUpdates': True}
        if private_endpoints:  ## David personnal
            config |= {'apiKey': 'V2KfGbMd9Zd9fATONTESrbtUtkEHFcVDr6xAI4KyGBjKs7z08pQspTaPhqITwh1M',
            'secret': api_params['binance']['key']}
        super().__init__(parameters)
        super(ccxt.pro.binance,self).__init__(config=config)
        self.state = CeFiAPI.State()

        self.options['tradesLimit'] = VenueAPI.cache_size # TODO: shoud be in signalengine with a different name. inherited from ccxt....

        self.peg_rules: dict[str, PegRule] = dict()

   ### get all static fields TODO: only works for perps
    async def fetch_futures(self):
        if 'fetch_futures' in BinanceAPI.Static._cache:
            return BinanceAPI.Static._cache['fetch_futures']

        includeExpired = False
        includeIndex = False
        includeInverse = False

        defaultOption = self.options['fetchMarkets']
        self.options['fetchMarkets'] = ['linear','spot'] + (['inverse'] if includeInverse else [])
        all_markets = await self.fetch_markets({'permissions': 'MARGIN'})

        # otc_file = configLoader.get_static_params_used()

        perp_markets = [f for f in all_markets
                     if f['swap']
                     and f['active']
                     and (f['info']['underlyingType'] == 'COIN' or includeIndex)]
        margin_markets = [f for f in all_markets
                     if f['margin']
                     and f['active']]

        trading_fees = await self.fetch_trading_fees()
        funding_rates = await safe_gather([self.fetch_funding_rate(symbol=f['id']) for f in perp_markets])
        # perp_tickers = await self.fetch_tickers(symbols=[f['id'] for f in future_list], params={'type':'future'})
        # delivery_tickers = await self.fetch_tickers(symbols=[f['id'] for f in future_list], params={'type': 'delivery'})
        open_interests = await safe_gather([self.fapiPublicGetOpenInterest({'symbol':f['id']}) for f in perp_markets])
        coin_details = await BinanceAPI.Static.fetch_coin_details(self)

        result = []
        for i in range(0, len(funding_rates)):
            funding_rate = funding_rates[i]
            market = next(m for m in perp_markets if m['symbol'] == funding_rate['symbol'])
            margin_market = next((m for m in margin_markets if m['symbol'] == funding_rate['symbol'].split(':')[0]), None)
            open_interest = next((m for m in open_interests if m['symbol'] == market['id']), None)

            index = funding_rate['indexPrice']
            mark = funding_rate['markPrice']
            expiryTime = dateutil.parser.isoparse(market['expiryDatetime']).replace(
                tzinfo=timezone.utc) if market['expiryDatetime'] is not None else np.NaN
            if market['expiryDatetime'] is not None:
                future_carry = calc_basis(mark, index, expiryTime,
                                          datetime.utcnow().replace(tzinfo=timezone.utc))
            elif market['swap']:
                future_carry = funding_rate['fundingRate']*3*365.25
            else:
                future_carry = 0

            if self.safe_string(market, 'symbol') not in trading_fees:
                continue

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
                'underlyingType': market['info']['underlyingType'],
                'underlyingSubType': market['info']['underlyingSubType'],
                'spot_ticker': '{}/{}'.format(market['base'], market['quote']),
                'spotMargin': margin_market['margin'] if margin_market is not None else False,
                'cash_borrow': None,
                'future_carry': future_carry,
                'openInterestUsd': float(open_interest['openInterest'])*mark,
                'expiryTime': expiryTime,
                'taker_fee': trading_fees[self.safe_string(market, 'symbol')]['taker'],
                'maker_fee': trading_fees[self.safe_string(market, 'symbol')]['maker'],
                'maintMarginPercent': coin_details.loc[market['id'],'maintMarginPercent'].squeeze(),
                'requiredMarginPercent': coin_details.loc[market['id'],'requiredMarginPercent'].squeeze(),
                'PortfolioCollateralRate': coin_details.loc[market['id'],'PortfolioCollateralRate'].squeeze()
            })

        BinanceAPI.Static._cache['fetch_futures'] = result
        return result

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

    def replay_missed_messages(self):
        # replay missed _messages.
        while self.message_missed:
            data = self.message_missed.popleft()
            channel = data['x']
            self.logger.warning(f'replaying {channel} after recon')
            if channel == 'TRADE':
                fill = self.parse_trade(data)
                self.strategy.position_manager.process_fill(fill | {'orderTrigger': 'replayed'})
                self.strategy.order_manager.process_fill(fill | {'orderTrigger': 'replayed'})
            elif channel == 'NEW':
                order = self.parse_order(data)
                if order['symbol'] in self.data:
                    self.strategy.order_manager.acknowledgment(order | {'comment': 'websocket_acknowledgment','orderTrigger': 'replayed'})

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
        max_funding_data = int(30)  # in days, limit is 92 only
        resolution = pd.Timedelta(self.describe()['timeframes']['1d']).total_seconds()

        e = end.timestamp()
        s = start.timestamp()
        f = max_funding_data * resolution
        start_times = [int(round(e - k * f)) for k in range(1 + int((e - s) / f)) if e - k * f > s] + [s]

        lists = await safe_gather([
            self.fetch_borrow_rate_history(coin, since=int(start_time * 1000), limit=max_funding_data)
            for start_time in start_times])
        borrow = [y for x in lists for y in x]

        if len(borrow) > 0:
            data = pd.DataFrame(borrow)
            data['timestamp'] = data['timestamp'].apply(lambda x: datetime.fromtimestamp(x/1000).replace(hour=8*int(datetime.fromtimestamp(x/1000).hour/8), minute=0, second=0, microsecond=0,tzinfo=None))
            data = data.set_index('timestamp')[['rate']] * 365.25
            data.rename(columns={'rate': coin + '_rate_borrow'}, inplace=True)
            data = data[~data.index.duplicated()].sort_index()

            if dirname != '':
                filename = os.path.join(dirname, coin + '_borrow.csv')
                await async_to_csv(data, os.path.join(dirname, coin + '_borrow.csv'), mode='a', header=not os.path.isfile(filename))

    ######### annualized funding for perps, time is fixing / payment time.
    @ignore_error
    async def funding_history(self, future,
                              start=(datetime.now(tz=timezone.utc).replace(minute=0, second=0,
                                                                           microsecond=0)) - timedelta(days=30),
                              end=(datetime.now(tz=timezone.utc).replace(minute=0, second=0, microsecond=0)),
                              dirname=''):

        max_funding_data = int(100)  # in hour. limit is 100 :(
        resolution = pd.Timedelta(self.describe()['timeframes'][BinanceAPI.funding_frequency]).total_seconds()

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
            data['datetime'] = data['datetime'].apply(lambda x: dateutil.parser.parse(x).replace(hour=8*int(dateutil.parser.parse(x).hour/8),minute=0, second=0, microsecond=0,tzinfo=None))
            data[self.market(future['symbol'])['id'] + '_rate_funding'] = data['fundingRate'] * pd.Timedelta('365d').total_seconds() / pd.Timedelta(self.funding_frequency).total_seconds()
            data = data[['datetime', self.market(future['symbol'])['id'] + '_rate_funding']].set_index('datetime')
            data = data[~data.index.duplicated()].sort_index()

            if dirname != '':
                filename = os.path.join(dirname,self.market(future['symbol'])['id'] + '_funding.csv')
                await async_to_csv(data, filename, mode='a', header=not os.path.isfile(filename))

    #### annualized rates for futures and perp, volumes are daily
    @ignore_error
    async def rate_history( self, future,
                           end=datetime.now(tz=timezone.utc).replace(minute=0, second=0, microsecond=0),
                           start=datetime.now(tz=timezone.utc).replace(minute=0, second=0, microsecond=0) - timedelta(
                               days=30),
                           timeframe=funding_frequency,
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
        indexes['volume'] = indexes['volume'] * indexes['c'] *  24 * 3600 / resolution

        ###### marks
        mark = pd.DataFrame([dict(zip(column_names, row)) for row in mark]).astype(dtype={'t': 'int64'}).set_index('t')
        mark['volume'] = mark['volume'] * mark['c'] * 24 * 3600 / resolution

        mark.columns = ['mark_' + column for column in mark.columns]
        indexes.columns = ['indexes_' + column for column in indexes.columns]

        ##### openInterestUsd
        max_oi_data = 500
        e = end.timestamp()
        s = start.timestamp()
        f = max_oi_data * resolution
        start_times = [int(round(s + k * f)) for k in range(1 + int((e - s) / f)) if s + k * f < e]

        # https://binance-docs.github.io/apidocs/futures/en/#open-interest-statistics --> timeframes in "5m","15m","30m","1h","2h","4h","6h","12h","1d"
        timeframe = timeframe if timeframe in ["5m","15m","30m","1h","2h","4h","6h","12h","1d"] else '1d'
        openInterest_list = await safe_gather([
            self.fetch_open_interest_history(symbol, timeframe=timeframe,
                                                 since=int(start_time * 1000), limit=max_oi_data)
            for start_time in start_times if start_time > (datetime.now() - timedelta(days=30)).timestamp()])
        openInterest_list = [y for x in openInterest_list for y in x]
        openInterest = pd.DataFrame(openInterest_list, columns=['timestamp', 'openInterestAmount']).astype(
            dtype={'timestamp': 'int64'}).set_index('timestamp') if len(openInterest_list) > 0 else pd.DataFrame()

        data = mark.join(indexes, how='inner') #TODO:  .join(openInterest, how='outer')

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
            data['rate_c'] = (data['mark_c'] / data['indexes_c'] - 1) * 365.25
            data['rate_h'] = (data['mark_h'] / data['indexes_h'] - 1) * 365.25
            data['rate_l'] = (data['mark_l'] / data['indexes_l'] - 1) * 365.25
        else:
            raise Exception('what is ' + future['symbol'] + ' ?')

        data.columns = [self.market(future['symbol'])['id'] + '_' + c for c in data.columns]
        data.index = [datetime.fromtimestamp(x / 1000).replace(hour=8*int(datetime.fromtimestamp(x / 1000).hour/8),
                                                               minute=0, second=0, microsecond=0,tzinfo=None) for x in data.index]
        data = data[~data.index.duplicated()].sort_index()

        if dirname != '':
            filename = os.path.join(dirname,self.market(future['symbol'])['id'] + '_futures.csv')
            await async_to_csv(data,filename,mode='a', header=not os.path.isfile(filename))

    ## populates future_price or spot_price depending on type
    @ignore_error
    async def spot_history(self, symbol,
                           end=(datetime.now(tz=timezone.utc).replace(minute=0, second=0, microsecond=0)),
                           start=(datetime.now(tz=timezone.utc).replace(minute=0, second=0, microsecond=0)) - timedelta(
                               days=30),
                           timeframe=funding_frequency,
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
        data['volume'] = data['volume'] * data['c'] * 24 * 3600 / resolution
        data.columns = [symbol.replace('/', '') + '_price_' + column for column in data.columns]
        data.index = [datetime.fromtimestamp(x / 1000).replace(hour=8*int(datetime.fromtimestamp(x / 1000).hour/8),minute=0, second=0, microsecond=0,tzinfo=None) for x in data.index]
        data = data[~data.index.duplicated()].sort_index()
        if dirname != '':
            filename = os.path.join(dirname,symbol.replace('/', '') + '_price.csv')
            await async_to_csv(data,filename,mode='a', header=not os.path.isfile(filename))

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

    async def create_taker_hedge(self,symbol, size, comment='takerhedge'):
        '''cancel and trade.
        risk: trade may be partial and cancel may have failed !!'''
        # remove open order dupes is any (shouldn't happen)
        pending_new_histories = self.strategy.order_manager.filter_order_histories([symbol],
                                                                                   self.strategy.order_manager.openStates)
        if len(pending_new_histories)>0:
            self.strategy.logger.info('orders {} should not be open'.format(
                [order[-1]['clientOrderId'] for order in pending_new_histories]))
            return

        # sweep_price = self.sweep_price_atomic(symbol, size * self.mid(symbol))
        coro = [self.create_order(symbol, 'market', ('buy' if size > 0 else 'sell'), abs(size), price=None,
                                  params={'comment': comment})]
        cancelable_orders = await self.fetch_open_orders(symbol)
        coro += [self.cancel_order(order['clientOrderId'], 'cancel_symbol')
                 for order in cancelable_orders]
        if len(cancelable_orders)>0: print(f'cancelable_orders {cancelable_orders}')
        await safe_gather(coro, semaphore=self.strategy.rest_semaphore)

    async def create_order(self, symbol, type, side, amount, price=None, params=dict(),previous_clientOrderId=None,peg_rule: PegRule=None):
        '''if not new, cancel previous first
        if acknowledged, place order.'''
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
            if isinstance(e,ccxt.pro.InsufficientFunds):
                self.strategy.logger.info(f'{clientOrderId} too big: {rounded_amount*self.mid(symbol)}')
            elif isinstance(e,ccxt.pro.RateLimitExceeded):
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
        except ccxt.pro.CancelPending as e:
            self.strategy.order_manager.cancel_sent({'clientOrderId': clientOrderId,
                                        'symbol': symbol,
                                        'status': str(e),
                                        'comment': trigger})
            return True
        except ccxt.pro.InvalidOrder as e: # could be in flight, or unknown
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