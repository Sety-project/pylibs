import asyncio
import shutil

import aiohttp
import typing
import logging
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import json
import requests
import os
from pathlib import Path
# from pfoptimizer.deribit_portoflio import black_scholes as bs
# from pfoptimizer.deribit_portoflio import orderbook_slippage
from utils.async_utils import safe_gather, async_wrap
from dateutil.parser import parse

log = logging.getLogger("amberdata")
logging.basicConfig(level=logging.INFO)
# path = '/historical/derivs/perpetuals/{market}/{symbol}'
# catalog = json.loads(requests.get("https://api.laevitas.ch/api/catalog/v2",headers=resp.header).text)['api_list']
# argument_catalog = pd.DataFrame(catalog)

class DeFiLlama:
    def __init__(self):
        self.url = 'https://api.llama.fi/protocol'
    def all_tvl(self, protocol: str):
        return json.loads(requests.get(f'{self.url}/{protocol}').text)
# v = DeFiLlama().all_tvl('uniswap')

class AmberdataAPI:
    safe_gather_limit = 100
    date_type_map = {
        # 'funding_rate': ('funding-rates', timedelta(days=1).total_seconds()*1000, 'list'),
        'open_interest': ('open-interest', timedelta(days=1).total_seconds()*1000, 'list'),
        #'liquidation_order': ('liquidations', None),
        #'long-short-ratio': ('long-short-ratio', None),
        #'order_book_event': ('order-book-events', None),
        #'order_book_snapshot': ('order-book-snapshots', timedelta(days=1).total_seconds()*1000, 'list'),
        'ohlcv': ('ohlcv', timedelta(days=1).total_seconds()*1000, 'list'),
        #'ticker': ('tickers', timedelta(hours=1).total_seconds()*1000, 'dict'),
        #'trade': 'trades'
    }
    v3_launch_date = datetime(2019, 6, 1)
    def __init__(self):
        self.headers = {
            "accept": "application/json",
            "x-api-key": "UAK3b4019254bda8e0181f362bb89b244a2",
            "x-oracle": "UAK3b4019254bda8e0181f362bb89b244a2",
            'Content-Type': 'application/json',
            'Accept-Language': 'en-US,en;q=0.9'
        }
        # self.option_reference = self.get_and_cache_option_reference()

    def get_and_cache_option_reference(self) -> dict:
        def get_option_reference():
            try:
                return self.paginate(
                "https://web3api.io/api/v2/market/options/exchanges/reference?exchange=deribit&includeInactive=true",
                'dict')
            except Exception as e:
                if len(e.args) > 0 and isinstance(e.args[0], dict):
                    return e.args[0]['partial_result']
                else:
                    raise e

        max_i = 10  # if max_i == 1 just read previous
        i = 0
        while i < max_i:
            # read previous data from file
            if os.path.isfile('option_reference.json'):
                with open('option_reference.json', 'r') as fp:
                    previous_data = json.load(fp)
            else:
                previous_data = dict()

            # get new data, unless max_i==1. Then only read previous.
            if max_i > 1:
                additional_data = get_option_reference()
                additional_data = {key: value
                                   for key, value in additional_data.items()
                                   if key not in previous_data}
                # break if nothing is new
                nb_additional = len(additional_data)
                if nb_additional == 0:
                    log.info(f'{nb_additional} added')
                    break
            else:
                additional_data = dict()

            # write new data
            with open('option_reference.json', 'w+') as fp:
                json.dump(additional_data, fp)

            i += 1

        return previous_data | additional_data

    @staticmethod
    def regular_dates(start_date: int, end_date: int, frequency: int) -> list[tuple[int, int]]:
        if frequency is None:
            return [(start_date, end_date)]

        t = max(start_date, end_date - frequency)
        result = [(int(t), int(end_date))]
        while t > start_date:
            result.append((int(max(start_date, t - frequency)), int(t)))
            t -= frequency
        return result

    async def async_requests(self, queries: list[typing.Tuple[str, typing.Any]], **kwargs) -> dict[str, typing.Any]:
        async with aiohttp.ClientSession(headers=self.headers) as session:
            responses = await safe_gather([session.request(method='GET', url=url, data=data) for (url, data) in queries],
                                          n=AmberdataAPI.safe_gather_limit)
            results = [json.loads(await response.content.read())['data'] for response in responses]
        return dict(zip(queries, results))

    def async_get(self, queries: list[str], **kwargs) -> dict[str, typing.Any]:
        async def async_wrapper():
            async with aiohttp.ClientSession(headers=self.headers) as session:
                responses = await safe_gather([session.get(url) for url in queries],
                                              n=AmberdataAPI.safe_gather_limit)
            return dict(zip(queries, responses))
        return asyncio.run(async_wrapper())


    def paginate(self, query: str, data_type: str) -> typing.Union[dict, list]:
        result = eval(data_type)()

        current_query = query
        while True:
            try:
                current = requests.get(current_query, headers=self.headers)
                loaded = json.loads(current.text)
                if data_type == 'dict':
                    result |= loaded['payload']['data']['deribit']
                elif data_type == 'list':
                    result += loaded['payload']['data']
                else:
                    raise ValueError

                if 'next' not in loaded['payload']['metadata']:
                    log.info('{} done'.format(current_query))
                    return result
                else:
                    next_query = loaded['payload']['metadata']['next']
                    if next_query is not None \
                            and next_query != '' \
                            and next_query != query \
                            and next_query.split('page=')[1] != 'Infinity':
                        current_query = next_query
                        result_stub = loaded['payload']['data']
                        result_length = len(result_stub['deribit'] if 'deribit' in result_stub else result_stub)
                        log.debug(f'next page after {result_length} records for {current_query}')
                    else:
                        log.info(f'{current_query} done')
                        return result
            except Exception as e:
                log.warning({'current_query': current_query,
                                 'text': current.text,
                                 'partial_result': result})

    def supported_cex(self, instrument_type='futures'):
        date_ranges = self.paginate(
            f"https://web3api.io/api/v2/market/{instrument_type}/exchanges/information?includeDates=true&includeInactive=true",
            'dict')
        references = self.paginate(
            f"https://web3api.io/api/v2/market/{instrument_type}/exchanges/reference?includeInactive=true",
            'dict')

        result = dict()
        for exchange, instruments in date_ranges.items():
            result[exchange] = dict()
            for instrument, data in instruments.items():
                if instrument in references[exchange]:
                    result[exchange][instrument] = dict()
                    for data_type, date_range in data.items():
                        if date_range['startDate'] is not None \
                                and date_range['endDate'] is not None \
                                and data_type in AmberdataAPI.date_type_map:
                            data_type_query = AmberdataAPI.date_type_map[data_type]
                            data_values = eval(data_type_query[2])()
                            for (start, end) in AmberdataAPI.regular_dates(date_range['startDate'],
                                                                           date_range['endDate'],
                                                                           data_type_query[1]):
                                query = f'https://web3api.io/api/v2/market/futures/{data_type_query[0]}/{instrument}/historical?exchange={exchange}&startDate={start}&endDate={end}'
                                if data_type_query[2] == 'list':
                                    data_values += self.paginate(query, data_type_query[2])
                                elif data_type_query[2] == 'dict':
                                    data_values |= self.paginate(query, data_type_query[2])
                            if data_values:
                                result[exchange][instrument][data_type] = data_values
                                log.info(f'{exchange}_{instrument}_{data_type_query[0]}')
                                pd.DataFrame(data_values).to_csv(f'{exchange}_{instrument}_{data_type_query[0]}.csv')
        return result

    def all_supported_dex(self):
        response = requests.get("https://web3api.io/api/v2/market/defi/dex/exchanges", headers=self.headers)
        return json.loads(response.text)['payload']

    def v3_pools(self,
                 start: datetime = (datetime.now()-timedelta(days=90)).strftime('%Y-%m-%d'),
                 end: datetime = datetime.now().strftime('%Y-%m-%d'),
                 filename: Path = os.path.join(Path.home(), 'mktdata', 'saved.json'),
                 enrich: bool = False,
                 basic_filter: typing.Callable = None,
                 advanced_filter: typing.Callable = None) -> dict:
        pools = self.paginate("https://web3api.io/api/v2/market/defi/dex/pairs?exchange=uniswapv3", 'list')
        if basic_filter is not None:
            pools = list(filter(basic_filter, pools))
        if enrich:
            enrichments = self.async_get([('https://web3api.io/api/v2/market/defi/metrics/exchanges/uniswapv3/pairs/{}/latest'.format(pool['pairAddress']), None)
                                               for pool in pools])
            for pool in pools:
                enrichment_buffer = enrichments['https://web3api.io/api/v2/market/defi/metrics/exchanges/uniswapv3/pairs/{}/latest'.format(pool['pairAddress'])]
                try:
                    enrichment = eval(asyncio.run(enrichment_buffer.read()))
                    pool |= enrichment
                    log.info('pool {} enriched'.format(pool['pairName']))
                except Exception as e:
                    log.warning('pool {} -> {}'.format(pool['pairName'], str(e)))

        if advanced_filter is not None:
            pools = list(filter(advanced_filter, pools))

        return pools

    async def async_query(self, query):
        return async_wrap(self.paginate)(query['query'], 'list')

    async def lens(self, protocol: str, poolAddress: str,
                   startDate: datetime, endDate: datetime,
                   filename: str=None):
        queries = []
        for start, end in AmberdataAPI.regular_dates(startDate.timestamp() * 1000,
                                                     endDate.timestamp() * 1000,
                                                     24 * 3600 * 1000):
            queries.append({'start': start,
                            'query': f"https://web3api.io/api/v2/defi/dex/{protocol}/pools/{poolAddress}?startDate={int(start/1000)}&endDate={int(end/1000)}&size=1000"})

        results = await safe_gather([async_wrap(self.paginate)(query['query'], 'list') for query in queries],
                                    return_exceptions=True)
        results = sum(results, [])
        log.info('{} done'.format(poolAddress))
        if filename is not None:
            with open(filename, 'w') as f:
                json.dump(results, f)

        return results

    def lyra_greeks(self):
        url = "https://app.pinkswantrading.com/graphql"
        kwargs = {'symbol': 'sETH',
                  'dateStart': '2023-01-01',
                  'dateEnd': '2023-01-25',
                  'network': 'optimism'}
        payload="{\"query\":\"\\tquery LyraPoolGreekExposureTimeseries($symbol: String $dateStart: String, $dateEnd: String, $network: NetworkEnumType) {        LyraPoolGreekExposureTimeseries: LyraPoolGreekExposureTimeseries(symbol: $symbol, dateStart: $dateStart, dateEnd: $dateEnd, network: $network) {\\n            date \\n            netDelta \\n            netStdVega \\n            optionNetDelta \\n            baseBalance \\n            poolNetDelta \\n            hedgerNetDelta \\n        }\\n    }\",\"variables\":{\"symbol\":\"sETH\",\"dateStart\":\"2023-01-01\",\"dateEnd\":\"2023-01-25\",\"network\":\"optimism\"}}"
        # kwargs['symbol'],
        #     kwargs['dateStart'],
        #     kwargs['dateEnd'],
        #     kwargs['network']
        # )
        response = requests.request("GET", url, headers=self.headers, data=payload)
        return json.loads(response.text)

    async def deribit_smile_genesisvolatility(self, currency: str,
                                        start: datetime, 
                                        end: datetime, 
                                        frequency: timedelta,
                                        output_path: str=os.path.join(Path.home(), 'mktdata', 'genesisvolatility', 'surface_history.csv')) -> pd.DataFrame:
        '''
        full volsurface history as multiindex dataframe: date as milli x (tenor as years, strike of 'atm')
        '''
        url = "https://app.pinkswantrading.com/graphql"
        queries = []
        for timestamp, _ in AmberdataAPI.regular_dates(start.timestamp() * 1000,
                                                       end.timestamp() * 1000,
                                                       frequency.total_seconds() * 1000):
            startDate = datetime.fromtimestamp(timestamp / 1000).strftime('%Y-%m-%d')
            payload = "{\"query\":\"query MoneynessSurface($symbol:BTCOrETHEnumType, $date:String){\\n  MoneynessSurface(symbol:$symbol, date:$date) {\\n    date\\n    currency\\n    expiration\\n    timeleft\\n    spot\\n    fwd\\n    d40\\n    d20\\n    d10\\n    d05\\n    d025\\n    atm\\n    u025\\n    u05\\n    u10\\n    u20\\n    u40\\n  }\\n}\",\"variables\":{\"symbol\":\"my_currency\",\"date\":\"my_startDate\"}}"
            payload = payload.replace('my_currency', currency)
            payload = payload.replace('my_startDate', startDate)
            queries.append((url, payload))

        results = await self.async_requests(queries)
        results = sum([smiles_chunk['MoneynessSurface'] for smiles_chunk in results.values()], [])
        results = pd.DataFrame(results)
        results['date'] = results['date'].apply(lambda x: datetime.fromtimestamp(float(x) / 1000))

        if output_path is not None:
            if os.path.isfile(output_path):
                previous_data = pd.read_csv(output_path, parse_dates='date', usecols=['date'])
                results = results[~previous_data['date']]
            results.to_csv(output_path, mode='a', header=not os.path.exists(output_path))
            shutil.copy2(output_path,
                         output_path.replace('.csv', '_{}.csv'.format(datetime.utcnow().strftime(
                             "%Y-%m-%d-%Hh"))))

        return results

    def option_order_book(self,
                          coin: str,
                          days_to_expiry: int,
                          startDate: datetime, endDate: datetime):
        # data_availability = self.paginate('https://web3api.io/api/v2/market/options/order-book-snapshots/information', 'list')

        labels = []
        queries = []
        for timestamp, _ in AmberdataAPI.regular_dates(startDate.timestamp() * 1000,
                                                     endDate.timestamp() * 1000,
                                                     7 * 24 * 3600 * 1000):
            expiry = (datetime.fromtimestamp(timestamp/1000) + timedelta(days=days_to_expiry)).replace(hour=8,minute=0,second=0,microsecond=0)
            for strike in np.linspace(1000, 3000, 50):  # instrument, data in self.option_reference.items():
                #if ('expiration' in data) and (data['expiration'] != '') and (parse(data['expiration']) == expiry) and data['quoteAsset'] == coin and data['contractType'] != 'perpetual':
                # found = next((available for available in data_availability if available['instrument'] == instrument), None)
                #    if True:  #found is not None and found['startDate'] <= data['expiration'] <= found['endDate']:
                start = timestamp - 5 * 60 * 1000
                end = timestamp + 5 * 60 * 1000
                instrument = '{}-{}-{}-C'.format(coin, expiry.strftime("%d%b%y").upper(), int(strike))
                queries.append({'timestamp': timestamp,
                               'name': instrument,
                               'strike': strike,
                               'expiry': expiry,
                               'query': f"https://web3api.io/api/v2/market/options/order-book-snapshots/{instrument}/historical?exchange=deribit&startDate={int(start/1000)}&endDate={int(end/1000)}"})

        data = []
        for order_book in self.async_get([x['query'] for x in queries]).values():
            try:
                data += [eval(asyncio.run(order_book.read()))['payload']['data']]
            except:
                pass
        slippages = map(orderbook_slippage, data)
    def run_filtered_pools(self,
                           startDate: datetime,
                           endDate: datetime
                           ):
        tokens = ['WETH', 'USDC', 'WBTC', 'DAI', 'LDO', 'GMX', 'UNI', 'LINK', 'MATIC', '1INCH']
        basic_filter = lambda x: (x['baseSymbol'] in tokens) & (x['quoteSymbol'] in tokens)
        # basic_filter = lambda x: (x['poolFees'] == '0.0005') & (x['pairName'] == 'USDC_WETH')
        def advanced_filter(x):
            if (('liquidityTotalUSD' in x) & ('feesUSD' in x)):
                return ((x['liquidityTotalUSD'] > 1e7) & (x['feesUSD']/x['liquidityTotalUSD'] > 1e-4))
            else:
                return False
        filtered_pools = self.v3_pools(enrich=False, basic_filter=basic_filter, advanced_filter=None)

        log.info('Found {} pools'.format(len(filtered_pools)))
        protocol = 'uniswapv3'
        uniswap_events = asyncio.run(safe_gather([self.lens(protocol=protocol,
                                                            poolAddress=pool['pairAddress'],
                                                            startDate=startDate,
                                                            endDate=endDate,
                                                            filename='{}_{}_{}_{}_{}.json'.format(protocol,
                                                                                         pool['pairName'],
                                                                                         startDate.strftime('%Y-%m-%d'),
                                                                                         endDate.strftime('%Y-%m-%d'),
                                                            pool['pairAddress']))
                                     for pool in filtered_pools]))

api = AmberdataAPI()
asyncio.run(api.deribit_smile_genesisvolatility('ETH',
                                    start=datetime(2022, 1, 1),
                                    end=datetime.now(),
                                    frequency=timedelta(hours=1)))
api.run_filtered_pools(startDate=AmberdataAPI.v3_launch_date,
                       endDate=datetime.now())
options = api.option_order_book('ETH',7,
                                datetime.now() - timedelta(days=90),
                                datetime.now())
lyra = AmberdataAPI().lyra_greeks()
s = AmberdataAPI().supported_cex()
a = AmberdataAPI().all_supported_dex()