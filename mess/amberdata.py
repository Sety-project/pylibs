import asyncio
import aiohttp
import typing
import pandas as pd
import datetime
import json
import requests
import os
from pathlib import Path
from pfoptimizer.deribit_portoflio import black_scholes as bs
from pfoptimizer.deribit_portoflio import orderbook_slippage
from utils.async_utils import safe_gather
from dateutil.parser import parse

# path = '/historical/derivs/perpetuals/{market}/{symbol}'
# catalog = json.loads(requests.get("https://api.laevitas.ch/api/catalog/v2",headers=resp.header).text)['api_list']
# argument_catalog = pd.DataFrame(catalog)


class AmberdataAPI:
    safe_gather_limit = 100
    date_type_map = {
        # 'funding_rate': ('funding-rates', datetime.timedelta(days=1).total_seconds()*1000, 'list'),
        'open_interest': ('open-interest', datetime.timedelta(days=1).total_seconds()*1000, 'list'),
        #'liquidation_order': ('liquidations', None),
        #'long-short-ratio': ('long-short-ratio', None),
        #'order_book_event': ('order-book-events', None),
        #'order_book_snapshot': ('order-book-snapshots', datetime.timedelta(days=1).total_seconds()*1000, 'list'),
        'ohlcv': ('ohlcv', datetime.timedelta(days=1).total_seconds()*1000, 'list'),
        #'ticker': ('tickers', datetime.timedelta(hours=1).total_seconds()*1000, 'dict'),
        #'trade': 'trades'
    }
    def __init__(self):
        self.headers = {
            "accept": "application/json",
            "x-api-key": "UAK3b4019254bda8e0181f362bb89b244a2",
            'Content-Type': 'application/json',
            'Accept-Language': 'en-US,en;q=0.9'
        }
        self.option_reference = self.get_and_cache_option_reference()

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

        if os.path.isfile('option_reference.json'):
            with open('option_reference.json', 'r') as fp:
                option_reference = json.load(fp)
        else:
            option_reference = dict()

        i = 0
        while i < 0:
            additional_data = get_option_reference()
            if os.path.isfile('option_reference.json'):
                with open('option_reference.json', 'r') as fp:
                    previous_data = json.load(fp)
                additional_data = {key: value
                                   for key, value in additional_data.items()
                                   if key not in previous_data}
                nb_additional = len(additional_data)
                print(f'{nb_additional} added')
                if nb_additional == 0:
                    print('all done')
                    break
            else:
                previous_data = dict()

            option_reference = previous_data | additional_data
            with open('option_reference.json', 'w+') as fp:
                json.dump(additional_data, fp)

        return option_reference

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

    def async_requests(self, queries: list[str]) -> dict[str, typing.Any]:
        async def async_wrapper():
            async with aiohttp.ClientSession(headers=self.headers) as session:
                responses = await safe_gather([session.get(url) for url in queries], n=AmberdataAPI.safe_gather_limit)
            return dict(zip(queries, responses))
        return asyncio.run(async_wrapper())

    def paginate(self, query: str, data_type: str):
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
                    print('{} has no next'.format(loaded['payload']))
                    return result
                else:
                    next_query = loaded['payload']['metadata']['next']
                    if next_query is not None \
                            and next_query != '' \
                            and next_query != query:
                        current_query = next_query
                        result_stub = loaded['payload']['data']
                        result_length = len(result_stub['deribit'] if 'deribit' in result_stub else result_stub)
                        print(f'next page after {result_length} records')
                    else:
                        print('next is None')
                        return result
            except Exception as e:
                raise Exception({'current_query': current_query,
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
                                print(f'{exchange}_{instrument}_{data_type_query[0]}')
                                pd.DataFrame(data_values).to_csv(f'{exchange}_{instrument}_{data_type_query[0]}.csv')
        return result

    def all_supported_dex(self):
        response = requests.get("https://web3api.io/api/v2/market/defi/dex/exchanges", headers=self.headers)
        return json.loads(response.text)['payload']

    def v3_pools(self,
            start: datetime = (datetime.datetime.now()-datetime.timedelta(days=90)).strftime('%Y-%m-%d'),
            end: datetime = datetime.datetime.now().strftime('%Y-%m-%d'),
            filename: Path = os.path.join(Path.home(), 'mktdata', 'saved.json')) -> dict:
        response = self.paginate("https://web3api.io/api/v2/market/defi/dex/pairs?exchange=uniswapv3", 'list')
        return response

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

    def option_order_book(self,
                          coin: str,
                          days_to_expiry: int,
                          startDate: datetime, endDate: datetime):
        data_availability = self.paginate('https://web3api.io/api/v2/market/options/order-book-snapshots/information', 'list')

        labels= []
        queries = []
        for timestamp, _ in AmberdataAPI.regular_dates(startDate.timestamp() * 1000,
                                                     endDate.timestamp() * 1000,
                                                     7 * 24 * 3600 * 1000):
            expiry = (datetime.datetime.fromtimestamp(timestamp/1000) + datetime.timedelta(days=days_to_expiry)).replace(hour=8,minute=0,second=0,microsecond=0)
            for instrument, data in self.option_reference.items():
                if ('expiration' in data) and (data['expiration'] != '') and (parse(data['expiration']) == expiry) and data['quoteAsset'] == coin and data['contractType'] != 'perpetual':
                    found = next((available for available in data_availability if available['instrument'] == instrument), None)
                    if found is not None and found['startDate'] <= data['expiration'] <= found['endDate']:
                        start = timestamp - 5 * 60 * 1000
                        end = timestamp + 5 * 60 * 1000
                        queries.append({'timestamp':timestamp,
                                       'name':instrument,
                                       'strike': data['strikePrice'],
                                       'expiry': expiry,
                                       'query': f"https://web3api.io/api/v2/market/options/order-book-snapshots/{instrument}/historical?exchange=deribit&startDate={int(start/1000)}&endDate={int(end/1000)}"})

        data = []
        for order_book in self.async_requests([x['query'] for x in queries]).values():
            try:
                data += [eval(asyncio.run(order_book.read()))['payload']['data']]
            except:
                pass
        slippages = map(orderbook_slippage, data)

api = AmberdataAPI()
options = api.option_order_book('ETH',7,
                                datetime.datetime.now() - datetime.timedelta(days=90),
                                datetime.datetime.now())
lyra = AmberdataAPI().lyra_greeks()
s = AmberdataAPI().supported_cex()
a = AmberdataAPI().all_supported_dex()
b = AmberdataAPI().v3_pools()
print(a)