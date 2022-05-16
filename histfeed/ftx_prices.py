import datetime
from dotenv import load_dotenv
import os
import json
import pandas as pd
import plotly.graph_objects as go
import requests
import time
from datetime import timedelta
# from client import FtxClient
from histfeed.TimeoutHTTPAdapter import *
from urllib3.util.retry import Retry

def test():
#Get all market data
    endpoint_url = 'https://ftx.com/api/markets'
    # Get all market data as JSON
    all_markets = requests.get(endpoint_url).json()
    # Convert JSON to Pandas DataFrame
    df = pd.DataFrame(all_markets['result'])
    df.set_index('name', inplace=True)

# Get single market data
    base_currency = 'BTC'
    quote_currency = 'USD'
    # Specify the base and quote currencies to get single market data
    request_url = f'{endpoint_url}/{base_currency}/{quote_currency}'
    df = pd.DataFrame(requests.get(request_url).json())
    df['result']

def test_2():
    #Get historical data
    # 1 day = 60 * 60 * 24 seconds
    daily=str(60*60*24)

    daily=str(60*60)
    # Start date = 2021-01-01
    # start_date = datetime(2021, 1, 1).timestamp()
    # Get the historical market data as JSON
    # historical = requests.get(
    #     f'{request_url}/candles?resolution={daily}&start_time={start_date}'
    # ).json()

    start = datetime.datetime.now(tz=datetime.timezone.utc).replace(minute=0, second=0, microsecond=0)-timedelta(days=500)
    end = datetime.datetime.now(tz=datetime.timezone.utc).replace(minute=0, second=0, microsecond=0)

    max_funding_data = int(500)  # in hour. limit is 500
    resolution = daily

    e = end.timestamp()
    s = start.timestamp()
    f = max_funding_data * int(resolution)
    start_times=[int(round(s+k*f)) for k in range(1+int((e-s)/f)) if s+k*f<e]

    http = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    http.mount("https://", TimeoutHTTPAdapter(max_retries=retries, timeout=2.5))

    j = 0
    for k in range(0, 101):
        for start_time in start_times:
            # start_time = end_time
            # end_time = start_time+f-int(resolution)
            print(j)
            req = f'https://ftx.com/api/funding_rates?future=1INCH-PERP&end_time={start_time + f}&start_time={start_time}'
            try:
                # Work but slow and I don't like the code actually
                # for i in range(0,11):
                #     # historical = requests.get(req, timeout=10)
                #     historical = http.get(req)
                #     if historical.status_code == 200:
                #         break
                #         # Data Storage/treatment
                #     if historical is None:
                #         print(f'Request timed out: {req}')
                # this I like works faster and looks reliable
                historical = http.get(req)
                if historical.status_code != 200:
                    print(f'Request failed/timed out: {req}')
            except:
                print("Request did not work")
                historical = None

            # Treatment with historical --> Get the data we need
            historical = None
            j = j+1

    print(f'j={j}')
