#!/usr/bin/env python3
from utils.async_utils import *
import logging

import numpy as np
import json, dateutil

from cryptography.fernet import Fernet
import ccxt.async_support as ccxt
from pathlib import Path
import os

def load_vault():
    api_param_path = os.path.join(Path.home(), '.cache', 'setyvault', 'api_param')
    with open(api_param_path) as fp:
        api_param = fp.read().encode()
    return api_param

# def set_api_params(api_param):
#     api_params = pd.read_excel('/home/vic/pylibs/SystematicCeFi/DerivativeArbitrage/Runtime/configs/static_params.xlsx', sheet_name='api', index_col='key')
#     api_params = api_params.applymap(lambda x: Fernet(api_param).decrypt(x.encode()).decode() if type(x)==str else ''.encode())
#     return api_params

def decode_api_params(api_param):

    api_keys_path = os.path.join(Path.home(), '.cache', 'setyvault', 'api_keys.json')
    with open(api_keys_path) as json_file:
        data = json.load(json_file)

    clear_dict = {}
    for exchange_name in data:
        clear_dict[exchange_name] = {}
        clear_dict[exchange_name]['key'] = Fernet(api_param).decrypt(data[exchange_name]['key'].encode()).decode()
        clear_dict[exchange_name]['comment'] = data[exchange_name]['comment']
        if 'key2' in data[exchange_name]:
            clear_dict[exchange_name]['key2'] = Fernet(api_param).decrypt(data[exchange_name]['key2'].encode()).decode()

    return clear_dict

api_params = decode_api_params(load_vault()) #was returning a df before, now a list

'''
biz logic helpers
'''

########## only for dated futures
def calc_basis(f,s,T,t):
    basis = np.log(float(f)/float(s))
    res= (T-t)
    return basis/np.max([1, res.days])*365.25

async def open_exchange(exchange_name,subaccount,config={}):
    '''
    ccxt exchange object factory.
    '''
    if exchange_name == 'ftx':
        exchange = ccxt.ftx(config={ ## David personnal
            'enableRateLimit': True,
            'apiKey': 'ZUWyqADqpXYFBjzzCQeUTSsxBZaMHeufPFgWYgQU',
            'secret': api_params[exchange_name]['key'],  #[key_value['key'] for key_value in api_params if key_value['exchange']=='ftx'][0],  #sapi_params.loc[exchange_name,'value'], # api_params.loc[exchange_name,'value'],
            'asyncio_loop': config['asyncio_loop'] if 'asyncio_loop' in config else asyncio.get_running_loop()
        } | config)
        if subaccount!='': exchange.headers= {'FTX-SUBACCOUNT': subaccount}

    elif exchange_name in ['binance','binanceusdm','binancecoin']:
        exchange = getattr(ccxt,exchange_name)(config={# subaccount convexity
        'enableRateLimit': True,
        'apiKey': 'V2KfGbMd9Zd9fATONTESrbtUtkEHFcVDr6xAI4KyGBjKs7z08pQspTaPhqITwh1M',
        'secret': api_params['binance']['key'],
    }|config)
    elif exchange_name == 'okex5':
        exchange = ccxt.okex5(config={
            'enableRateLimit': True,
            'apiKey': '6a72779d-0a4a-4554-a283-f28a17612747',
            'secret': api_params[exchange_name]['key'],
            'secret2': api_params.loc[exchange_name,'key2'],
        }|config)
        if subaccount != 'convexity':
            logging.warning('subaccount override: convexity')
            exchange.headers = {'FTX-SUBACCOUNT': 'convexity'}
    elif exchange_name == 'huobi':
        exchange = ccxt.huobi(config={
            'enableRateLimit': True,
            'apiKey': 'b7d9d6f8-ce6a01b8-8b6ab42f-mn8ikls4qg',
            'secret': api_params[exchange_name]['key'],
        }|config)
    elif exchange_name == 'deribit':
        exchange = ccxt.deribit(config={
            'enableRateLimit': True,
            'apiKey': '4vc_41O4',
            'secret': api_params[exchange_name]['key'],
        }|config)
    elif exchange_name == 'kucoin':
        exchange = ccxt.kucoin(config={
                                           'enableRateLimit': True,
                                           'apiKey': '62091838bff2a30001b0d3f6',
                                           'secret': api_params[exchange_name]['key'],
                                       } | config)
    elif exchange_name == 'paradigm':
        from mess.paradigm_tape import paradigm_request
        exchange = paradigm_request(path='/v1/fs/trade_tape',
                                  access_key='EytZmov5bDDPGXqvYviriCs8',
                                  secret_key=api_params[exchange_name]['key'])
    elif exchange_name in api_params:
        exchange = api_params[exchange_name]
    #subaccount_list = pd.DataFrame((exchange.privateGetSubaccounts())['result'])
    else:
        print('what exchange?')
    if isinstance(exchange, ccxt.Exchange):
        exchange.checkRequiredCredentials()  # raises AuthenticationError
        await exchange.load_markets()
        await exchange.load_fees()

    return exchange