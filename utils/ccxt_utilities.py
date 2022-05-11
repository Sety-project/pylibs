#!/usr/bin/env python3
from async_utils import *
import sys,os,shutil
import logging

from datetime import *
import dateutil
import numpy as np
import pandas as pd
import scipy

from cryptography.fernet import Fernet
import ccxt.async_support as ccxt

api_params = pd.read_excel('Runtime/configs/static_params.xlsx',sheet_name='api',index_col='key')
with open('Runtime/configs/api_param') as fp:
    api_param = fp.read().encode()
api_params  = api_params.applymap(lambda x: Fernet(api_param).decrypt(x.encode()).decode() if type(x)==str else ''.encode())

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
    if exchange_name=='ftx':
        exchange = ccxt.ftx(config={ ## David personnal
            'enableRateLimit': True,
            'apiKey': 'ZUWyqADqpXYFBjzzCQeUTSsxBZaMHeufPFgWYgQU',
            'secret': api_params.loc[exchange_name,'value'],
            'asyncio_loop': config['asyncio_loop'] if 'asyncio_loop' in config else asyncio.get_running_loop()
        }|config)
        if subaccount!='': exchange.headers= {'FTX-SUBACCOUNT': subaccount}

    elif exchange_name == 'binance':
        exchange = ccxt.binance(config={# subaccount convexity
        'enableRateLimit': True,
        'apiKey': 'V2KfGbMd9Zd9fATONTESrbtUtkEHFcVDr6xAI4KyGBjKs7z08pQspTaPhqITwh1M',
        'secret': api_params.loc[exchange_name,'value'],
    }|config)
    elif exchange_name == 'okex5':
        exchange = ccxt.okex5(config={
            'enableRateLimit': True,
            'apiKey': '6a72779d-0a4a-4554-a283-f28a17612747',
            'secret': api_params.loc[exchange_name,'value'],
            'secret': api_params.loc[exchange_name,'comment'],
        }|config)
        if subaccount != 'convexity':
            logging.warning('subaccount override: convexity')
            exchange.headers = {'FTX-SUBACCOUNT': 'convexity'}
    elif exchange_name == 'huobi':
        exchange = ccxt.huobi(config={
            'enableRateLimit': True,
            'apiKey': 'b7d9d6f8-ce6a01b8-8b6ab42f-mn8ikls4qg',
            'secret': api_params.loc[exchange_name,'value'],
        }|config)
    elif exchange_name == 'deribit':
        exchange = ccxt.deribit(config={
            'enableRateLimit': True,
            'apiKey': '4vc_41O4',
            'secret': api_params.loc[exchange_name,'value'],
        }|config)
    elif exchange_name == 'kucoin':
        exchange = ccxt.kucoin(config={
                                           'enableRateLimit': True,
                                           'apiKey': '62091838bff2a30001b0d3f6',
                                           'secret': api_params.loc[exchange_name,'value'],
                                       } | config)
    elif exchange_name == 'paradigm':
        raise Exception('not implemented')
        exchange = ccxt.paradigm(config={
                                          'enableRateLimit': True,
                                          'apiKey': 'EytZmov5bDDPGXqvYviriCs8',
                                          'secret': api_params.loc[exchange_name, 'value'],
                                      } | config)
    #subaccount_list = pd.DataFrame((exchange.privateGetSubaccounts())['result'])
    else: print('what exchange?')
    exchange.checkRequiredCredentials()  # raises AuthenticationError
    await exchange.load_markets()
    await exchange.load_fees()
    return exchange