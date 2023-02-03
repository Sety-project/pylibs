#!/usr/bin/env python3
import functools
import logging
import retry
from utils.async_utils import async_wrap
from utils.config_loader import configLoader
import os
import json
import pandas as pd
import numpy as np
from datetime import timedelta, datetime, timezone
import pyarrow, pyarrow.parquet,s3fs
from typing import Any
from ccxt import NetworkError

# this is for jupyter
# import cufflinks as cf
# cf.go_offline()
# cf.set_config_file(offline=False, world_readable=True)

# Commented out to avoid histfeed failing to run...
# if not 'Runtime' in os.listdir('.'):
#     # notebooks run one level down...
#     os.chdir('../')
#     if not 'Runtime' in os.listdir('.'):
#         raise Exception("This needs to run in DerivativesArbitrage, where Runtime/ is located")

'''
I/O helpers
'''

async def async_read_csv(*args,**kwargs):
    coro = async_wrap(pd.read_csv)
    return await coro(*args,**kwargs)

def to_csv(*args,**kwargs):
    return args[0].to_csv(args[1],**kwargs)

async def async_to_csv(*args,**kwargs):
    coro = async_wrap(to_csv)
    return await coro(*args,**kwargs)

def to_parquet(df,filename,mode="w"):
    if mode == 'a' and os.path.isfile(filename):
        previous = from_parquet(filename)
        df = pd.concat([previous,df],axis=0)
        df = df[~df.index.duplicated()].sort_index()
    pq_df = pyarrow.Table.from_pandas(df)
    pyarrow.parquet.write_table(pq_df, filename)
    return None

async def async_to_parquet(df,filename,mode="w"):
    coro = async_wrap(to_parquet)
    return await coro(df,filename,mode)

def from_parquets_s3(filenames,columns=None):
    '''columns = list of columns. All if None
    filename = list'''
    kwargs = {'columns':columns} if columns else dict()
    return pyarrow.parquet.ParquetDataset(filenames,filesystem=s3fs.S3FileSystem()).read_pandas(**kwargs).to_pandas()

async def async_from_parquet_s3(filename,columns=None):
    coro = async_wrap(from_parquets_s3)
    return await coro(filename,columns)

def from_parquet(filename):
    return pyarrow.parquet.read_table(filename).to_pandas()

async def async_from_parquet(filename):
    coro = async_wrap(from_parquet)
    return await coro(filename)

'''
misc helpers
'''

import collections
def flatten(dictionary, parent_key=False, separator='.'):
    """
    All credits to https://github.com/ScriptSmith
    Turn a nested dictionary into a flattened dictionary
    :param dictionary: The dictionary to flatten
    :param parent_key: The string to prepend to dictionary's keys
    :param separator: The string used to separate flattened keys
    :return: A flattened dictionary
    """

    items = []
    for key, value in dictionary.items():
        new_key = str(parent_key) + separator + key if parent_key else key
        if isinstance(value, collections.MutableMapping):
            items.extend(flatten(value, new_key, separator).items())
        elif isinstance(value, list):
            for k, v in enumerate(value):
                items.extend(flatten({str(k): v}, new_key).items())
        else:
            items.append((new_key, value))
    return dict(items)

def deepen(dictionary, parent_key=False, separator='.'):
    """
    flatten^-1
    """
    top_keys = set(key.split(separator)[0] for key in dictionary.keys())
    result = {}
    for top_key in top_keys:
        sub_dict={}
        sub_result={}
        for key, value in dictionary.items():
            if key.split(separator)[0]==top_key:
                if separator in key:
                        sub_dict|={key.split(separator, 1)[1]:value}
                else:
                    sub_result |={key:value}
        if sub_dict != {}:
            result |= {top_key:deepen(sub_dict,parent_key=parent_key,separator=separator)}
        else:
            result |= sub_result

    return result

class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, np.bool_):
            return bool(obj)
        if isinstance(obj, pd.core.generic.NDFrame):
            return obj.to_json()
        if isinstance(obj, collections.deque):
            return list(obj)
        if isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        return super(NpEncoder, self).default(obj)

def parse_time_param(param):
    if 'mn' in param:
        horizon = int(param.split('mn')[0])
        horizon = timedelta(minutes=horizon)
    elif 'h' in param:
        horizon = int(param.split('h')[0])
        horizon = timedelta(hours=horizon)
    elif 'd' in param:
        horizon = int(param.split('d')[0])
        horizon = timedelta(days=horizon)
    elif 'w' in param:
        horizon = int(param.split('w')[0])
        horizon = timedelta(weeks=horizon)
    return horizon

def myUtcNow(return_type='float'):
    result = datetime.utcnow()
    if return_type == 'datetime':
        return result
    result = result.replace(tzinfo=timezone.utc).timestamp() * 1000
    if return_type == 'float':
        return result
    result = int(result)
    if return_type == 'int':
        return result
    raise Exception(f'invalid return_type {return_type}')


@retry.retry((NetworkError, ConnectionError), tries=3, delay=1,backoff=2)
def ignore_error(func):
    @functools.wraps(func)
    async def wrapper_ignore_error(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            logger = logging.getLogger(func.__module__.split('.')[0])
            logger.warning(f'{e} running {str(func)} {args}{kwargs}',exc_info=False)
    return wrapper_ignore_error

def nested_dict_to_tuple(nested: dict) -> dict[tuple,Any]:
    '''function to convert nest dict into dict of tuples.
    Useful to create dataframes from nested dict'''
    result = dict()
    for key, data in nested.items():
        if isinstance(data, dict):
            innerDict = nested_dict_to_tuple(data)
            result |= {(key,) + index: value for index, value in innerDict.items()}
        else:
            result |= {(key,): data}
    return result
#
# d={'a':0,'b': {'c':5,'d':4}}
# res = nested_dict_to_dataframe(d)
# print(res)