#!/usr/bin/env python3
import functools
import importlib
import inspect
import subprocess
import logging
from utils.async_utils import async_wrap
import sys,os,shutil,platform
import json
import pandas as pd
import numpy as np
from datetime import timedelta, datetime
import pyarrow, pyarrow.parquet,s3fs

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
    return args[0].to_csv(*args[1:],**kwargs)

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
            return None
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


def build_logging(app_name,log_mapping={logging.INFO:'info.log',logging.WARNING:'warning.log',logging.CRITICAL:'program_flow.log'}):
    '''log_mapping={logging.DEBUG:'debug.log'...
    3 handlers: >=debug, ==info and >=warning'''

    class MyFilter(object):
        '''this is to restrict info logger to info only'''
        def __init__(self, level):
            self.__level = level
        def filter(self, logRecord):
            return logRecord.levelno <= self.__level

    # mkdir log repos if does not exist
    log_path = os.path.join(os.sep, "tmp", app_name)
    if not os.path.exists(log_path):
        os.umask(0)
        os.makedirs(log_path, mode=0o777)

    logging.basicConfig()
    logger = logging.getLogger(app_name)

    # logs
    for level,filename in log_mapping.items():
        handler = logging.FileHandler(os.path.join(os.sep,log_path,f'{datetime.utcnow().strftime("%Y%m%d_%H%M%S")}_{filename}'), mode='w')
        handler.setLevel(level)
        handler.setFormatter(logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s'))
        #handler.addFilter(MyFilter(level))
        logger.addHandler(handler)

    # handler_alert = logging.handlers.SMTPHandler(mailhost='smtp.google.com',
    #                                              fromaddr='david@pronoia.link',
    #                                              toaddrs=['david@pronoia.link'],
    #                                              subject='auto alert',
    #                                              credentials=('david@pronoia.link', ''),
    #                                              secure=None)
    # handler_alert.setLevel(logging.CRITICAL)
    # handler_alert.setFormatter(logging.Formatter(f"%(levelname)s: %(message)s"))
    # self.myLogger.addHandler(handler_alert)

    logger.setLevel(min(log_mapping.keys()))

    return logger

class MyModules:
    current_module_list = ['histfeed', 'pfoptimizer', 'riskpnl', 'tradeexecutor', 'ux']

    def __init__(self,filename,examples,args_validation,kwargs_validation):
        self.filename = filename
        self.name = os.path.split(os.path.split(filename)[0])[1]
        if self.name not in MyModules.current_module_list:
            raise Exception(f'new module {self.name} :(\nAdd it to the list :) ')
        self.examples = examples
        self.args_validation = args_validation
        self.kwargs_validation = kwargs_validation

    def validate_args(self,args):
        '''check all args are present and valid'''
        for i,arg in enumerate(self.args_validation):
            if not self.args_validation[i][1](args[i]):
                error_msg = f'{self.args_validation[i][0]} {self.args_validation[i][2]}'
                logging.getLogger(self.name).critical(error_msg)
                raise Exception(error_msg)

    def validate_kwargs(self,kwargs):
        '''check all kwargs are valid'''
        for key,arg in kwargs.items():
            if not self.kwargs_validation[key][0](arg):
                error_msg = f'{key} {self.kwargs_validation[key][1]}'
                logging.getLogger(self.name).critical(error_msg)
                raise Exception(error_msg)

    def get_short_name(self):
        return self.name.split('_')[-1]

    def run_test(self):
        results = {example:subprocess.run(f'{self.filename} {example}')
                   for example in self.examples}
        return results

    @staticmethod
    def load_all_modules():
        '''not sure how to use that but i keep it there....'''
        global modules
        for mod_name in modules:
            importlib.import_module(f'{mod_name}.main')

def api_factory(examples,args_validation,kwargs_validation):
    def api(func):
        '''
        NB: the main function has to remove __logger from kwargs --> logger = kwargs.pop('__logger')
        '''
        @functools.wraps(func)
        def wrapper_api(*args, **kwargs):
            args=args[1:]

            # build logger for current module
            module = MyModules(inspect.stack()[1][1],examples,args_validation,kwargs_validation)
            module_name = module.get_short_name()
            logger = build_logging(module_name, {logging.INFO: 'info.log'})

            # print arguments
            logger.info(f'running {module_name} {args} ')
            if '__logger' in kwargs:
                raise Exception('__logger kwarg key is reserved')

            #validate arguments
            module.validate_args(args)
            module.validate_kwargs(kwargs)

            # call and log exceptions or result
            try:
                return func(*args, **(kwargs| {'__logger':logger}))
            except Exception as e:
                logger.critical(str(e))
                raise e
            else:
                logger.info(f'command {[(i, values[i]) for i in args]} returned {value}')
        return wrapper_api
    return api
