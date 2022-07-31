import logging,os,sys,datetime,subprocess,importlib,functools,pathlib
from utils.io_utils import parse_time_param
from utils.config_loader import configLoader
import click

def build_logging(app_name,
                  log_date=datetime.datetime.utcnow(),
                  log_mapping={logging.INFO:'info.log',logging.WARNING:'warning.log',logging.CRITICAL:'program_flow.log'}):
    '''log_mapping={logging.DEBUG:'debug.log'...
    3 handlers: >=debug, ==info and >=warning
    if not log_date no date in filename'''

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
        handler = logging.FileHandler(os.path.join(os.sep,log_path,f'{log_date.strftime("%Y%m%d_%H%M%S")}_{filename}' if log_date else filename), mode='w')
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

def extract_args_kwargs(command,not_passed_token="not_passed"):
    args = [arg.split('=')[0] for arg in command if len(arg.split('=')) == 1]
    args = args[1:]
    kwargs = dict()
    for arg in command:
        key_value = arg.split('=')
        if len(key_value) == 2 and key_value[1] != not_passed_token:
            kwargs |= {key_value[0]:key_value[1]}
    return args,kwargs

def api(func):
    '''
    NB: the main function has to remove __logger from kwargs --> logger = kwargs.pop('__logger')
    '''

    @functools.wraps(func)
    def wrapper_api(*args, **kwargs):

        # build logger for current module
        module = MyModules.current_module_list[func.__module__.split('.')[0]]
        module_name = module.get_short_name()
        logger = build_logging(module_name, log_mapping={logging.INFO: 'info.log'})

        # print arguments
        logger.info(f'running {module_name} args={args} kwargs={kwargs}')
        if '__logger' in kwargs:
            raise Exception('__logger kwarg key is reserved')

        # validate arguments
        module.validate_args(args)
        module.validate_kwargs(kwargs)

        # call and log exceptions or result
        try:
            result = func(*args, **(kwargs | {'__logger': logger}))
            logger.info(f'command returned {str(result)}')
            return result
        except Exception as e:
            logger.critical(str(e),stack_info=True)
            raise e

    return wrapper_api

class MyModules:
    current_module_list = dict()

    def __init__(self,name,testbed,args_validation,kwargs_validation):
        self.name = name
        if self.name in MyModules.current_module_list:
            raise Exception(f'module {self.name} already in the list')
        self.testbed = testbed
        self.args_validation = args_validation
        self.kwargs_validation = kwargs_validation

    def validate_args(self,args):
        '''check all args are present and valid'''
        for i,arg in enumerate(self.args_validation):
            if not self.args_validation[i][1](args[i]):
                error_msg = f'{self.args_validation[i][0]} {args[i]} {self.args_validation[i][2]}'
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

    def generate_run_sh(self):
        content = '#!/bin/bash \n \n python3 main.py'+ ''.join(
            [f' ${arg[0].upper()}' for arg in self.args_validation] + \
            [f' {kwarg}=${kwarg.upper()}' for kwarg in self.kwargs_validation])
        root_dir = pathlib.Path(__file__).resolve().parent.parent
        filename = os.path.join(os.sep, root_dir, self.name, 'run.sh')
        with open(filename,'w+') as fp:
            fp.write(content)

    @staticmethod
    def register(name,testbed,args_validation,kwargs_validation):
        module = MyModules(name,testbed,args_validation,kwargs_validation)
        MyModules.current_module_list |= {name: module}
        #module.generate_run_sh()

    @staticmethod
    def load_all_modules():
        '''not sure how to use that but i keep it there....'''
        for mod_name in MyModules.current_module_list:
            importlib.import_module(f'{mod_name}.main')

    def run_test(self):
        root_dir = pathlib.Path(__file__).resolve().parent.parent
        filename = os.path.join(os.sep, root_dir, self.name, 'main.py')
        results = {test:subprocess.run(f'{sys.executable} {filename} {test}',shell=True) # subprocess.run
                   for test in self.testbed}
        return results

MyModules.register(name='histfeed',
                   testbed=["build ftx wide 1"],
                   args_validation=[['run_type',lambda x: x in ["build", "correct", "get"],'not in {}'.format(["build", "correct", "get"])],
                                    ['exchange',lambda x: x in ["ftx"],'not in {}'.format(["ftx"])],
                                    ['universe',lambda x: x in configLoader.get_universe_pool(),'not in {}'.format(configLoader.get_universe_pool())]],
                   kwargs_validation={'nb_days': [lambda x: isinstance(int(x), int), 'not an int']})
MyModules.register(name='pfoptimizer',
                   testbed=["sysperp ftx subaccount=debug",
                            "basis ftx instrument_type=perpetual depth=100000",
                            "unwind ftx subaccount=debug",
                            "flatten ftx subaccount=debug",
                            "spread ftx subaccount=debug coin=ETH cash_size=10"],
                   args_validation=[
                       ['run_type', lambda x: x in ["sysperp", "backtest", "depth", "basis", "unwind","flatten","spread"],'not in {}'.format(["sysperp", "backtest", "depth", "basis", "unwind","flatten","spread"])],
                       ['exchange', lambda x: x in ["ftx"], 'not in {}'.format(["ftx"])]],
                   kwargs_validation={'instrument_type':[lambda x: ["perpetual", "future", "all"],'not in {}'.format(["perpetual", "future", "all"])],
                                      'subaccount':[lambda x: True,'not found'],
                                      'depth':[lambda x: isinstance(float(x),float),'need a float'],
                                      'config':[lambda x: os.path.isdir(os.path.join(os.sep,configLoader.get_config_folder_path(config_name=x))),'not found'],
                                      'coin':[lambda x: isinstance(str(x),str),'need a str'],
                                      'cash_size':[lambda x: isinstance(float(x),float),'need a float']})
MyModules.register(name='riskpnl',
                   testbed=["risk ftx debug nb_runs=1",
                             "plex ftx debug period=2d"],
                   args_validation=[
                       ['run_type', lambda x: x in ["risk", "plex", "batch_summarize_exec_logs", "fromoptimal"],'not in {}'.format(["risk", "plex", "batch_summarize_exec_logs", "fromoptimal"])],
                       ['exchange', lambda x: x in ["ftx"], 'not in {}'.format(["ftx"])],
                       ['subaccount', lambda x: True, 'not in {}'.format([""])]],
                   kwargs_validation={'nb_runs':[lambda x: isinstance(int(x),int),'integer needed'],
                                      'period':[lambda x: isinstance(parse_time_param(x),datetime.timedelta),'time period needed'],
                                      'dirname':[lambda x: os.path.isdir(x),'not found'],
                                      'filename':[lambda x: True,'not found'],# skew it....
                                      'config':[lambda x: os.path.isdir(os.path.join(os.sep,configLoader.get_config_folder_path(config_name=x))),'not found']})
MyModules.register(name='ux',
                   testbed=[""],
                   args_validation=[],
                   kwargs_validation={})

MyModules.register(name='tradeexecutor',
                   testbed=["weights_ftx_debug_ETH.csv"], # ,"unwind exchange=ftx subaccount=debug config=prod"
                   args_validation=[
                       ['order', lambda x: isinstance(x,str),'not a str']],
                   kwargs_validation={'exchange':[lambda x: x in ['ftx'],'not in {}'.format(['ftx'])],
                                      'subaccount':[lambda x: True,'not found'],
                                      'config':[lambda x: os.path.isdir(os.path.join(os.sep,configLoader.get_config_folder_path(config_name=x))),'not found']})