import os
from utils.io_utils import *
from pathlib import Path

class configLoader():

    __universe = None
    __pfoptimizer_params = None
    __home = Path.home()
    __config_folder_path = os.path.join(__home, "config")

    @staticmethod
    def set_universe():
        f = open(os.path.join(configLoader.__config_folder_path, "universe.json"), "r")
        configLoader.__universe = json.loads(f.read())

    @staticmethod
    def set_pfoptimizer_params():
        f = open(os.path.join(configLoader.__config_folder_path, "pfoptimizer_params.json"), "r")
        configLoader.__pfoptimizer_params = json.loads(f.read())

    @staticmethod
    def get_universe():
        if configLoader.__universe is None:            # Read only once, lazy
            configLoader.set_universe()
        return configLoader.__universe

    @staticmethod
    def get_pfoptimizer_params():
        if configLoader.__pfoptimizer_params is None:   # Read only once, lazy
            configLoader.set_pfoptimizer_params()
        return configLoader.__pfoptimizer_params

    @staticmethod
    def get_bases(bases_filter):
        if configLoader.__universe is None:             # Read only once, lazy
            configLoader.set_universe()

        if bases_filter != 'all':
            res = [symbol_name for symbol_name in configLoader.__universe
                   if configLoader.__universe[symbol_name]["tier"] == bases_filter]
        else:
            res = list(configLoader.__universe.keys())
        return res