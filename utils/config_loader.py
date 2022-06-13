import os
from utils.io_utils import *
from pathlib import Path

class configLoader():
    _home = Path.home()
    _config_folder_path = os.path.join(_home, "config")
    _pfoptimizer_folder_path = os.path.join(_home, "config", "pfoptimizer")
    _mktdata_folder_path = os.path.join(_home, "mktdata")

    _universe_pool = ["all", "max", "wide", "institutional"]
    _universe = None             # dict
    _universe_params = None      # dict
    _pfoptimizer_params = None   # dict
    _static_params_used = None   # Dataframe
    _static_params_saved = None  # Dataframe
    _executor_params = None      # dict
    _current_weights = None      # Dataframe (from excel read)

    _universe_filename = "universe.json"
    _universe_params_filename = "universe_params.json"          # Used by pfoptimizer
    _pfoptimizer_params_filename = "pfoptimizer_params.json"    # Used by pfoptimizer
    _static_params_filename = "static_params.xlsx"
    _executor_params_filename = "tradeexecutor_params.json"     # Used by tradeexecutor
    _current_weights_filename = "current_weights.xlsx"

    ### SETTERS ###
    @staticmethod
    def set_universe():
        try:
            f = open(os.path.join(configLoader._config_folder_path, configLoader._universe_filename), "r")
            configLoader._universe = json.loads(f.read())
        except FileNotFoundError:
            raise FileNotFoundError(f"File {os.path.join(configLoader._config_folder_path, configLoader._universe_filename)} not found")

    @staticmethod
    def set_pfoptimizer_params():
        try:
            f = open(os.path.join(configLoader._config_folder_path, configLoader._pfoptimizer_params_filename), "r")
            configLoader._pfoptimizer_params = json.loads(f.read())
        except FileNotFoundError:
            raise FileNotFoundError(f"File {os.path.join(configLoader._config_folder_path, configLoader._pfoptimizer_params_filename)} not found")

    @staticmethod
    def set_static_params():
        try:
            configLoader._static_params_used = pd.read_excel(os.path.join(configLoader._config_folder_path, configLoader._static_params_filename), sheet_name='used').set_index('coin')
            configLoader._static_params_saved = pd.read_excel(os.path.join(configLoader._config_folder_path, configLoader._static_params_filename), sheet_name='saved').set_index('coin')
        except FileNotFoundError:
            raise FileNotFoundError(f"File {os.path.join(configLoader._config_folder_path, configLoader._static_params_filename)} not found")

    @staticmethod
    def set_universe_params():
        try:
            f = open(os.path.join(configLoader._config_folder_path, configLoader._universe_params_filename), "r")
            configLoader._universe_params = json.loads(f.read())
        except FileNotFoundError:
            raise FileNotFoundError(f"File {os.path.join(configLoader._config_folder_path, configLoader._universe_params_filename)} not found")

    @staticmethod
    def set_executor_params():
        ''' Used by tradeexecutor to get execution params'''
        try:
            f = open(os.path.join(configLoader._config_folder_path, configLoader._executor_params_filename), "r")
            configLoader._executor_params = json.loads(f.read())
        except FileNotFoundError:
            raise FileNotFoundError(
                f"File {os.path.join(configLoader._config_folder_path, configLoader._executor_params_filename)} not found")

    @staticmethod
    def set_current_weights():
        ''' Used by tradeexecutor (only) to read current weights to have '''
        try:
            configLoader._current_weights = pd.read_excel(os.path.join(configLoader._pfoptimizer_folder_path, configLoader._current_weights_filename))
        except FileNotFoundError:
            raise FileNotFoundError(
                f"File {os.path.join(configLoader._pfoptimizer_folder_path, configLoader._current_weights_filename)} not found")

    ### GETTERS ###
    @staticmethod
    def get_config_folder_path():
        return configLoader._config_folder_path

    @staticmethod
    def get_mktdata_folder_path():
        return configLoader._mktdata_folder_path

    @staticmethod
    def get_mktdata_folder_for_exchange(exchange):
        return os.path.join(configLoader._mktdata_folder_path, exchange)

    @staticmethod
    def get_universe_filename():
        return configLoader._universe_filename

    @staticmethod
    def get_pfoptimizer_params_filename():
        return configLoader._pfoptimizer_params_filename

    @staticmethod
    def get_universe_pool():
        return configLoader._universe_pool

    @staticmethod
    def get_universe():
        if configLoader._universe is None:             # Read only once, lazy
            configLoader.set_universe()
        return configLoader._universe

    @staticmethod
    def get_pfoptimizer_params():
        if configLoader._pfoptimizer_params is None:   # Read only once, lazy
            configLoader.set_pfoptimizer_params()
        return configLoader._pfoptimizer_params

    @staticmethod
    def get_bases(bases_filter):
        if configLoader._universe is None:             # Read only once, lazy
            configLoader.set_universe()

        if bases_filter != 'max':
            res = [symbol_name for symbol_name in configLoader._universe
                   if configLoader._universe[symbol_name]["tier"] == bases_filter]
        else:
            # Need all records, only if filter is max
            res = list(configLoader._universe.keys())
        return res

    @staticmethod
    def get_static_params_used():   # Dataframe
        if configLoader._static_params_used is None:   # Read only once, lazy
            configLoader.set_static_params()
        return configLoader._static_params_used

    @staticmethod
    def get_static_params_saved():   # Dataframe
        if configLoader._static_params_saved is None:  # Read only once, lazy
            configLoader.set_static_params()
        return configLoader._static_params_saved

    @staticmethod
    def get_universe_params():   # Dataframe
        if configLoader._universe_params is None:  # Read only once, lazy
            configLoader.set_universe_params()
        return configLoader._universe_params

    @staticmethod
    def get_executor_params():   # dict
        if configLoader._executor_params is None:  # Read only once, lazy
            configLoader.set_executor_params()
        return configLoader._executor_params

    @staticmethod
    def get_current_weights():   # Excel file
        ''' Used by trade_executor to access current_weights '''
        if configLoader._current_weights is None:  # Read only once, lazy
            configLoader.set_current_weights()
        return configLoader._current_weights

    # PERSIST params
    @staticmethod
    def persist_universe_params(new_params):
        try:
            with open(os.path.join(configLoader._config_folder_path, configLoader._universe_params_filename), "w") as outfile:
                json.dump(new_params, outfile)
        except FileNotFoundError:
            raise FileNotFoundError(f"Cannot write file {os.path.join(configLoader._config_folder_path, configLoader._universe_params_filename)}")