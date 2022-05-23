import os
from utils.io_utils import *
from pathlib import Path

class configLoader():

    def __init__(self, env="dev"):
        ''' Define base parameters for the configs '''
        self.ROOT_CONFIG_FOLDER = "config"
        self.home = Path.home()
        self.env = env

        #These are the base config attributes
        self._pfoptimizer_params = {}
        self._universe = {}

        # Automatically sets the configs where possible
        self.set_pfoptimizer_params()

    def set_universe(self, universe_filter):
        f = open(os.path.join(self.home, self.ROOT_CONFIG_FOLDER, self.env, "universe.json"), "r")
        data = json.loads(f.read())
        if universe_filter != 'all':
            res = [symbol_name for symbol_name in data if data[symbol_name][universe_filter]]
        else:
            res = list(data.keys())
        self._universe = res

    def set_pfoptimizer_params(self):
        f = open(os.path.join(self.home, self.ROOT_CONFIG_FOLDER, self.env, "pfoptimizer_params.json"), "r")
        self._pfoptimizer_params = json.loads(f.read())

    def get_universe(self):
        return self._universe

    def get_pfoptimizer_params(self):
        return self._pfoptimizer_params