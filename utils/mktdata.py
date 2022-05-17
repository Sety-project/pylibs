import os
from utils.io_utils import *
from pathlib import Path

def get_universe(universe_filter):
    home = Path.home()
    f = open(os.path.join(home, "mktdata", "universe.json"), "r")
    data = json.loads(f.read())
    if universe_filter != 'all':
        res = [symbol_name for symbol_name in data if data[symbol_name][universe_filter]]
    else:
        res = list(data.keys())
    return res