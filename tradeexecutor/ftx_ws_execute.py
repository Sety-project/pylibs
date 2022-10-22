import os.path

from tradeexecutor.strategy import Strategy
from utils.config_loader import *
from utils.ccxt_utilities import *
from utils.api_utils import api
from utils.async_utils import *


# parameters guide
# 'time_budget': scaling aggressiveness to 0 at time_budget (in seconds)
# 'global_beta': other coin weight in the global risk
# 'cache_size': missed messages cache
# 'entry_tolerance': green light if basket better than quantile(entry_tolerance)
# 'rush_in_tolerance': mkt enter on both legs if basket better than quantile(rush_tolerance)
# 'rush_out_tolerance': mkt close on both legs if basket worse than quantile(rush_tolerance)
# 'stdev_window': stdev horizon for scaling parameters. in sec.
# 'edit_price_tolerance': place on edit_price_tolerance (in minutes) *  stdev
# 'aggressive_edit_price_increments': in priceIncrement
# 'edit_trigger_tolerance': chase at edit_trigger_tolerance (in minutes) *  stdev
# 'stop_tolerance': stop at stop_tolerance (in minutes) *  stdev
# 'volume_share': % of mkt volume * edit_price_tolerance. so farther is bigger.
# 'check_frequency': risk recon frequency. in seconds
# 'delta_limit': in % of pv

async def main_coroutine(order_name, **kwargs):
    parameters = configLoader.get_executor_params(order=order_name,dirname=kwargs['config'])
    order = os.path.join(os.sep, configLoader.get_config_folder_path(config_name=kwargs['config']), "pfoptimizer", order_name)
    parameters["signal_engine"]['filename'] = order

    while True:
        try:
            strategy = await Strategy.build(parameters)
            await strategy.run()
        except Exception as e:
            await strategy.exit_gracefully()
            if not isinstance(e, Strategy.ReadyToShutdown):
                logger = logging.getLogger('tradeexecutor')
                logger.critical(str(e), exc_info=True)
            else:
                raise e

@api
def main(*args,**kwargs):
    '''
        examples:
            tradeexecutor unwind exchange=ftx subaccount=debug config=prod
            tradeexecutor /home/david/config/pfoptimizer/weight_shard_0.csv config=prod
        args:
           order = 'unwind', 'flatten', or filename -> /home/david/config/pfoptimizer/weight_shard_0.csv
        kwargs:
           config =  inserts a subdirectory for config ~/config/config_dir/tradeexecutor_params.json (optional)
           exchange = 'ftx' (mandatory for 'unwind', 'flatten')
           subaccount = 'SysPerp' (mandatory for 'unwind', 'flatten')
           nb_runs = 1
   '''

    order_name = args[0]
    #config_name = kwargs.pop('config') if 'config' in kwargs else None
    logger = kwargs.pop('__logger')
    if 'config' not in kwargs:
        kwargs['config'] = None

    asyncio.run(main_coroutine(order_name, **kwargs)) # --> I am filled or I timed out and I have flattened position

 