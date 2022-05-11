import pandas as pd

static_params = pd.read_excel('Runtime/configs/static_params.xlsx',sheet_name='params',index_col='key')
NB_BLOWUPS = int(static_params.loc['NB_BLOWUPS','value'])#3)
SHORT_BLOWUP = float(static_params.loc['SHORT_BLOWUP','value'])# = 0.3
LONG_BLOWUP = float(static_params.loc['LONG_BLOWUP','value'])# = 0.15
EQUITY = str(static_params.loc['EQUITY','value']) #can be subaccount name, filename or number
OPEN_ORDERS_HEADROOM = float(static_params.loc['OPEN_ORDERS_HEADROOM','value'])#=1e5
SIGNAL_HORIZON = pd.Timedelta(static_params.loc['SIGNAL_HORIZON','value'])
HOLDING_PERIOD = pd.Timedelta(static_params.loc['HOLDING_PERIOD','value'])
SLIPPAGE_OVERRIDE = float(static_params.loc['SLIPPAGE_OVERRIDE','value'])
CONCENTRATION_LIMIT = float(static_params.loc['CONCENTRATION_LIMIT','value'])
MKTSHARE_LIMIT = float(static_params.loc['MKTSHARE_LIMIT','value'])
MINIMUM_CARRY = float(static_params.loc['MINIMUM_CARRY','value'])
EXCLUSION_LIST = [c for c in static_params.loc['REBASE_TOKENS','value'].split('+')]+[c for c in static_params.loc['EXCLUSION_LIST','value'].split('+')]
DELTA_BLOWUP_ALERT = float(static_params.loc['DELTA_BLOWUP_ALERT','value'])
UNIVERSE = str(static_params.loc['UNIVERSE','value'])
TYPE_ALLOWED = [c for c in static_params.loc['TYPE_ALLOWED','value'].split('+')]