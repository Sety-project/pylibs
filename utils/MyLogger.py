from datetime import *

import pandas as pd

from histfeed.ftx_history import fetch_trades_history
from utils.ccxt_utilities import open_exchange
from utils.io_utils import *
from utils.async_utils import *
from utils.api_utils import build_logging,extract_args_kwargs

class ExecutionLogger:
    '''it s a logger that can also write jsons, and summarize them'''
    def __init__(self,log_date=datetime.utcnow(),exchange_name='ftx'):
        log_path = os.path.join(os.sep, 'tmp', 'tradeexecutor', 'archive')
        if not os.path.exists(log_path):
            os.umask(0)
            os.makedirs(log_path, mode=0o777)
        self.json_filename = os.path.join(os.sep, 'tmp', 'tradeexecutor', 'archive',
                                          f'{log_date.strftime("%Y%m%d_%H%M%S")}.json')

        #hash_id = hash(json.dumps(config|{'order_name':order_name}|({'timestamp': log_date.timestamp()} if log_date else {}),sort_keys=True))
        self.mktdata_dirname = configLoader.get_mktdata_folder_for_exchange(f'{exchange_name}_tickdata')
        if not os.path.exists(self.mktdata_dirname):
            os.umask(0)
            os.makedirs(self.mktdata_dirname, mode=0o777)

        self.history = []

    async def write_data(self, data):
        async with aiofiles.open(self.json_filename, 'w') as file:
            await file.write(json.dumps(data))

    @staticmethod
    def batch_summarize_exec_logs(exchange='ftx', subaccount='',dirname=os.path.join(os.sep, 'tmp','','tradeexecutor'),
                                  start=datetime(1970, 1, 1),
                                  end=datetime.now(),
                                  rebuild=True,
                                  add_history_context=False):
        '''pd.concat all logs
        add log_time column
        move unreadable log sets to unreadable subdir'''
        archive_dirname = os.path.join(os.sep, dirname, 'archive')
        unreadable_dirname = os.path.join(os.sep, archive_dirname, 'unreadable')
        if not os.path.exists(unreadable_dirname):
            os.umask(0)
            os.makedirs(unreadable_dirname, mode=0o777)

        # read already compiled logs
        tab_list = ['inventory_manager', 'parameters', 'by_coin', 'by_symbol', 'by_clientOrderId', 'order_manager', 'risk_recon'] + (
            ['history'] if add_history_context else [])
        try:
            if rebuild: raise Exception('not an exception: just skip history on rebuild=True')
            concatenated_logs = {tab: pd.read_csv(os.path.join(os.sep, dirname, f'all_{tab}.csv'), index_col='index') for
                             tab in tab_list}
            max_compiled_date = concatenated_logs['inventory_manager']['timestamp'].max()
        except Exception as e:
            concatenated_logs = {tab: pd.DataFrame() for tab in tab_list}
            max_compiled_date = datetime(1970,1,1)

        # find files not compiled yet
        filenames = []
        for f in os.listdir(archive_dirname):
            try:
                modification_date = os.path.getmtime(os.path.join(os.sep, archive_dirname, f))
                if max(max_compiled_date,start) <= datetime.utcfromtimestamp(modification_date) <= end and not os.path.isdir(os.path.join(os.sep, archive_dirname, f)):
                    filenames += [f]
            except Exception as e:  # always outside (start,end)
                continue

        concatenated_logs,removed_filenames = ExecutionLogger.concatenate_logs(filenames, archive_dirname)

        compiled_logs = ExecutionLogger.summarize_exec_logs(concatenated_logs,start=start,end=end,add_history_context=add_history_context)
        for key, value in compiled_logs.items():
            value.to_csv(os.path.join(dirname, f'all_{key}.csv'))

        return f'moved {len(removed_filenames)} logs to unreadable'

    @staticmethod
    def concatenate_logs(filenames,archive_dirname):
        result = None
        removed_filenames = []
        for filename in filenames:
            try:
                with open(os.path.join(os.sep, archive_dirname, filename), 'r') as file:
                    d = json.load(file)
                new_log = {'parameters': pd.DataFrame(pd.Series(d['parameters'])).T,
                           'inventory_manager': pd.DataFrame(
                               sum([list(row.values()) for row in d['inventory_manager']], [])),
                           'order_manager': pd.DataFrame(sum(list(d['order_manager'].values()), [])),
                           'position_manager': pd.DataFrame(d['position_manager'])}
            except Exception as e:
                if os.path.isfile(os.path.join(os.sep, archive_dirname, filename)):
                    removed_filenames.extend(filename)
                    shutil.move(os.path.join(os.sep, archive_dirname, filename),
                                os.path.join(os.sep, os.path.join(os.sep, archive_dirname, 'unreadable'), filename))
            else:
                if not result:
                    result = new_log
                else:
                    for key, value in new_log.items():
                        result[key] = pd.concat([result[key], value], axis=0)

        return result,removed_filenames

    @staticmethod
    def summarize_exec_logs(df,start,end,add_history_context=False):
        '''summarize DataFrame'''
        parameters = df['parameters']
        inventory_manager = df['inventory_manager']
        order_manager = df['order_manager']
        position_manager = df['position_manager'].rename(columns={'delta_timestamp':'timestamp'})

        for df in [parameters,inventory_manager,order_manager,position_manager]:
            df = df[(df['timestamp']>start.timestamp()*1000)&(df['timestamp']<end.timestamp()*1000)]

        if order_manager.empty:
            raise Exception('no fill to parse')

        order_manager.loc[order_manager['state'] == 'filled', 'filled'] = order_manager.loc[
            order_manager['state'] == 'filled', 'amount']
        if order_manager[order_manager['filled'] > 0].empty:
            raise Exception('no fill to parse')

        # pick biggest 'filled' for each clientOrderId
        temp_events = dict()
        for clientOrderId, clientOrderId_data in order_manager.groupby(by='clientOrderId'):
            if clientOrderId_data['filled'].max() > 0:
                temp_events[clientOrderId] = dict()
                temp_events[clientOrderId]['inception_event'] = clientOrderId_data[clientOrderId_data['state'] == 'pending_new'].iloc[0]
                order_timestamp = inventory_manager.loc[inventory_manager['timestamp']<temp_events[clientOrderId]['inception_event']['timestamp'],'timestamp'].max()
                temp_events[clientOrderId]['order_ref'] = inventory_manager.loc[(inventory_manager['timestamp']==order_timestamp)&(inventory_manager['symbol']==clientOrderId_data['symbol'].unique()[0]),'spot_price'].unique()[0]
                temp_events[clientOrderId]['ack_event'] = clientOrderId_data[clientOrderId_data['state'].isin(['acknowledged', 'partially_filled', 'filled'])].iloc[0]
                temp_events[clientOrderId]['last_fill_event'] = clientOrderId_data[clientOrderId_data['filled'] > 0].iloc[-1]
                temp_events[clientOrderId]['filled'] = clientOrderId_data['filled'].max()

        by_clientOrderId = pd.DataFrame({
            clientOrderId:
                {'symbol': clientOrderId_data['inception_event']['symbol'],
                 'coin': clientOrderId_data['inception_event']['symbol'].split('/')[0],
                 'order_ref': clientOrderId_data['order_ref'],
                 'pending_local': clientOrderId_data['inception_event']['timestamp'],
                 'pending_to_ack_local': clientOrderId_data['ack_event']['timestamp'] -
                                        clientOrderId_data['inception_event']['timestamp'],
                 'mid_at_pending': 0.5 * (
                             clientOrderId_data['inception_event']['bid'] + clientOrderId_data['inception_event'][
                         'ask']),
                 'amount': clientOrderId_data['inception_event']['amount'] * (
                     1 if clientOrderId_data['last_fill_event']['side'] == 'buy' else -1),

                 'filled': clientOrderId_data['filled'] * (
                     1 if clientOrderId_data['last_fill_event']['side'] == 'buy' else -1),
                 'price': clientOrderId_data['last_fill_event']['price'],
                 # TODO: what happens when there are only partial fills ?
                 'fee': sum(x['cost'] * (1 if x['currency'] == 'USD' else
                                         (x['currency'] == clientOrderId_data['last_fill_event']['price'] if
                                          clientOrderId_data['inception_event']['symbol'][:3] else
                                          np.NAN))
                            for x in clientOrderId_data['last_fill_event']['fees']),
                 'last_fill_local': clientOrderId_data['last_fill_event']['timestamp']}
            for clientOrderId, clientOrderId_data in temp_events.items()}).T.reset_index()

        if __debug__:
            #partially = data[(data['amount'] != data['filled']) & (data['filled'] > 0)]
            pass

        temp = dict()
        for symbol, symbol_data in by_clientOrderId.groupby(by='symbol'):
            temp[symbol] = dict()
            temp[symbol]['time_to_execute'] = symbol_data['last_fill_local'].max() - symbol_data['pending_local'].min()
            temp[symbol]['slippage_bps'] = 10000 * np.sign(symbol_data['filled'].sum()) * (
                             (symbol_data['filled'] * (symbol_data['price'] / symbol_data['order_ref'] - 1)).sum() / symbol_data['filled'].sum())

            temp[symbol]['fee'] = 10000 * symbol_data['fee'].sum() / np.abs((symbol_data['filled'] * symbol_data['price']).sum())
            temp[symbol]['filledUSD'] = (symbol_data['filled'] * symbol_data['price']).sum()
            temp[symbol]['coin'] = symbol.split('/')[0]

        by_symbol = pd.DataFrame(temp).T.reset_index()

        by_coin = pd.DataFrame({
            coin:
                {'premium_vs_inception_bps': (coin_data['slippage_bps'] * coin_data['filledUSD'].apply(np.abs)).sum() /
                                             coin_data['filledUSD'].apply(np.abs).sum() * 2,
                 'perleg_fee_bps': coin_data['fee'].sum() / coin_data['filledUSD'].apply(np.abs).sum() * 2,
                 'perleg_filled_USD': coin_data['filledUSD'].apply(np.abs).sum() / 2
                 }
            for coin, coin_data in by_symbol.groupby(by='coin')}).T.reset_index()

        fill_history = []
        if add_history_context:
            for symbol, symbol_data in by_clientOrderId.groupby(by='symbol'):
                df = symbol_data[['last_fill_local', 'filled', 'price']]
                df['last_fill_local'] = df['last_fill_local'].apply(
                    lambda t: datetime.utcfromtimestamp(t / 1000).replace(tzinfo=timezone.utc))
                df.set_index('last_fill_local', inplace=True)
                df = df.rename(columns={
                    'price': symbol.replace('/USD:USD', '-PERP').replace('/USD', '') + '_fills_price',
                    'filled': symbol.replace('/USD:USD', '-PERP').replace('/USD', '') + '_fills_filled'})
                fill_history += [df]

            async def build_vwap(start, end):
                date_start = datetime.utcfromtimestamp(start / 1000).replace(tzinfo=timezone.utc)
                date_end = datetime.utcfromtimestamp(end / 1000).replace(tzinfo=timezone.utc)
                exchange = await open_exchange('ftx', 'SysPerp')  # subaccount doesn't matter
                await exchange.load_markets()
                trades_history_list = await safe_gather([fetch_trades_history(
                    exchange.market(symbol)['id'], exchange, date_start, date_end, frequency=timedelta(seconds=1))
                    for symbol in by_symbol['index'].values])
                await exchange.close()

                return pd.concat([x['vwap'] for x in trades_history_list], axis=1, join='outer')

            history = pd.concat([asyncio.run(
                build_vwap(order_received_timestamp, by_clientOrderId['last_fill_local'].max()))] + fill_history).sort_index()

        by_symbol = pd.concat([by_symbol,
                               pd.DataFrame({'index': ['average'],
                                             'fee': [(by_symbol['filledUSD'].apply(np.abs) * by_symbol['fee']).sum() /
                                                     by_symbol['filledUSD'].apply(np.abs).sum()],
                                             'filledUSD': by_symbol['filledUSD'].apply(np.abs).sum(),
                                             'slippage_bps': [
                                                 (by_symbol['filledUSD'].apply(np.abs) * by_symbol[
                                                     'slippage_bps']).sum() /
                                                 by_symbol['filledUSD'].apply(np.abs).sum()]})],
                              axis=0, ignore_index=True)

        by_coin = pd.concat([by_coin,
                             pd.DataFrame({'index': ['average'],
                                           'perleg_fee_bps': [(by_coin['perleg_filled_USD'].apply(np.abs) * by_coin[
                                               'perleg_fee_bps']).sum() /
                                                              by_coin['perleg_filled_USD'].apply(np.abs).sum()],
                                           'perleg_filled_USD': by_coin['perleg_filled_USD'].apply(np.abs).sum(),
                                           'premium_vs_inception_bps': [
                                               (by_coin['perleg_filled_USD'].apply(np.abs) * by_coin[
                                                   'premium_vs_inception_bps']).sum() /
                                               by_coin['perleg_filled_USD'].apply(np.abs).sum()]})],
                            axis=0, ignore_index=True)

        return {    'inventory_manager': inventory_manager,
                    'parameters': parameters,
                    'by_coin': by_coin,
                    'by_symbol': by_symbol,
                    'by_clientOrderId': by_clientOrderId,
                    'order_manager': order_manager,
                    'position_manager': position_manager
                    } | ({'history': history} if add_history_context else {})

if __name__ == "__main__":
    args, kwargs = extract_args_kwargs(sys.argv)
    ExecutionLogger.batch_summarize_exec_logs(*args,**kwargs)