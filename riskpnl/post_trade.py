import logging
import os
import asyncio
import pandas as pd
import json
from datetime import *
from datetime import timezone
from datetime import timedelta
import shutil
import numpy as np
from utils.async_utils import safe_gather
from utils.ccxt_utilities import open_exchange
from histfeed.ftx_history import fetch_trades_history

def batch_summarize_exec_logs(dirname = os.path.join(os.sep, 'tmp', 'tradeexecutor'),
                              start=datetime(1970,1,1),
                              end=datetime.now(),
                              rebuild = True,
                              add_history_context = False):
    '''pd.concat all logs
    add log_time column
    move unreadable log sets to unreadable subdir'''
    archive_dirname = os.path.join(os.sep,dirname, 'archive')
    unreadable_dirname = os.path.join(os.sep, archive_dirname, 'unreadable')
    if not os.path.exists(unreadable_dirname):
        os.umask(0)
        os.makedirs(unreadable_dirname, mode=0o777)

    all_files = os.listdir(archive_dirname)
    all_dates = []
    date_string_length = len(end.strftime("%Y%m%d_%H%M%S"))
    for f in all_files:
        try:
            date_from_path = datetime.strptime(f[:date_string_length],"%Y%m%d_%H%M%S")
            if start <= date_from_path <= end:
                all_dates += [f[:date_string_length]]
        except Exception as e:# always outside (start,end)
            continue

    tab_list = ['request', 'parameters','by_coin', 'by_symbol', 'by_clientOrderId', 'data', 'risk_recon'] + (
        ['history'] if add_history_context else [])
    try:
        if rebuild: raise('not an exception: just skip history on rebuild=True')
        compiled_logs = {tab: pd.read_csv(os.path.join(os.sep,dirname,f'all_{tab}.csv'),index_col='index') for tab in tab_list}
        existing_dates = set(compiled_logs['request']['log_time'].unique())
    except Exception as e:
        compiled_logs = {tab:pd.DataFrame() for tab in tab_list}
        existing_dates = set()

    removed_logs = []
    for date in set(all_dates) - existing_dates:
        try:
            new_logs = summarize_exec_logs(os.path.join(os.sep,archive_dirname,date),add_history_context)
            for key in tab_list:
                new_logs[key]['log_time'] = datetime.strptime(date,"%Y%m%d_%H%M%S").replace(tzinfo=timezone.utc)
                compiled_logs[key] = pd.concat([compiled_logs[key],new_logs[key]],axis=0)
        except Exception as e:
            for suffix in ['events', 'request', 'risk_reconciliations']:
                filename = f'{date}_{suffix}.json'
                if os.path.isfile(os.path.join(os.sep, archive_dirname, filename)):
                    removed_logs.extend(filename)
                    shutil.move(os.path.join(os.sep, archive_dirname, filename), os.path.join(os.sep, unreadable_dirname, filename))
    print(f'moved {len(removed_logs)} logs to unreadable')

    for key,value in compiled_logs.items():
        value.to_csv(os.path.join(dirname,f'all_{key}.csv'))
    print(f'printed summaries to {dirname}')

def summarize_exec_logs(path_date, add_history_context = False):
    '''compile json logs into DataFrame summaries'''
    with open(f'{path_date}_events.json', 'r') as file:
        d = json.load(file)
        events = {clientId: pd.DataFrame(data).reset_index() for clientId, data in d.items()}
    with open(f'{path_date}_risk_reconciliations.json', 'r') as file:
        d = json.load(file)
        risk = pd.DataFrame(d).reset_index()
    with open(f'{path_date}_request.json', 'r') as file:
        d = json.load(file)
        parameters = pd.Series(d.pop('parameters')).reset_index()
        request = pd.DataFrame(d).T.reset_index()

    data = pd.concat([data for clientOrderId, data in events.items()], axis=0)
    data.loc[data['lifecycle_state']=='filled','filled'] = data.loc[data['lifecycle_state']=='filled','amount']
    if data[data['filled']>0].empty:
        raise Exception('no fill to parse')

    # pick biggest 'filled' for each clientOrderId
    temp_events = {clientOrderId:
                       {'inception_event':clientOrderId_data[clientOrderId_data['lifecycle_state']=='pending_new'].iloc[0],
                        'last_fill_event':clientOrderId_data[clientOrderId_data['filled']>0].iloc[-1]}
                   for clientOrderId, clientOrderId_data in data.groupby(by='clientOrderId') if clientOrderId_data['filled'].max()>0}
    by_clientOrderId = pd.DataFrame({
        clientOrderId:
            {'symbol':clientOrderId_data['inception_event']['symbol'],
             'coin': clientOrderId_data['inception_event']['symbol'].split('/')[0],
             'slice_started' : clientOrderId_data['inception_event']['timestamp'],
             'mid_at_inception' : 0.5*(clientOrderId_data['inception_event']['bid']+clientOrderId_data['inception_event']['ask']),
             'amount' : clientOrderId_data['inception_event']['amount']*(1 if clientOrderId_data['last_fill_event']['side']=='buy' else -1),

             'filled' : clientOrderId_data['last_fill_event']['filled']*(1 if clientOrderId_data['last_fill_event']['side']=='buy' else -1),
             'price' : clientOrderId_data['last_fill_event']['price'],
             #TODO: what happens when there are only partial fills ?
             'fee': sum(x['cost']*(1 if x['currency']=='USD' else
                                   (x['currency'] == clientOrderId_data['last_fill_event']['price'] if clientOrderId_data['inception_event']['symbol'][:3] else
                                    np.NAN))
                        for x in clientOrderId_data['last_fill_event']['fees']),
             'slice_ended' : clientOrderId_data['last_fill_event']['timestamp']}
        for clientOrderId,clientOrderId_data in temp_events.items()}).T.reset_index()

    if __debug__:
        partially = data[(data['amount'] != data['filled']) & (data['filled'] > 0)]
        pass

    by_symbol = pd.DataFrame({
        symbol:
            {'time_to_execute':symbol_data['slice_ended'].max()-symbol_data['slice_started'].min(),
             'slippage_bps': 10000*np.sign(symbol_data['filled'].sum())*((symbol_data['filled']*symbol_data['price']).sum()/symbol_data['filled'].sum()/request.loc[request['index']==symbol,'spot'].squeeze()-1),
             'fee': 10000*symbol_data['fee'].sum()/np.abs((symbol_data['filled']*symbol_data['price']).sum()),
             'filledUSD': (symbol_data['filled']*symbol_data['price']).sum(),
             'coin': symbol.split('/')[0]
             }
        for symbol,symbol_data in by_clientOrderId.groupby(by='symbol')}).T.reset_index()

    by_coin = pd.DataFrame({
        coin:
            {'premium_vs_inception_bps': (coin_data['slippage_bps']*coin_data['filledUSD'].apply(np.abs)).sum()/coin_data['filledUSD'].apply(np.abs).sum()*2,
             'perleg_fee_bps': coin_data['fee'].sum()/coin_data['filledUSD'].apply(np.abs).sum()*2,
             'perleg_filled_USD': coin_data['filledUSD'].apply(np.abs).sum()/2
             }
        for coin,coin_data in by_symbol.groupby(by='coin')}).T.reset_index()

    fill_history = []
    if add_history_context:
        for symbol, symbol_data in by_clientOrderId.groupby(by='symbol'):
            df = symbol_data[['slice_ended','filled','price']]
            df['slice_ended'] = df['slice_ended'].apply(lambda t:datetime.utcfromtimestamp(t/1000).replace(tzinfo=timezone.utc))
            df.set_index('slice_ended', inplace=True)
            df = df.rename(columns={
                'price':symbol.replace('/USD:USD','-PERP').replace('/USD','')+'_fills_price',
                'filled':symbol.replace('/USD:USD','-PERP').replace('/USD','')+'_fills_filled'})
            fill_history += [df]

        async def build_vwap(start, end):
            date_start = datetime.utcfromtimestamp(start / 1000).replace(tzinfo=timezone.utc)
            date_end = datetime.utcfromtimestamp(end / 1000).replace(tzinfo=timezone.utc)
            exchange = await open_exchange('ftx','SysPerp') # subaccount doesn't matter
            await exchange.load_markets()
            trades_history_list = await safe_gather([fetch_trades_history(
                exchange.market(symbol)['id'], exchange, date_start,date_end, frequency=timedelta(seconds=1))
                for symbol in by_symbol['index'].values])
            await exchange.close()

            return pd.concat([x['vwap'] for x in trades_history_list], axis=1,join='outer')

        start_time = request['inception_time']
        history = pd.concat([asyncio.run(build_vwap(start_time,by_clientOrderId['slice_ended'].max()))]+fill_history).sort_index()

    by_symbol = pd.concat([by_symbol,
                           pd.DataFrame({'index': ['average'],
                                         'fee': [(by_symbol['filledUSD'].apply(np.abs) * by_symbol['fee']).sum() /
                                                 by_symbol['filledUSD'].apply(np.abs).sum()],
                                         'filledUSD': by_symbol['filledUSD'].apply(np.abs).sum(),
                                         'slippage_bps': [
                                             (by_symbol['filledUSD'].apply(np.abs) * by_symbol['slippage_bps']).sum() /
                                             by_symbol['filledUSD'].apply(np.abs).sum()]})],
                          axis=0,ignore_index=True)

    by_coin = pd.concat([by_coin,
                           pd.DataFrame({'index': ['average'],
                                         'perleg_fee_bps': [(by_coin['perleg_filled_USD'].apply(np.abs) * by_coin['perleg_fee_bps']).sum() /
                                                 by_coin['perleg_filled_USD'].apply(np.abs).sum()],
                                         'perleg_filled_USD': by_coin['perleg_filled_USD'].apply(np.abs).sum(),
                                         'premium_vs_inception_bps': [
                                             (by_coin['perleg_filled_USD'].apply(np.abs) * by_coin['premium_vs_inception_bps']).sum() /
                                             by_coin['perleg_filled_USD'].apply(np.abs).sum()]})],
                          axis=0,ignore_index=True)

    return {
        'request':request,
        'parameters':parameters,
        'by_coin':by_coin,
        'by_symbol':by_symbol,
        'by_clientOrderId':by_clientOrderId,
        'data':data,
        'risk_recon':risk
    } | ({'history':history} if add_history_context else {})