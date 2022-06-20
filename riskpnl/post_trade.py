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

def batch_log_reader(dirname = os.path.join(os.sep, 'tmp', 'tradeexecutor','archive')):
    '''pd.concat all logs
    add log_time column
    move unreadable log sets to unreadable subdir'''
    tab_list = ['by_symbol', 'request', 'by_clientOrderId', 'data', 'history', 'risk_recon']
    all_files = os.listdir(dirname)
    all_dates = set([f[:15] for f in all_files])

    try:
        compiled_logs = pd.read_excel(dirname.replace('/archive','/all_exec.xlsx'),sheet_name=tab_list,index_col='index')
        existing_dates = set(compiled_logs['request']['log_time'].apply(lambda d: d.strftime("%Y-%m-%d-%H-%M")))
    except Exception as e:
        compiled_logs = {tab:pd.DataFrame() for tab in tab_list}
        existing_dates = set()

    for date in all_dates - existing_dates:
        try:
            new_logs = log_reader(dirname, date)
            for key in tab_list:
                compiled_logs[key] = pd.concat([compiled_logs[key],new_logs[key]],axis=0)
        except Exception as e:
            for suffix in ['events', 'request', 'risk_reconciliations']:
                filename = os.path.join(os.sep, dirname,f'{date}_{suffix}.json')
                if os.path.isfile(filename):
                    shutil.move(filename, os.path.join(os.sep, dirname,'unreadable',f'{date}_{suffix}.json'))
    log_writer(os.path.join(os.sep, 'tmp', 'tradeexecutor','all'),compiled_logs)

def log_reader(dirname = os.path.join(os.sep, 'tmp', 'tradeexecutor'),date = 'latest'):
    '''compile json logs, or move to 'unreadable' directory if it fails'''
    path = os.path.join(os.sep,dirname,date)
    with open(f'{path}_events.json', 'r') as file:
        d = json.load(file)
        events = {clientId: pd.DataFrame(data).reset_index() for clientId, data in d.items()}
    with open(f'{path}_risk_reconciliations.json', 'r') as file:
        d = json.load(file)
        risk = pd.DataFrame(d).reset_index()
    with open(f'{path}_request.json', 'r') as file:
        d = json.load(file)
        parameters = d.pop('parameters')
        start_time = parameters['inception_time']
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
             'slice_started' : clientOrderId_data['inception_event']['timestamp'],
             'mid_at_inception' : 0.5*(clientOrderId_data['inception_event']['bid']+clientOrderId_data['inception_event']['ask']),
             'amount' : clientOrderId_data['inception_event']['amount']*(1 if clientOrderId_data['last_fill_event']['side']=='buy' else -1),

             'filled' : clientOrderId_data['last_fill_event']['filled']*(1 if clientOrderId_data['last_fill_event']['side']=='buy' else -1),
             'price' : clientOrderId_data['last_fill_event']['price'],
             'fee': sum(data.loc[data['clientOrderId']==clientOrderId,'fee'].dropna().apply(lambda x:x['cost'])),
             'slice_ended' : clientOrderId_data['last_fill_event']['timestamp']}
        for clientOrderId,clientOrderId_data in temp_events.items()}).T.reset_index()

    by_symbol = pd.DataFrame({
        symbol:
            {'time_to_execute':symbol_data['slice_started'].max()-symbol_data['slice_started'].min(),
             'slippage_bps': 10000*np.sign(symbol_data['filled'].sum())*((symbol_data['filled']*symbol_data['price']).sum()/symbol_data['filled'].sum()/request.loc[request['index']==symbol,'spot'].squeeze()-1),
             'fee': 10000*symbol_data['fee'].sum()/np.abs((symbol_data['filled']*symbol_data['price']).sum()),
             'filledUSD': (symbol_data['filled']*symbol_data['price']).sum()
             }
        for symbol,symbol_data in by_clientOrderId.groupby(by='symbol')}).T.reset_index()

    fill_history = []
    for symbol, symbol_data in by_clientOrderId.groupby(by='symbol'):
        df = symbol_data[['slice_ended','filled','price']]
        df['slice_ended'] = df['slice_ended'].apply(lambda t:datetime.utcfromtimestamp(t/1000).replace(tzinfo=timezone.utc))
        df.set_index('slice_ended', inplace=True)
        df = df.rename(columns={
            'price':symbol.replace('/USD:USD','-PERP').replace('/USD','')+'/fills/price',
            'filled':symbol.replace('/USD:USD','-PERP').replace('/USD','')+'/fills/filled'})
        fill_history += [df]

    async def build_vwap(start, end):
        date_start = datetime.utcfromtimestamp(start / 1000).replace(tzinfo=timezone.utc)
        date_end = datetime.utcfromtimestamp(end / 1000).replace(tzinfo=timezone.utc)
        exchange = await open_exchange('ftx','SysPerp')
        await exchange.load_markets()
        trades_history_list = await safe_gather([fetch_trades_history(
            exchange.market(symbol)['id'], exchange, date_start,date_end, frequency=timedelta(seconds=1))
            for symbol in by_symbol['index'].values])
        await exchange.close()

        return pd.concat([x['vwap'] for x in trades_history_list], axis=1,join='outer')

    history = pd.concat([asyncio.run(build_vwap(start_time,by_clientOrderId['slice_ended'].max()))]+fill_history).sort_index()

    by_symbol = by_symbol.append(pd.Series({'index': 'average', 'fee': (by_symbol['filledUSD'].apply(
        np.abs) * by_symbol['fee']).sum() / by_symbol['filledUSD'].apply(
        np.abs).sum()}), ignore_index=True)
    by_symbol = by_symbol.append(pd.Series({'index': 'average', 'slippage_bps': (by_symbol['filledUSD'].apply(
        np.abs) * by_symbol['slippage_bps']).sum() / by_symbol['filledUSD'].apply(
        np.abs).sum()}), ignore_index=True)

    result = {'by_symbol':by_symbol,
            'request':request,
            'by_clientOrderId':by_clientOrderId,
            'data':data,
            'history':history,
            'risk_recon':risk}
    log_time = datetime.strptime(date, "%Y%m%d_%H%M%S")
    return {key: pd.concat([value,pd.Series(name='log_time',
                                            index=value.index,
                                            data=log_time)],axis=1)
            for key,value in result.items()}

def log_writer(path,tab_dict):
    with pd.ExcelWriter(f'{path}_exec.xlsx', engine='xlsxwriter', mode="w") as writer:
        for tab_name,df in tab_dict.items():
            try:
                df.index = [t.replace(tzinfo=None) for t in df.index]
            except Exception as e:
                pass

            if not df.empty:
                df.to_excel(writer, sheet_name=tab_name)