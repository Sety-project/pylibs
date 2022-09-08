# built ins
import base64
import hmac
import time

import pandas as pd

import utils.ccxt_utilities
# installed
import requests

def sign_request(secret_key, method, path, body):
    signing_key = base64.b64decode(secret_key)
    timestamp = str(int(time.time() * 1000)).encode('utf-8')
    message = b'\n'.join([timestamp, method.upper(), path, body])
    digest = hmac.digest(signing_key, message, 'sha256')
    signature = base64.b64encode(digest)
    return timestamp, signature

def paradigm_request(path,access_key,secret_key):
    # Request Host
    host = 'https://api.fs.chat.paradigm.co'
    # GET /v1/fs/trades
    method = 'GET'
    payload = ''
    timestamp, signature = sign_request(secret_key=secret_key,
                                        method=method.encode('utf-8'),
                                        path=path.encode('utf-8'),
                                        body=payload.encode('utf-8'),
                                        )
    headers = {
        'Paradigm-API-Timestamp': timestamp,
        'Paradigm-API-Signature': signature,
        'Authorization': f'Bearer {access_key}'
    }
    # Send request
    response = requests.get(host + path,
                            headers=headers)
    return response.json()['results']

def fetch_trades_history(query = '/v1/fs/trade_tape',
                         access_key='EytZmov5bDDPGXqvYviriCs8',
                         secret_key = utils.ccxt_utilities.api_params['paradigm']['key']):
    return paradigm_request(query, access_key, secret_key)

def fetch_order_book(query = '/v1/fs/strategies/58987290357137409/order-book',
                         access_key='EytZmov5bDDPGXqvYviriCs8',
                         secret_key = utils.ccxt_utilities.api_params['paradigm']['key']):
    result = paradigm_request(query, access_key, secret_key)
    asks = pd.DataFrame(result['asks'],columns=['price','amount_decimal'],dtype=float)
    asks['usdAmt'] = (asks['price'] * asks['amount_decimal']).cumsum()/asks['amount_decimal'].cumsum()
    bids = pd.DataFrame(result['bids'], columns=['price', 'amount_decimal'], dtype=float)
    bids['usdAmt'] = (bids['price'] * bids['amount_decimal']).cumsum() / bids['amount_decimal'].cumsum()

if __name__ == "__main__":
    import sys
    from utils.ccxt_utilities import api_params
    args = list(sys.argv[1:])
    if len(args) <1:
        args = ['/v1/fs/trade_tape']
    access_key = 'EytZmov5bDDPGXqvYviriCs8' # '1fXKxjO15fqJiQNrpMo8ey4g'
    secret_key = api_params['paradigm']['key'] # b'ApjxPBh2kkIU7AFDStwZUYWliJdHWWyhRhHuVwCJjig//RMF'
    paradigm_request(args[0],access_key,secret_key)

    fetch_order_book()