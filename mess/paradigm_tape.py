# built ins
import base64
import hmac
import time
import utils.ccxt_utilities
# installed
import requests

access_key = 'paradigm'
secret_key = b'<secret-key>'


def sign_request(secret_key, method, path, body):
    signing_key = base64.b64decode(secret_key)
    timestamp = str(int(time.time() * 1000)).encode('utf-8')
    message = b'\n'.join([timestamp, method.upper(), path, body])
    digest = hmac.digest(signing_key, message, 'sha256')
    signature = base64.b64encode(digest)
    return timestamp, signature


def paradigm_request(path,access_key,secret_key):
    # Request Host
    host = 'https://api.fs.test.paradigm.co'
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
    return response

if __name__ == "__main__":
    import sys
    from utils.ccxt_utilities import api_params
    args = list(sys.argv[1:])
    if len(args) <1:
        args = ['/v1/fs/trade_tape']
    access_key = 'EytZmov5bDDPGXqvYviriCs8',
    secret_key = api_params['paradigm']['key']
    paradigm_request(args[0],access_key,secret_key)