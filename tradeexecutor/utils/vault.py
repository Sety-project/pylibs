import boto3
import os
import json, logging
from web3 import Web3
import hvac
from hvac.api.auth_methods import Kubernetes

class VaultEthereumWallet():

    def __init__(self, wallet_id=""):
        if len(wallet_id) == 0:
            wallet_id = os.environ.get("VAULT_WALLET_ID")

        self.requests = VaultEthereumWallet.load_vault_requests()
        self.chain_map = {
            1: "ethereum",
            137: "matic",
            10: "optimism"
        }
        self.chain_cache = {}

        self.id = wallet_id

        get_address_path = f"vault-ethereum/accounts/{self.id}"
        res_dict = self.requests.get(
            f"/v1/{get_address_path}",
        )

        self.address = res_dict['data']['address']

    @staticmethod
    def load_vault_requests():
        vault_addr = os.environ.get("VAULT_ADDR")
        vault_auth_method = os.environ.get("VAULT_AUTH_METHOD")
        vault_auth_role = os.environ.get("VAULT_AUTH_ROLE")

        assert vault_addr is not None and len(vault_addr) > 0, "VAULT_ADDR environment variable not set"

        if vault_auth_method == "AWS_IAM":
            logging.info(f"Logging to vault with AWS IAM method on: {vault_addr}")
            session = boto3.Session()
            credentials = session.get_credentials()
            client = hvac.Client(url=vault_addr)
            auth_res = client.auth.aws.iam_login(credentials.access_key, credentials.secret_key, credentials.token)
            return client.adapter

        if vault_auth_method == "KUBERNETES":
            logging.info(f"Logging to vault with Kubernetes method on: {vault_addr}")

            f = open('/var/run/secrets/kubernetes.io/serviceaccount/token')
            jwt = f.read()
            Kubernetes(client.adapter).login(role=vault_auth_role, jwt=jwt)
            return client.adapter

        if vault_auth_method == "TOKEN":
            logging.info(f"Logging to vault with token method on: {vault_addr}")
            client = hvac.Client(url=vault_addr)
            client.token = os.environ.get("VAULT_TOKEN")
            return client.adapter

    @staticmethod
    def create_wallet(wallet_id):

        vault_requests = VaultEthereumWallet.load_vault_requests()
        new_wallet_path = f"vault-ethereum/accounts/{wallet_id}"

        res_dict = vault_requests.put(
            f"/v1/{new_wallet_path}",
            data=""
        )

        wallet = VaultEthereumWallet(wallet_id=wallet_id)
        return wallet

    @staticmethod
    def import_wallet(wallet_id, mnemonic, derivation_path_index):

        vault_requests = VaultEthereumWallet.load_vault_requests()
        new_wallet_path = f"vault-ethereum/accounts/{wallet_id}"

        res_dict = vault_requests.put(
            f"/v1/{new_wallet_path}",
            data={
                'mnemonic': mnemonic,
                'index': derivation_path_index
            }
        )

        wallet = VaultEthereumWallet(wallet_id=wallet_id)
        return wallet

    def get_chains(self):
        get_chains_path = f"vault-ethereum/chains?list=true"

        res_dict = self.requests.get(
            f"/v1/{get_chains_path}",
        )

        return res_dict['data']['keys']

    @staticmethod
    def static_get_chains():
        vault_requests = VaultEthereumWallet.load_vault_requests()
        get_chains_path = f"vault-ethereum/chains?list=true"

        res_dict = vault_requests.get(
            f"/v1/{get_chains_path}",
        )

        return res_dict['data']['keys']

    def get_chain(self, chain):
        if self.chain_cache[chain]:
            return self.chain_cache[chain]
        get_chain_path = f"vault-ethereum/chains/{chain}"

        res_dict = self.requests.get(
            f"/v1/{get_chain_path}",
        )
        self.chain_cache[chain] = res_dict['data']
        return res_dict['data']

    def get_chain_by_id(self, chain_id):
        chain = self.chain_map[chain_id]
        if self.chain_cache[chain]:
            return self.chain_cache[chain]

        get_chain_path = f"vault-ethereum/chains/{chain}"

        res_dict = self.requests.get(
            f"/v1/{get_chain_path}",
        )

        self.chain_cache[chain] = res_dict['data']
        return res_dict['data']

    @staticmethod
    def static_get_chain(chain):
        vault_requests = VaultEthereumWallet.load_vault_requests()
        get_chain_path = f"vault-ethereum/chains/{chain}"

        res_dict = vault_requests.get(
            f"/v1/{get_chain_path}",
        )

        return res_dict['data']

    def sign_message(self, msg):
        sign_message_path = f"vault-ethereum/accounts/{self.id}/sign"

        res_dict = self.requests.put(
            f"/v1/{sign_message_path}",
            data=json.dumps({
                'message': msg
            })
        )

        return res_dict['data']['signature']

    def sign_and_send_tx(self, tx):
        error = None

        chain = self.get_chain_by_id(chain_id=tx.chainId)

        w3 = Web3(Web3.HTTPProvider(chain['rpc_url']))
        nonce = w3.eth.get_transaction_count(
            Web3.toChecksumAddress(self.address)
        )

        gas_price = w3.eth.generate_gas_price() * 1.5

        sign_tx_path = f"vault-ethereum/accounts/{self.id}/sign-tx"

        formated_tx = {
            'to': tx.to,
            'encoding': 'hex',
            'amount': int(tx.value),
            'data': tx.data[2:],
            'chain': tx.chain,
            'nonce': nonce,
            'gas_price': str(gas_price),
            'gas_limit': str("10000000")
        }

        res_dict = self.requests.put(
            f"/v1/{sign_tx_path}",
            data=json.dumps(formated_tx)
        )

        tx_hash = res_dict['data']['transaction_hash']

        print(f"send raw tx on {chain['rpc_url']}")
        try:
            w3.eth.send_raw_transaction(res_dict['data']['signed_transaction'])
            tx_receipt = w3.eth.wait_for_transaction_receipt(tx_hash)
            success = True
        except Exception as e:
            success = False
            error = str(e)

            tx_receipt = None

        return success, tx_receipt, chain['rpc_url'], error