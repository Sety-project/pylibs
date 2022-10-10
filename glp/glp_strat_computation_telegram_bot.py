import datetime
import os, time, json, schedule
from web3 import Web3
from utils.api_utils import api
from utils.io_utils import *

from constants import NullAddress, USDC_ABI, PositionRouterABI, PositionRouter, RewardRouter, RewardRouterABI, VaultUtilsABI, VaultUtils, GLPVault, MIM, WETH, WBTC, WAVAX, USDC, USDC_E, BTC_E, GLPSC, GLP, GLPManagerABI, GLPManagerAdd, ReaderABI, ReaderAdd, VaultAdd, VaultABI, Amine_Account, fsGLP
import urllib.request
import requests
import asyncio


def get_liquidation_fees(fees_trade, size_, borrow_fee_, liquidation_fee, collateral_, average_price_):
  try:
    liquidation_fees = fees_trade*size_+borrow_fee_+liquidation_fee
    liquidation_from_delta_fee = average_price_-(liquidation_fees - collateral_)*average_price_/size_
    return liquidation_from_delta_fee
  except:
    print(size_,"return infinity")
    return 1e400
    
w3 = Web3(Web3.HTTPProvider("https://api.avax.network/ext/bc/C/rpc"))

glp_add = "0x9ab2De34A33fB459b538c43f251eB825645e8595"

contract_ = w3.eth.contract(abi=GLPVault,address=glp_add)
contract_glp = w3.eth.contract(abi=GLPSC,address=GLP)
contract_glp_manager = w3.eth.contract(abi=GLPManagerABI,address=GLPManagerAdd)
contract_position = w3.eth.contract(abi=ReaderABI, address=ReaderAdd)
contract_vault = w3.eth.contract(abi=VaultABI, address=VaultAdd)
contract_vault_utils = w3.eth.contract(abi=VaultUtilsABI, address=VaultUtils)
contract_reward_router = w3.eth.contract(abi=RewardRouterABI, address=RewardRouter)
contract_position_router = w3.eth.contract(abi=PositionRouterABI, address=PositionRouter)
contract_USDC = w3.eth.contract(abi=USDC_ABI, address=USDC)

class GLPState:
    tokensAdresses = {'MIM': {'adress': "0x130966628846BFd36ff31a822705796e8cb8C18D",'decimal':1e18},
              'WETH': {'adress': "0x49D5c2BdFfac6CE2BFdB6640F4F80f226bc10bAB",'decimal':1e18},
              'WBTC': {'adress': "0x50b7545627a5162F82A992c33b87aDc75187B218",'decimal':1e8},
              'WAVAX': {'adress': "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",'decimal':1e18},
              'USDC': {'adress': "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",'decimal':1e6},
              'USDC_E': {'adress': "0xA7D7079b0FEaD91F3e65f86E8915Cb59c1a4C664",'decimal':1e6},
              'BTC_E': {'adress': "0x152b9d0FdC40C096757F570A51E494bd4b943E50",'decimal':1e8}}
    sillyMapping = {'MIM':'MIM',
                    'BTC.b': 'BTC_E',
                    'ETH': 'WETH',
                    'BTC': 'WBTC',
                    'USDC.e': 'USDC_E',
                    'AVAX': 'WAVAX',
                    'USDC': 'USDC'}
    allTokens = list(tokensAdresses.keys())
    volatileTokens = ['WETH', 'WBTC', 'WAVAX', 'BTC_E']

    class Position:
        def __init__(self,collateral,entryFundingRate,size,lastIncreasedTime):
            self.collateral = collateral
            self.entryFundingRate = entryFundingRate
            self.size = size
            self.lastIncreasedTime = lastIncreasedTime

    def __init__(self):
        self.poolAmount = {key: 0 for key in GLPState.allTokens}
        self.guaranteedUsd = {key: 0 for key in GLPState.volatileTokens}
        self.reservedAmounts = {key: 0 for key in GLPState.volatileTokens}
        self.globalShortSizes = {key: 0 for key in GLPState.volatileTokens}
        self.prices = {key: None for key in GLPState.allTokens}
        self.globalShortAveragePrices = {key: None for key in GLPState.volatileTokens}
        self.positions: list[GLPState.Position] = []
        self.fee = 0.001
        self.estimatedAum = None
        self.actualAum = None
        self.check = dict(GLPState.tokensAdresses) # just for the keys...

    def to_dict(self) -> dict:
        return dict(self.__dict__.items())

    @staticmethod
    def build():
        result: GLPState = GLPState()

        for name, token in GLPState.tokensAdresses.items():
            result.poolAmount[name] = float(contract_vault.functions.poolAmounts(token['adress']).call())/token['decimal']
            result.prices[name] = {'up':contract_vault.functions.getMaxPrice(token['adress']).call()/1e30,
                                   'down':contract_vault.functions.getMinPrice(token['adress']).call()/1e30}
            if name in GLPState.volatileTokens:
                result.guaranteedUsd[name] = float(contract_vault.functions.guaranteedUsd(token['adress']).call())/1e30
                result.reservedAmounts[name] = float(contract_vault.functions.reservedAmounts(token['adress']).call())/token['decimal']
                result.globalShortSizes[name] = float(contract_vault.functions.globalShortSizes(token['adress']).call())/1e30/1e30*token['decimal']
                result.globalShortAveragePrices[name] = float(contract_vault.functions.globalShortAveragePrices(token['adress']).call())/1e30

        result.positions: list[GLPState.Position] = []
        result.fee = 0.001
        totalSupply = contract_glp.functions.totalSupply().call()/1e18
        result.estimatedAum = result.estimate()/totalSupply
        result.actualAum = contract_glp_manager.functions.getAumInUsdg(False).call()/1e18/totalSupply

        return result

    def estimate(self) -> float:
        '''=  (incl long collateral) - shorts size + pnl positions
        mintAndStakeGlp: 0x006ac9fb77641150b1e4333025cb92d0993a878839bb22008c0f354dfdcaf5e7
        unstakeAndRedeemGlpETH: 0xe5004b114abd13b32267514808e663c456d1803ace40c0a4ae7421571155fdd3'''
        aum = 0
        for token in GLPState.allTokens:
            aum += self.poolAmount[token] * self.prices[token]['down']
            if token in GLPState.volatileTokens:
                aum += self.guaranteedUsd[token] - self.reservedAmounts[token] * self.prices[token]['down']
                # add pnl from shorts
                aum += (self.prices[token]['down']/self.globalShortAveragePrices[token]-1)*self.globalShortSizes[token]
        return aum

    def check_add_weights(self) -> None:
        weight_contents = urllib.request.urlopen("https://gmx-avax-server.uc.r.appspot.com/tokens").read()
        weight_json = json.loads(weight_contents)
        weight_tokens = {}
        price_tokens = {}
        cum_usdg = 0
        for cur_contents in weight_json:
            token = GLPState.sillyMapping[cur_contents['data']['symbol']]
            self.check[token]['weight'] = float(cur_contents["data"]["usdgAmount"]) / float(cur_contents["data"]["maxPrice"])
            for attribute in ['fundingRate','poolAmount','reservedAmount','redemptionAmount','globalShortSize','guaranteedUsd']:
                self.check[token][attribute] = float(cur_contents['data'][attribute])

    def increasePosition(self,_collateralToken: str,collateral:float, token: str,_sizeDelta: float,_isLong: bool):
        '''
        createIncreasePositionETH (short): 0x0fe07013cca821bcea7bae4d013ab8fd288dbc3a26ed9dfbd10334561d3ffa91
        setPricesWithBitsAndExecute: 0x8782436fd0f365aeef20fc8fbf9fa01524401ea35a8d37ad7c70c96332ce912b
        '''
        price = self.prices[token]
        fee = self.fee
        #self.positions += GLPSimulator.Position(collateral - fee, price, _sizeDelta, myUtcNow())
        self.reservedAmounts[token] += _sizeDelta
        if _isLong:
            self.guaranteedUsd[token] += _sizeDelta + fee - collateral
            self.poolAmount[token] += collateral - fee
        else:
            self.globalShortAveragePrices[token] = (price*_sizeDelta + self.globalShortAveragePrices[token]*self.globalShortSizes)/(_sizeDelta+self.globalShortSizes[token])
            self.globalShortSizes += _sizeDelta

class GLPTimeSeries:
    def __init__(self):
        self.series: list[dict[datetime, GLPState]]= []
        self.past_data, self.output_filename = initialize_output_file('granular_history.json')
    
    def increment(self,updateTime: float = datetime.now().timestamp()) -> None:
        current = GLPState.build()
        current.check_add_weights()

        self.past_data[updateTime] = {updateTime:current.to_dict()}
        with open(self.output_filename, "w") as f:
            json.dump(self.past_data, f, indent=1)


###### glp redemption


###### variable list

### guaranteedUsd = longs exposure to 100% fall (size-collateral)
## upon increasePosition
# guaranteedUsd += (_sizeDelta - (collateralDeltaUsd+fee)) if _isLong else 0
## upon decrease
# guaranteedUsd -= (deltacollateral - _sizeDelta) if _isLong else 0
# emit IncreaseGuaranteedUsd(_token, _usdAmount);
# emit DecreaseGuaranteedUsd(_token, _usdAmount);

### reserved = gross exposure
## upon increasePosition
# reservedAmounts[_token] += _sizeDelta/priceMin
## upon decrease
# emit IncreaseReservedAmount(_token, _amount);
# emit DecreaseReservedAmount(_token, _amount);

### collateral


### poolAmounts[_token]
## upon directPoolDeposit, buyUSDG, swap...
## upon increasePosition
# poolAmounts[_token] += (collateralDelta - fee/priceMax) if _isLong else 0 --> fees go to feeReserves[_collateralToken] not pool
## upon _decreasePosition
# _reduceCollateral: poolAmounts[_token] -= pnl if short + fee if long
#                    returns (pnl if profit else 0) + max(0,_collateralDelta)
# if long: poolAmounts[_token] -= (pnl if profit else 0) + max(0,_collateralDelta) + fee
# if short: poolAmounts[_token] -= pnl if short

# emit IncreasePoolAmount(_token, _amount);
# emit DecreasePoolAmount(_token, _amount);

def compute_strat(new_usdc=0,return_data=False):
  aums_down = contract_glp_manager.functions.getAumInUsdg(False).call()
  aums_up = contract_glp_manager.functions.getAumInUsdg(True).call()
  glp_total_supply = contract_glp.functions.totalSupply().call()/10**18

  hedge_avax = contract_position.functions.getPositions(VaultAdd, Amine_Account, [USDC], [WAVAX], [False]).call()
  hedge_btc = contract_position.functions.getPositions(VaultAdd, Amine_Account, [USDC], [WBTC], [False]).call()
  hedge_eth = contract_position.functions.getPositions(VaultAdd, Amine_Account, [USDC], [WETH], [False]).call()

  fees_sell_GLP = contract_vault_utils.functions.getSellUsdgFeeBasisPoints(USDC, 100).call()
  fees_buy_GLP = contract_vault_utils.functions.getBuyUsdgFeeBasisPoints(USDC, 100).call()

  quantity_fsglp = contract_position.functions.getTokenBalances(Amine_Account, [fsGLP]).call()[0] / 10 ** 18

  fees_trade = 0.1/100
  slippage = 0.2/100
  slippage_glp = 0.05/100

  liquidation_fee = int(5*10**30)

  collateral_avax = hedge_avax[1]
  collateral_btc = hedge_btc[1]
  collateral_eth = hedge_eth[1]

  size_avax = hedge_avax[0]
  size_btc = hedge_btc[0]
  size_eth = hedge_eth[0]

  average_price_avax = hedge_avax[2]
  average_price_eth = hedge_eth[2]
  average_price_btc = hedge_btc[2]

  entryFundingRate_avax = int(hedge_avax[3])
  entryFundingRate_btc = int(hedge_btc[3])
  entryFundingRate_eth = int(hedge_eth[3])

  borrow_fee_avax = float(contract_vault.functions.getFundingFee(Amine_Account, USDC, WAVAX, False, size_avax, entryFundingRate_avax).call())
  borrow_fee_eth = float(contract_vault.functions.getFundingFee(Amine_Account, USDC, WETH, False, size_eth, entryFundingRate_eth).call())
  borrow_fee_btc = float(contract_vault.functions.getFundingFee(Amine_Account, USDC, WBTC, False, size_btc, entryFundingRate_btc).call())

  sign_avax = 1 if hedge_avax[7]>0 else -1
  sign_btc = 1 if hedge_btc[7]>0 else -1
  sign_eth = 1 if hedge_eth[7]>0 else -1

  pnl_avax = sign_avax*hedge_avax[8]-borrow_fee_avax-fees_trade*size_avax
  pnl_btc = sign_btc*hedge_btc[8]-borrow_fee_btc-fees_trade*size_btc
  pnl_eth = sign_eth*hedge_eth[8]-borrow_fee_eth-fees_trade*size_eth



  liquidation_from_delta_avax_fee = get_liquidation_fees(fees_trade, size_avax, borrow_fee_avax, liquidation_fee, collateral_avax, average_price_avax)
  liquidation_from_delta_eth_fee = get_liquidation_fees(fees_trade, size_eth, borrow_fee_eth, liquidation_fee, collateral_eth, average_price_eth)
  liquidation_from_delta_btc_fee = get_liquidation_fees(fees_trade, size_btc, borrow_fee_btc, liquidation_fee, collateral_btc, average_price_btc)



  net_position_avax = (pnl_avax + collateral_avax)/10**30
  net_position_btc = (pnl_btc + collateral_btc)/10**30
  net_position_eth = (pnl_eth + collateral_eth)/10**30

  collateral_avax_base_30 = collateral_avax/10**30
  collateral_btc_base_30 = collateral_btc/10**30
  collateral_eth_base_30 = collateral_eth/10**30
  

  #print(net_position_avax, net_position_btc, net_position_eth)
  #contents = urllib.request.urlopen("https://gmx-avax-server.uc.r.appspot.com/prices").read()
  #prices = json.loads(contents)

  weight_contents = urllib.request.urlopen("https://gmx-avax-server.uc.r.appspot.com/tokens").read()
  weight_json = json.loads(weight_contents)

  weight_tokens = {}
  price_tokens = {}
  cum_usdg = 0
  for cur_contents in weight_json:
    cur_token = cur_contents["id"]
    cur_usdg =  float(cur_contents["data"]["usdgAmount"])/10**30
    weight_tokens[cur_token] = cur_usdg
    price_tokens[cur_token] = float(cur_contents["data"]["maxPrice"])/10**30
    cum_usdg += cur_usdg 

  for cur_token, cur_usdg in weight_tokens.items():
    weight_tokens[cur_token] /= cum_usdg

  #print(float(prices[WBTC])/10**30, float(prices[WETH])/10**30, float(prices[WAVAX])/10**30)

  volatile_token_weight = weight_tokens[WAVAX] + weight_tokens[WETH]+ weight_tokens[WBTC]+ weight_tokens[BTC_E]

  price_glp = (aums_down+aums_up)/(2*glp_total_supply)/10**18


  total_price_glp = quantity_fsglp * price_glp
  #print(total_price_glp*(1-fees_sell_GLP/10000), price_glp, quantity_fsglp, fees_sell_GLP)
  value_invested_usd = total_price_glp + net_position_avax+net_position_btc+net_position_eth + new_usdc

  leverage = 2
  nex_glp_position_without_fees = value_invested_usd/(1+volatile_token_weight/leverage)

  fees_glp_balance = fees_sell_GLP
  if nex_glp_position_without_fees>value_invested_usd:
    fees_glp_balance = fees_buy_GLP


  nex_glp_position = nex_glp_position_without_fees - abs(nex_glp_position_without_fees-total_price_glp)*fees_glp_balance/10000
  

  
  short_avax_position = weight_tokens[WAVAX]*nex_glp_position/leverage
  short_eth_position = weight_tokens[WETH]*nex_glp_position/leverage
  short_btc_position = weight_tokens[WBTC]*nex_glp_position/leverage
  hedge_distortion = leverage*((short_avax_position-collateral_avax_base_30) + (short_eth_position-collateral_eth_base_30) + (short_btc_position-collateral_btc_base_30))/total_price_glp
  delta_avax_short = short_avax_position-net_position_avax
  delta_eth_short = short_eth_position-net_position_eth
  delta_btc_short = short_btc_position-net_position_btc
  delta_glp = nex_glp_position_without_fees-total_price_glp

  delta_avax_short = delta_avax_short if delta_avax_short>0 else delta_avax_short*collateral_avax/10**30/net_position_avax*leverage
  delta_eth_short = delta_eth_short if delta_eth_short>0 else delta_eth_short*collateral_eth/10**30/net_position_eth*leverage
  delta_btc_short = delta_btc_short if delta_btc_short>0 else delta_btc_short*collateral_btc/10**30/net_position_btc*leverage
  delta_glp = delta_glp if delta_glp>0 else delta_glp/price_glp
  data = {
    "glp_price":price_glp,
    "weights":weight_tokens,
    "liquidations":{
      "avax":liquidation_from_delta_avax_fee,
      "eth":liquidation_from_delta_eth_fee,
      "btc":liquidation_from_delta_btc_fee
    },
    "prices":price_tokens,
    "targets":{"avax":short_avax_position,
              "eth":short_eth_position,
              "btc":short_btc_position,
              "glp":nex_glp_position_without_fees,
              },
    "current":{"avax":net_position_avax,
              "eth":net_position_eth,
              "btc":net_position_btc,
              "glp":total_price_glp,
              "sum":value_invested_usd},
    "update":{"avax":delta_avax_short,
              "eth":delta_eth_short,
              "btc":delta_btc_short,
              "glp":delta_glp},
    "collateral":{"avax":collateral_avax_base_30,
                "eth":collateral_eth_base_30,
                "btc":collateral_btc_base_30
    },
    "Hedge Distortion":hedge_distortion
  }
  if return_data:
    return data
  output = "************** Liquidation ************** \n Avax Liquidation : "+ str(round(liquidation_from_delta_avax_fee/10**30, 2))+ " & Price at "+ str(round(price_tokens[WAVAX],2))+ "\n ETH Liquidation : "+\
           str(round(liquidation_from_delta_eth_fee/10**30,2)) + " & Price at "+ str(round(price_tokens[WETH],2))+ "\n BTC Liquidation : "+ str(round(liquidation_from_delta_btc_fee/10**30,2))+ " & Price at "+ str(round(price_tokens[WBTC],2))+ "\n"
  output += "************** GLP Price ************** \n"+\
            "GLP Price = " + str(round(price_glp,2)) + "\n"
  
  output += "************** Weight Solution ************** \n"+\
            "Weight AVAX : "+ str(round(100*weight_tokens[WAVAX],2))+"\n"+\
            "Weight ETH : "+ str(round(100*weight_tokens[WETH],2))+"\n"+\
            "Weight BTC : "+ str(round(100*weight_tokens[WBTC],2))+"\n"
      
  output += "************** Target Solution ************** \n"+\
          " Target Short Avax : "+ str(round(short_avax_position,2))+"\n"+\
          " Target Short Eth : "+ str(round(short_eth_position,2))+"\n"+\
          " Target Short Btc : "+ str(round(short_btc_position,2))+"\n"+\
          " Target GLP : "+ str(round(nex_glp_position_without_fees,2))+"\n"+\
          " Target Position : "+ str(round(short_avax_position+short_btc_position+short_eth_position+nex_glp_position,2))+"\n"

  output += "************** Current Position ************** \n"+\
          " Current Avax : "+ str(round(net_position_avax,2))+ "\n"+\
          " Current Eth : "+ str(round(net_position_eth,2))+"\n"+\
          " Current Btc : "+ str(round(net_position_btc,2))+"\n"+\
          " Current GLP : "+ str(round(total_price_glp,2))+"\n"+\
          " Sum : "+ str(round(value_invested_usd,2))+ "\n"

  output += "************** Current Collateral ************** \n"+\
          " Collateral Avax : "+ str(round(collateral_avax_base_30,2))+ "\n"+\
          " Collateral Eth : "+ str(round(collateral_eth_base_30,2))+"\n"+\
          " Collateral Btc : "+ str(round(collateral_btc_base_30,2))+"\n"
  
  output += "************** Hedge Distortion ************** \n"+\
          " Hedge Distortion : "+ str(round(100*hedge_distortion,2))+ "% \n"

  
  output += "************** Update ************** \n"+\
          " Delta avax : "+ str(round(delta_avax_short,2))+"\n"+\
          " Delta eth : "+ str(round(delta_eth_short,2))+"\n"+\
          " Delta btc : "+ str(round(delta_btc_short,2))+"\n"+\
          " Delta GLP : "+ str(round(delta_glp,2))+ "\n"

  return output

def save_data():
    data, filename = initialize_output_file('backtest.json')

    new_data = compute_strat(return_data=True)
    data[int(time.time())] = new_data
    print(new_data)
    with open(filename, "w") as f:
        json.dump(data, f, indent=1)


def initialize_output_file(filename: str):
    dir_name = configLoader.get_mktdata_folder_for_exchange('glp')
    if not os.path.exists(dir_name):
        os.umask(0)
        os.makedirs(dir_name, mode=0o777)
    filename = os.path.join(os.sep, dir_name, filename)
    try:
        with open(filename, "r") as f:
            data = json.load(f)
    except:
        data = dict()
    return data, filename

@api
def main(*args, **kwargs):
    time_series: GLPTimeSeries = GLPTimeSeries()
    schedule.every(1).minutes.do(time_series.increment)
    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except Exception as E:
            print(E)
            pass

"""avax_fees_no_basis = 0.02 
avax_fees = int(avax_fees_no_basis *10**18)
print(avax_fees_no_basis, [USDC], WAVAX, int(abs(delta_avax_short*10**30)/2), int(abs(delta_avax_short*10**30)), False, Account, int(price_tokens[WAVAX]*(1+slippage)*10**30), 0, avax_fees , False)
print([USDC], WETH, int(abs(delta_eth_short*10**30)/2), int(abs(delta_eth_short*10**30)), False, Account, int(price_tokens[WETH]*(1+slippage)*10**30), 0, avax_fees , False)
print([USDC], WBTC, int(abs(delta_btc_short*10**30)/2), int(abs(delta_btc_short*10**30)), False, Account, int(price_tokens[WBTC]*(1+slippage)*10**30), 0, avax_fees , False)
print(USDC,int(delta_glp*10**6), 0, int(10**18*delta_glp*(1-fees_buy_GLP/10000)/price_glp))"""

"""print(USDC, int(abs(delta_glp)*10**18), int(abs(delta_glp)*price_glp*(1-fees_glp_balance/10000)*10**6*(1-slippage_glp)), Account)
print(avax_fees_no_basis, [USDC], WAVAX, int(delta_avax_short*10**6),  0, int(leverage * delta_avax_short * (1-leverage*fees_trade)*10**30), False, int(price_tokens[WAVAX]*(1-slippage)*10**30), avax_fees , NullAddress)
print(avax_fees_no_basis, [USDC], WETH, int(delta_eth_short*10**6),  0, int(leverage * delta_eth_short * (1-leverage*fees_trade)*10**30), False, int(price_tokens[WETH]*(1-slippage)*10**30), avax_fees , NullAddress)
print(avax_fees_no_basis, [USDC], WBTC, int(delta_btc_short*10**6),  0, int(leverage * delta_btc_short * (1-leverage*fees_trade)*10**30), False, int(price_tokens[WBTC]*(1-slippage)*10**30), avax_fees , NullAddress)
"""
########### Interaction with Smart ######
     
"""def send_tx(store_transaction, private_key, nonce):
  signed_store_txn = w3.eth.account.sign_transaction(store_transaction, private_key = private_key)
  transaction_hash = w3.eth.send_raw_transaction(signed_store_txn.rawTransaction)
  tx_receipt = w3.eth.wait_for_transaction_receipt(transaction_hash)
  return nonce+1
  
nonce_bot = w3.eth.getTransactionCount(Account)
if delta_glp<0:
  unstake_glp_tx = contract_reward_router.functions.unstakeandRedeemGlp(USDC, int(abs(delta_glp)*10**18), int(abs(delta_glp)*price_glp*(1-fees_glp_balance/10000)*10**6*(1-slippage_glp)), Account).buildTransaction({"chainId":CHAIN_ID, "from":Account, "nonce":nonce_bot})
  nonce_bot = send_tx(unstake_glp_tx, PRIVATE_KEY_BOT, nonce_bot)

if delta_avax_short<0:
  avax_decrease_tx = contract_position_router.functions.createDecreasePosition(avax_fees_no_basis, [USDC], WAVAX, int(abs(delta_avax_short*10**30)/2), int(abs(delta_avax_short*10**30)), False, Account, int(price_tokens[WAVAX]*(1+slippage)*10**30), 0, avax_fees , False).buildTransaction({"chainId":CHAIN_ID, "from":Account, "nonce":nonce_bot})
  nonce_bot = send_tx(avax_decrease_tx, PRIVATE_KEY_BOT, nonce_bot)
  
if delta_eth_short<0:
  eth_decrease_tx = contract_position_router.functions.createDecreasePosition(avax_fees_no_basis, [USDC], WETH, int(abs(delta_eth_short*10**30)/2), int(abs(delta_eth_short*10**30)), False, Account, int(price_tokens[WETH]*(1+slippage)*10**30), 0, avax_fees , False).buildTransaction({"chainId":CHAIN_ID, "from":Account, "nonce":nonce_bot})
  nonce_bot = send_tx(eth_decrease_tx, PRIVATE_KEY_BOT, nonce_bot)
  
if delta_btc_short<0:
  btc_decrease_tx = contract_position_router.functions.createDecreasePosition(avax_fees_no_basis, [USDC], WBTC, int(abs(delta_btc_short*10**30)/2), int(abs(delta_btc_short*10**30)), False, Account, int(price_tokens[WBTC]*(1+slippage)*10**30), 0, avax_fees , False).buildTransaction({"chainId":CHAIN_ID, "from":Account, "nonce":nonce_bot})
  nonce_bot = send_tx(btc_decrease_tx, PRIVATE_KEY_BOT, nonce_bot)

balanceOf_USDC = int(contract_USDC.functions.balanceOf(Account).call())


if delta_avax_short>0:
  avax_increase_tx = contract_position_router.functions.createIncreasePosition(avax_fees_no_basis, [USDC], WAVAX, int(delta_avax_short*10**6),  0, int(leverage * delta_avax_short * (1-leverage*fees_trade)*10**30), False, int(price_tokens[WAVAX]*(1-slippage)*10**30), avax_fees , NullAddress).buildTransaction({"chainId":CHAIN_ID, "from":Account, "nonce":nonce_bot})
  nonce_bot = send_tx(avax_increase_tx, PRIVATE_KEY_BOT, nonce_bot)

if delta_eth_short>0:
  eth_increase_tx = contract_position_router.functions.createIncreasePosition(avax_fees_no_basis, [USDC], WETH, int(delta_eth_short*10**6),  0, int(leverage * delta_eth_short * (1-leverage*fees_trade)*10**30), False, int(price_tokens[WETH]*(1-slippage)*10**30), avax_fees , NullAddress).buildTransaction({"chainId":CHAIN_ID, "from":Account, "nonce":nonce_bot})
  nonce_bot = send_tx(eth_increase_tx, PRIVATE_KEY_BOT, nonce_bot)

if delta_btc_short>0:
  btc_increase_tx = contract_position_router.functions.createIncreasePosition(avax_fees_no_basis, [USDC], WBTC, int(delta_btc_short*10**6),  0, int(leverage * delta_btc_short * (1-leverage*fees_trade)*10**30), False, int(price_tokens[WBTC]*(1-slippage)*10**30), avax_fees , NullAddress).buildTransaction({"chainId":CHAIN_ID, "from":Account, "nonce":nonce_bot})
  nonce_bot = send_tx(btc_increase_tx, PRIVATE_KEY_BOT, nonce_bot)

if delta_glp>0:
  mint_glp_tx = contract_reward_router.functions.mintAndStakeGlp(USDC,int(delta_glp*10**6), 0, int(10**18*delta_glp*(1-fees_buy_GLP/10000)/price_glp)).buildTransaction({"chainId":CHAIN_ID, "from":Account, "nonce":nonce_bot}).buildTransaction({"chainId":CHAIN_ID, "from":Account, "nonce":nonce_bot})
  nonce_bot = send_tx(mint_glp_tx, PRIVATE_KEY_BOT, nonce_bot)"""





