import datetime
import time, schedule
from web3 import Web3
from utils.api_utils import api
from utils.io_utils import *

from constants import GLPSC, GLP, GLPManagerABI, GLPManagerAdd, VaultAdd, VaultABI
import urllib.request

w3 = Web3(Web3.HTTPProvider("https://api.avax.network/ext/bc/C/rpc"))
contract_glp = w3.eth.contract(abi=GLPSC,address=GLP)
contract_glp_manager = w3.eth.contract(abi=GLPManagerABI,address=GLPManagerAdd)
contract_vault = w3.eth.contract(abi=VaultABI, address=VaultAdd)

class GLPState:
    static = {'MIM': {'address': "0x130966628846BFd36ff31a822705796e8cb8C18D", 'decimal':1e18, 'volatile': False},
              'WETH': {'address': "0x49D5c2BdFfac6CE2BFdB6640F4F80f226bc10bAB",'decimal':1e18,'volatile': True},
              'WBTC': {'address': "0x50b7545627a5162F82A992c33b87aDc75187B218",'decimal':1e8,'volatile': True},
              'WAVAX': {'address': "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",'decimal':1e18,'volatile': True},
              'USDC': {'address': "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",'decimal':1e6,'volatile': False},
              'USDC_E': {'address': "0xA7D7079b0FEaD91F3e65f86E8915Cb59c1a4C664",'decimal':1e6,'volatile': False},
              'BTC_E': {'address': "0x152b9d0FdC40C096757F570A51E494bd4b943E50",'decimal':1e8,'volatile': True}}

    class Position:
        def __init__(self,collateral,entryFundingRate,size,lastIncreasedTime):
            self.collateral = collateral
            self.entryFundingRate = entryFundingRate
            self.size = size
            self.lastIncreasedTime = lastIncreasedTime

    def __init__(self):
        self.poolAmount = {key: 0 for key in GLPState.static}
        self.prices = {key: None for key in GLPState.static}

        self.guaranteedUsd = {key: 0 for key,data in GLPState.static.items() if data['volatile']}
        self.reservedAmounts = {key: 0 for key,data in GLPState.static.items() if data['volatile']}
        self.globalShortSizes = {key: 0 for key,data in GLPState.static.items() if data['volatile']}
        self.globalShortAveragePrices = {key: None for key,data in GLPState.static.items() if data['volatile']}

        self.positions: list[GLPState.Position] = []
        self.feeReserves: dict[str,dict] = dict(GLPState.static) # just for the keys...

        self.fee = 0.001
        self.estimatedAum = None
        self.actualAum = None
        self.check = dict(GLPState.static) # just for the keys...

    def to_dict(self) -> dict:
        return dict(self.__dict__)

    @staticmethod
    def build():
        result: GLPState = GLPState()

        for key, data in GLPState.static.items():
            result.poolAmount[key] = float(contract_vault.functions.poolAmounts(data['address']).call())/data['decimal']
            result.prices[key] = {'up':contract_vault.functions.getMaxPrice(data['address']).call()/1e30,
                                   'down':contract_vault.functions.getMinPrice(data['address']).call()/1e30}
            if data['volatile']:
                result.guaranteedUsd[key] = float(contract_vault.functions.guaranteedUsd(data['address']).call())/1e30
                result.reservedAmounts[key] = float(contract_vault.functions.reservedAmounts(data['address']).call())/data['decimal']
                result.globalShortSizes[key] = float(contract_vault.functions.globalShortSizes(data['address']).call())/1e30
                result.globalShortAveragePrices[key] = float(contract_vault.functions.globalShortAveragePrices(data['address']).call())/1e30
                result.feeReserves[key] = float(contract_vault.functions.feeReserves(data['address']).call())/1e30

        # result.positions = contract_vault.functions.positions().call()
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
        for key,data in GLPState.static.items():
            aum += self.poolAmount[key] * self.prices[key]['down']
            if data['volatile']:
                aum += self.guaranteedUsd[key] - self.reservedAmounts[key] * self.prices[key]['down']
                # add pnl from shorts
                aum += (self.prices[key]['down']/self.globalShortAveragePrices[key]-1)*self.globalShortSizes[key]
        return aum

    def check_add_weights(self) -> None:
        weight_contents = urllib.request.urlopen("https://gmx-avax-server.uc.r.appspot.com/tokens").read()
        weight_json = json.loads(weight_contents)
        sillyMapping = {'MIM': 'MIM',
                        'BTC.b': 'BTC_E',
                        'ETH': 'WETH',
                        'BTC': 'WBTC',
                        'USDC.e': 'USDC_E',
                        'AVAX': 'WAVAX',
                        'USDC': 'USDC'}

        for cur_contents in weight_json:
            token = sillyMapping[cur_contents['data']['symbol']]
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

        self.past_data[updateTime] = current.to_dict()
        with open(self.output_filename, "w") as f:
            json.dump(self.past_data, f, indent=1)

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
    schedule.every(5).minutes.do(time_series.increment)
    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except Exception as E:
            print(E)
            pass
