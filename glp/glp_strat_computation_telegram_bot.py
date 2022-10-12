import datetime
import logging
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
        self.tokenBalances = {key: 0 for key in GLPState.static}
        self.usdgAmounts = {key: 0 for key in GLPState.static}
        self.pricesUp = {key: None for key in GLPState.static}
        self.pricesDown = {key: None for key in GLPState.static}

        self.guaranteedUsd = {key: 0 for key,data in GLPState.static.items() if data['volatile']}
        self.reservedAmounts = {key: 0 for key,data in GLPState.static.items() if data['volatile']}
        self.globalShortSizes = {key: 0 for key,data in GLPState.static.items() if data['volatile']}
        self.globalShortAveragePrices = {key: None for key,data in GLPState.static.items() if data['volatile']}
        self.feeReserves: dict[str,dict] = {key:0 for key in GLPState.static}

        self.fee = 0.001
        self.totalSupply = None
        self.actualAum = None
        self.check = {key:dict() for key in GLPState.static}

    def to_dict(self) -> dict:
        result = dict(self.__dict__)
        result.pop('check')
        result.pop('usdgAmounts')
        return result

    @staticmethod
    def build():
        result: GLPState = GLPState()

        for key, data in GLPState.static.items():
            result.tokenBalances[key] = float(contract_vault.functions.tokenBalances(data['address']).call()) / data['decimal']
            result.poolAmount[key] = float(contract_vault.functions.poolAmounts(data['address']).call())/data['decimal']
            result.usdgAmounts[key] = float(contract_vault.functions.usdgAmounts(data['address']).call()) / data['decimal']
            result.pricesUp[key] = contract_vault.functions.getMaxPrice(data['address']).call()/1e30
            result.pricesDown[key] = contract_vault.functions.getMinPrice(data['address']).call()/1e30
            if data['volatile']:
                result.guaranteedUsd[key] = float(contract_vault.functions.guaranteedUsd(data['address']).call())/1e30
                result.reservedAmounts[key] = float(contract_vault.functions.reservedAmounts(data['address']).call())/data['decimal']
                result.globalShortSizes[key] = float(contract_vault.functions.globalShortSizes(data['address']).call())/1e30
                result.globalShortAveragePrices[key] = float(contract_vault.functions.globalShortAveragePrices(data['address']).call())/1e30
                result.feeReserves[key] = float(contract_vault.functions.feeReserves(data['address']).call())/data['decimal']

        # result.positions = contract_vault.functions.positions().call()
        result.fee = 0.001
        result.totalSupply = contract_glp.functions.totalSupply().call()/1e18
        result.actualAum = contract_glp_manager.functions.getAumInUsdg(False).call()/1e18/result.totalSupply

        return result

    '''
    the below is only for documentation
    '''

    def mintAndStakeGlpETH(self,_token,tokenAmount) -> None:
        '''mintAndStakeGlp: 0x006ac9fb77641150b1e4333025cb92d0993a878839bb22008c0f354dfdcaf5e7'''
        usdgAmount = tokenAmount*self.pricesDown[_token]
        fee = vaultUtils.getBuyUsdgFeeBasisPoints(_token, usdgAmount)*usdgAmount
        self.feeReserves[_token] += fee
        self.usdgAmount[_token] += (usdgAmount - fee) * self.pricesDown[_token]
        self.poolAmount[_token] += (usdgAmount - fee)

    def valuation(self, key = None) -> float:
        '''unstakeAndRedeemGlpETH: 0xe5004b114abd13b32267514808e663c456d1803ace40c0a4ae7421571155fdd3
        total is key is None'''
        if key is None:
            return sum(self.valuation(key) for key in GLPState.static)
        else:
            aum = self.poolAmount[key] * self.pricesDown[key]
            if GLPState.static[key]['volatile']:
                aum += self.guaranteedUsd[key] - self.reservedAmounts[key] * self.pricesDown[key]
                # add pnl from shorts
                aum += (self.pricesDown[key]/self.globalShortAveragePrices[key]-1)*self.globalShortSizes[key]
            return aum

    def partial_delta(self,key: str) -> float:
        '''so delta =  poolAmount - reservedAmounts + globalShortSizes/globalShortAveragePrices
        ~ what's ours + (collateral - N)^{longs}'''
        result = self.poolAmount[key]
        if GLPState.static[key]['volatile']:
            result += (- self.reservedAmounts[key] + self.globalShortSizes[key]/self.globalShortAveragePrices[key])
        return result

    def add_weights(self) -> None:
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

    def sanity_check(self):
        logger = logging.getLogger('glp')
        if abs(self.valuation()/self.totalSupply/self.actualAum -1) > GLPState.check_tolerance:
            logger.warning(f'val {self.valuation()} vs actual {self.actualAum/self.totalSupply}')

    def increasePosition(self, collateral:float, _indexToken: str, _collateralToken: str, _sizeDelta: float, _isLong: bool):
        '''
        for longs only:
          poolAmounts = coll - fee
          guaranteedUsd = _sizeDelta + fee + collateralUsd
        createIncreasePositionETH (short): 0x0fe07013cca821bcea7bae4d013ab8fd288dbc3a26ed9dfbd10334561d3ffa91
        setPricesWithBitsAndExecute: 0x8782436fd0f365aeef20fc8fbf9fa01524401ea35a8d37ad7c70c96332ce912b
        '''
        fee = self.fee
        #self.positions += GLPSimulator.Position(collateral - fee, price, _sizeDelta, myUtcNow())
        self.reservedAmounts[_collateralToken] += _sizeDelta / self.pricesDown[_collateralToken]
        if _isLong:
            self.guaranteedUsd[_collateralToken] += _sizeDelta + fee - collateral * self.pricesDown[_collateralToken]
            self.poolAmount[_collateralToken] += collateral - fee / self.pricesDown[_collateralToken]
        else:
            self.globalShortAveragePrices[_indexToken] = (self.pricesDown[_indexToken] * _sizeDelta + self.globalShortAveragePrices[_indexToken] * self.globalShortSizes) / (_sizeDelta + self.globalShortSizes[_collateralToken])
            self.globalShortSizes += _sizeDelta

    def decreasePosition(self, _collateralToken: str, collateral: float, token: str, _sizeDelta: float, _isLong: bool):
        self.reservedAmounts[token] = 0
        self.poolAmount[token] -= pnl
        if _isLong:
            self.guaranteedUsd[token] -= _sizeDelta - collateral * originalPx
            self.poolAmount[token] -= collateral

'''
relevant topics

- called everywhere
emit IncreasePoolAmount(_token, _amount) 0x976177fbe09a15e5e43f848844963a42b41ef919ef17ff21a17a5421de8f4737
emit DecreasePoolAmount(_token, _amount) 0x112726233fbeaeed0f5b1dba5cb0b2b81883dee49fb35ff99fd98ed9f6d31eb0

- only called on position increase
emit IncreaseReservedAmount(_token, _amount); 0xaa5649d82f5462be9d19b0f2b31a59b2259950a6076550bac9f3a1c07db9f66d
emit DecreaseReservedAmount(_token, _amount); 0x533cb5ed32be6a90284e96b5747a1bfc2d38fdb5768a6b5f67ff7d62144ed67b

- globalShortSize has no emit, so need following
emit IncreasePosition(key, _account, _collateralToken, _indexToken, _collateralDelta, _sizeDelta, _isLong, price, usdOut.sub(usdOutAfterFee)); 0x2fe68525253654c21998f35787a8d0f361905ef647c854092430ab65f2f15022
emit DecreasePosition(key, _account, _collateralToken, _indexToken, _collateralDelta, _sizeDelta, _isLong, price, usdOut.sub(usdOutAfterFee)); 0x93d75d64d1f84fc6f430a64fc578bdd4c1e090e90ea2d51773e626d19de56d30

- and for tracking usd value:
emit IncreaseGuaranteedUsd(_token, _usdAmount); 0xd9d4761f75e0d0103b5cbeab941eeb443d7a56a35b5baf2a0787c03f03f4e474
emit DecreaseGuaranteedUsd(_token, _usdAmount); 0x34e07158b9db50df5613e591c44ea2ebc82834eff4a4dc3a46e000e608261d68

'''

class GLPTimeSeries:
    def __init__(self):
        dir_name = configLoader.get_mktdata_folder_for_exchange('glp')
        if not os.path.exists(dir_name):
            os.umask(0)
            os.makedirs(dir_name, mode=0o777)
        self.output_filename = os.path.join(os.sep, dir_name,
                                            'granular_history_{}.json'.format(datetime.now().strftime("%Y%m%d_%H%M%S")))
        try:
            with open(self.output_filename, "r") as f:
                self.series = json.load(f)
        except:
            self.series = [dict()]
    
    def increment(self) -> None:
        current_state = GLPState.build()
        #current_state.add_weights()
        current_state.sanity_check()

        # compute plex fields
        updateTime = datetime.now().timestamp()
        previous = self.series[-1]
        current = current_state.to_dict()
        # compute risk
        current |= {'timestamp': updateTime,
                    'delta': {key: current_state.partial_delta(key) for key in GLPState.static},
                    'valuation': {key: current_state.valuation(key) for key in GLPState.static}}
        if len(previous) > 0:
            # compute plex
            current |= {'delta_pnl': {key: previous['delta'][key] * (current['pricesDown'][key] - previous['pricesDown'][key])
                                      for key in GLPState.static}}
            current |= {'other_pnl': {key: current['valuation'][key]-previous['valuation'][key]-current['delta_pnl'][key] for key in GLPState.static}}

            # compute totals
            for label in ['delta_pnl','valuation','other_pnl']:
                current[label]['total'] = sum(current[label].values())
            current['unexplained'] = (current['actualAum']-previous['actualAum']) - (current['valuation']-previous['valuation'])

        self.series.append(current)
        with open(self.output_filename, "w") as f:
            json.dump([{json.dumps(k): v for k, v in nested_dict_to_tuple(item).items()} for item in self.series], f, indent=1, cls=NpEncoder)
            logging.getLogger('glp').info(f'wrote to {self.output_filename} at {updateTime}')

def initialize_output_file(filename) -> list[dict[datetime, GLPState]]:
    dir_name = configLoader.get_mktdata_folder_for_exchange('glp')
    if not os.path.exists(dir_name):
        os.umask(0)
        os.makedirs(dir_name, mode=0o777)
    try:
        with open(filename, "r") as f:
            series = json.load(f)
    except:
        series = [dict()]
    return series, filename

def outputfile_to_dataframe(self,filename):


@api
def main(*args, **kwargs):
    time_series: GLPTimeSeries = GLPTimeSeries()
    schedule.every(1).minutes.do(time_series.increment)
    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except Exception as e:
            logging.getLogger('glp').critical(e)
            raise e
