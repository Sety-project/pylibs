from tradeexecutor.utils.io_utils import myUtcNow
from tradeexecutor.interface.position_manager import PositionManager

class GMXPositionManager(PositionManager):
    def __init__(self,parameters):
        super().__init__(parameters)

    def adjusted_delta(self, symbol):
        return self.data[symbol]['delta']

    async def reconcile(self):
        '''needs a reconciled venueAPI'''
        previous_delta = {symbol: {'delta': data['delta']} for symbol, data in self.data.items()}
        previous_pv = self.pv
        
        risk_timestamp = myUtcNow()
        glp_position = self.strategy.venue_api.state.depositBalances()
        self.pv = glp_position * self.strategy.venue_api.state.actualAum['total']
        self.data['GLP']['delta'] = self.pv

        delta_error = {symbol: self.data[symbol]['delta'] - (previous_delta[symbol]['delta'] if symbol in previous_delta else 0)
                       for symbol in ['GLP']}
        self.risk_reconciliations += [{'symbol': 'GLP',
                                       'delta_timestamp': risk_timestamp,
                                       'delta': self.data[symbol_]['delta'],
                                       #'netDelta': self.coin_delta(symbol_),
                                       'pv': self.pv,
                                       'pv_error': self.pv - (previous_pv or 0),
                                       'total_delta_error': sum(delta_error.values())}
                                      for symbol_ in ['GLP']]