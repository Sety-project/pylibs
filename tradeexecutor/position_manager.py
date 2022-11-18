from abc import abstractmethod
from tradeexecutor.interface.StrategyEnabler import StrategyEnabler

class PositionManager(StrategyEnabler):
    '''PositionManager owns pv,risk,margin. Even underlyings not being executed (but only one exchange)
    Records historical risks.
    structure is {symbol:[{'delta','delta_timestamp','delta_id',some static data},...]}
    able to reconcile to an exchange'''
    class LimitBreached(Exception):
        def __init__(self,check_frequency,limit):
            super().__init__()
            self.delta_limit = limit
            self.check_frequency = check_frequency

    def __init__(self,parameters):
        super().__init__(parameters)
        self.delta_adjustment: dict = dict({key: 0 for key in parameters['symbols']})

        self.pv = None
        self.margin = None
        self.limit = None
        self.risk_reconciliations = []

    def serialize(self) -> list[dict]:
        result = self.risk_reconciliations
        self.risk_reconciliations = []
        return result

    @abstractmethod
    def adjusted_delta(self,symbol):
        raise NotImplementedError

    @abstractmethod
    def reconcile(self):
        raise NotImplementedError
