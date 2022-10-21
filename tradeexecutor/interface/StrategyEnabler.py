from abc import ABC, abstractmethod
from datetime import datetime

class StrategyEnabler(ABC):
    def __init__(self, parameters):
        self.parameters: dict = parameters
        self.data: dict = dict({key: None for key in parameters['symbols']}) if 'symbol' in parameters else dict()
        self.strategy = None  # only assign strategy later
        self.createdAt = datetime.now()

    @abstractmethod
    async def reconcile(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def serialize(self) -> list[dict]:
        raise NotImplementedError
