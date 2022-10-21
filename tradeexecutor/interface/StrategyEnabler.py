from abc import ABC, abstractmethod
import functools

class StrategyEnabler(ABC):
    def __init__(self, parameters):
        self.parameters: dict = parameters
        self.data: dict = dict({key: None for key in parameters['symbols']}) if 'symbol' in parameters else dict()
        self.strategy = None  # only assign strategy later
        self.reconciled = False

    def unless_reconciled(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
                self = args[0]
                if not self.reconciled:
                    value = await func(*args, **kwargs)
                    self.reconciled = True
                    print(f'{type(self)} {func.__name__} +')
                else:
                    print(f'{type(self)} {func.__name__} 0')
                    pass
        return wrapper

    @abstractmethod
    @unless_reconciled
    async def reconcile(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def serialize(self) -> list[dict]:
        raise NotImplementedError
