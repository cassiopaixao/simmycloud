
from ..strategy import Strategy

class SchedulingStrategy(Strategy):

    def schedule_vm(self, vm):
        raise NotImplementedError
