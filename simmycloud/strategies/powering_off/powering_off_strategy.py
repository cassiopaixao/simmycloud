
from ..strategy import Strategy

class PoweringOffStrategy(Strategy):

    def power_off_if_necessary(self, server):
        raise NotImplementedError
