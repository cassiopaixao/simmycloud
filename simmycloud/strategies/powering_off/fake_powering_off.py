
from core.strategies import PoweringOffStrategy

class FakePoweringOff(PoweringOffStrategy):

    @PoweringOffStrategy.power_off_if_necessary_strategy
    def power_off_if_necessary(self, server):
        return server
