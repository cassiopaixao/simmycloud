
from core.strategies import PoweringOffStrategy

class FakePoweringOff(PoweringOffStrategy):

    @PoweringOffStrategy.power_off_if_necessary_statistics
    def power_off_if_necessary(self, server):
        return server
