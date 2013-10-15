
from strategies.powering_off.powering_off_strategy import PoweringOffStrategy

class FakePoweringOff(PoweringOffStrategy):

    def power_off_if_necessary(self, server):
        print('power_off_if_necessary({})'.format(server.name))
