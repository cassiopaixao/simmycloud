
import PoweringOffStrategy

class FakePoweringOff(PoweringOffStrategy):

    def power_off_if_necessary(self, server):
        print('power_off_if_necessary(%s)'.format(server.name))
