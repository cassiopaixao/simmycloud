
from core.strategies import PoweringOffStrategy

class PowerOffIfEmpty(PoweringOffStrategy):

    @PoweringOffStrategy.power_off_if_necessary_strategy
    def power_off_if_necessary(self, server):
        if len(server.vm_list()) == 0:
            self._config.environment.turn_off_server(server.name)
            return None
        return server
