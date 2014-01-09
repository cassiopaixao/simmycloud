
from core.strategies import PoweringOffStrategy

class PowerOffIfEmpty(PoweringOffStrategy):

    @PoweringOffStrategy.power_off_if_necessary_statistics
    def power_off_if_necessary(self, server):
        if server.cpu_alloc == 0 and server.mem_alloc == 0:
            self._config.environment.turn_off_server(server.name)
            return None
        return server
