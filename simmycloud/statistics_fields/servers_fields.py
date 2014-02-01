
import math

from core.statistics_manager import StatisticsField


class OnlineServersField(StatisticsField):
    def value(self):
        return len(self._config.environment.online_servers())


# QoS
class TightedVMsField(StatisticsField):
    def value(self):
        overloaded_servers = [s for s in self._config.environment.online_servers() if s.cpu_alloc > s.cpu and s.mem_alloc > s.mem]
        return sum([len(s.vm_list()) for s in overloaded_servers])


class OverloadedServersField(StatisticsField):
    def value(self):
        overloaded_servers = [s for s in self._config.environment.online_servers() if s.cpu_alloc > s.cpu and s.mem_alloc > s.mem]
        return len(overloaded_servers)


class VMsAllocatedField(StatisticsField):
    def value(self):
        return len(self._config.environment._vm_hosts)


class VMsInPoolField(StatisticsField):
    def value(self):
        return len(self._config.vms_pool.get_ordered_list())


class ServersTotalResidualCapacityField(StatisticsField):
    def value(self):
        def residual_capacity(s):
            return math.sqrt(math.pow(s.cpu - s.cpu_alloc, 2) + math.pow(s.mem - s.mem_alloc, 2))
        online_servers = self._config.environment.online_servers()
        return math.fsum([residual_capacity(server) for server in online_servers])


class ServerResidualCapacityPercentageField(StatisticsField):
    def set_server(self, server):
        self.server = server
        self._server_capacity_magnitude = self._mag(server.cpu, server.mem)

    def value(self):
        return self._mag(self.server.cpu - self.server.cpu_alloc,
                         self.server.mem - self.server.mem_alloc) / self._server_capacity_magnitude

    def _mag(self, x, y):
        return math.sqrt(math.pow(x, 2) + math.pow(y, 2))

