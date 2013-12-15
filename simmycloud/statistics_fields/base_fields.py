
import time
from core.statistics_manager import StatisticsField

class CounterField(StatisticsField):
    def listens_to(self):
        return [self.label()]

    def clear(self):
        self._counter = 0

    def notify(self, how_many=1):
        self._counter += how_many

    def value(self):
        return self._counter


class OnlineServersField(StatisticsField):
    def value(self):
        return len(self._config.environment.online_servers())


class EnvironmentField(StatisticsField):
    def value(self):
        return self._config.environment._builder.__class__.__name__


class SchedulingStrategyField(StatisticsField):
    def value(self):
        return self._config.strategies.scheduling.__class__.__name__


class MigrationStrategyField(StatisticsField):
    def value(self):
        return self._config.strategies.migration.__class__.__name__


class PoweringOffStrategyField(StatisticsField):
    def value(self):
        return self._config.strategies.powering_off.__class__.__name__


class OverloadedServersField(StatisticsField):
    def value(self):
        overloaded_servers = [s for s in self._config.environment.online_servers() if s.cpu_alloc > s.cpu and s.mem_alloc > s.mem]
        return len(overloaded_servers)


class MeanUseOfServersField(StatisticsField):
    def value(self):
        sum = 0
        online_servers = self._config.environment.online_servers()
        for server in online_servers:
            sum += (server.cpu_alloc * server.mem_alloc) / (server.cpu * server.mem)

        if len(online_servers) > 0:
            return sum / len(online_servers)
        else:
            return 0


class TimeIntervalSinceLastStatisticsField(StatisticsField):
    def clear(self):
        self.start_time = time.clock()

    def value(self):
        return time.clock() - self.start_time

class VMsAllocatedField(StatisticsField):
    def value(self):
        return len(self._config.environment._vm_hosts)

class SumOfUnallocatedVMsField(CounterField):
    _counter = 0

    def clear(self):
        pass

    def listens_to(self):
        return ['vms_not_allocated', 'couldnot_reallocate']
