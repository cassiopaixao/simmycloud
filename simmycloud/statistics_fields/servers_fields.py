###############################################################################
# The MIT License (MIT)
#
# Copyright (c) 2013 Cassio Paixao
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
###############################################################################


import math

from core.statistics_manager import StatisticsField


class OnlineServersField(StatisticsField):
    def value(self):
        return len(self._config.resource_manager.online_servers())


# SLA violations
class TightedVMsField(StatisticsField):
    def value(self):
        overloaded_servers = self._config.module['MeasurementReader'].overloaded_servers()
        return sum(len(s.vm_list()) for s in overloaded_servers)


class OverloadedServersField(StatisticsField):
    def value(self):
        overloaded_servers = self._config.module['MeasurementReader'].overloaded_servers()
        return len(overloaded_servers)


class VMsAllocatedField(StatisticsField):
    def value(self):
        return len(self._config.resource_manager._vm_hosts)


class VMsInPoolField(StatisticsField):
    def value(self):
        return len(self._config.vms_pool.get_ordered_list())

class HighPriorityVMsInPoolField(StatisticsField):
    def value(self):
        return len(self._config.vms_pool.get_high_priority_vms())

class LowPriorityVMsInPoolField(StatisticsField):
    def value(self):
        return len(self._config.vms_pool.get_low_priority_vms())


class ServersTotalResidualCapacityField(StatisticsField):
    def value(self):
        def residual_capacity(s):
            return math.sqrt(math.pow(s.cpu_free, 2) + math.pow(s.mem_free, 2))
        online_servers = self._config.resource_manager.online_servers()
        return math.fsum(residual_capacity(server) for server in online_servers)


class ServerResidualCapacityPercentageField(StatisticsField):
    def set_server(self, server):
        self.server = server
        self._server_capacity_magnitude = self._mag(server.cpu, server.mem)

    def value(self):
        return self._mag(self.server.cpu_free, self.server.mem_free) / self._server_capacity_magnitude

    def _mag(self, x, y):
        return math.sqrt(math.pow(x, 2) + math.pow(y, 2))

