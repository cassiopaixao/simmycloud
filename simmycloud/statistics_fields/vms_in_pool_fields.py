###############################################################################
# The MIT License (MIT)
#
# Copyright (c) 2014 Cássio Paixão
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

from operator import attrgetter

from core.statistics_manager import StatisticsField

class VMsInPoolMaxCPUField(StatisticsField):
    def value(self):
        vms_in_pool = self._config.vms_pool.get_ordered_list()
        return -1   if not vms_in_pool \
                    else max(vms_in_pool, key=attrgetter('cpu')).cpu

class VMsInPoolMaxMemField(StatisticsField):
    def value(self):
        vms_in_pool = self._config.vms_pool.get_ordered_list()
        return -1   if not vms_in_pool \
                    else max(vms_in_pool, key=attrgetter('mem')).mem

class VMsInPoolMaxLinearCPUField(StatisticsField):
    def value(self):
        vms_in_pool = self._config.vms_pool.get_ordered_list()
        return -1   if not vms_in_pool \
                    else max(vms_in_pool, key=lambda vm: vm.cpu+vm.mem).cpu

class VMsInPoolMaxLinearMemField(StatisticsField):
    def value(self):
        vms_in_pool = self._config.vms_pool.get_ordered_list()
        return -1   if not vms_in_pool \
                    else max(vms_in_pool, key=lambda vm: vm.cpu+vm.mem).mem

class VMsInPoolMinLinearCPUField(StatisticsField):
    def value(self):
        vms_in_pool = self._config.vms_pool.get_ordered_list()
        return -1   if not vms_in_pool \
                    else min(vms_in_pool, key=lambda vm: vm.cpu+vm.mem).cpu

class VMsInPoolMinLinearMemField(StatisticsField):
    def value(self):
        vms_in_pool = self._config.vms_pool.get_ordered_list()
        return -1   if not vms_in_pool \
                    else min(vms_in_pool, key=lambda vm: vm.cpu+vm.mem).mem

class VMsInPoolMinLinearFitsOnlineField(StatisticsField):
    def value(self):
        vms_in_pool = self._config.vms_pool.get_ordered_list()
        if vms_in_pool:
            vm = min(vms_in_pool, key=lambda vm: vm.cpu+vm.mem)
            for server in self._config.resource_manager.online_servers():
                if vm.cpu <= server.cpu_free and vm.mem <= server.mem_free:
                    return 'yes'
            return 'no'
        else:
            return ''

class VMsInPoolMinLinearFitsOfflineField(StatisticsField):
    def value(self):
        vms_in_pool = self._config.vms_pool.get_ordered_list()
        if vms_in_pool:
            vm = min(vms_in_pool, key=lambda vm: vm.cpu+vm.mem)
            for server in self._config.resource_manager.offline_servers():
                if vm.cpu <= server.cpu_free and vm.mem <= server.mem_free:
                    return 'yes'
            return 'no'
        else:
            return ''

class VMsInPoolListField(StatisticsField):
    def value(self):
        vms_in_pool = self._config.vms_pool.get_ordered_list()
        return ''   if not vms_in_pool \
                    else '  '.join(vm.dump() for vm in sorted(vms_in_pool, key=lambda vm: (vm.cpu+vm.mem), reverse=True))
