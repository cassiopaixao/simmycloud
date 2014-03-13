###############################################################################
# The MIT License (MIT)
#
# Copyright (c) 2013 Cássio Paixão
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

from core.statistics_manager import StatisticsField


class VMNameField(StatisticsField):
    def value(self, vm):
        return vm.name

class VMStretchField(StatisticsField):
    def value(self, vm):
        current_timestamp = self._config.simulation_info.current_timestamp
        submit_timestamp = self._config.environment.get_vm_allocation_data(vm.name).submit_time
        requested_processing_time = self._config.environment.get_vm_allocation_data(vm.name).process_time
        return float(current_timestamp - submit_timestamp) / requested_processing_time

class VMSubmitTimestampField(StatisticsField):
    def value(self, vm):
        return self._config.environment.get_vm_allocation_data(vm.name).submit_time

class VMFinishTimestampField(StatisticsField):
    def value(self, vm):
        return self._config.simulation_info.current_timestamp
