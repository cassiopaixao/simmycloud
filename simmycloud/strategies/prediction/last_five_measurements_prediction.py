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


from core.virtual_machine import VirtualMachine
from core.strategies import PredictionStrategy

class LastFiveMeasurementsPrediction(PredictionStrategy):

    def initialize(self):
        self.measurement_reader = self._config.module['MeasurementReader']

    @PredictionStrategy.predict_strategy
    def predict(self, vm_name):
        last_five = self._get_last_five(vm_name)
        cpu_average = float(sum(m[self.measurement_reader.CPU] for m in last_five)) / len(last_five)
        mem_average = float(sum(m[self.measurement_reader.CPU] for m in last_five)) / len(last_five)
        new_demands = VirtualMachine('')
        new_demands.cpu = cpu_average
        new_demands.mem = mem_average
        return new_demands

    def _get_last_five(self, vm_name):
        measurements = self.measurement_reader.n_measurements_till(
            vm_name,
            5,
            self._config.simulation_info.current_timestamp)
        return measurements
