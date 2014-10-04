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


from core.strategies import PredictionStrategy

class LastFiveMeasurementsPrediction(PredictionStrategy):

    def initialize(self):
        self.measurement_reader = self._config.module['MeasurementReader']

    @PredictionStrategy.predict_strategy
    def predict(self, vm_name):
        last_five = self._get_last_five(vm_name)
        cpu_average = sum(m[self.measurement_reader.CPU] for m in last_five) / len(last_five)
        mem_average = sum(m[self.measurement_reader.MEM] for m in last_five) / len(last_five)
        return (min(cpu_average, 1.0),
                min(mem_average, 1.0))

    def _get_last_five(self, vm_name):
        measurements = self.measurement_reader.n_measurements_till(
            vm_name,
            5,
            self._config.simulation_info.current_timestamp)
        return measurements
