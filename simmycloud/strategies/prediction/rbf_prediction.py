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

class RBFPrediction(PredictionStrategy):

    def initialize(self):
        self.measurement_reader = self._config.module['MeasurementReader']
        self.rbf_prediction = self._config.module['RBFTimeSeriesPrediction']
        self.rbf_window_size = int(self._config.params['rbf_time_series_prediction_window_size'])

    @PredictionStrategy.predict_strategy
    def predict(self, vm_name):
        last_measurements = self._get_last(vm_name, self.rbf_window_size)
        self._logger.debug('Prediction called for vm %s. Last measurements: [%s]',
                            vm_name,
                            ', '.join(['({},{})'.format(m[self.measurement_reader.CPU], m[self.measurement_reader.MEM]) for m in last_measurements]))

        if len(last_measurements) < self.rbf_window_size:
            self._logger.debug('No prediction. %d measurements found.', len(last_measurements))
            return None

        new_demands = VirtualMachine('')
        new_demands.cpu = min(self._prediction_for([m[self.measurement_reader.CPU] for m in last_measurements]), 1.0)
        new_demands.mem = min(self._prediction_for([m[self.measurement_reader.MEM] for m in last_measurements]), 1.0)
        return new_demands

    def _get_last(self, vm_name, window_size):
        return self.measurement_reader.n_measurements_till(
            vm_name,
            window_size,
            self._config.simulation_info.current_timestamp)

    def _prediction_for(self, values):
        try:
            return self.rbf_prediction.predict(values)
        except Exception as e:
            self._config.getLogger(self).debug('RBF exception error: %s', e)
            self._config.getLogger(self).debug('RBF exception values: [%s]',
                                               ', '.join([str(v) for v in values]))
            return float(sum(values)) / len(values)
