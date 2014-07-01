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

import logging

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
        if self._logger.level <= logging.DEBUG:
            self._logger.debug('Prediction called for vm %s. Last measurements: [%s]',
                                vm_name,
                                ', '.join('({},{})'.format(m[self.measurement_reader.CPU], m[self.measurement_reader.MEM]) for m in last_measurements))

        if len(last_measurements) < self.rbf_window_size:
            self._logger.debug('No prediction. %d measurements found.', len(last_measurements))
            return None

        return self._new_demands(last_measurements)

    def _get_last(self, vm_name, window_size):
        return self.measurement_reader.n_measurements_till(
            vm_name,
            window_size,
            self._config.simulation_info.current_timestamp)

    def _new_demands(self, measurements):
        try:
            new_demands = VirtualMachine('')
            new_demands.cpu = min(  self.rbf_prediction.predict(m[self.measurement_reader.CPU] for m in measurements),
                                    1.0)
            new_demands.mem = min(  self.rbf_prediction.predict(m[self.measurement_reader.MEM] for m in measurements),
                                    1.0)
            return new_demands
        except Exception as e:
            self._logger.info('RBF exception error: %s', e)
            self._logger.info('RBF exception values: [%s]',
                                               ', '.join('({},{})'.format(m[self.measurement_reader.CPU], m[self.measurement_reader.MEM]) for m in measurements))
            return None
