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


from core.statistics_manager import StatisticsModule

from statistics_fields.prediction_fields import AverageAbsolutePredictionErrorField, StdevAbsolutePredictionErrorField, AveragePercentilePredictionErrorField, StdevPercentilePredictionErrorField

class PredictionErrorStatistics(StatisticsModule):
    CSV_SEPARATOR = ','

    def __init__(self):
        self._next_control_time = 0
        self._interval = 1
        self._out = None
        self._fields = []
        self.prediction_base = []

    def initialize(self):
        self._next_control_time = int(self._config.params['first_prediction_time'])
        self._interval = int(self._config.params['prediction_interval'])
        self._filename = '{}_{}.csv'.format(self._config.params['statistics_filename_prefix'], 'prediction-errors')
        self._out = open(self._filename, "w")
        self._logger.info('%s initialized.', self.__class__.__name__)
        self._logger.info('%s file: %s', self.__class__.__name__, self._filename)
        self._logger.info('Next statistics time: %d', self._next_control_time)

    def listens_to(self):
        return ['timestamp_ended', 'simulation_started', 'simulation_finished']

    def notify_event(self, event, *args, **kwargs):
        if event == 'timestamp_ended' and kwargs.get('timestamp') >= self._next_control_time:
            self._update_base()
            self._persist()
            self._next_control_time += self._interval

        elif event == 'simulation_started':
            self._build()
            self._print_header()

        elif event == 'simulation_finished':
            self._persist()
            self._out.close()

    def _update_base(self):
        self.prediction_base[:] = []
        for vm_name in self._config.resource_manager.online_vms_names():
            prediction = self._config.strategies.prediction.predict(vm_name)
            if prediction:
                self.prediction_base.append((
                    (prediction.cpu, prediction.mem),
                    self._current_measurement(vm_name)
                    ))

    def _current_measurement(self, vm_name):
        m = self._config.module['MeasurementReader'].current_measurement(vm_name)
        return (max(m[0], 0.0000000001), max(m[1], 0.0000000001))

    def _persist(self):
        self._logger.info('Persisting statistics for time: %d', self._next_control_time)
        self._print_statistics()

    def _print_header(self):
        header = ['time']
        for field in self._fields:
            header.append(field.label().replace(' ','').replace(',', '|'))
        self._out.write(self.CSV_SEPARATOR.join(header))

    def _print_statistics(self):
        data = [self._next_control_time]
        for field in self._fields:
            data.append(field.value())

        self._out.write('\n')
        self._out.write(self.CSV_SEPARATOR.join(str(value) for value in data))

    def _build(self):
        cpu_fields = [  AverageAbsolutePredictionErrorField(  'cpu_average_abs_error' ),
                        StdevAbsolutePredictionErrorField(    'cpu_stdev_abs_error'   ),
                        AveragePercentilePredictionErrorField('cpu_average_perc_error'),
                        StdevPercentilePredictionErrorField(  'cpu_stdev_perc_error'  )]
        mem_fields = [  AverageAbsolutePredictionErrorField(  'mem_average_abs_error' ),
                        StdevAbsolutePredictionErrorField(    'mem_stdev_abs_error'   ),
                        AveragePercentilePredictionErrorField('mem_average_perc_error'),
                        StdevPercentilePredictionErrorField(  'mem_stdev_perc_error'  )]
        for f in cpu_fields:
            f.set_resource('cpu')
            f.set_prediction_base(self.prediction_base)
        for f in mem_fields:
            f.set_resource('mem')
            f.set_prediction_base(self.prediction_base)

        self._fields.extend(cpu_fields)
        self._fields.extend(mem_fields)
