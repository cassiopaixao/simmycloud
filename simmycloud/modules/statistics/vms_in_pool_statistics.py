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
from statistics_fields.vms_in_pool_fields import VMsInPoolMaxCPUField, VMsInPoolMaxMemField, VMsInPoolMaxLinearCPUField, VMsInPoolMaxLinearMemField, VMsInPoolMinLinearCPUField, VMsInPoolMinLinearMemField, VMsInPoolListField, VMsInPoolMinLinearFitsOnlineField, VMsInPoolMinLinearFitsOfflineField

class VMsInPoolStatistics(StatisticsModule):
    CSV_SEPARATOR = ','

    def __init__(self):
        self._next_control_time = 0
        self._interval = 1
        self._out = None
        self._fields = []

    def initialize(self):
        self._next_control_time = int(self._config.params['standard_statistics_start_time'])
        self._interval = int(self._config.params['standard_statistics_interval'])
        self._filename = '{}_{}.csv'.format(self._config.params['statistics_filename_prefix'], 'vms-in-pool')
        self._out = open(self._filename, "w")
        self._logger.info('%s initialized.', self.__class__.__name__)
        self._logger.info('%s file: %s', self.__class__.__name__, self._filename)
        self._logger.info('Next statistics time: %d', self._next_control_time)

        self._build()
        for field in self._fields:
            field.set_config(self._config)

    def listens_to(self):
        return ['timestamp_ended', 'simulation_started', 'simulation_finished']

    def notify_event(self, event, *args, **kwargs):
        if event == 'timestamp_ended' and kwargs.get('timestamp') >= self._next_control_time:
            self._persist()
            self._next_control_time += self._interval

        elif event == 'simulation_started':
            self._print_header()

        elif event == 'simulation_finished':
            self._persist()
            self._out.close()

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
        self._fields.append(VMsInPoolMaxCPUField('max_cpu'))
        self._fields.append(VMsInPoolMaxMemField('max_mem'))
        self._fields.append(VMsInPoolMaxLinearCPUField('max_linear_cpu'))
        self._fields.append(VMsInPoolMaxLinearMemField('max_linear_mem'))
        self._fields.append(VMsInPoolMinLinearCPUField('min_linear_cpu'))
        self._fields.append(VMsInPoolMinLinearMemField('min_linear_mem'))
        self._fields.append(VMsInPoolMinLinearFitsOnlineField('min_linear_fits_online'))
        self._fields.append(VMsInPoolMinLinearFitsOfflineField('min_linear_fits_offline'))
        self._fields.append(VMsInPoolListField('vms_in_pool'))
