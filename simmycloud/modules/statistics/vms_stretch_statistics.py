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


from core.statistics_manager import StatisticsModule

from statistics_fields.vms_fields import VMNameField, VMStretchField, VMSubmitTimestampField, VMFinishTimestampField

class VMsStretchStatistics(StatisticsModule):
    CSV_SEPARATOR = ','

    def __init__(self):
        self._out = None
        self._fields = []

    def initialize(self):
        self._filename = '{}_{}.csv'.format(self._config.params['statistics_filename_prefix'], 'vms-stretch')
        self._out = open(self._filename, "w")
        self._logger.info('%s initialized.', self.__class__.__name__)
        self._logger.info('%s file: %s', self.__class__.__name__, self._filename)
        self._build()

        for field in self._fields:
            field.set_config(self._config)

    def listens_to(self):
        return ['vm_finished', 'simulation_started', 'simulation_finished']

    def notify_event(self, event, *args, **kwargs):
        if event == 'vm_finished':
            self._print_statistics(kwargs.get('vm'))
            self._out.flush()

        elif event == 'simulation_started':
            self._print_header()

        elif event == 'simulation_finished':
            self._out.close()

    def _print_header(self):
        header = [field.label() for field in self._fields]
        self._out.write(self.CSV_SEPARATOR.join(header))

    def _print_statistics(self, vm):
        data = [str(field.value(vm)) for field in self._fields]

        self._out.write('\n')
        self._out.write(self.CSV_SEPARATOR.join(data))

    def _build(self):
        self._fields.append(VMNameField('vm_name'))
        self._fields.append(VMStretchField('stretch'))
        self._fields.append(VMSubmitTimestampField('submit_time'))
        self._fields.append(VMFinishTimestampField('finish_time'))
