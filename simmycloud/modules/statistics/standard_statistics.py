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

from collections import defaultdict

from core.statistics_manager import StatisticsModule

from statistics_fields.base_fields import CounterField, EnvironmentField, PredictionStrategyField, MigrationStrategyField, PoweringOffStrategyField, SchedulingStrategyField, TimeIntervalSinceLastStatisticsField
from statistics_fields.servers_fields import OnlineServersField, OverloadedServersField, TightedVMsField, VMsInPoolField, HighPriorityVMsInPoolField, LowPriorityVMsInPoolField, ServersTotalResidualCapacityField

class StandardStatistics(StatisticsModule):
    CSV_SEPARATOR = ','

    def __init__(self):
        self._next_control_time = 0
        self._interval = 1
        self._out = None
        self._fields = []
        self._listeners = defaultdict(list)

    def initialize(self):
        self._next_control_time = int(self._config.params['standard_statistics_start_time'])
        self._interval = int(self._config.params['standard_statistics_interval'])
        self._filename = '{}_{}.csv'.format(self._config.params['statistics_filename_prefix'], 'standard')
        self._out = open(self._filename, "w")
        self._logger.info('StandardStatistics initialized.')
        self._logger.info('StandardStatistics file: %s', self._filename)
        self._logger.info('Next statistics time: %d', self._next_control_time)

        self._build()

        for field in self._fields:
            field.set_config(self._config)

        self._print_header()

    def listens_to(self):
        return list(set(['timestamp_ended', 'simulation_finished'] + list(self._listeners.keys())))

    def notify_event(self, event, *args, **kwargs):
        for listener in self._listeners[event]:
            listener.notify(*args, **kwargs)

        if event == 'timestamp_ended' and kwargs.get('timestamp') >= self._next_control_time:
            self._persist()
            self._clear_fields()
            self._next_control_time += self._interval
            self._logger.info('Next statistics time: %d', self._next_control_time)
        elif event == 'simulation_finished':
            self._persist()
            self._out.close()

    def _persist(self):
        self._logger.info('Persisting statistics for time: %d', self._next_control_time)
        self._print_statistics()
        self._out.flush()
        self._logger.info('Persisted statistics for time: %d', self._next_control_time)

    def _print_header(self):
        header = []
        header.append('time')
        for field in self._fields:
            header.append(field.label())
        self._out.write(self.CSV_SEPARATOR.join(header))

    def _print_statistics(self):
        data = []
        data.append(self._next_control_time)
        for field in self._fields:
            data.append(field.value())

        data[:] = [str(value) for value in data]

        self._out.write('\n')
        self._out.write(self.CSV_SEPARATOR.join(data))

    def _clear_fields(self):
        for field in self._fields:
            field.clear()

    def _add_field(self, field):
        self._fields.append(field)
        for event in field.listens_to():
            self._listeners[event].append(field)

    def _build(self):
        self._add_field(EnvironmentField('environment'))
        self._add_field(SchedulingStrategyField('scheduling'))
        self._add_field(PredictionStrategyField('prediction'))
        self._add_field(MigrationStrategyField('migration'))
        self._add_field(PoweringOffStrategyField('powering_off'))
        self._add_field(TightedVMsField('sla_violations'))
        self._add_field(OnlineServersField('online_servers'))
        self._add_field(OverloadedServersField('overloaded_servers'))
        self._add_field(CounterField('vms_migrated'))
        self._add_field(VMsInPoolField('vms_in_pool'))
        self._add_field(HighPriorityVMsInPoolField('high_priority_vms_in_pool'))
        self._add_field(LowPriorityVMsInPoolField('low_priority_vms_in_pool'))
        self._add_field(ServersTotalResidualCapacityField('online_residual_capacity'))
        self._add_field(CounterField('submit_events'))
        self._add_field(CounterField('update_events'))
        self._add_field(CounterField('finish_events'))
        self._add_field(CounterField('outdated_finish_events'))
        self._add_field(CounterField('servers_turned_on'))
        self._add_field(CounterField('servers_turned_off'))
        self._add_field(CounterField('vms_not_allocated'))
        self._add_field(CounterField('couldnot_reallocate'))
        self._add_field(TimeIntervalSinceLastStatisticsField('interval_time'))
