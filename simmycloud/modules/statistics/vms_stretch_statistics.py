
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
