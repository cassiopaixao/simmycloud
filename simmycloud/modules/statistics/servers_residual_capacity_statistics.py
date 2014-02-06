
from core.statistics_manager import StatisticsModule

from statistics_fields.servers_fields import ServerResidualCapacityPercentageField

class ServersResidualCapacityStatistics(StatisticsModule):
    CSV_SEPARATOR = ','

    def __init__(self):
        self._next_control_time = 0
        self._interval = 1
        self._out = None
        self._fields = []

    def initialize(self):
        self._next_control_time = int(self._config.params['servers_statistics_start_time'])
        self._interval = int(self._config.params['servers_statistics_interval'])
        self._filename = '{}_{}.csv'.format(self._config.params['statistics_filename_prefix'], 'servers-res-cap')
        self._out = open(self._filename, "w")
        self._logger.info('%s initialized.', self.__class__.__name__)
        self._logger.info('%s file: %s', self.__class__.__name__, self._filename)
        self._logger.info('Next statistics time: %d', self._next_control_time)

    def listens_to(self):
        return ['timestamp_ended', 'simulation_started', 'simulation_finished']

    def notify_event(self, event, *args, **kwargs):
        if event == 'timestamp_ended' and kwargs.get('timestamp') >= self._next_control_time:
            self._persist()
            self._next_control_time += self._interval

        elif event == 'simulation_started':
            self._build()
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

        data[:] = [str(value) for value in data]

        self._out.write('\n')
        self._out.write(self.CSV_SEPARATOR.join(data))

    def _build(self):
        for server in self._config.environment.all_servers():
            server_field = ServerResidualCapacityPercentageField(server.describe())
            server_field.set_server(server)
            self._fields.append(server_field)
        self._fields = sorted(self._fields, key=lambda fld: int(fld.server.name))
