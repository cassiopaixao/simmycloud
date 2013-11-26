
class StatisticsManager:

    CSV_SEPARATOR = ','

    def __init__(self):
        self.next_control_time = 0
        self.interval = 1
        self._out = None
        self._logger = None
        self._counters = _Counter()

    def set_config(self, config):
        self._config = config

    def initialize(self):
        self.next_control_time = int(self._config.params['statistics_start_time'])
        self.interval = int(self._config.params['statistics_interval'])
        self.filename = self._config.params['statistics_filename']
        self._out = open(self.filename, "w")
        self._logger = self._config.getLogger(self)
        self._logger.info('StatisticsManager initialized.')
        self._logger.info('Statistics file: {}'.format(self.filename))
        self._logger.info('Next statistics time: {}'.format(self.next_control_time))

    def start(self):
        self._print_header()

    def persist(self):
        self._logger.info('Persisting statistics for time: {}'.format(self.next_control_time))
        self._print_statistics()
        self._logger.info('Persisted statistics for time: {}'.format(self.next_control_time))
        self.next_control_time += self.interval
        self._logger.info('Next statistics time: {}'.format(self.next_control_time))
        self._clear_counters()

    def finish(self):
        self._print_statistics()
        self._out.close()

    def add_to_counter(self, counter, quantity=1):
        self._counters[counter] += quantity

    def _print_all(self):
        print('TIME: {}'.format(self.next_control_time))
        for server in self._config.environment.online_servers():
            print(server.dump())

    def _print_header(self):
        header = []
        header.append('time')
        header.append('scheduling_strategy')
        header.append('migration_strategy')
        header.append('powering_off_strategy')
        header.append('submit_events')
        header.append('update_events')
        header.append('finish_events')
        header.append('online_servers')
        header.append('servers_turned_on')
        header.append('servers_turned_off')
        header.append('vms_not_allocated')
        header.append('vms_migrated')
        self._out.write(self.CSV_SEPARATOR.join(header))

    def _print_statistics(self):
        data = []
        data.append(self.next_control_time)
        data.append(self._config.strategies.scheduling.__class__.__name__)
        data.append(self._config.strategies.migration.__class__.__name__)
        data.append(self._config.strategies.powering_off.__class__.__name__)
        data.append(self._counters['submit_events'])
        data.append(self._counters['update_events'])
        data.append(self._counters['finish_events'])
        data.append(len(self._config.environment.online_servers()))
        data.append(self._counters['servers_turned_on'])
        data.append(self._counters['servers_turned_off'])
        data.append(self._counters['vms_not_allocated'])#
        data.append(self._counters['vms_migrated'])#
        data[:] = [str(value) for value in data]
        self._out.write('\n')
        self._out.write(self.CSV_SEPARATOR.join(data))

    def _clear_counters(self):
        self._counters = _Counter()


class _Counter(dict):
    def __missing__(self, key):
        return 0
