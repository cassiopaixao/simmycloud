from collections import defaultdict

class StatisticsManager:

    CSV_SEPARATOR = ','

    def __init__(self, builder):
        self._builder = builder
        self.next_control_time = 0
        self.interval = 1
        self._out = None
        self._logger = None
        self._fields = []
        self._listeners = defaultdict(list)

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

        self._logger.info('StatisticsBuilder: {}'.format(self._builder.__class__.__name__))
        self._builder.build(self)

        for field in self._fields:
            field.set_config(self._config)

    def add_field(self, field):
        self._fields.append(field)
        for event in field.listens_to():
            self._listeners[event].append(field)

    def start(self):
        self._print_header()

    def persist(self):
        self._logger.info('Persisting statistics for time: {}'.format(self.next_control_time))
        self._print_statistics()
        self._logger.info('Persisted statistics for time: {}'.format(self.next_control_time))
        self.next_control_time += self.interval
        self._logger.info('Next statistics time: {}'.format(self.next_control_time))
        self._clear_fields()

    def finish(self):
        self._print_statistics()
        self._out.close()

    def notify_event(self, event, *args, **kwargs):
        for listener in self._listeners[event]:
            listener.notify(*args, **kwargs)

    def _print_all(self):
        print('TIME: {}'.format(self.next_control_time))
        for server in self._config.environment.online_servers():
            print(server.dump())

    def _print_header(self):
        header = []
        header.append('time')
        for field in self._fields:
            header.append(field.label())
        self._out.write(self.CSV_SEPARATOR.join(header))

    def _print_statistics(self):
        data = []
        data.append(self.next_control_time)
        for field in self._fields:
            data.append(field.value())

        data[:] = [str(value) for value in data]

        self._out.write('\n')
        self._out.write(self.CSV_SEPARATOR.join(data))

    def _clear_fields(self):
        for field in self._fields:
            field.clear()


class StatisticsManagerBuilder:
    @staticmethod
    def build(cls):
        return StatisticsManager.new()


class StatisticsField:
    def __init__(self, label):
        self._label = label
        self.clear()

    def set_config(self, config):
        self._config = config

    def clear(self):
        pass

    def notify(event, *args, **kwargs):
        pass

    def listens_to(self):
        return []

    def label(self):
        return self._label

    def value(self):
        return 0
