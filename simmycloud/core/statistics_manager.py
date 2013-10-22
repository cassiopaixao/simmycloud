
class StatisticsManager:

    def __init__(self):
        self.next_control_time = 0
        self.interval = 1

    def set_config(self, config):
        self._config = config

    def initialize(self):
        self.next_control_time = int(self._config.param('statistics.start_time'))
        self.interval = int(self._config.param('statistics.interval'))

    def start(self):
        pass

    def persist(self):
        self._print_all()
        pass

    def finish(self):
        self._print_all()
        pass

    def _print_all(self):
        print('TIME: {}'.format(self.next_control_time))
        for server in self._config.environment.online_servers():
            print(server.dump())
