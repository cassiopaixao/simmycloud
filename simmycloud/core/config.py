
class Config:
    def __init__(self):
        self.environment = None
        self.strategies = _Strategies()
        self.statistics = None
        self.params = dict()

    def initialize_all(self):
        self.strategies.set_config(self)
        self.statistics.set_config(self)

        self.strategies.initialize_all()
        self.statistics.initialize()

    def param(self, key):
        return self.params[key]

    def set_param(self, key, value):
        self.params[key] = value


class _Strategies:
    def __init__(self):
        self.scheduling = None
        self.migration = None
        self.powering_off = None

    def set_config(self, config):
        self._config = config

    def initialize_all(self):
        self.scheduling.set_config(self._config)
        self.migration.set_config(self._config)
        self.powering_off.set_config(self._config)

        self.scheduling.initialize()
        self.migration.initialize()
        self.powering_off.initialize()


# class ConfigBuilder:
#     # Creates a Config object based on... file?
#     def build(filename):
#         config = Config()
#         # build the Config object
#         return config
