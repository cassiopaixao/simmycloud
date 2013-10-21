
class Config:
    def __init__(self):
        self.environment = None
        self.strategies = _Strategies()
        self.input_directory = None

    def initialize_all(self):
        self.strategies.initialize_all(self)


class _Strategies:
    def __init__(self):
        self.scheduling = None
        self.migration = None
        self.powering_off = None

    def initialize_all(self, config_obj):
        self.scheduling.set_config(config_obj)
        self.migration.set_config(config_obj)
        self.powering_off.set_config(config_obj)

        self.scheduling.initialize()
        self.migration.initialize()
        self.powering_off.initialize()


# class ConfigBuilder:
#     # Creates a Config object based on... file?
#     def build(filename):
#         config = Config()
#         # build the Config object
#         return config
