
class Config:
    def __init__(self):
        self.environment = None
        self.strategies = _Strategies()
        self.source_directory = None


class _Strategies:
    def __init__(self):
        self.scheduling = None
        self.migration = None
        self.powering_off = None


class ConfigBuilder:
    # Creates a Config object based on... file?
    def build(filename):
        config = Config()
        # build the Config object
        return config
