
class Config:
    def __init__(self):
        self.environment = None
        self.schedule_strategy = None
        self.migration_strategy = None
        self.turn_off_strategy = None

class ConfigBuilder:
    # Creates a Config object based on a file content
    def build(filename):
        config = Config()
        # build the Config object
        return config
