
class Strategy:
    def set_config(self, config):
        self._config = config

    """ This method will be called before the simulation starts.
        Override this method if you want to configure something in your
        strategy.
        The self._config object will already be available. """
    def initialize(self):
        None
