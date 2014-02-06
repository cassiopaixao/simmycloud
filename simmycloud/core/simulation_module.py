
class SimulationModule:

    def set_config(self, config):
        self._config = config
        self._logger = self._config.getLogger(self)

    """ This method will be called on the first request to this module.
        Override this method if you want to configure something in your
        module.
        The self._config and self._logger objects will already be available. """
    def initialize(self):
        pass
