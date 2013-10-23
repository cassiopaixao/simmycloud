
from strategies.strategy import Strategy

class PoweringOffStrategy(Strategy):

    def power_off_if_necessary_statistics(func):
        def new_power_off_if_necessary(self, *args, **kwargs):
            res = func(self, *args, **kwargs)
            if res is None:
                self._config.statistics.add_to_counter('servers_turned_off')
            return res
        return new_power_off_if_necessary

    @power_off_if_necessary_statistics
    def power_off_if_necessary(self, server):
        raise NotImplementedError
