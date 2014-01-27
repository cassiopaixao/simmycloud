
import time
from core.statistics_manager import StatisticsField

class CounterField(StatisticsField):
    def listens_to(self):
        return [self.label()]

    def clear(self):
        self._counter = 0

    def notify(self, how_many=1):
        self._counter += how_many

    def value(self):
        return self._counter


class EnvironmentField(StatisticsField):
    def value(self):
        return self._config.environment._builder.__class__.__name__


class SchedulingStrategyField(StatisticsField):
    def value(self):
        return self._config.strategies.scheduling.__class__.__name__


class MigrationStrategyField(StatisticsField):
    def value(self):
        return self._config.strategies.migration.__class__.__name__


class PoweringOffStrategyField(StatisticsField):
    def value(self):
        return self._config.strategies.powering_off.__class__.__name__


class PredictionStrategyField(StatisticsField):
    def value(self):
        return self._config.strategies.prediction.__class__.__name__


class TimeIntervalSinceLastStatisticsField(StatisticsField):
    def clear(self):
        self.start_time = time.clock()

    def value(self):
        return time.clock() - self.start_time
