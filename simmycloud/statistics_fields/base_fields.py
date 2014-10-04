###############################################################################
# The MIT License (MIT)
#
# Copyright (c) 2013 Cassio Paixao
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
###############################################################################


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
        return self._config.resource_manager._builder.__class__.__name__


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
