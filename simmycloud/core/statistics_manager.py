###############################################################################
# The MIT License (MIT)
#
# Copyright (c) 2013 Cássio Paixão
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

from collections import defaultdict

class StatisticsManager:

    def __init__(self):
        self._logger = None
        self._modules = list()
        self._listeners = defaultdict(list)

    def set_config(self, config):
        self._config = config
        for module in self._modules:
            module.set_config(config)

    def initialize(self):
        self._logger = self._config.getLogger(self)
        self._logger.info('Initializing StatisticsManager.')

        for module in self._modules:
            self._logger.info('Initializing Statistics Module: %s', module.__class__.__name__)
            module.initialize()
            for event in module.listens_to():
                self._listeners[event].append(module)

        self._logger.info('StatisticsManager initialized.')

    def notify_event(self, event, *args, **kwargs):
        self._logger.debug('Notified event: %s', event)
        for listener in self._listeners[event]:
            listener.notify_event(event, *args, **kwargs)

    def add_module(self, module):
        self._modules.append(module)


class StatisticsModule:

    def set_config(self, config):
        self._config = config
        self._logger = self._config.getLogger(self)

    """ This method will be called before the simulation starts.
        Override this method if you want to configure something in your module.
        The self._config and self._logger objects will already be available. """
    def initialize(self):
        pass

    def listens_to(self):
        raise NotImplementedError

    def notify_event(self, event, *args, **kwargs):
        raise NotImplementedError


class StatisticsField:
    def __init__(self, label):
        self._label = label
        self.clear()

    def set_config(self, config):
        self._config = config

    def clear(self):
        pass

    def notify(event, *args, **kwargs):
        pass

    def listens_to(self):
        return []

    def label(self):
        return self._label

    def value(self):
        return 0
