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


import logging
import configparser
import re

from logging.handlers import RotatingFileHandler

from core.environment import Environment
from core.statistics_manager import StatisticsManager
from core.event import EventsQueue
from core.vms_pool import PendingVMsPool
from core.cloud_simulator import SimulationInfo

class Config:
    def __init__(self):
        self.identifier = ''
        self.logging_level = logging.INFO
        self.environment = None
        self.strategies = _Strategies()
        self.statistics = None
        self.events_queue = EventsQueue()
        self.vms_pool = PendingVMsPool()
        self.simulation_info = SimulationInfo()
        self.params = dict()
        self.module = dict()

    def initialize(self):
        logger = logging.getLogger(self.identifier)
        handler = RotatingFileHandler(filename = self.params['log_filename'],
                                      mode = 'w',
                                      maxBytes = int(self.params['log_max_file_size']),
                                      backupCount = int(self.params['log_max_backup_files']),
                                      encoding = 'utf8'
                                      )
        formatter = logging.Formatter('%(levelname)s - %(name)s: %(message)s')
        handler.setFormatter(formatter)
        logger.setLevel(self.logging_level)
        logger.addHandler(handler)
        self._initialize_all()
        self._initialize_modules()

    def _initialize_all(self):
        self.events_queue.set_config(self)
        self.environment.set_config(self)
        self.statistics.set_config(self)
        self.strategies.set_config(self)

        self.events_queue.initialize()
        self.environment.initialize()
        self.statistics.initialize()
        self.strategies.initialize_all()

    def _initialize_modules(self):
        for module in self.module.values():
            module.set_config(self)
            module.initialize()

    def getLogger(self, obj):
        return logging.getLogger("{}.{}".format(self.identifier, obj.__class__.__name__))


class _Strategies:
    def __init__(self):
        self.prediction = None
        self.scheduling = None
        self.migration = None
        self.powering_off = None

    def set_config(self, config):
        self._config = config

    def initialize_all(self):
        self.prediction.set_config(self._config)
        self.scheduling.set_config(self._config)
        self.migration.set_config(self._config)
        self.powering_off.set_config(self._config)

        self.prediction.initialize()
        self.scheduling.initialize()
        self.migration.initialize()
        self.powering_off.initialize()


# use http://docs.python.org/3/library/configparser.html
class ConfigBuilder:

    @classmethod
    def build_all(cls, filename):
        configs = configparser.ConfigParser()
        configs.read(filename)

        config_list = []

        for section_name in configs.sections():
            section = configs[section_name]
            config = Config()

            config.identifier = section_name
            config.logging_level = cls._get_logging_level(section['logging_level'])
            config.strategies.prediction = cls._get_object(section['prediction_strategy'])
            config.strategies.scheduling = cls._get_object(section['scheduling_strategy'])
            config.strategies.migration = cls._get_object(section['migration_strategy'])
            config.strategies.powering_off = cls._get_object(section['powering_off_strategy'])
            config.environment = Environment(cls._get_object(section['environment_builder']))
            # statistics
            config.statistics = StatisticsManager()
            statistics_modules = [value.strip() for value in section['statistics_modules'].split(',') if value.strip()]
            for module in statistics_modules:
                config.statistics.add_module(cls._get_object(module))
            #modules
            modules = [value.strip() for value in section['modules'].split(',') if value.strip()]
            for module in modules:
                module_name = module.split('.')[-1]
                config.module[module_name] = cls._get_object(module)

            config.params = dict(section)

            config_list.append(config)

        return config_list

    @classmethod
    def _get_object(cls, classpath):
        module_name = re.match('(.*)\.[^.]*', classpath).group(1)
        class_name = re.match('[^.]*\.(.*)', classpath).group(1)
        module = __import__(module_name)
        obj = eval('module.{}()'.format(class_name))
        return obj

    @classmethod
    def _get_logging_level(cls, log_level):
        if log_level in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']:
            return eval('logging.{}'.format(log_level))
        else:
            return None
