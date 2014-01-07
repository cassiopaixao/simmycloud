
import logging
import configparser
import re

from logging.handlers import RotatingFileHandler

from core.environment import Environment
from core.statistics_manager import StatisticsManager
from core.event import EventsQueue
from core.vms_pool import PendingVMsPool

class Config:
    def __init__(self):
        self.identifier = ''
        self.logging_level = logging.INFO
        self.environment = None
        self.strategies = _Strategies()
        self.statistics = None
        self.events_queue = EventsQueue()
        self.vms_pool = PendingVMsPool()
        self.params = dict()

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

    def _initialize_all(self):
        self.events_queue.set_config(self)
        self.environment.set_config(self)
        self.statistics.set_config(self)
        self.strategies.set_config(self)

        self.events_queue.initialize()
        self.environment.initialize()
        self.statistics.initialize()
        self.strategies.initialize_all()

    def getLogger(self, obj):
        return logging.getLogger("{}.{}".format(self.identifier, obj.__class__.__name__))


class _Strategies:
    def __init__(self):
        self.scheduling = None
        self.migration = None
        self.powering_off = None

    def set_config(self, config):
        self._config = config

    def initialize_all(self):
        self.scheduling.set_config(self._config)
        self.migration.set_config(self._config)
        self.powering_off.set_config(self._config)

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
            config.strategies.scheduling = cls._get_object(section['scheduling_strategy'])
            config.strategies.migration = cls._get_object(section['migration_strategy'])
            config.strategies.powering_off = cls._get_object(section['powering_off_strategy'])
            config.environment = Environment(cls._get_object(section['environment_builder']))
            config.statistics = StatisticsManager(cls._get_object(section['statistics_manager']))

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
