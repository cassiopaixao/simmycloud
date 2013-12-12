
import logging
import configparser

from logging.handlers import RotatingFileHandler

from strategies.scheduling.fake_scheduling import FakeScheduling
from strategies.scheduling.first_fit import FirstFit
from strategies.migration.fake_migration import FakeMigration
from strategies.migration.migrate_if_overload import MigrateIfOverload
from strategies.powering_off.fake_powering_off import FakePoweringOff
from strategies.powering_off.power_off_if_empty import PowerOffIfEmpty

from builders.environment.test_environment_builder import TestEnvironmentBuilder
from builders.environment.google_environment_builder import GoogleEnvironmentBuilder

from builders.statistics_manager.standard_statistics import StandardStatistics

from core.environment import Environment
from core.statistics_manager import StatisticsManager

class Config:
    def __init__(self):
        self.identifier = ''
        self.logging_level = logging.INFO
        self.environment = None
        self.strategies = _Strategies()
        self.statistics = None
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
        self.environment.set_config(self)
        self.statistics.set_config(self)
        self.strategies.set_config(self)

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
# TODO load strategies dinamically
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
            config.strategies.scheduling = cls._get_scheduling_object(section['scheduling_strategy'])
            config.strategies.migration = cls._get_migration_object(section['migration_strategy'])
            config.strategies.powering_off = cls._get_powering_off_object(section['powering_off_strategy'])
            config.environment = cls._get_environment_object(section['environment_builder'])
            config.statistics = cls._get_statistics_manager_object(section['statistics_manager'])

            config.params = dict(section)


            config_list.append(config)

        return config_list

    @classmethod
    def _get_scheduling_object(cls, strategy):
        if strategy == 'strategies.scheduling.fake_scheduling.FakeScheduling':
            return FakeScheduling()
        elif strategy == 'strategies.scheduling.first_fit.FirstFit':
            return FirstFit()
        else:
            return None

    @classmethod
    def _get_migration_object(cls, strategy):
        if strategy == 'strategies.migration.fake_migration.FakeMigration':
            return FakeMigration()
        elif strategy == 'strategies.migration.migrate_if_overload.MigrateIfOverload':
            return MigrateIfOverload()
        else:
            return None

    @classmethod
    def _get_powering_off_object(cls, strategy):
        if strategy == 'strategies.powering_off.fake_powering_off.FakePoweringOff':
            return FakePoweringOff()
        elif strategy == 'strategies.powering_off.power_off_if_empty.PowerOffIfEmpty':
            return PowerOffIfEmpty()
        else:
            return None

    @classmethod
    def _get_environment_object(cls, environment):
        environment_builder = None
        if environment == 'builders.environment.test_environment_builder.TestEnvironmentBuilder':
            environment_builder = TestEnvironmentBuilder()
        elif environment == 'builders.environment.google_environment_builder.GoogleEnvironmentBuilder':
            environment_builder = GoogleEnvironmentBuilder()
        else:
            return None

        return Environment(environment_builder)

    @classmethod
    def _get_statistics_manager_object(cls, statistics):
        sm_builder = None
        if statistics == 'builders.statistics_manager.standard_statistics.StandardStatistics':
            sm_builder = StandardStatistics()
        else:
            return None

        return StatisticsManager(sm_builder)

    @classmethod
    def _get_logging_level(cls, log_level):
        if log_level in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']:
            return eval('logging.{}'.format(log_level))
        else:
            return None
