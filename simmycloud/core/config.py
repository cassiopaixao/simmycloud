
import configparser

from strategies.scheduling.fake_scheduling import FakeScheduling
from strategies.scheduling.first_fit import FirstFit
from strategies.migration.fake_migration import FakeMigration
from strategies.powering_off.fake_powering_off import FakePoweringOff

from core.environment import EnvironmentBuilder
from core.statistics_manager import StatisticsManager

class Config:
    def __init__(self):
        self.environment = None
        self.strategies = _Strategies()
        self.statistics = None
        self.params = dict()

    def initialize_all(self):
        self.environment.set_config(self)
        self.strategies.set_config(self)
        self.statistics.set_config(self)

        self.strategies.initialize_all()
        self.statistics.initialize()


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
    def build(cls, filename):
        configs = configparser.ConfigParser()
        configs.read(filename)

        config_list = []

        for section_name in configs.sections():
            section = configs[section_name]
            config = Config()

            config.strategies.scheduling = cls._get_scheduling_object(section['scheduling_strategy'])
            config.strategies.migration = cls._get_migration_object(section['migration_strategy'])
            config.strategies.powering_off = cls._get_powering_off_object(section['powering_off_strategy'])

            config.params = dict(section)

            # TODO put the environment definition in external file
            environment = EnvironmentBuilder.build_test_environment()
            config.environment = environment

            config.statistics = StatisticsManager()

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
        else:
            return None

    @classmethod
    def _get_powering_off_object(cls, strategy):
        if strategy == 'strategies.powering_off.fake_powering_off.FakePoweringOff':
            return FakePoweringOff()
        else:
            return None
