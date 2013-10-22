#!/usr/bin/python
#

import sys

from core.cloud_simulator import CloudSimulator
from core.config import Config

# from strategies.scheduling.fake_scheduling import FakeScheduling
from strategies.scheduling.first_fit import FirstFit
from strategies.migration.fake_migration import FakeMigration
from strategies.powering_off.fake_powering_off import FakePoweringOff

from core.environment import Environment
from core.server import Server
from core.statistics_manager import StatisticsManager

config = Config()
# config.strategies.scheduling = FakeScheduling()
config.strategies.scheduling = FirstFit()
config.strategies.migration = FakeMigration()
config.strategies.powering_off = FakePoweringOff()

# http://www.tutorialspoint.com/python/python_command_line_arguments.htm
config.set_param('input_directory', sys.argv[1])

config.set_param('statistics.interval', 300)
config.set_param('statistics.start_time', 0)


environment = Environment()
environment.add_servers_of_type(Server('', 1.0, 0.5), 4)
config.environment = environment

config.statistics = StatisticsManager()

cloud_simulator = CloudSimulator(config)

# cloud_simulator.verify_input()

cloud_simulator.simulate()
