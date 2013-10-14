#!/usr/bin/python
#

import sys

from core.cloud_simulator import CloudSimulator
from core.config import Config

from strategies.scheduling.fake_scheduling import FakeScheduling
from strategies.migration.fake_migration import FakeMigration
from strategies.powering_off.fake_powering_off import FakePoweringOff

from core.environment import Environment
from core.server import Server

config = Config()
config.strategies.scheduling = FakeScheduling()
config.strategies.migration = FakeMigration()
config.strategies.powering_off = FakePoweringOff()

config.source_directory = sys.argv[1]

environment = Environment()
environment.add_servers_of_type(Server('', 1.0, 0.5))
config.environment = environment

cloud_simulator = CloudSimulator(config)

cloud_simulator.simulate()

print('ok')
