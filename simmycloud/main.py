#!/usr/bin/python
#

from core.cloud_simulator import CloudSimulator
from core.config import Config

config = Config()
""" populate config here """
cloud_simulator = CloudSimulator(config)

cloud_simulator.simulate()

print('ok')
