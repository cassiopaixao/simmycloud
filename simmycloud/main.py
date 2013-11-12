#!/usr/bin/python
#

import sys

from core.cloud_simulator import CloudSimulator
from core.config import ConfigBuilder

if len(sys.argv) == 1:
    print('Usage: python3 {} CONFIG_FILE [--verify|--filter]\n'.format(sys.argv[0]))
    exit()

config = ConfigBuilder.build(sys.argv[1])[0]

cloud_simulator = CloudSimulator(config)

if len(sys.argv) > 2:
    if sys.argv[2] == '--verify':
        cloud_simulator.verify_input()
    elif sys.argv[2] == '--filter':
        cloud_simulator.filter_input()
    else:
        print('Usage: python3 {} CONFIG_FILE [--verify|--filter]\n'.format(sys.argv[0]))
        exit()
else:
    cloud_simulator.simulate()
