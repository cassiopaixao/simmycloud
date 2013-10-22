#!/usr/bin/python
#

import sys

from core.cloud_simulator import CloudSimulator
from core.config import ConfigBuilder

if len(sys.argv) == 1:
    print('Usage: python3 {} CONFIG_FILE [--verify]\n'.format(sys.argv[0]))
    exit()

config = ConfigBuilder.build(sys.argv[1])[0]

cloud_simulator = CloudSimulator(config)

if len(sys.argv) > 2 and sys.argv[2] == '--verify':
    cloud_simulator.verify_input()

else:
    cloud_simulator.simulate()
