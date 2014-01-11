#!/usr/bin/python
#

import sys

from core.cloud_simulator import CloudSimulator
from core.config import ConfigBuilder

def print_usage():
    print('Usage: python3 {} CONFIG_FILE INSTANCE_NAME'.format(sys.argv[0]))
    print('CONFIG_FILE - file with simulations configs')
    print('INSTANCE_NAME - name of the simulation you want to run/verify/filter, identified in CONFIG_FILE')
    print()


if len(sys.argv) < 3:
    print_usage()
    exit()

configs = ConfigBuilder.build_all(sys.argv[1])
configs[:] = [c for c in configs if c.identifier == sys.argv[2]]

if len(configs) == 1:
    config = configs[0]
else:
    print_usage()
    exit()

cloud_simulator = CloudSimulator(config)

try:
    cloud_simulator.simulate()
except Exception as e:
    config.getLogger(__name__).critical(e)
    config.getLogger(__name__).exception(e)
