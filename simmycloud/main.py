#!/usr/bin/python
#
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
