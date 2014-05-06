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


import re
import fileinput
import os.path

from core.simulation_module import SimulationModule

class MeasurementReader(SimulationModule):

    def initialize(self):
        self._directory = self._config.params['measurements_directory']
        self._last_overloaded_servers = []
        self._last_overloaded_servers_check = -1

    def current_measurement(self, vm_name):
        return self.measurements_till(vm_name,
                                      self._config.simulation_info.current_timestamp)[-1]

    def measurements_till(self, vm_name, time_limit=None):
        path = re.sub('(\d+)-(\d+)', '\\1/\\1-\\2.csv', vm_name)
        filepath = '{}/{}'.format(self._directory, path)

        measurements = []
        measurements.append({'cpu': self._config.resource_manager.get_vm_allocation_data(vm_name).submit_cpu_demand,
                             'mem': self._config.resource_manager.get_vm_allocation_data(vm_name).submit_mem_demand
                             })

        if not os.path.isfile(filepath): #verifies if file exists
            return measurements

        opened_file = fileinput.input(filepath)

        line = opened_file.readline()
        while len(line) > 0:
            start_time, end_time, cpu, mem = line.split(',')
            if time_limit is not None and int(start_time) > time_limit: break

            measurements.append({'cpu': float(cpu),
                                 'mem': float(mem)
                                 })

            line = opened_file.readline()
        opened_file.close()

        return measurements

    def overloaded_servers(self):
        if self._last_overloaded_servers_check != self._config.simulation_info.current_timestamp:
            self._last_overloaded_servers = self._check_overloaded_servers()
            self._last_overloaded_servers_check = self._config.simulation_info.current_timestamp
        return self._last_overloaded_servers

    def _check_overloaded_servers(self):
        overloaded_servers = []
        for server in self._config.resource_manager.online_servers():
            free_cpu, free_mem = server.cpu, server.mem
            for vm in server.vm_list():
                measurement = self.current_measurement(vm.name)
                free_cpu = free_cpu - measurement['cpu']
                free_mem = free_mem - measurement['mem']
            if free_cpu < 0 or free_mem < 0:
                overloaded_servers.append(server)

        return overloaded_servers
