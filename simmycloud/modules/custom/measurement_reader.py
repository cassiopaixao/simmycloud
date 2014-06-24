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
from math import fsum
import logging

from core.simulation_module import SimulationModule

class MeasurementReader(SimulationModule):
    CPU = 0
    MEM = 1
    TIME = 2

    def initialize(self):
        self._last_overloaded_servers = []
        self._last_overloaded_servers_check = -1
        self._cached = CachedMeasurement(self._config.params['measurements_directory'],
                                         int(self._config.params['measurements_interval_time']),
                                         int(self._config.params['measurements_cache_intervals_ahead']))
        self._cached.set_config(self._config)

    def current_measurement(self, vm_name):
        return self.measurements_interval(vm_name,
                                          self._config.simulation_info.current_timestamp,
                                          self._config.simulation_info.current_timestamp)[-1]

    def n_measurements_till(self, vm_name, n, till_time):
        return self._cached.n_measurements_till(vm_name, n, till_time)

    def measurements_interval(self, vm_name, from_time, till_time):
        return self._cached.measurements_interval(vm_name, from_time, till_time)

    def overloaded_servers(self):
        if self._last_overloaded_servers_check != self._config.simulation_info.current_timestamp:
            self._last_overloaded_servers = self._check_overloaded_servers()
            self._last_overloaded_servers_check = self._config.simulation_info.current_timestamp
        return self._last_overloaded_servers

    def free_cache_for_vm(self, vm_name):
        self._cached.free_measurements_of_vm(vm_name)

    def _check_overloaded_servers(self):
        self._logger.debug('Verifying overloaded servers on time %d',
                            self._config.simulation_info.current_timestamp)
        overloaded_servers = []
        for server in self._config.resource_manager.online_servers():
            free_cpu, free_mem = server.cpu, server.mem
            for vm in server.vm_list():
                measurement = self.current_measurement(vm.name)
                free_cpu = free_cpu - measurement[MeasurementReader.CPU]
                free_mem = free_mem - measurement[MeasurementReader.MEM]
            if free_cpu < 0 or free_mem < 0:
                overloaded_servers.append(server)

        if overloaded_servers and self._logger.level <= logging.DEBUG:
            for s in overloaded_servers:
                cpu_use = fsum(self.current_measurement(vm.name)[MeasurementReader.CPU] for vm in s.vm_list)
                mem_use = fsum(self.current_measurement(vm.name)[MeasurementReader.MEM] for vm in s.vm_list)
                self._logger.debug('Server %s is overloaded. Measured: %f, %f',
                                    s.dump(), cpu_use, mem_use)
        else:
            self._logger.debug('No server is overloaded')

        return overloaded_servers


class CachedMeasurement:

    def __init__(self, input_directory, interval_time, cache_intervals_ahead):
        self._input_directory = input_directory
        self._interval_time = interval_time
        self._cache_intervals_ahead = cache_intervals_ahead
        self._measurements = {}

    def set_config(self, config):
        self._config = config

    def measurements_interval(self, vm_name, from_time, till_time):
        if vm_name not in self._measurements:
            self._caches_measurements(vm_name, from_time, till_time)

        from_time = from_time - self._interval_time

        vm_allocation_data = self._config.resource_manager.get_vm_allocation_data(vm_name)
        # (cpu, mem, time)
        measurements = [(   vm_allocation_data.submit_cpu_demand,
                            vm_allocation_data.submit_mem_demand,
                            vm_allocation_data.submit_time
                        )]
        measurements.extend(m for m in self._measurements[vm_name] if m[MeasurementReader.TIME] > from_time and m[MeasurementReader.TIME] <= till_time)

        return measurements

    def n_measurements_till(self, vm_name, n, till_time):
        from_time = till_time - (n+1)*self._interval_time

        return self.measurements_interval(vm_name, from_time, till_time)[-n:]

    def _caches_measurements(self, vm_name, from_time, till_time):
        path = re.sub('(\d+)-(\d+)', '\\1/\\1-\\2.csv', vm_name)
        filepath = '{}/{}'.format(self._input_directory, path)

        measurements = []

        if os.path.isfile(filepath): #verifies if file exists
            opened_file = fileinput.input(filepath)

            line = opened_file.readline()
            while len(line) > 0:
                start_time, end_time, cpu, mem = line.split(',')
                if int(end_time)   < from_time:
                    line = opened_file.readline()
                    continue
                if int(start_time) > till_time: break

                # (cpu, mem, start_time)
                measurements.append( (float(cpu), float(mem), int(start_time)) )

                line = opened_file.readline()

            opened_file.close()

        self._measurements[vm_name] = measurements

    def free_measurements_of_vm(self, vm_name):
        self._measurements.pop(vm_name, None)
