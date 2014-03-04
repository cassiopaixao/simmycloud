
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
        measurements.append({'cpu': self._config.environment.get_vm_allocation_data(vm_name).submit_cpu_demand,
                             'mem': self._config.environment.get_vm_allocation_data(vm_name).submit_mem_demand
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
        for server in self._config.environment.online_servers():
            free_cpu, free_mem = server.cpu, server.mem
            for vm in server.vm_list():
                measurement = self.current_measurement(vm.name)
                free_cpu = free_cpu - measurement['cpu']
                free_mem = free_mem - measurement['mem']
            if free_cpu < 0 or free_mem < 0:
                overloaded_servers.append(server)

        return overloaded_servers
