
import re
import fileinput

from core.simulation_module import SimulationModule

class MeasurementReader(SimulationModule):

    def initialize(self):
        self._directory = self._config.params['measurements_directory']

    def current_measurement(self, vm_name):
        return self.measurements_till(vm_name,
                                      self._config._simulation_info.current_timestamp)[-1]

    def measurements_till(self, vm_name, time_limit=None):
        path = re.sub('(\d+)-(\d+)', '\1/\1-\2.csv', vm_name)
        filepath = '{}/{}'.format(self._directory, path)
        opened_file = fileinput.input(filepath)

        measurements = []
        measurements.append({'cpu': self._config.environment.vm_status[vm_name].submit_cpu_demand,
                             'mem': self._config.environment.vm_status[vm_name].submit_mem_demand
                             })

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

class CachedMeasurementReader(SimulationModule):

    def initialize(self):
        self._directory = self._config.params['measurements_directory']
        self._cache_size = int(self._config.params['measurements_cache'])
        self._cache = dict()

    def current_measurement(self, vm_name):
        current_timestamp = self._config._simulation_info.current_timestamp
        if self._cache.has_key(vm_name):
            start_time, end_time, measurement = self._cached_measurements_till(vm_name, current_timestamp)[-1]

            if end_time < current_timestamp:
                # updates vm's cache
                self._cache_measurements(vm_name, current_timestamp)
                start_time, end_time, measurement = self._cached_measurements_till(vm_name, current_timestamp)[-1]

        return measurement

    def _cached_measurements_till(self, vm_name, last_timestamp):
        return [measurement for measurement in self._cache[vm_name] if measurement[0] <= last_timestamp]

    def _cache_measurements(self, vm_name, start_cache_time=0):
        opened_file = self._open_file_of_vm(vm_name)

        if self._cache.has_key(vm_name): del self._cache[vm_name]

        measurements = []

        line = opened_file.readline()
        while len(line) > 0:
            end_time = int(line.split(',')[1])
            if end_time >= start_cache_time:
                break
            line = opened_file.readline()

        i = 0
        while i < self._cache_size:
            if len(line) == 0: break
            start_time, end_time, cpu, mem = line.split(',')
            measurements.append( (int(start_time), int(end_time), (float(cpu), float(mem))) )
            line = opened_file.readline()
            i = i+1
        opened_file.close()

        if len(measurements) < self._cache_size:
            measurements.insert(0, (self._config.environment.vm_status[vm_name].submit_time,
                                    self._config.environment.vm_status[vm_name].submit_time,
                                    (self._config.environment.vm_status[vm_name].submit_cpu_demand,
                                     self._config.environment.vm_status[vm_name].submit_mem_demand))
                                )

        self._cache[vm_name] = measurements

    def _open_file_of_vm(self, vm_name):
        path = re.sub('(\d+)-(\d+)', '\1/\1-\2.csv', vm_name)
        filepath = '{}/{}'.format(self._directory, path)
        opened_file = fileinput.input(filepath)
        return opened_file
