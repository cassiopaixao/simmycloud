
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
        path = re.sub('(\d+)-(\d+)', '/\1/\1-\2.csv', vm_name)
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

            opened_file.readline()
        opened_file.close()

        return measurements
