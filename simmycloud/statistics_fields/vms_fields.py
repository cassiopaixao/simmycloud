

from core.statistics_manager import StatisticsField


class VMNameField(StatisticsField):
    def value(self, vm):
        return vm.name

class VMStretchField(StatisticsField):
    def value(self, vm):
        current_timestamp = self._config.simulation_info.current_timestamp
        submit_timestamp = self._config.environment.get_vm_allocation_data(vm.name).submit_time
        requested_processing_time = self._config.environment.get_vm_allocation_data(vm.name).process_time
        return float(current_timestamp - submit_timestamp) / requested_processing_time
