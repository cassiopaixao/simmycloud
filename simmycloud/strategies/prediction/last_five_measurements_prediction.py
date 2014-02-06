
from core.virtual_machine import VirtualMachine
from core.strategies import PredictionStrategy

class LastFiveMeasurementsPrediction(PredictionStrategy):

    def initialize(self):
        self.measurement_reader = self._config.module['MeasurementReader']

    @PredictionStrategy.predict_strategy
    def predict(self, vm):
        last_five = self._get_last_five(vm)
        cpu_average = float(sum([m['cpu'] for m in last_five])) / len(last_five)
        mem_average = float(sum([m['mem'] for m in last_five])) / len(last_five)
        new_demands = VirtualMachine()
        new_demands.cpu = cpu_average
        new_demands.mem = mem_average
        return new_demands

    def _get_last_five(self, vm):
        measurements = self.measurement_reader.measurements_till(
            vm.name,
            self._config.simulation_info.current_timestamp)
        return measurements[-5:]
