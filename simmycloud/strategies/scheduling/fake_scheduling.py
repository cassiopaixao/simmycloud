
from core.strategies import SchedulingStrategy

class FakeScheduling(SchedulingStrategy):

    @SchedulingStrategy.schedule_vm_strategy
    def schedule_vm(self, vm):
        pass
