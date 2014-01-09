
from core.strategies import SchedulingStrategy

class FakeScheduling(SchedulingStrategy):

    @SchedulingStrategy.schedule_vm_statistics
    def schedule_vm(self, vm):
        pass
