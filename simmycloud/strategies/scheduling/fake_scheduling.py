
from strategies.scheduling.scheduling_strategy import SchedulingStrategy

class FakeScheduling(SchedulingStrategy):

    @SchedulingStrategy.schedule_vm_statistics
    def schedule_vm(self, vm):
        pass
