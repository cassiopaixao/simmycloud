
from scheduling_strategy import SchedulingStrategy

class FakeScheduling(SchedulingStrategy):

    def schedule_vm(self, vm):
        print('schedule_vm({})'.format(vm.name))
