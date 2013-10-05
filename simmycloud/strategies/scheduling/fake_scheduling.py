
import SchedulingStrategy

class FakeScheduling(SchedulingStrategy):

    def schedule_vm(self, vm):
        print('schedule_vm(%s)'.format(vm.name))
