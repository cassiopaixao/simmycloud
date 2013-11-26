
from strategies.strategy import Strategy

class SchedulingStrategy(Strategy):

    def schedule_vm_statistics(func):
        def new_schedule_vm(self, *args, **kwargs):
            res = func(self, *args, **kwargs)
            if res is None:
                self._config.statistics.add_to_counter('vms_not_allocated')
                self._config.getLogger(self).debug('VM {} was not allocated'.format(args[0].name))
            self._config.getLogger(self).debug('VM {} was allocated to server {}'.format(args[0].name, res.name))
            return res
        return new_schedule_vm

    @schedule_vm_statistics
    def schedule_vm(self, vm):
        raise NotImplementedError
