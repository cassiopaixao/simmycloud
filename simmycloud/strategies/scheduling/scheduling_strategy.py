
from strategies.strategy import Strategy

class SchedulingStrategy(Strategy):

    def schedule_vm_statistics(func):
        def new_schedule_vm(self, *args, **kwargs):
            server = func(self, *args, **kwargs)
            if server is not None:
                self._config.getLogger(self).debug('VM {} was allocated to server {}'.format(args[0].name, server.name))
            else:
                self._config.statistics.notify_event('vms_not_allocated')
                self._config.getLogger(self).debug('VM {} was not allocated'.format(args[0].name))
            return server
        return new_schedule_vm

    @schedule_vm_statistics
    def schedule_vm(self, vm):
        raise NotImplementedError
