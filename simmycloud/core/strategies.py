
class Strategy:
    def set_config(self, config):
        self._config = config

    """ This method will be called before the simulation starts.
        Override this method if you want to configure something in your
        strategy.
        The self._config object will already be available. """
    def initialize(self):
        None


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


class MigrationStrategy(Strategy):

    def migrate_from_server_if_necessary_statistics(func):
        def new_migrate_vm(self, *args, **kwargs):
            return func(self, *args, **kwargs)
        return new_migrate_vm

    def migrate_vm_statistics(func):
        def new_migrate_vm(self, *args, **kwargs):
            old_server = self._config.environment.get_server_of_vm(args[0].name)
            res = func(self, *args, **kwargs)
            new_server = self._config.environment.get_server_of_vm(args[0].name)
            if old_server != new_server:
                self._config.statistics.notify_event('vms_migrated')
            if new_server is None:
                self._config.statistics.notify_event('couldnot_reallocate')
            return res
        return new_migrate_vm

    @migrate_from_server_if_necessary_statistics
    def migrate_from_server_if_necessary(self, server):
        raise NotImplementedError

    @migrate_vm_statistics
    def migrate_vm(self, vm):
        raise NotImplementedError


class PoweringOffStrategy(Strategy):

    def power_off_if_necessary_statistics(func):
        def new_power_off_if_necessary(self, *args, **kwargs):
            res = func(self, *args, **kwargs)
            if res is None:
                self._config.statistics.notify_event('servers_turned_off')
            return res
        return new_power_off_if_necessary

    @power_off_if_necessary_statistics
    def power_off_if_necessary(self, server):
        raise NotImplementedError
