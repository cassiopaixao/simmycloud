
from strategies.strategy import Strategy

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
