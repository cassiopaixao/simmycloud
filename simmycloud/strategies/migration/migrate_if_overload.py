
from strategies.migration.migration_strategy import MigrationStrategy

class MigrateIfOverload(MigrationStrategy):

    @MigrationStrategy.migrate_from_server_if_necessary_statistics
    def migrate_from_server_if_necessary(self, server):
        if self.is_overloaded(server):
            vms = sorted(server.vm_list(), key=lambda vm: vm.cpu + vm.mem)
            for vm in vms:
                self.migrate_vm(vm)
                if not self.is_overloaded(server):
                    break


    @MigrationStrategy.migrate_vm_statistics
    def migrate_vm(self, vm):
        self._config.environment.free_vm_resources(vm)
        self._config.strategies.scheduling.schedule_vm(vm)

    def is_overloaded(self, server):
        return server.cpu_alloc > server.cpu or server.mem_alloc > server.mem
