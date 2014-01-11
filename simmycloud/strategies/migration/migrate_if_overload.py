
from core.strategies import MigrationStrategy

class MigrateIfOverload(MigrationStrategy):

    @MigrationStrategy.migrate_from_server_if_necessary_strategy
    def migrate_from_server_if_necessary(self, server):
        if server.is_overloaded():
            vms = sorted(server.vm_list(), key=lambda vm: vm.cpu + vm.mem)
            for vm in vms:
                self.migrate_vm(vm)
                if not server.is_overloaded():
                    break


    @MigrationStrategy.migrate_vm_strategy
    def migrate_vm(self, vm):
        self._config.environment.free_vm_resources(vm)
        self._config.strategies.scheduling.schedule_vm(vm)
