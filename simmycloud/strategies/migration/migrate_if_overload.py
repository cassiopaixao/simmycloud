
from core.strategies import MigrationStrategy
from core.cloud_simulator import CloudUtils

class MigrateIfOverload(MigrationStrategy):

    @MigrationStrategy.migrate_from_server_if_necessary_strategy
    def migrate_from_server_if_necessary(self, server):
        if CloudUtils.is_overloaded(server):
            vms = sorted(server.vm_list(), key=lambda vm: vm.cpu + vm.mem)
            for vm in vms:
                self.migrate_vm(vm)
                if not CloudUtils.is_overloaded(server):
                    break


    @MigrationStrategy.migrate_vm_strategy
    def migrate_vm(self, vm):
        self.__config__.environment.free_vm_resources(vm)
        self.__config__.strategies.scheduling.schedule_vm(vm)
