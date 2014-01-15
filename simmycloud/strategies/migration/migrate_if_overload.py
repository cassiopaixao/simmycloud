
from core.strategies import MigrationStrategy

class MigrateIfOverload(MigrationStrategy):

    @MigrationStrategy.list_of_vms_to_migrate_strategy
    def list_of_vms_to_migrate(self, list_of_online_servers):
        vms_to_migrate = list()
        for server in list_of_online_servers:
            if server.is_overloaded():
                vms = sorted(server.vm_list(), key=lambda vm: vm.cpu + vm.mem)
                cpu_exceeded = server.cpu_alloc - server.cpu
                mem_exceeded = server.mem_alloc - server.mem
                for vm in vms:
                    cpu_exceeded = cpu_exceeded - vm.cpu
                    mem_exceeded = mem_exceeded - vm.mem
                    vms_to_migrate.append(vm)
                    if cpu_exceeded <= 0 and mem_exceeded <= 0:
                        break
        return vms_to_migrate

    @MigrationStrategy.migrate_vm_strategy
    def migrate_vm(self, vm):
        self._config.strategies.scheduling.schedule_vm(vm)
