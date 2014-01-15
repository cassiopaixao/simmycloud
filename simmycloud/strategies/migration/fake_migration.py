
from core.strategies import MigrationStrategy

class FakeMigration(MigrationStrategy):

    @MigrationStrategy.list_of_vms_to_migrate_strategy
    def list_of_vms_to_migrate(self, list_of_online_servers):
        return []

    @MigrationStrategy.migrate_vm_strategy
    def migrate_vm(self, vm):
        pass
