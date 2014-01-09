
from core.strategies import MigrationStrategy

class FakeMigration(MigrationStrategy):

    @MigrationStrategy.migrate_from_server_if_necessary_strategy
    def migrate_from_server_if_necessary(self, server):
        pass

    @MigrationStrategy.migrate_vm_strategy
    def migrate_vm(self, vm):
        pass
