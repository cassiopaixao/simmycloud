
from strategies.migration.migration_strategy import MigrationStrategy

class FakeMigration(MigrationStrategy):

    @MigrationStrategy.migrate_from_server_if_necessary_statistics
    def migrate_from_server_if_necessary(self, server):
        pass

    @MigrationStrategy.migrate_vm_statistics
    def migrate_vm(self, vm):
        pass
