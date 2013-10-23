
from strategies.migration.migration_strategy import MigrationStrategy

class FakeMigration(MigrationStrategy):

    def migrate_from_server_if_necessary(self, server):
        pass

    def migrate_vm(self, vm):
        pass
