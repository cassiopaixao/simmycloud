
from strategies.migration.migration_strategy import MigrationStrategy

class FakeMigration(MigrationStrategy):

    def migrate_from_server_if_necessary(self, server):
        print('migrate_from_server_if_necessary({})'.format(server.name))

    def migrate_vm(self, vm):
        print('migrate_vm({})'.format(vm.name))
