
import MigrationStrategy

class FakeMigration(MigrationStrategy):

    def migrate_from_server_if_necessary(self, server):
        print('migrate_from_server_if_necessary(%s)'.format(server.name))

    def migrate_vm(self, vm):
        print('migrate_vm(%s)'.format(vm.name))
