
import Strategy

class MigrationStrategy(Strategy):

    def migrate_from_server_if_necessary(server):
        raise NotImplementedError

    def migrate_vm(vm):
        raise NotImplementedError
