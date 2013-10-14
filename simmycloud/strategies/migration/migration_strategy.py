
from ..strategy import Strategy

class MigrationStrategy(Strategy):

    def migrate_from_server_if_necessary(self, server):
        raise NotImplementedError

    def migrate_vm(self, vm):
        raise NotImplementedError
