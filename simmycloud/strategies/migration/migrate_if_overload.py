###############################################################################
# The MIT License (MIT)
#
# Copyright (c) 2013 Cássio Paixão
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
###############################################################################


from core.strategies import MigrationStrategy

class MigrateIfOverload(MigrationStrategy):

    @MigrationStrategy.list_of_vms_to_migrate_strategy
    def list_of_vms_to_migrate(self, list_of_online_servers):
        vms_to_migrate = list()
        overloaded_servers = (s for s in self._config.resource_manager.online_servers() if s.is_overloaded())
        for server in overloaded_servers:
            vms = sorted(server.vm_list(), key=self.size_of_vm)
            cpu_exceeded = -server.cpu_free
            mem_exceeded = -server.mem_free
            for vm in vms:
                cpu_exceeded -= vm.cpu
                mem_exceeded -= vm.mem
                vms_to_migrate.append(vm)
                if cpu_exceeded <= 0 and mem_exceeded <= 0:
                    break
        return vms_to_migrate

    @MigrationStrategy.migrate_vms_strategy
    def migrate_vms(self, vms):
        self._config.strategies.scheduling.schedule_vms(vms)

    def size_of_vm(self, vm):
        return vm.cpu + vm.mem
