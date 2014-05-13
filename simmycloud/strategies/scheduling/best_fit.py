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


from core.strategies import SchedulingStrategy

class BestFit(SchedulingStrategy):

    @SchedulingStrategy.schedule_vms_strategy
    def schedule_vms(self, vms):
        for vm in vms:
            self.schedule_vm(vm)

    def schedule_vm(self, vm):
        server = self.get_best_fit(vm,
                                   self._config.resource_manager.online_servers())

        if server is None:
            server = self.get_best_fit(vm,
                                       self._config.resource_manager.offline_servers())
            if server is not None:
                self._config.resource_manager.turn_on_server(server.name)

        if server is not None:
            self._config.resource_manager.schedule_vm_at_server(vm, server.name)

        return server


    def get_best_fit(self, vm, servers):
        best = None
        max_dot_product = -1
        for server in servers:
            if server.cpu - server.cpu_alloc >= vm.cpu and \
               server.mem - server.mem_alloc >= vm.mem:
                if self.dot_product(server, vm) > max_dot_product:
                    best = server
                    max_dot_product = self.dot_product(server, vm)
        return best

    def dot_product(self, server, vm):
        cpu_product = (server.cpu - server.cpu_alloc) * vm.cpu
        mem_product = (server.mem - server.mem_alloc) * vm.mem
        return cpu_product + mem_product
