
from strategies.scheduling.scheduling_strategy import SchedulingStrategy

class FirstFit(SchedulingStrategy):

    @SchedulingStrategy.schedule_vm_statistics
    def schedule_vm(self, vm):
        server = self.get_first_fit(vm,
                                    self._config.environment.online_servers())

        if server is None:
            server = self.get_first_fit(vm,
                                        self._config.environment.offline_servers())
            if server is not None:
                self._config.environment.turn_on_server(server.name)

        if server is not None:
            self._config.environment.schedule_vm_at_server(vm, server.name)

        return server


    def get_first_fit(self, vm, servers):
        for server in servers:
            if server.cpu - server.cpu_alloc >= vm.cpu and \
               server.mem - server.mem_alloc >= vm.mem:
                return server
        return None
