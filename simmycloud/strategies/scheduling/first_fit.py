
from strategies.scheduling.scheduling_strategy import SchedulingStrategy

class FirstFit(SchedulingStrategy):

    def schedule_vm(self, vm):
        servers = self._config.environment.online_servers()
        for server in servers:
            if server.cpu - server.cpu_alloc >= vm.cpu and \
               server.mem - server.mem_alloc >= vm.mem:
                self._config.environment.schedule_vm_at_server(vm, server.name)
                return server

        offline_servers = self._config.environment.offline_servers()
        for server in offline_servers:
            if server.cpu - server.cpu_alloc >= vm.cpu and \
               server.mem - server.mem_alloc >= vm.mem:
                self._config.environment.turn_on_server(server.name)
                self._config.environment.schedule_vm_at_server(vm, server.name)
                return server

        return None
