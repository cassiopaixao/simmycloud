
from core.strategies import SchedulingStrategy

class FirstFit(SchedulingStrategy):

    @SchedulingStrategy.schedule_vm_strategy
    def schedule_vm(self, vm):
        server = self.get_first_fit(vm,
                                    self.__config__.environment.online_servers())

        if server is None:
            server = self.get_first_fit(vm,
                                        self.__config__.environment.offline_servers())
            if server is not None:
                self.__config__.environment.turn_on_server(server.name)

        if server is not None:
            self.__config__.environment.schedule_vm_at_server(vm, server.name)

        return server


    def get_first_fit(self, vm, servers):
        for server in servers:
            if server.cpu - server.cpu_alloc >= vm.cpu and \
               server.mem - server.mem_alloc >= vm.mem:
                return server
        return None
