
from core.server import Server
from core.event import EventBuilder

class Environment:

    def __init__(self, environment_builder):
        self._builder = environment_builder
        self._online_servers = {}
        self._offline_servers = {}
        self._vm_hosts = {}
        self._vm_status = {}
        self._logger = None
        self._config = None

    def _add_finish_event(self, vm):
        finish_at = self._current_timestamp() + self._vm_status[vm.name].remaining_time
        self._vm_status[vm.name].last_finish_time = finish_at
        self._config.events_queue.add_event(
                EventBuilder.build_finish_event(finish_at,
                                                vm.name))

    def _update_vm_status_freeing_resources(self, vm):
        vm_status = self._vm_status[vm.name]
        vm_status.remaining_time = vm_status.last_finish_time - self._current_timestamp()
        vm_status.last_finish_time = None

    def _current_timestamp(self):
        return self._config.simulation_info.current_timestamp

    def set_config(self, config):
        self._config = config

    def initialize(self):
        self._logger = self._config.getLogger(self)
        self._logger.info('EnvironmentBuilder: {}'.format(self._builder.__class__.__name__))
        self._builder.build(self)

    def add_servers_of_type(self, server, quantity=1):
        self._logger.debug('Adding {} server(s) of type {}'.format(quantity, server.describe()))
        servers_count = len(self._online_servers) + len(self._offline_servers) + 1
        while quantity > 0:
            self._offline_servers[str(servers_count)] = Server(str(servers_count),
                                                               server.cpu,
                                                               server.mem
                                                               )
            quantity -= 1
            servers_count += 1

    def turn_on_server(self, server_name):
        self._config.statistics.notify_event('servers_turned_on')
        self._logger.debug('Server {} being turned on'.format(server_name))
        server = self._offline_servers.pop(server_name)
        self._online_servers[server_name] = server
        return server

    def turn_off_server(self, server_name):
        self._config.statistics.notify_event('servers_turned_off')
        self._logger.debug('Server {} being turned off'.format(server_name))
        server = self._online_servers.pop(server_name)
        self._offline_servers[server_name] = server

    def schedule_vm_at_server(self, vm, server_name):
        self._logger.debug('Allocating VM {} to server {}'.format(vm.dump(), server_name))
        self._online_servers[server_name].schedule_vm(vm)
        self._vm_hosts[vm.name] = server_name
        self._add_finish_event(vm)

    def update_vm_demands(self, vm):
        server = self.get_server_of_vm(vm.name)
        if server is not None:
            self._logger.debug('Updating VM demands: {}'.format(vm.dump()))
            server.update_vm(vm)
        else:
            self._logger.info('Tried to update VM but not found: {}'.format(vm.dump()))

    def free_vm_resources(self, vm):
        server = self.get_server_of_vm(vm.name)
        if server is not None:
            self._logger.debug('Freeing VM resources from server {}: {}'.format(server.describe(), vm.dump()))
            self._vm_hosts.pop(vm.name)
            self._update_vm_status_freeing_resources(vm)
            server.free_vm(vm)
        else:
            self._logger.info('Tried to free VM but not found: {}'.format(vm.dump()))

    def get_server_of_vm(self, vm_name):
        server_name = self._vm_hosts.get(vm_name)
        return self._online_servers[server_name] if server_name is not None \
                                                 else None

    def add_vm(self, vm, process_time):
        self._vm_status[vm.name] = _VirtualMachineStatus(vm.name,
                                                         self._current_timestamp(),
                                                         process_time)

    def is_it_time_to_finish_vm(self, vm):
        return self._current_timestamp() == self._vm_status[vm.name].last_finish_time

    def online_servers(self):
        return self._online_servers.values()

    def offline_servers(self):
        return self._offline_servers.values()

    def all_servers(self):
        return self._online_servers.values() + self._online_servers.values()


class EnvironmentBuilder:

    @staticmethod
    def build():
        raise NotImplementedError


class _VirtualMachineStatus:
    def __init__(self, vm_name='', submit_time=0, process_time=0):
        self.vm_name = vm_name
        self.submit_time = submit_time
        self.process_time = process_time
        self.remaining_time = process_time
        self.last_finish_time = None
