
from core.event import EventType

class CloudSimulator:

    def __init__(self, config):
        self._server_of_vm = dict()
        self._config = config


    def simulate(self):
        self._initialize()

        while(self._has_event()):
            self._process_event(self._next_event())


    def _initialize(self):
        print('cloud simulator initialized')


    def _has_event(self):
        return False


    def _process_event(self, event):
        strategies = self._config.strategies
        environment = self._config.environment

        if event.type == EventType.SUBMIT:
            strategies.scheduling.schedule_vm(event.vm)

        elif event.type == EventType.UPDATE:
            server = environment.get_server_of_vm(event.vm.name)
            server.update_vm(event.vm)
            strategies.migration.migrate_from_server_if_necessary(server)

        elif event.type == EventType.FINISH:
            server = environment.get_server_of_vm(event.vm.name)
            server.free_vm(event.vm.name)
            strategies.powering_off.power_off_if_necessary(server)

        else:
            #deu zica
            print('Unknown event: %s'.format(event.to_str))
