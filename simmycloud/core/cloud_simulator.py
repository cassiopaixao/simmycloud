
from core.event import EventType, EventQueue
from core.vm_machine_state import VMMachineState

class CloudSimulator:

    def __init__(self, config):
        self._server_of_vm = dict()
        self._config = config

    def simulate(self):
        self._initialize()
        stats = self._config.statistics
        stats.start()
        while True:
            event = self._event_queue.next_event()
            if event is None:
                break
            self._process_event(event)
            while event.time > stats.next_control_time:
                stats.persist()
        stats.finish()

    def verify_input(self):
        machine_state = VMMachineState()
        machine_state.set_config(self._config)
        machine_state.verify()

    def _initialize(self):
        self._event_queue = EventQueue()
        self._event_queue.set_config(self._config)
        self._event_queue.initialize()
        self._config.initialize_all()

    def _process_event(self, event):
        strategies = self._config.strategies
        environment = self._config.environment

        if event.type == EventType.SUBMIT:
            self._config.statistics.add_to_counter('submit_events')
            strategies.scheduling.schedule_vm(event.vm)

        elif event.type == EventType.UPDATE:
            self._config.statistics.add_to_counter('update_events')
            server = environment.get_server_of_vm(event.vm.name)
            server.update_vm(event.vm)
            strategies.migration.migrate_from_server_if_necessary(server)

        elif event.type == EventType.FINISH:
            self._config.statistics.add_to_counter('finish_events')
            server = environment.get_server_of_vm(event.vm.name)
            server.free_vm(event.vm.name)
            strategies.powering_off.power_off_if_necessary(server)

        else:
            #deu zica
            print('Unknown event: %s'.format(event.dump()))
