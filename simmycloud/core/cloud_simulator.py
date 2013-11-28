
from core.event import EventType, EventQueue
from core.vm_machine_state import InputFilter, InputVerifier

class CloudSimulator:

    def __init__(self, config):
        self._server_of_vm = dict()
        self._config = config
        self._logger = None
        self._event_queue = None

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
        input_verifier = InputVerifier()
        input_verifier.set_config(self._config)
        if input_verifier.is_valid():
            print("Input is valid\n")
        else:
            print("Input is invalid\n")
            input_verifier.print_statistics()

    def filter_input(self):
        input_filter = InputFilter()
        input_filter.set_config(self._config)
        input_filter.filter()

    def _initialize(self):
        self._config.initialize()
        self._logger = self._config.getLogger(self)
        self._event_queue = EventQueue()
        self._event_queue.set_config(self._config)
        self._event_queue.initialize()

    def _process_event(self, event):
        strategies = self._config.strategies

        if event.type == EventType.SUBMIT:
            self._config.statistics.add_to_counter('submit_events')
            strategies.scheduling.schedule_vm(event.vm)

        elif event.type == EventType.UPDATE:
            self._config.statistics.add_to_counter('update_events')
            self._config.environment.update_vm_demands(event.vm)
            server = self._config.environment.get_server_of_vm(event.vm.name)
            if server is not None:
                strategies.migration.migrate_from_server_if_necessary(server)

        elif event.type == EventType.FINISH:
            self._config.statistics.add_to_counter('finish_events')
            self._config.environment.free_vm_resources(event.vm)
            server = self._config.environment.get_server_of_vm(event.vm.name)
            if server is not None:
                strategies.powering_off.power_off_if_necessary(server)

        else:
            #deu zica
            print('Unknown event: %s'.format(event.dump()))
