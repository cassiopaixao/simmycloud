
from core.event import EventType, SubmitEventsQueue, UpdateEventsQueue, FinishEventsQueue
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
            # change:
            while event.time > stats.next_control_time:
                stats.persist()
            # to:
            # stats.notify('time', timestamp)
            # but only when one timestamp has been passed (it should be verified
            # before the event processing)
            # end
        stats.finish()

    # deprecated
    def verify_input(self):
        input_verifier = InputVerifier()
        input_verifier.set_config(self._config)
        if input_verifier.is_valid():
            print("Input is valid\n")
        else:
            print("Input is invalid\n")
        input_verifier.print_statistics()

    # deprecated
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
        server = None

        if event.type == EventType.SCHEDULE:
            self._config.statistics.notify_event('submit_events')
            server = strategies.scheduling.schedule_vm(event.vm)

        elif event.type == EventType.UPDATE_RUNNING:
            self._config.statistics.notify_event('update_events')
            self._config.environment.update_vm_demands(event.vm)
            server = self._config.environment.get_server_of_vm(event.vm.name)
            if server is not None:
                strategies.migration.migrate_from_server_if_necessary(server)
                strategies.powering_off.power_off_if_necessary(server)

        elif event.type == EventType.FINISH:
            self._config.statistics.notify_event('finish_events')
            self._config.environment.free_vm_resources(event.vm)
            server = self._config.environment.get_server_of_vm(event.vm.name)
            if server is not None:
                strategies.powering_off.power_off_if_necessary(server)

        elif event.type in [EventType.SUBMIT, EventType.UPDATE_PENDING]:
            pass

        else:
            self._logger.error('Unknown event: %s'.format(event.dump()))
            raise Exception('Unknown event: %s'.format(event.dump()))

        if server is not None and CloudUtils.is_overloaded(server):
            self._logger.error('Server is overloaded: %s', server.dump())
            for vm in server.vm_list():
                self._logger.error('VM in server %s: %s', server.name, vm.dump())
            raise Exception('Server is overloaded: %s', server.dump())


class EventQueue:
    def __init__(self):
        self._clear()

    def _clear(self):
        self.__submit_events__ = SubmitEventsQueue()
        self.__update_events__ = UpdateEventsQueue()
        self.__finish_events__ = FinishEventsQueue()
        # self.__last_timestamp__ = 0
        self.__submit_event_in_queue__ = None
        self.__logger__ = None

    def set_config(self, config):
        self.__config__ = config
        self.__submit_events__.set_config(config)

    def initialize(self):
        self.__submit_events__.initialize()
        self.__logger__ = self.__config__.getLogger(self)

    # 1º update event
    # 2º finish event
    # 3º submit event
    def next_event(self):
        event = None
        if len(self.__update_events__) > 0:
            event = self.__update_events__.pop()
        else:
            if self.__submit_event_in_queue__ is not None:
                event = self.__submit_event_in_queue__
            else:
                event = self.__submit_events__.next_event()
                self.__submit_event_in_queue__ = event

            if event is not None:
                finish_event = self.__finish_events__.first_before_or_equal(event.timestamp)
            else:
                finish_event = self.__finish_events__.first()

            if finish_event is not None:
                event = finish_event
            else:
                self.__submit_event_in_queue__ = None

        self.__logger__.debug('Event to process: %s', (event.dump() if event is not None
                                                                    else 'none'))
        return event


class CloudUtils:
    @staticmethod
    def is_overloaded(server):
        return (server.cpu_alloc > server.cpu or server.mem_alloc > server.mem)
