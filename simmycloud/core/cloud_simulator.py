
from core.event import EventType

class CloudSimulator:

    def __init__(self, config):
        self._server_of_vm = dict()
        self._config = config
        self._logger = None

    def simulate(self):
        self._initialize()
        stats = self._config.statistics
        stats.start()
        while True:
            event = self._config.events_queue.next_event()
            if event is None:
                break
            self._process_event(event)
        stats.finish()

    def _initialize(self):
        self._config.initialize()
        self._logger = self._config.getLogger(self)

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


class CloudUtils:
    @staticmethod
    def is_overloaded(server):
        return (server.cpu_alloc > server.cpu or server.mem_alloc > server.mem)
