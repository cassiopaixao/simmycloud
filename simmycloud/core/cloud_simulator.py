
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
            self._logger.debug('Processing event: %s', event.dump())
            self._process_event(event)
        stats.finish()

    def _initialize(self):
        self._config.initialize()
        self._logger = self._config.getLogger(self)

    def _process_event(self, event):
        strategies = self._config.strategies
        self._config.simulation_info.current_event = event
        self._config.simulation_info.current_timestamp = int(event.time)

        if event.type == EventType.SUBMIT:
            self._config.statistics.notify_event('submit_events')
            self._config.environment.add_vm(event.vm, event.process_time)
            strategies.scheduling.schedule_vm(event.vm)

        elif event.type == EventType.UPDATE:
            self._config.statistics.notify_event('update_events')
            self._config.environment.update_vm_demands(event.vm)

        elif event.type == EventType.UPDATES_FINISHED:
            should_migrate = strategies.migration.list_of_vms_to_migrate(
                self._config.environment.online_servers())
            for vm in should_migrate:
                self._config.environment.free_vm_resources(vm)
            strategies.migration.migrate_all(should_migrate)

        elif event.type == EventType.FINISH:
            if self._config.environment.is_it_time_to_finish_vm(event.vm):
                self._config.statistics.notify_event('finish_events')
                self._config.environment.free_vm_resources(event.vm)
            else:
                self._config.statistics.notify_event('outdated_finish_events')

        elif event.type == EventType.NOTIFY:
            # TODO implement this case.
            pass

        else:
            self._logger.error('Unknown event: %s'.format(event.dump()))
            raise Exception('Unknown event: %s'.format(event.dump()))


class SimulationInfo:
    def __init__(self):
        self.current_timestamp = 0
        self.current_event = None
