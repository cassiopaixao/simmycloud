
from core.event import EventType, EventBuilder

class CloudSimulator:

    def __init__(self, config):
        self._server_of_vm = dict()
        self._config = config
        self._logger = None

    def simulate(self):
        self._initialize()
        while True:
            event = self._config.events_queue.next_event()
            if event is None:
                self._config.statistics.notify_event('simulation_finished',
                                                     timestamp=self._config.simulation_info.current_timestamp)
                break
            self._logger.debug('Processing event: %s', event.dump())
            self._process_event(event)

    def _initialize(self):
        self._config.initialize()
        self._logger = self._config.getLogger(self)
        self._add_prediction_time(int(self._config.params['first_prediction_time']))
        self._add_simulation_started_event()

    def _process_event(self, event):
        strategies = self._config.strategies
        self._update_simulation_info(event)

        if event.type == EventType.SUBMIT:
            self._config.statistics.notify_event('submit_events')
            self._config.environment.add_vm(event.vm, event.process_time)
            strategies.scheduling.schedule_vm(event.vm)

        elif event.type == EventType.TIME_TO_PREDICT:
            for server in self._config.environment.online_servers():
                for vm in server.vm_list():
                    prediction = strategies.prediction.predict(vm)
                    if prediction is not None:
                        self._config.events_queue.add_event(
                            EventBuilder.build_update_event(self._config.simulation_info.current_timestamp,
                                                            vm.name,
                                                            prediction.cpu,
                                                            prediction.mem))
            self._add_prediction_time(
                self._config.simulation_info.current_timestamp + strategies.prediction.next_prediction_interval())

        elif event.type == EventType.UPDATE:
            self._config.statistics.notify_event('update_events')
            self._config.environment.update_vm_demands(event.vm)

        elif event.type == EventType.UPDATES_FINISHED:
            should_migrate = strategies.migration.list_of_vms_to_migrate(
                self._config.environment.online_servers())
            for vm in should_migrate:
                self._config.environment.free_vm_resources(vm)
            strategies.migration.migrate_all(should_migrate)
            self._try_to_allocate_vms_in_pool()

        elif event.type == EventType.FINISH:
            if self._config.environment.is_it_time_to_finish_vm(event.vm):
                self._config.statistics.notify_event('finish_events')
                self._config.environment.free_vm_resources(event.vm)
                self._config.statistics.notify_event('vm_finished',
                                                     vm= event.vm)
            else:
                self._config.statistics.notify_event('outdated_finish_events')

        elif event.type == EventType.NOTIFY:
            self._config.statistics.notify_event(event.message)

        else:
            self._logger.error('Unknown event: %s'.format(event.dump()))
            raise Exception('Unknown event: %s'.format(event.dump()))

    def _add_prediction_time(self, timestamp):
        # TODO find a smarter way to identify finish of simulation
        if self._config.simulation_info.last_event is not None and \
            self._config.simulation_info.last_event.type == EventType.TIME_TO_PREDICT and  \
            len(self._config.environment.online_vms_names()) == 0:
            return

        self._config.events_queue.add_event(
            EventBuilder.build_time_to_predict_event(timestamp))

    def _add_simulation_started_event(self):
        self._config.events_queue.add_event(
            EventBuilder.build_notify_event(0, 'simulation_started'))

    def _update_simulation_info(self, event):
        self._config.simulation_info.last_event = self._config.simulation_info.current_event
        self._config.simulation_info.current_event = event
        self._config.simulation_info.current_timestamp = int(event.time)
        self._notify_if_timestamp_changed()

    def _notify_if_timestamp_changed(self):
        if self._config.simulation_info.last_event is not None and \
           self._config.simulation_info.current_timestamp > self._config.simulation_info.last_event.time:
            self._config.statistics.notify_event('timestamp_ended',
                                                 timestamp=self._config.simulation_info.last_event.time)
            self._config.statistics.notify_event('timestamp_starting',
                                                 timestamp=self._config.simulation_info.current_timestamp)

    def _try_to_allocate_vms_in_pool(self):
        for vm in self._config.vms_pool.get_ordered_list():
            server = self._config.strategies.scheduling.schedule_vm(vm)
            if server != None:
                self._config.vms_pool.remove(vm)


class SimulationInfo:
    def __init__(self):
        self.current_timestamp = 0
        self.current_event = None
        self.last_event = None
