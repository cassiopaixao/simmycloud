###############################################################################
# The MIT License (MIT)
#
# Copyright (c) 2013 Cássio Paixão
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
###############################################################################


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
            self._update_simulation_info(event)
            self._logger.debug('Processing event: %s', event.dump())
            self._process_event(event)

    def _initialize(self):
        self._config.initialize()
        self._logger = self._config.getLogger(self)
        self._add_prediction_time(int(self._config.params['first_prediction_time']))
        self._add_simulation_started_event()

    def _process_event(self, event):
        strategies = self._config.strategies

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
            self._verify_machines_to_turn_off()

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

        if self._does_simulation_finished():
            self._verify_machines_to_turn_off()
            self._config.events_queue.clear()

    def _does_simulation_finished(self):
        if len(self._config.environment.online_vms_names()) == 0 and \
            len(self._config.vms_pool) == 0 and \
            not self._config.events_queue.has_submit_events():
            return True
        return False

    def _add_prediction_time(self, timestamp):
        self._config.events_queue.add_event(
            EventBuilder.build_time_to_predict_event(timestamp))
        self._config.events_queue.add_event(
            EventBuilder.build_updates_finished_event(timestamp))

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

    def _verify_machines_to_turn_off(self):
        online_servers = list(self._config.environment.online_servers())
        for i in range(len(online_servers)):
            self._config.strategies.powering_off.power_off_if_necessary(online_servers[i])


class SimulationInfo:
    def __init__(self):
        self.current_timestamp = 0
        self.current_event = None
        self.last_event = None
