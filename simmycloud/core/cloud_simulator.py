###############################################################################
# The MIT License (MIT)
#
# Copyright (c) 2013 Cassio Paixao
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
        self._config = config
        self._logger = None

    def simulate(self):
        self._initialize()
        while True:
            events = self._config.events_queue.next_events()
            if events is None:
                self._config.statistics.notify_event('simulation_finished',
                                                     timestamp=self._config.simulation_info.current_timestamp)
                break
            self._update_simulation_info(events[0])
            self._logger.debug('Processing %d events of type: %s', len(events), EventType.get_type(events[0].type))
            for event in events:
                self._logger.debug('Will process event: %s', event.dump())

            self._config.simulation_info.scope.append(events[0].type)
            self._logger.debug('Adding %s to scope.', EventType.get_type(events[0].type))
            self._process_events(events)
            removed_from_scope = self._config.simulation_info.scope.pop()
            self._logger.debug('Removing %s from scope.', EventType.get_type(removed_from_scope))

    def _initialize(self):
        self._config.initialize()
        self._logger = self._config.getLogger(self)
        self._add_prediction_time(int(self._config.params['first_prediction_time']))
        self._add_vms_pool_verification_time(int(self._config.params['vms_pool_first_verification']))
        self._vms_pool_verification_interval = int(self._config.params['vms_pool_verification_interval'])
        self._add_punish_vms_time(int(self._config.params['vms_punishment_first_time']))
        self._punish_vms_interval = int(self._config.params['vms_punishment_interval'])
        self._add_simulation_started_event()

    def _process_events(self, events):
        strategies = self._config.strategies
        events_type = events[0].type

        if events_type in [EventType.TIME_TO_PREDICT, EventType.UPDATES_FINISHED, EventType.VERIFY_VMS_POOL, EventType.PUNISH_VMS]:
            if len(events) > 1:
                self._logger.warning('There should be only one event of type %s. %d found.',
                                     EventType.get_type(events[0].type), len(events))
            self._process_event(events[0])

        elif events_type == EventType.SUBMIT:
            self._config.statistics.notify_event('submit_events', len(events))
            vms = []
            for event in events:
                self._config.resource_manager.add_vm(event.vm, event.process_time)
                vms.append(event.vm)
            strategies.scheduling.schedule_vms(vms)

        elif events_type == EventType.UPDATE:
            self._config.statistics.notify_event('update_events', len(events))
            for event in events:
                self._config.resource_manager.update_vm_demands(event.vm)

        elif events_type == EventType.FINISH:
            updated_servers = []
            for event in events:
                if self._config.resource_manager.is_it_time_to_finish_vm(event.vm):
                    updated_servers.append(self._config.resource_manager.get_server_of_vm(event.vm.name))
                    self._config.statistics.notify_event('finish_events')
                    self._config.resource_manager.free_vm_resources(event.vm)
                    self._config.statistics.notify_event('vm_finished',
                                                         vm= event.vm)
                    # this should be removed. A global observer/notifier can be used
                    if 'MeasurementReader' in self._config.module:
                        self._config.module['MeasurementReader'].free_cache_for_vm(event.vm.name)
                else:
                    self._config.statistics.notify_event('outdated_finish_events')
            self._verify_machines_to_turn_off(set(updated_servers))

        elif events_type == EventType.NOTIFY:
            for event in events:
                self._config.statistics.notify_event(event.message)

        else:
            self._logger.error('Unknown event: %s'.format(event.dump()))
            raise Exception('Unknown event: %s'.format(event.dump()))

        if self._does_simulation_finished():
            self._verify_machines_to_turn_off()
            self._config.events_queue.clear()

    def _process_event(self, event):
        strategies = self._config.strategies

        if event.type == EventType.TIME_TO_PREDICT:
            for server in self._config.resource_manager.online_servers():
                for vm in server.vm_list():
                    prediction = strategies.prediction.predict(vm.name)
                    if prediction is not None:
                        self._config.events_queue.add_event(
                            EventBuilder.build_update_event(self._config.simulation_info.current_timestamp,
                                                            vm.name,
                                                            prediction[0],
                                                            prediction[1]))
            self._add_prediction_time(
                self._config.simulation_info.current_timestamp + strategies.prediction.next_prediction_interval())

        elif event.type == EventType.UPDATES_FINISHED:
            self._config.simulation_info.scope.append('migration')
            should_migrate = strategies.migration.list_of_vms_to_migrate(
                self._config.resource_manager.online_servers())
            for vm in should_migrate:
                self._config.resource_manager.free_vm_resources(vm)
            strategies.migration.migrate_vms(should_migrate)
            self._config.simulation_info.scope.pop()
            self._verify_machines_to_turn_off()

        elif event.type == EventType.VERIFY_VMS_POOL:
            self._try_to_allocate_vms_in_pool()

        elif event.type == EventType.PUNISH_VMS:
            self._punish_performance_of_vms_at_overloaded_servers()

        else:
            self._logger.error('Unknown event: %s'.format(event.dump()))
            raise Exception('Unknown event: %s'.format(event.dump()))


    def _punish_performance_of_vms_at_overloaded_servers(self):
        measurement_reader = self._config.module['MeasurementReader']
        overloaded_servers = measurement_reader.overloaded_servers()
        for server in overloaded_servers:
            cpu_demmand = mem_demmand = 0.0
            for vm in server.vm_list():
                measurement = measurement_reader.current_measurement(vm.name)
                cpu_demmand += measurement[measurement_reader.CPU]
                mem_demmand += measurement[measurement_reader.MEM]
            cpu_exceeded = max(0.0, cpu_demmand - server.cpu)
            mem_exceeded = max(0.0, mem_demmand - server.mem)
            time_punish_factor = min(1.0, cpu_exceeded + mem_exceeded) # in [0.0, 1.0]
            time_punish = int(self._vms_pool_verification_interval * time_punish_factor)
            self._logger.debug('Will punish %d VMs of server %s. Time punish factor: %.4f; Time punish: %d',
                               len(server.vm_list()), server.name, time_punish_factor, time_punish)
            for vm in server.vm_list():
                self._config.resource_manager.add_processing_time_to_vm(vm, time_punish)
            self._config.statistics.notify_event('processing_time_added_to_vms', time_punish*len(server.vm_list()))

        self._add_punish_vms_time(self._config.simulation_info.current_timestamp + self._punish_vms_interval)

    def _does_simulation_finished(self):
        if len(self._config.resource_manager.online_vms_names()) == 0 and \
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
        if event.time > self._config.simulation_info.current_timestamp:
            self._notify_timestamp_change(self._config.simulation_info.current_timestamp, event.time)
        self._config.simulation_info.current_timestamp = event.time

    def _notify_timestamp_change(self, last_timestamp, new_timestamp):
        self._config.statistics.notify_event('timestamp_ended',    timestamp=last_timestamp)
        self._config.statistics.notify_event('timestamp_starting', timestamp=new_timestamp )
        self._logger.info('Timestamp start: %d', new_timestamp)

    def _try_to_allocate_vms_in_pool(self):
        vms = list(self._config.vms_pool.get_ordered_list())
        self._config.strategies.scheduling.schedule_vms(self._config.vms_pool.get_high_priority_vms())
        self._config.strategies.scheduling.schedule_vms(self._config.vms_pool.get_low_priority_vms())
        for vm in vms:
            if self._config.resource_manager.get_server_of_vm(vm.name) is not None:
                self._config.vms_pool.remove(vm)

        self._add_vms_pool_verification_time(self._config.simulation_info.current_timestamp + self._vms_pool_verification_interval)

    def _add_vms_pool_verification_time(self, timestamp):
        self._config.events_queue.add_event(EventBuilder.build_verify_vms_pool_event(timestamp))

    def _add_punish_vms_time(self, timestamp):
        self._config.events_queue.add_event(EventBuilder.build_punish_vms_event(timestamp))

    def _verify_machines_to_turn_off(self, servers=None):
        self._config.strategies.powering_off.power_off_if_necessary(servers)


class SimulationInfo:
    def __init__(self):
        self.current_timestamp = 0
        self.scope = []
