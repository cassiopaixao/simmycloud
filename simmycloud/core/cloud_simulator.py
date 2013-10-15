
from core.event import EventType, EventBuilder
import os
import fileinput
import re

class CloudSimulator:

    def __init__(self, config):
        self._server_of_vm = dict()
        self._config = config

    def simulate(self):
        self._initialize()

        while True:
            event = self._event_queue.next_event()
            if event == None:
                break
            self._process_event(event)

    def verify_input(self):
        machine_state = VMMachineState(self._config.input_directory)
        machine_state.verify()

    def _initialize(self):
        self._event_queue = EventQueue(self._config.input_directory)
        self._event_queue.initialize()
        print('cloud simulator initialized')


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


class EventQueue:

    def __init__(self, source_directory):
        self._fileset = FileSetReader(source_directory)

    def initialize(self):
        self._fileset.initialize()

    def next_event(self):
        line =  self._fileset.next_line()
        return EventBuilder.build(line)


class FileSetReader:
    def __init__(self, directory):
        self._files = os.listdir(directory)
        self._files[:] = [f for f in self._files if re.match('.*\.csv$', f) != None]
        self._files[:] = sorted(self._files)
        self._files[:] = [directory + '/' + f for f in self._files]
        self._opened_file = None
        self._file_index = -1

    def initialize(self):
        self._load_next_file()

    def next_line(self):
        line = self._opened_file.readline()
        if len(line) == 0:
            self._load_next_file()
            if self._opened_file == None:
                return None
            line = self._opened_file.readline()
        return line

    def _load_next_file(self):
        if self._opened_file != None:
            self._opened_file.close()
        self._file_index += 1
        if self._file_index < len(self._files):
            self._opened_file = fileinput.input(self._files[self._file_index])
        else:
            self._opened_file = None


class VMMachineState:

    UNKNOWN = 0
    RUNNING = 1
    DEAD = 2

    def __init__(self, input_directory):
        self._vms = dict()
        self._event_queue = EventQueue(input_directory)

    def verify(self):
        self._event_queue.initialize()

        while True:
            event = self._event_queue.next_event()
            if event == None:
                break
            self._process_event(event)

        self._statistics()

    def _process_event(self, event):
        new_state = VMMachineState.UNKNOWN
        current_state = self._vms.get(event.vm.name)

        if current_state == VMMachineState.RUNNING:
            if event.type == EventType.UPDATE:
                new_state = VMMachineState.RUNNING
            elif event.type == EventType.FINISH:
                new_state = VMMachineState.DEAD

        elif current_state == VMMachineState.DEAD:
            if event.type == EventType.SUBMIT:
                new_state = VMMachineState.RUNNING

        elif current_state == None:
            if event.type == EventType.SUBMIT:
                new_state = VMMachineState.RUNNING

        self._vms[event.vm.name] = new_state

    def _statistics(self):
        finished = 0
        running = []
        unknown = []

        for vm_name, state in self._vms.items():
            if state == VMMachineState.DEAD:
                finished += 1

            elif state == VMMachineState.RUNNING:
                running.append(vm_name)

            elif state == VMMachineState.UNKNOWN:
                unknown.append(vm_name)

        print('{} {} {}'.format(finished, len(running), len(unknown)))
        print(' '.join(running))
        print(' '.join(unknown))

        if len(unknown) > 0:
            raise Exception('There are tasks in unknown states.')
