
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


    def _initialize(self):
        self._event_queue = EventQueue(self._config.source_directory)
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
