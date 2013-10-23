
import os
import fileinput
import re

from core.virtual_machine import VirtualMachine

class EventType:
    UNKNOWN = 0
    SUBMIT = 1
    UPDATE = 2
    FINISH = 3


class Event:

    def __init__(self, event_type, time=0, vm_name='', cpu=0.0, mem=0.0):
        self.type = event_type
        self.time = time
        self.vm = VirtualMachine(vm_name, cpu, mem)

    def dump(self):
        return '{}, {}, [{}]'.format(self.type,
                                     self.time,
                                     self.vm.dump()
                                     )


class EventBuilder:
    @staticmethod
    def build(csv_line):
        if csv_line == None or len(csv_line) == 0:
            return None
        data = csv_line.split(',')
        return Event(EventBuilder.get_event_type(int(data[5])),
                     int(data[0]),
                     '{}-{}'.format(data[2], data[3]),
                     float(data[9]),
                     float(data[10])
            )

    @staticmethod
    def get_event_type(source_value):
        if source_value == 0:
            return EventType.SUBMIT
        elif source_value in [1,7,8]:
            return EventType.UPDATE
        elif source_value in [2,3,4,5,6]:
            return EventType.FINISH
        return EventType.UNKNOWN

class EventQueue:

    def __init__(self):
        self._fileset = FileSetReader()

    def set_config(self, config):
        self._config = config
        self._fileset.set_config(config)

    def initialize(self):
        self._fileset.initialize()

    def next_event(self):
        line =  self._fileset.next_line()
        return EventBuilder.build(line)


class FileSetReader:
    def __init__(self):
        self._opened_file = None
        self._file_index = -1

    def set_config(self, config):
        self._config = config

    def initialize(self):
        directory = self._config.params['input_directory']
        self._files = os.listdir(directory)
        self._files[:] = [f for f in self._files if re.match('.*\.csv$', f) != None]
        self._files[:] = sorted(self._files)
        self._files[:] = [directory + '/' + f for f in self._files]

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
