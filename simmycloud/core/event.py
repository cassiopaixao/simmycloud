
import os
import fileinput
import re

from core.virtual_machine import VirtualMachine

class EventType:
    UNKNOWN = 0
    SUBMIT = 1
    UPDATE_PENDING = 2
    SCHEDULE = 3
    UPDATE_RUNNING = 4
    FINISH = 5

    @staticmethod
    def get_type(type_number):
        if type_number in range(1,6):
            types = ['', 'SUBMIT', 'UPDATE_PENDING', 'SCHEDULE', 'UPDATE_RUNNING', 'FINISH']
            return types[type_number]
        return 'UNKNOWN'


class Event:

    def __init__(self, event_type, time=0, vm_name='', cpu=0.0, mem=0.0):
        self.type = event_type
        self.time = time
        self.vm = VirtualMachine(vm_name, cpu, mem)

    def dump(self):
        return '{}, {}, [{}]'.format(EventType.get_type(self.type),
                                     self.time,
                                     self.vm.dump()
                                     )


class EventBuilder:

    @staticmethod
    def build(csv_line):
        if csv_line == None or len(csv_line) == 0:
            return None
        data = csv_line.split(',')
        # ignores 'missing event' records
        event_type = EventBuilder.get_event_type(int(data[5])) if not data[1] \
                                                               else EventType.UNKNOWN
        event = Event(event_type,
                      int(data[0] if data[0] else 0),
                      '{}-{}'.format(data[2], data[3]),
                      float(data[9] if data[9] else 0),
                      float(data[10] if data[10] else 0)
            )
        return event

    @staticmethod
    def get_event_type(source_value):
        if source_value == 0:
            return EventType.SUBMIT
        elif source_value == 1:
            return EventType.SCHEDULE
        elif source_value in [2,3,4,5,6]:
            return EventType.FINISH
        elif source_value == 7:
            return EventType.UPDATE_PENDING
        elif source_value == 8:
            return EventType.UPDATE_RUNNING
        return EventType.UNKNOWN


class EventQueue:

    def __init__(self):
        self._clear()

    def _clear(self):
        self._fileset = FileSetReader()
        self._event = None
        self._loggger = None

    def set_config(self, config):
        self._config = config
        self._fileset.set_config(config)

    def initialize(self):
        self._fileset.initialize()
        self._logger = self._config.getLogger(self)

    def next_event(self):
        self._line =  self._fileset.next_line()
        event = EventBuilder.build(self._line)
        self._logger.debug('Event built: {}'.format(event.dump() if event is not None
                                                                 else 'none'))
        return event

    def current_event(self):
        return self._event

    def current_line(self):
        return self._fileset.current_line()


class FileSetReader:
    def __init__(self):
        self._clear()

    def _clear(self):
        self._opened_file = None
        self._file_index = -1
        self._line = None
        self._logger = None

    def set_config(self, config):
        self._config = config

    def initialize(self):
        self._clear()
        directory = self._config.params['input_directory']
        self._files = os.listdir(directory)
        self._files[:] = [f for f in self._files if re.match('.*\.csv$', f) != None]
        self._files[:] = sorted(self._files)
        self._files[:] = [directory + '/' + f for f in self._files]
        self._logger = self._config.getLogger(self)

        self._load_next_file()

    def next_line(self):
        self._line = self._opened_file.readline()
        if len(self._line) == 0:
            self._load_next_file()
            if self._opened_file == None:
                return None
            self._line = self._opened_file.readline()
        # self._logger.debug('Line read: %s', self._line.strip())
        return self._line

    def current_line(self):
        return self._line

    def _load_next_file(self):
        if self._opened_file != None:
            self._logger.debug('Closing file: %s', self._opened_file._filename)
            self._opened_file.close()
        self._file_index += 1
        if self._file_index < len(self._files):
            self._logger.debug('Opening file: %s', self._files[self._file_index])
            self._opened_file = fileinput.input(self._files[self._file_index])
        else:
            self._logger.debug('No more files to open.')
            self._opened_file = None
