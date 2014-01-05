
import os
import fileinput
import re
import heapq
from collections import defaultdict

from core.virtual_machine import VirtualMachine

class EventType:
    UNKNOWN = 0
    SUBMIT = 1
    SCHEDULE = 2
    UPDATE = 3
    FINISH = 4
    NOTIFY = 5

    @staticmethod
    def get_type(type_number):
        if type_number in range(1,6):
            types = ['', 'SUBMIT', 'SCHEDULE', 'UPDATE', 'FINISH', 'NOTIFY']
            return types[type_number]
        return 'UNKNOWN'


class Event:

    def __init__(self, event_type, time=0, vm_name='', cpu=0.0, mem=0.0, process_time=0):
        self.type = event_type
        self.time = time
        self.process_time = process_time
        self.vm = VirtualMachine(vm_name, cpu, mem)

    def dump(self):
        return '{}, {}, [[{}], {}]'.format(EventType.get_type(self.type),
                                           self.time,
                                           self.vm.dump(),
                                           self.process_time
                                           )


class EventBuilder:

    @staticmethod
    def build_submit_event(csv_line):
        if csv_line == None or len(csv_line) == 0:
            return None

        # timestamp,vm_name,cpu,mem,process_time
        data = csv_line.split(',')
        event = Event(EventType.SUBMIT,
                      int(data[0] if data[0] else 0),
                      data[1],
                      float(data[2] if data[2] else 0),
                      float(data[3] if data[3] else 0),
                      int(data[4] if data[4] else 0)
            )
        return event

    @staticmethod
    def build_update_event(timestamp, vm_name, cpu, mem):
        event = Event(EventType.UPDATE,
                      timestamp,
                      vm_name,
                      float(cpu),
                      float(mem)
            )
        return event

    @staticmethod
    def build_finish_event(timestamp, vm_name):
        event = Event(EventType.FINISH,
                      timestamp,
                      vm_name
            )
        return event


class SubmitEventsQueue:

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
        event = EventBuilder.build_submit_event(self._line)
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
        if self._line is not None:
            self._line = self._line.strip()
        # self._logger.debug('Line read: %s', self._line.strip())
        return self._line

    def current_line(self):
        return self._line

    def _load_next_file(self):
        if self._opened_file != None:
            self._logger.info('Closing file: %s', self._opened_file._filename)
            self._opened_file.close()
        self._file_index += 1
        if self._file_index < len(self._files):
            self._logger.info('Opening file: %s', self._files[self._file_index])
            self._opened_file = fileinput.input(self._files[self._file_index])
        else:
            self._logger.info('No more files to open.')
            self._opened_file = None

class EventsQueue:
    def __init__(self):
        self._clear()

    def _clear(self):
        self.__logger__ = None
        self.__config__ = None
        self.__heap__ = list()
        self.__submit_events__ = SubmitEventsQueue()
        self.__counters__ = defaultdict(lambda: 0)
        self.__last_timestamp__ = -1

    # TODO: too much dependent of simulation rules to be hard coded
    __PRIORITY__ = [EventType.NOTIFY,
                    EventType.FINISH,
                    EventType.UPDATE,
                    EventType.SCHEDULE,
                    EventType.SUBMIT,
                    EventType.UNKNOWN
                    ]
    def _get_priority(self, event_type):
        return EventsQueue.__PRIORITY__.index(event_type)

    def _add_submit_event(self):
        new_event = self.__submit_events__.next_event()
        if new_event is not None:
            self.add_event(new_event)

    def set_config(self, config):
        self.__config__ = config
        self.__submit_events__.set_config(config)

    def initialize(self):
        self.__submit_events__.initialize()
        self.__logger__ = self.__config__.getLogger(self)

    def add_event(self, event):
        heapq.heappush(self.__heap__, (event.timestamp,
                                       self._get_priority(event.type),
                                       self.__counters__[event.timestamp],
                                       event
                                       ))
        self.__counters__[event.timestamp] = self.__counters__[event.timestamp] + 1

    def next_event(self):
        event = None
        if len(self.__heap__) == 0:
            self._add_submit_event()

        if len(self.__heap__) > 0:
            event = heapq.heappop(self.__heap__)

        if event is not None:
            if event.type == EventType.SUBMIT:
                self._add_submit_event()
            if event.timestamp > self.__last_timestamp__:
                self.__logger__.debug('Timestamp %d is over, had %d events.',
                                      self.__last_timestamp__,
                                      self.__counters__[self.__last_timestamp__])
                del self.__counters__[self.__last_timestamp__]
                self.__last_timestamp__ = event.timestamp

        self.__logger__.debug('Event to process: %s', (event.dump() if event is not None
                                                                    else 'none'))
        return event
