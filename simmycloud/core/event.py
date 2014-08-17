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


import os
import fileinput
import re
import heapq
from collections import defaultdict, deque

from core.virtual_machine import VirtualMachine

class EventType:
    UNKNOWN = 0
    SUBMIT = 1
    UPDATE = 2
    FINISH = 3
    NOTIFY = 4
    UPDATES_FINISHED = 5
    VERIFY_VMS_POOL = 6
    TIME_TO_PREDICT = 7

    @staticmethod
    def get_type(type_number):
        if type_number in range(1,8):
            types = ['', 'SUBMIT',
                         'UPDATE',
                         'FINISH',
                         'NOTIFY',
                         'UPDATES_FINISHED',
                         'VERIFY_VMS_POOL',
                         'TIME_TO_PREDICT']
            return types[type_number]
        return 'UNKNOWN'


class Event:

    def __init__(self, event_type, time=0, vm_name='', cpu=0.0, mem=0.0, process_time=0, message=''):
        self.type = event_type
        self.time = time
        self.process_time = process_time
        self.vm = VirtualMachine(vm_name, cpu, mem)
        self.message = message

    def dump(self):
        return '{}, {}, [[{}], {} | {}]'.format(EventType.get_type(self.type),
                                                self.time,
                                                self.vm.dump(),
                                                self.process_time,
                                                self.message
                                                )


class EventBuilder:

    @staticmethod
    def build_submit_event(csv_line):
        if csv_line == None or len(csv_line) == 0:
            return None

        # timestamp,vm_name,cpu,mem,process_time
        data = csv_line.split(',')
        return Event(EventType.SUBMIT,
                     time=int(data[0] if data[0] else 0),
                     vm_name=data[1],
                     cpu=float(data[2] if data[2] else 0),
                     mem=float(data[3] if data[3] else 0),
                     process_time=int(data[4] if data[4] else 0)
            )

    @staticmethod
    def build_update_event(timestamp, vm_name, cpu, mem):
        return Event(EventType.UPDATE,
                     time=timestamp,
                     vm_name=vm_name,
                     cpu=cpu,
                     mem=mem
            )

    @staticmethod
    def build_finish_event(timestamp, vm_name):
        return Event(EventType.FINISH,
                     time=timestamp,
                     vm_name=vm_name
            )

    @staticmethod
    def build_notify_event(timestamp, message):
        return Event(EventType.NOTIFY,
                     time=timestamp,
                     message=message
            )

    @staticmethod
    def build_updates_finished_event(timestamp):
        return Event(EventType.UPDATES_FINISHED,
                     time=timestamp
            )

    @staticmethod
    def build_verify_vms_pool_event(timestamp):
        return Event(EventType.VERIFY_VMS_POOL,
                     time=timestamp
            )

    @staticmethod
    def build_time_to_predict_event(timestamp):
        return Event(EventType.TIME_TO_PREDICT,
                     time=timestamp
            )


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
        self._max_submit_timestamp = self._config.params.get('max_submit_timestamp')
        if self._max_submit_timestamp:
            self._max_submit_timestamp = int(self._max_submit_timestamp)

    def next_event(self):
        self._line =  self._fileset.next_line()
        event = EventBuilder.build_submit_event(self._line)
        if self._max_submit_timestamp and event and event.time > self._max_submit_timestamp:
            self._logger.info('Max submit timestamp reached (%d). Ignoring upcoming submits.', self._max_submit_timestamp)
            self._logger.debug('First event ignored: %s', event.dump())
            return None
        return event

    def current_event(self):
        return self._event

    def current_line(self):
        return self._fileset.current_line()


class FileSetReader:
    def __init__(self):
        self._clear()

    def _clear(self):
        self._files = deque()
        self._line = None
        self._lines = deque()
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
        self._files = deque(self._files)
        self._logger = self._config.getLogger(self)

        self._load_next_file()

    def next_line(self):
        if len(self._lines) == 0:
            self._load_next_file()
            self._line = None
        if len(self._lines) > 0:
            self._line = self._lines.popleft()
        return self._line

    def current_line(self):
        return self._line

    def _load_next_file(self):
        if len(self._files) > 0:
            filename = self._files.popleft()
            self._logger.info('Opening file: %s', filename)
            opened_file = fileinput.input(filename)
            self._get_all_lines(opened_file)
            self._logger.info('Closing file: %s', filename)
            opened_file.close()

        else:
            self._logger.info('No more files to open.')

    def _get_all_lines(self, opened_file):
        line = opened_file.readline()
        while len(line) > 0:
            self._lines.append(line)
            line = opened_file.readline()


class EventsQueue:
    def __init__(self):
        self._clear()

    def _clear(self):
        self._logger = None
        self._config = None
        self._heap = list()
        self._submit_events = SubmitEventsQueue()
        self._counters = defaultdict(lambda: 0)
        self._last_timestamp = -1

    # TODO: too much dependent of simulation rules to be hard coded
    _PRIORITY = [EventType.NOTIFY,
                    EventType.FINISH,
                    EventType.TIME_TO_PREDICT,
                    EventType.UPDATE,
                    EventType.UPDATES_FINISHED,
                    EventType.VERIFY_VMS_POOL,
                    EventType.SUBMIT,
                    EventType.UNKNOWN
                    ]
    def _get_priority(self, event_type):
        return EventsQueue._PRIORITY.index(event_type)

    def _add_new_submit_event(self):
        new_event = self._submit_events.next_event()
        if new_event is not None:
            self.add_event(new_event)
        else:
            self._has_submit_events = False

    def set_config(self, config):
        self._config = config
        self._submit_events.set_config(config)

    def initialize(self):
        self._submit_events.initialize()
        self._logger = self._config.getLogger(self)
        self._has_submit_events = True
        self._add_new_submit_event()

    def clear(self):
        self._heap = list()

    def has_submit_events(self):
        return self._has_submit_events

    def add_event(self, event):
        heapq.heappush(self._heap, (event.time,
                                    self._get_priority(event.type),
                                    self._counters[event.time],
                                    event
                                    ))
        self._counters[event.time] = self._counters[event.time] + 1

    def next_events(self):
        if len(self._heap) == 0:
            return None

        events = [heapq.heappop(self._heap)[-1]]
        if events[0].type == EventType.SUBMIT: self._add_new_submit_event()

        while len(self._heap) > 0 and\
              self._heap[0][-1].time == events[0].time and\
              self._heap[0][-1].type == events[0].type:
            events.append(heapq.heappop(self._heap)[-1])
            if events[-1].type == EventType.SUBMIT: self._add_new_submit_event()

        if events[0].time > self._last_timestamp:
            self._logger.debug('Timestamp %d is over, had %d events.',
                               self._last_timestamp,
                               self._counters[self._last_timestamp])
            del self._counters[self._last_timestamp]
            self._last_timestamp = events[0].time

        return events
