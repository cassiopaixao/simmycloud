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


import re, os
from core.event import EventType, EventQueue

class VMMachineState:

    UNKNOWN = 0
    PENDING = 1
    RUNNING = 2
    DEAD = 3
    INVALID_DEMAND = 4

    def __init__(self):
        self._vms = dict()
        self._event_queue = EventQueue()

    def set_config(self, config):
        self._config = config
        self._event_queue.set_config(config)

    def initialize(self):
        self._event_queue.initialize()

    def run(self):
        while True:
            event = self._event_queue.next_event()
            if event == None:
                break
            self._process_event(event)

    def _process_event(self, event):
        new_state = VMMachineState.UNKNOWN
        current_state = self._vms.get(event.vm.name)

        if current_state in [VMMachineState.UNKNOWN,
                             VMMachineState.INVALID_DEMAND]:
            return

        if current_state == None: # UNSUBMITTED
            if event.type == EventType.SUBMIT:
                new_state = VMMachineState.PENDING

        elif current_state == VMMachineState.PENDING:
            if event.type == EventType.UPDATE_PENDING:
                new_state = VMMachineState.PENDING
            elif event.type == EventType.SCHEDULE:
                new_state = VMMachineState.RUNNING
            elif event.type == EventType.FINISH:
                new_state = VMMachineState.DEAD

        elif current_state == VMMachineState.RUNNING:
            if event.type == EventType.UPDATE_RUNNING:
                new_state = VMMachineState.RUNNING
            elif event.type == EventType.FINISH:
                new_state = VMMachineState.DEAD

        elif current_state == VMMachineState.DEAD:
            if event.type == EventType.SUBMIT:
                new_state = VMMachineState.PENDING


        if new_state == VMMachineState.RUNNING and (0 in [event.vm.cpu, event.vm.mem]):
            new_state = VMMachineState.INVALID_DEMAND

        self._vms[event.vm.name] = new_state


class InputVerifier(VMMachineState):
    def is_valid(self):
        self.initialize()
        self.run()

        for vm_name, state in self._vms.items():
            if state in [VMMachineState.UNKNOWN, VMMachineState.INVALID_DEMAND]:
                return False
        return True

    def print_statistics(self):
        pending = 0
        running = 0
        finished = 0
        unknown = []
        invalid_demand = []

        for vm_name, state in self._vms.items():

            if state == VMMachineState.PENDING:
                pending += 1

            elif state == VMMachineState.RUNNING:
                running += 1

            elif state == VMMachineState.DEAD:
                finished += 1

            elif state == VMMachineState.UNKNOWN:
                unknown.append(vm_name)

            elif state == VMMachineState.INVALID_DEMAND:
                invalid_demand.append(vm_name)

        print('pending: {}'.format(pending))
        print('running: {}'.format(running))
        print('finished: {}'.format(finished))
        print('unknown: {}'.format(len(unknown)))
        print('invalid_demand: {}'.format(len(invalid_demand)))
        print(' '.join(unknown))
        print(' '.join(invalid_demand))


class InputFilter(VMMachineState):
    def filter(self):
        self.initialize()

        output_directory = re.sub('/?$', '_filtered/', self._config.params['input_directory'])
        try:
            os.makedirs(output_directory)
        except OSError:
            raise Exception("Output directory '%s' already exists. Can't filter due to possible conflicts." % output_directory)

        self.run()

        output_file_number = 1
        event_counter = 0

        filtered = {}

        for vm_name, state in self._vms.items():
            if state in [VMMachineState.UNKNOWN, VMMachineState.INVALID_DEMAND]:
                filtered[vm_name] = True

        out = open(("%s/%05d.csv" % (output_directory, output_file_number)), "w")
        self._event_queue.initialize()
        while True:
            event = self._event_queue.next_event()
            if event == None:
                break
            elif not event.vm.name in filtered:
                out.write(self._event_queue.current_line())
                event_counter += 1
                if event_counter == 1000000:
                    out.close()
                    output_file_number += 1
                    out = open(("%s/%05d.csv" % (output_directory, output_file_number)), "w")
                    event_counter = 0
        out.close()
