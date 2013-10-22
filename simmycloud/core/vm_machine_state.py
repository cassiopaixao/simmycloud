
from core.event import EventType, EventQueue

class VMMachineState:

    UNKNOWN = 0
    RUNNING = 1
    DEAD = 2

    def __init__(self):
        self._vms = dict()
        self._event_queue = EventQueue()

    def set_config(self, config):
        self._config = config
        self._event_queue.set_config(config)

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
