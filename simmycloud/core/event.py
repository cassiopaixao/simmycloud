
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


class EventBuilder:
    @staticmethod
    def build(csv_line):
        if csv_line == None or len(csv_line) == 0:
            return None
        data = csv_line.split(',')
        return Event(EventBuilder.get_event_type(int(data[5])),
                     data[0],
                     '{}-{}'.format(data[2], data[3]),
                     data[9],
                     data[10]
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
