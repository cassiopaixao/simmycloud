
class EventType:
    SUBMIT = 1
    UPDATE = 2
    FINISH = 3


class Event:

    def __init__(self, event_type, cpu=0.0, mem=0.0):
        self.type = event_type
        self.cpu = cpu
        self.mem = mem
