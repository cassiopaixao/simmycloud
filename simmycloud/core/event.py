
class EventType:
    SUBMIT = 1
    UPDATE = 2
    FINISH = 3

    @classmethod
    def values(cls):
        return [cls.SUBMIT,
                cls.UPDATE,
                cls.FINISH,
                ]


class Event:

    def __init__(self, event_type, cpu=0.0, mem=0.0):
        self.type = event_type if event_type in EventType.values() else None
        self.cpu = cpu
        self.mem = mem
