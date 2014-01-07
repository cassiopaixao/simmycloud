
from collections import deque


class PendingVMsPool:
    HIGH_PRIORITY = 0
    LOW_PRIORITY = 1

    def __init__(self):
        self._clear()

    def _clear(self):
        self.__logger__ = None
        self.__config__ = None
        self.__high_priority__ = deque()
        self.__low_priority__ = deque()

    def set_config(self, config):
        self.__config__ = config

    def initialize(self):
        self.__logger__ = self.__config__.getLogger(self)

    def add_vm(self, vm, priority=LOW_PRIORITY):
        if priority == PendingVMsPool.HIGH_PRIORITY:
            self.__high_priority__.append(vm)
        else:
            self.__low_priority__.append(vm)

    def pop_next(self):
        if len(self.__high_priority__) > 0:
            return self.__high_priority__.popleft()
        elif len(self.__low_priority__) > 0:
            return self.__low_priority__.popleft()
        return None

    def get_ordered_list(self):
        return list(self.__high_priority__) + list(self.__low_priority__)

    def remove(self, vm):
        if vm in self.__high_priority__:
            self.__high_priority__.remove(vm)
        else:
            self.__low_priority__.remove(vm)
