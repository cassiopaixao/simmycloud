
from collections import deque


class PendingVMsPool:
    HIGH_PRIORITY = 0
    LOW_PRIORITY = 1

    def __init__(self):
        self._clear()

    def __len__(self):
        return len(self._high_priority) + len(self._low_priority)

    def _clear(self):
        self._logger = None
        self._config = None
        self._high_priority = deque()
        self._low_priority = deque()

    def set_config(self, config):
        self._config = config

    def initialize(self):
        self._logger = self._config.getLogger(self)

    def add_vm(self, vm, priority=LOW_PRIORITY):
        if priority == PendingVMsPool.HIGH_PRIORITY:
            self._high_priority.append(vm)
        else:
            self._low_priority.append(vm)

    def pop_next(self):
        if len(self._high_priority) > 0:
            return self._high_priority.popleft()
        elif len(self._low_priority) > 0:
            return self._low_priority.popleft()
        return None

    def get_ordered_list(self):
        return list(self._high_priority) + list(self._low_priority)

    def remove(self, vm):
        if vm in self._high_priority:
            self._high_priority.remove(vm)
        else:
            self._low_priority.remove(vm)
