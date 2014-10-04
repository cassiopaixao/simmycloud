###############################################################################
# The MIT License (MIT)
#
# Copyright (c) 2013 Cassio Paixao
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

    def get_high_priority_vms(self):
        return list(self._high_priority)

    def get_low_priority_vms(self):
        return list(self._low_priority)

    def remove(self, vm):
        if vm in self._high_priority:
            self._high_priority.remove(vm)
        else:
            self._low_priority.remove(vm)
