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

from core.strategies import SchedulingStrategy
from strategies.scheduling.gabay_zaourar_2013 import GabayZaourarAlgorithm, fits

class BestFitDecreasing(GabayZaourarAlgorithm):

    @SchedulingStrategy.schedule_vms_strategy
    def schedule_vms(self, vms):

        self.compute_sizes(vms, self._config.resource_manager.all_servers())
        sorted_items = sorted(list(vms),
                              key=lambda item: self.s_i[item.name], reverse=True)
        sorted_bins = sorted(list(self._config.resource_manager.online_servers()),
                             key=lambda bin: self.s_b[bin.name])

        for item in sorted_items:
            feasible_bin = None
            for bin in sorted_bins:
                if fits(item, bin):
                    feasible_bin = bin
                    break

            if feasible_bin is None:
                feasible_bin = self.get_smallest_feasible_bin(item,
                                                              self._config.resource_manager.offline_servers())
                if feasible_bin is not None:
                    self._config.resource_manager.turn_on_server(feasible_bin.name)
                    sorted_bins = sorted(list(self._config.resource_manager.online_servers()),
                                         key=lambda bin: self.s_b[bin.name])

            if feasible_bin is not None:
                self._config.resource_manager.schedule_vm_at_server(item, feasible_bin.name)
