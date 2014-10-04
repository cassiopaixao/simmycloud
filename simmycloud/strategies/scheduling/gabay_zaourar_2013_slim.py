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

import heapq
import sys
from multiprocessing import Pool

from core.strategies import SchedulingStrategy


class GabayZaourarSlimAlgorithm(SchedulingStrategy):

    def initialize(self):
        coeficient = self._config.params['bfd_measure_coeficient_function']
        self.processors_to_use = int(self._config.params['bfd_processors_to_use'])
        self.parallel_serial_threshold = int(self._config.params['bfd_parallel_serial_threshold'])
        if coeficient == '1/C(j)':
            self.coeficient_function = frac_1_cj
        elif coeficient == '1/R(j)':
            self.coeficient_function = frac_1_rj
        elif coeficient == 'R(j)/C(j)':
            self.coeficient_function = frac_rj_cj
        else:
            raise 'Set bfd_measure_coeficient_function param with one of these values: 1/C(j), 1/R(j) or R(j)/C(j).'

    def initialize_item_heap(self, items):
        self._item_heap = list()
        for item in items:
            heapq.heappush(self._item_heap, (sys.float_info.max - self.s_i[item.name], len(self._item_heap), item))

    def initialize_bin_heap(self, bins):
        self._bin_heap = list()
        for bin in bins:
            heapq.heappush(self._bin_heap, (self.s_b[bin.name], len(self._bin_heap), bin))

    def compute_sizes(self, items, bins):
        global alpha_cpu, alpha_mem, beta_cpu, beta_mem

        alpha = self.coeficient_function(items, bins)
        alpha_cpu, alpha_mem = alpha[0], alpha[1]

        beta = alpha
        beta_cpu, beta_mem = beta[0], beta[1]

        self.s_i = {}
        self.s_b = {}

        if len(bins) + len(items) > self.parallel_serial_threshold:
            with Pool(processes=self.processors_to_use) as pool:
                item_values = pool.map(_gabay_zaourar_compute_item_size, items)
                for item_value in item_values:
                    self.s_i[item_value[0]] = item_value[1]

                bin_values = pool.map(_gabay_zaourar_compute_bin_size, bins)
                for bin_value in bin_values:
                    self.s_b[bin_value[0]] = bin_value[1]
        else:
            for item in items:
                self.s_i[item.name] = alpha_cpu*item.cpu + alpha_mem*item.mem

            for bin in bins:
                self.s_b[bin.name] = beta_cpu*bin.cpu_free + beta_mem*bin.mem_free


    def get_biggest_item(self):
        return heapq.heappop(self._item_heap)[2] if len(self._item_heap) > 0 else None

    def get_smallest_bin(self):
        return heapq.heappop(self._bin_heap)[2] if len(self._bin_heap) > 0 else None

    def get_smallest_feasible_bin(self, item, bins):
        feasible_bins = (bin for bin in bins if fits(item, bin))
        try:
            return min(feasible_bins, key=self.size_of_bin)
        except ValueError:
            return None

    def get_biggest_feasible_item(self, items, bin):
        feasible_items = (item for item in items if fits(item, bin))
        try:
            return max(feasible_items, key=self.size_of_item)
        except ValueError:
            return None

    def size_of_bin(self, bin):
        return self.s_b[bin.name]

    def size_of_item(self, item):
        return self.s_i[item.name]


class BFDItemCentricSlim(GabayZaourarSlimAlgorithm):

    @SchedulingStrategy.schedule_vms_strategy
    def schedule_vms(self, vms):
        unpacked_items = list(vms)

        self.compute_sizes(unpacked_items,
                           self._config.resource_manager.all_servers())
        self.initialize_item_heap(unpacked_items)

        while len(unpacked_items) > 0:
            biggest_item = self.get_biggest_item()

            bin = self.get_smallest_feasible_bin(biggest_item,
                                                 self._config.resource_manager.online_servers())

            if bin is None:
                bin = self.get_smallest_feasible_bin(biggest_item,
                                                     self._config.resource_manager.offline_servers())
                if bin is not None:
                    self._config.resource_manager.turn_on_server(bin.name)

            if bin is not None:
                self._config.resource_manager.schedule_vm_at_server(biggest_item, bin.name)

            unpacked_items.remove(biggest_item)


class BFDBinCentricSlim(GabayZaourarSlimAlgorithm):

    @SchedulingStrategy.schedule_vms_strategy
    def schedule_vms(self, vms):
        remaining_vms = self.schedule_vms_at_servers(vms, self._config.resource_manager.online_servers())
        if len(remaining_vms) > 0:
            remaining_vms = self.schedule_vms_at_servers(vms, self._config.resource_manager.offline_servers(), active_bins=False)

    def schedule_vms_at_servers(self, vms, bins, active_bins=True):
        list_of_bins = list(bins)
        unpacked_items = list(vms)

        self.compute_sizes(unpacked_items, list_of_bins)
        self.initialize_bin_heap()

        while len(list_of_bins) > 0:
            smallest_bin = self.get_smallest_bin()
            have_used_bin = False

            item = self.get_biggest_feasible_item(unpacked_items, smallest_bin)
            while item is not None:
                if not have_used_bin and not active_bins:
                    self._config.resource_manager.turn_on_server(smallest_bin.name)

                self._config.resource_manager.schedule_vm_at_server(item, smallest_bin.name)
                have_used_bin = True
                unpacked_items.remove(item)

                item = self.get_biggest_feasible_item(unpacked_items, smallest_bin)

            list_of_bins.remove(smallest_bin)

        return unpacked_items


def fits(item, bin):
    return bin.cpu_free >= item.cpu and bin.mem_free >= item.mem

gabay_zaourar_slim_min_divisor = 0.0000000001
gabay_zaourar_slim_one = 1.0

def frac_1_cj(items, bins):
    return (gabay_zaourar_slim_one/max(sum(b.cpu_free for b in bins), gabay_zaourar_slim_min_divisor),
            gabay_zaourar_slim_one/max(sum(b.mem_free for b in bins), gabay_zaourar_slim_min_divisor))

def frac_1_rj(items, bins):
    return (gabay_zaourar_slim_one/max(sum(i.cpu for i in items), gabay_zaourar_slim_min_divisor),
            gabay_zaourar_slim_one/max(sum(i.mem for i in items), gabay_zaourar_slim_min_divisor))

def frac_rj_cj(items, bins):
    frac_cj = frac_1_cj(items, bins)
    frac_rj = frac_1_rj(items, bins)

    # (1/C(j))/(1/R(j)) = (1/C(j))*R(j) = R(j)/C(j)
    return (frac_cj[0]/frac_rj[0],
            frac_cj[1]/frac_rj[1])


alpha_cpu = alpha_mem = beta_cpu = beta_mem = None

def _gabay_zaourar_compute_item_size(item):
    return (item.name, alpha_cpu*item.cpu + alpha_mem*item.mem)

def _gabay_zaourar_compute_bin_size(bin):
    return (bin.name, beta_cpu*bin.cpu_free + beta_mem*bin.mem_free)
