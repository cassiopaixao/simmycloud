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

from multiprocessing import Pool

from core.strategies import SchedulingStrategy


alpha_cpu = alpha_mem = beta_cpu = beta_mem = None

def _bfd_compute_item_size(item):
    return (item.name, alpha_cpu*item.cpu + alpha_mem*item.mem)

def _bfd_compute_bin_size(bin):
    return (bin.name, beta_cpu*(bin.cpu - bin.cpu_alloc) + beta_mem*(bin.mem - bin.mem_alloc))


class BFD(SchedulingStrategy):

    def initialize(self):
        coeficient = self._config.params['bfd_measure_coeficient_function']
        self.processors_to_use = int(self._config.params['bfd_processors_to_use'])
        if coeficient == '1/C(j)':
            self.coeficient_function = frac_1_cj
        elif coeficient == '1/R(j)':
            self.coeficient_function = frac_1_rj
        elif coeficient == 'R(j)/C(j)':
            self.coeficient_function = frac_rj_cj
        else:
            raise 'Set bfd_measure_coeficient_function param with one of these values: 1/C(j), 1/R(j) or R(j)/C(j).'

    def compute_sizes(self, items, bins):
        global alpha_cpu, alpha_mem, beta_cpu, beta_mem

        alpha = self.coeficient_function(items, bins)
        alpha_cpu, alpha_mem = alpha['cpu'], alpha['mem']

        beta = alpha
        beta_cpu, beta_mem = beta['cpu'], beta['mem']

        self.s_i = {}
        self.s_b = {}

        with Pool(processes=self.processors_to_use) as pool:
            # for item in items:
            #     self.s_i[item.name] = alpha_cpu*item.cpu + alpha_mem*item.mem
            item_values = pool.map(_bfd_compute_item_size, items)
            for item_value in item_values:
                self.s_i[item_value[0]] = item_value[1]

            # for bin in bins:
            #     self.s_b[bin.name] = beta_cpu*(bin.cpu - bin.cpu_alloc) + beta_mem*(bin.mem - bin.mem_alloc)
            bin_values = pool.map(_bfd_compute_bin_size, bins)
            for bin_value in bin_values:
                self.s_b[bin_value[0]] = bin_value[1]

    def get_biggest_item(self, items):
        return max(items, key=lambda item: self.s_i[item.name])

    def get_smallest_bin(self, bins):
        return min(bins, key=lambda bin: self.s_b[bin.name])

    def get_smallest_feasible_bin(self, item, bins):
        sorted_bins = sorted(bins, key=lambda bin: self.s_b[bin.name])
        for bin in sorted_bins:
            if fits(item, bin):
                return bin
        return None

    def get_biggest_feasible_item(self, items, bin):
        sorted_items = sorted(items, key=lambda item: self.s_i[item.name], reverse=True)
        for item in sorted_items:
            if fits(item, bin):
                return item
        return None


class BFDItemCentric(BFD):

    @SchedulingStrategy.schedule_vms_strategy
    def schedule_vms(self, vms):
        unpacked_items = list(vms)

        while len(unpacked_items) > 0:
            self.compute_sizes(unpacked_items,
                               self._config.resource_manager.online_servers())
            biggest_item = self.get_biggest_item(unpacked_items)

            bin = self.get_smallest_feasible_bin(biggest_item,
                                                 self._config.resource_manager.online_servers())

            if bin is None:
                self.compute_sizes(unpacked_items,
                                   self._config.resource_manager.offline_servers())
                bin = self.get_smallest_feasible_bin(biggest_item,
                                                     self._config.resource_manager.offline_servers())
                if bin is not None:
                    self._config.resource_manager.turn_on_server(bin.name)

            if bin is not None:
                self._config.resource_manager.schedule_vm_at_server(biggest_item, bin.name)

            unpacked_items.remove(biggest_item)


class BFDBinCentric(BFD):

    @SchedulingStrategy.schedule_vms_strategy
    def schedule_vms(self, vms):
        remaining_vms = self.schedule_vms_at_servers(vms, self._config.resource_manager.online_servers())
        if len(remaining_vms) > 0:
            remaining_vms = self.schedule_vms_at_servers(vms, self._config.resource_manager.offline_servers(), active_bins=False)

    def schedule_vms_at_servers(self, vms, bins, active_bins=True):
        list_of_bins = list(bins)
        unpacked_items = list(vms)

        while len(list_of_bins) > 0:
            self.compute_sizes(unpacked_items, list_of_bins)

            smallest_bin = self.get_smallest_bin(list_of_bins)
            have_used_bin = False

            item = self.get_biggest_feasible_item(unpacked_items, smallest_bin)
            while item is not None:
                self.compute_sizes(unpacked_items, list_of_bins)

                if not have_used_bin and not active_bins:
                    self._config.resource_manager.turn_on_server(smallest_bin.name)

                self._config.resource_manager.schedule_vm_at_server(item, smallest_bin.name)
                have_used_bin = True
                unpacked_items.remove(item)

                item = self.get_biggest_feasible_item(unpacked_items, smallest_bin)

            list_of_bins.remove(smallest_bin)

        return unpacked_items


def fits(item, bin):
    return bin.cpu - bin.cpu_alloc >= item.cpu and \
           bin.mem - bin.mem_alloc >= item.mem


def frac_1_cj(items, bins):
    return {'cpu': 1.0/max(sum([b.cpu - b.cpu_alloc for b in bins]), 0.000001),
            'mem': 1.0/max(sum([b.mem - b.mem_alloc for b in bins]), 0.000001)}

def frac_1_rj(items, bins):
    return {'cpu': 1.0/max(sum([i.cpu for i in items]), 0.000001),
            'mem': 1.0/max(sum([i.mem for i in items]), 0.000001)}

def frac_rj_cj(items, bins):
    frac_cj = frac_1_cj(items, bins)
    frac_rj = frac_1_rj(items, bins)

    # (1/C(j))/(1/R(j)) = (1/C(j))*R(j) = R(j)/C(j)
    return {'cpu': frac_cj['cpu']/frac_rj['cpu'],
            'mem': frac_cj['mem']/frac_rj['mem']}
