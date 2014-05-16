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


from core.strategies import SchedulingStrategy

class BFDItemCentric(SchedulingStrategy):

    def initialize(self):
        coeficient = self._config.params['bfd_measure_coeficient_function']
        if coeficient == '1/C(j)':
            self.coeficient_function = self._frac_1_cj
        elif coeficient == '1/R(j)':
            self.coeficient_function = self._frac_1_rj
        elif coeficient == 'R(j)/C(j)':
            self.coeficient_function = lambda items, bins: \
                                        self._frac_1_cj(items, bins) / self._frac_1_rj(items, bins)
        else:
            raise 'Set bfd_measure_coeficient_function param with one of these values: 1/C(j), 1/R(j) or R(j)/C(j).'


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


    def compute_sizes(self, items, bins):
        alpha = self.coeficient_function(items, bins)
        alpha_cpu, alpha_mem = alpha['cpu'], alpha['mem']

        beta = alpha
        beta_cpu, beta_mem = beta['cpu'], beta['mem']

        self.s_i = {}
        for item in items:
            self.s_i[item.name] = alpha_cpu*item.cpu + alpha_mem*item.mem

        self.s_b = {}
        for bin in bins:
            self.s_b[bin.name] = beta_cpu*(bin.cpu - bin.cpu_alloc) + beta_mem*(bin.mem - bin.mem_alloc)


    def get_biggest_item(self, items):
        return max(items, key=lambda item: self.s_i[item.name])

    def get_smallest_feasible_bin(self, item, bins):
        sorted_bins = sorted(bins, key=lambda bin: self.s_b[bin.name])
        for bin in sorted_bins:
            if self.fits(item, bin):
                return bin
        return None

    def fits(self, item, bin):
        return bin.cpu - bin.cpu_alloc >= item.cpu and \
               bin.mem - bin.mem_alloc >= item.mem


    def _frac_1_cj(self, items, bins):
        return {'cpu': 1.0/sum([b.cpu - b.cpu_alloc for b in bins]),
                'mem': 1.0/sum([b.mem - b.mem_alloc for b in bins])}

    def _frac_1_rj(self, items, bins):
        return {'cpu': 1.0/sum([i.cpu for i in items]),
                'mem': 1.0/sum([i.mem for i in items])}
