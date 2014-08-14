###############################################################################
# The MIT License (MIT)
#
# Copyright (c) 2014 Cássio Paixão
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

from operator import attrgetter
from math import ceil, floor
from collections import defaultdict

from core.strategies import SchedulingStrategy

class GroupingVSVBP(SchedulingStrategy):

    def initialize(self):
        self.classifications = int(self._config.params['grouping_vsvbp_classifications'])
        self.servers_groups = None
        self.vms_groups = None


    @SchedulingStrategy.schedule_vms_strategy
    def schedule_vms(self, vms):
        self.servers_turned_on = {}
        remaining_vms = self.schedule_vms_at_servers(vms, self._config.resource_manager.online_servers(), should_turn_on_servers=False)
        if remaining_vms:
            self.schedule_vms_at_servers(remaining_vms, self._config.resource_manager.offline_servers(), should_turn_on_servers=True)

    def schedule_vms_at_servers(self, vms, servers, should_turn_on_servers=False):
        if not vms or not servers: return vms

        cpu_max = max(max(servers, key=attrgetter('cpu_free')).cpu_free, 0.0000000001)
        mem_max = max(max(servers, key=attrgetter('mem_free')).mem_free, 0.0000000001)
        cpu_coef = cpu_max / self.classifications
        mem_coef = mem_max / self.classifications

        self.classify_servers(servers, cpu_coef, mem_coef)
        self.classify_vms(vms, cpu_coef, mem_coef)

        for resource_class in reversed(range(1, self.classifications+1)):
            servers_in_resource_class = self.servers_groups[resource_class]
            while True:
                item_sets_in_resource_class = self.vms_groups.item_sets_in_resource_class(resource_class)
                if not item_sets_in_resource_class or not servers_in_resource_class: break
                scheduled_sets = self.schedule_item_sets(item_sets_in_resource_class, servers_in_resource_class, should_turn_on_servers)
                if not scheduled_sets: break

                self.vms_groups.remove_item_sets(scheduled_sets, resource_class)
                self.merge_item_sets(resource_class)

        remaining_vms = []
        for resource_class in range(1, self.classifications+1):
            for item_set in self.vms_groups.item_sets_in_resource_class(resource_class):
                remaining_vms.extend(item_set.items)

        remaining_vms = self.schedule_vms_directly(remaining_vms, servers, should_turn_on_servers)
        return remaining_vms


    def classify_servers(self, servers, cpu_coef, mem_coef):
        self.servers_groups = defaultdict(list)
        for server in servers:
            self.group_for_server(server, cpu_coef, mem_coef).append(server)

    def classify_vms(self, vms, cpu_coef, mem_coef):
        self.vms_groups = GroupsManager(self.classifications)
        for vm in vms:
            item_set = ItemSet(vm)
            self.group_for_item_set(item_set, cpu_coef, mem_coef).add_item_set(item_set)

    def group_for_item_set(self, item_set, cpu_coef, mem_coef):
        cpu_class = ceil(min(max(cpu_coef*item_set.cpu,1),self.classifications))
        mem_class = ceil(min(max(mem_coef*item_set.mem,1),self.classifications))
        return self.vms_groups.group(cpu_class, mem_class)

    def group_for_server(self, server, cpu_coef, mem_coef):
        cpu_class = server.cpu_free*cpu_coef
        mem_class = server.mem_free*mem_coef
        resource_class = ceil(min(max(cpu_class, mem_class, 1), self.classifications))
        return self.servers_groups[resource_class]

    def schedule_item_sets(self, item_sets, servers, should_turn_on_servers):
        #first fit
        scheduled_item_sets = []
        for item_set in item_sets:
            for server in servers:
                if GroupingVSVBP.fits(item_set, server):
                    if should_turn_on_servers: self.guarantee_turned_on_server(server)
                    for vm in item_set.items:
                        self._config.resource_manager.schedule_vm_at_server(vm, server.name)
                    scheduled_item_sets.append(item_set)
                    break
        return scheduled_item_sets

    def schedule_vms_directly(self, vms, servers, should_turn_on_servers):
        remaining_vms = []
        #first fit
        for vm in vms:
            scheduled = False
            for server in servers:
                if vm.cpu <= server.cpu_free and vm.mem <= server.mem_free:
                    if should_turn_on_servers: self.guarantee_turned_on_server(server)
                    self._config.resource_manager.schedule_vm_at_server(vm, server.name)
                    scheduled = True
                    break
            if not scheduled: remaining_vms.append(vm)
        return remaining_vms

    def merge_item_sets(self, resource_class):
        for i in reversed(range(1, resource_class)):
            for j in reversed(range(1, resource_class)):
                for a in range(resource_class-i):
                    for b in range(resource_class-j):
                        self.vms_groups.merge(  self.vms_groups.group(i, j),
                                                self.vms_groups.group(resource_class-i-a, resource_class-j-b) )

    def guarantee_turned_on_server(self, server):
        if server.name not in self.servers_turned_on:
            self._config.resource_manager.turn_on_server(server.name)
            self.servers_turned_on[server.name] = True


    @staticmethod
    def fits(item_set, server):
        return item_set.cpu <= server.cpu_free and item_set.mem <= server.mem_free


class GroupsManager:
    def __init__(self, classifications):
        self.classifications = classifications
        self.groups = {}
        for i in range(1, self.classifications+1):
            for j in range(1, self.classifications+1):
                self.groups['{},{}'.format(i,j)] = Group(i, j)

    def merge(self, group_a, group_b):
        destination_group = self.destination_group(group_a, group_b)
        if destination_group is None: return

        item_sets_to_merge = floor(len(group_a)/2) if group_a is group_b else min(len(group_a), len(group_b))

        for i in range(item_sets_to_merge):
            a_item_set = group_a.pop()
            b_item_set = group_b.pop()
            a_item_set.merge_with(b_item_set)
            destination_group.add_item_set(a_item_set)

    def item_sets_in_resource_class(self, resource_class):
        item_sets = []
        for group in self.groups_in_resource_class(resource_class):
            item_sets.extend(group.item_sets)
        return item_sets

    def groups_in_resource_class(self, resource_class):
        groups = [self.groups['{},{}'.format(resource_class, resource_class)]]
        for i in range(1, resource_class):
            groups.append(self.groups['{},{}'.format(i, resource_class)])
            groups.append(self.groups['{},{}'.format(resource_class, i)])
        return groups

    def remove_item_sets(self, item_sets, resource_class):
        groups = self.groups_in_resource_class(resource_class)
        for item_set in item_sets:
            removed = False
            for group in groups:
                if item_set in group.item_sets:
                    group.remove_item_set(item_set)
                    removed = True
                    break
            if not removed:
                raise Exception('ItemSet not found to be removed at resource_class {}: ItemSet({}, {})'.format(
                    resource_class, item_set.cpu, item_set.mem))

    def group(self, cpu_class, mem_class):
        return self.groups.get('{},{}'.format(cpu_class, mem_class))

    def destination_group(self, group_a, group_b):
        return self.group(  group_a.cpu_class + group_b.cpu_class,
                            group_a.mem_class + group_b.mem_class)


class Group:
    def __init__(self, cpu_class, mem_class):
        self.item_sets = []
        self.cpu_class = cpu_class
        self.mem_class = mem_class

    def add_item_sets(self, item_sets):
        self.item_sets.extend(item_sets)

    def add_item_set(self, item_set):
        self.item_sets.append(item_set)

    def remove_item_set(self, item_set):
        self.item_sets.remove(item_set)

    def pop_item_set(self):
        return self.item_sets.pop()

    def __len__(self):
        return len(self.item_sets)


class ItemSet:
    def __init__(self, vm):
        self.items = [vm]
        self.cpu = vm.cpu
        self.mem = vm.mem

    def merge_with(self, item_set):
        self.items.extend(item_set.items)
        self.cpu += item_set.cpu
        self.mem += item_set.mem
