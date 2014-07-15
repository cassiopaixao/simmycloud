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
        remaining_vms = self.schedule_vms_at_servers(vms, self._config.resource_manager.online_servers())
        if len(remaining_vms) > 0:
            remaining_vms = self.schedule_vms_at_servers(vms, self._config.resource_manager.offline_servers(), turn_on_servers=True)

    def schedule_vms_at_servers(self, vms, servers, turn_on_servers=False):
        if not vms or not servers:
            return vms

        max_cpu = max(max(servers, key=attrgetter('cpu_free')).cpu_free, 0.0000000001)
        max_mem = max(max(servers, key=attrgetter('mem_free')).mem_free, 0.0000000001)

        self.map_servers_to_groups(servers, max_cpu, max_mem)
        self.map_vms_to_groups(vms, max_cpu, max_mem)

        self.turned_on_servers = dict()

        for resource_class in reversed(range(1, self.classifications+1)):
            self._logger.debug('Will try to schedule vms_sets at resource_class %d', resource_class)
            servers_of_resource_class = self.servers_of_resource_class(resource_class)
            while 1:
                item_sets = self.vms_groups.item_sets_of_resource_class(resource_class)
                if not item_sets: break;
                scheduled_sets = self.schedule_item_sets_at_servers(item_sets, servers_of_resource_class, turn_on_servers)
                self._logger.debug('Scheduled %d item_sets', len(scheduled_sets))
                if not scheduled_sets: break
                # self._logger.debug( 'Scheduled sets to be removed at resource_class %i: %s',
                #                     resource_class,
                #                     ' '.join(i.dump() for i in scheduled_sets))
                self.vms_groups.remove_item_sets(scheduled_sets, resource_class)
                self.group_vms_till_resource_class(resource_class)
                item_sets = self.vms_groups.item_sets_of_resource_class(resource_class)

        remaining_vms = []
        for i in range(1, self.classifications+1):
            remaining_vms.extend(self.vms_groups.items_of_resource_class(i))

        remaining_vms = self.schedule_vms_directly(remaining_vms, servers, turn_on_servers)

        return remaining_vms


    def schedule_item_sets_at_servers(self, item_sets, servers, turn_on_servers=False):
        scheduled_item_sets = []
        #firstfit
        for item_set in item_sets:
            # self._logger.debug('Trying to schedule item_set: %s', item_set.dump())
            for server in servers:
                if GroupingVSVBP.fits(item_set, server):
                    if turn_on_servers and server.name not in self.turned_on_servers:
                        self._config.resource_manager.turn_on_server(server.name)
                        self.turned_on_servers[server.name] = True
                    # self._logger.debug('Scheduled item_set: %s', item_set.dump())
                    scheduled_item_sets.append(item_set)
                    for vm in item_set.items:
                        self._config.resource_manager.schedule_vm_at_server(vm, server.name)
                    break
        return scheduled_item_sets

    def schedule_vms_directly(self, vms, servers, turn_on_servers=False):
        remaining_vms = list(vms)
        #firstfit
        for vm in vms:
            # self._logger.debug('Trying to allocate VM %s', vm.name)
            for server in servers:
                # self._logger.debug('Trying to allocate VM at server %s', server.name)
                if vm.cpu <= server.cpu_free and vm.mem <= server.mem_free:
                    # self._logger.debug('Server %s can accomodate VM %s', server.dump(), vm.dump())
                    if turn_on_servers and server.name not in self.turned_on_servers:
                        # self._logger.debug('Server %s wasn\'t online', server.name)
                        self._config.resource_manager.turn_on_server(server.name)
                        self.turned_on_servers[server.name] = True

                    remaining_vms.remove(vm)
                    self._config.resource_manager.schedule_vm_at_server(vm, server.name)
                    break

        return remaining_vms

    def group_vms_till_resource_class(self, resource_class):
        for i in reversed(range(1, resource_class)):
            for j in reversed(range(1, resource_class)):
                group_i_j = self.vms_groups.group(i, j)
                if not group_i_j.item_sets: continue
                for a in range(resource_class-i):
                    for b in range(resource_class-j):
                        self.vms_groups.merge(group_i_j,
                                              self.vms_groups.group(resource_class-i-a, resource_class-j-b))

    def map_servers_to_groups(self, servers, max_cpu, max_mem):
        self.servers_groups = defaultdict(list)
        cpu_coef = self.classifications / max_cpu
        mem_coef = self.classifications / max_mem
        self._logger.debug('cpu,mem max: %.4f, %.4f', max_cpu, max_mem)
        self._logger.debug('cpu,mem coef: %.4f, %.4f', cpu_coef, mem_coef)
        for s in servers:
            self._logger.debug('Server %s (cap: %.4f,%.4f; free: %.04f,%.4f)',
                s.name,
                s.cpu, s.mem,
                s.cpu_free, s.mem_free)
            resource_class = self.valid_resource_class(max( ceil(s.cpu_free*cpu_coef),
                                                            ceil(s.mem_free*mem_coef)))
            self.servers_groups[resource_class].append(s)

    def servers_of_resource_class(self, resource_class):
        return self.servers_groups[resource_class]

    def map_vms_to_groups(self, vms, max_cpu, max_mem):
        self.vms_groups = GroupManager(self.classifications)
        cpu_coef = self.classifications / max_cpu
        mem_coef = self.classifications / max_mem
        for vm in vms:
            g = self.vms_groups.group(  self.valid_resource_class(ceil(vm.cpu*cpu_coef)),
                                        self.valid_resource_class(ceil(vm.mem*mem_coef)))
            g.add_item_set(ItemSet(vm, vm.cpu, vm.mem))
            # self._logger.debug( 'VM %s added to group %i,%i',
            #                     vm.dump(),
            #                     g.cpu_class,
            #                     g.mem_class)

    def valid_resource_class(self, calculated_resource_class):
        if calculated_resource_class in range(1, self.classifications+1):
            return calculated_resource_class
        else:
            return min(max(calculated_resource_class, 1), self.classifications)

    @staticmethod
    def fits(item_set, server):
        return item_set.cpu <= server.cpu_free and item_set.mem <= server.mem_free


class GroupManager:
    def __init__(self, classifications):
        self.classifications = classifications
        self.groups = dict()
        for i in range(1, self.classifications+1):
            for j in range(1, self.classifications+1):
                self.groups['{},{}'.format(i,j)] = Group(i, j)

    """ returns the group of the resource class requested. """
    def group(self, cpu_class, mem_class):
        return self.groups['{},{}'.format(cpu_class, mem_class)]

    """ merges two groups
        removes the merged item_sets and adds the merged ones to
        respective group """
    def merge(self, a, b):
        if a is b:
            try: self.destination_group_merging(a, b)
            except KeyError: return

        destination_group = self.destination_group_merging(a, b)
        item_sets_to_merge = floor(len(a.item_sets)/2) if a is b else min(len(a.item_sets), len(b.item_sets))

        merged_item_sets = []
        for i in range(item_sets_to_merge):
            a_item_set = a.item_sets.pop()
            b_item_set = b.item_sets.pop()
            a_item_set.extend(b_item_set)
            merged_item_sets.append(a_item_set)

        destination_group.add_item_sets(merged_item_sets)

    def destination_group_merging(self, a, b):
        return self.group(a.cpu_class + b.cpu_class, a.mem_class + b.mem_class)

    def item_sets_of_resource_class(self, resource_class):
        item_sets = list(self.group(resource_class, resource_class).item_sets)
        for i in range(1, resource_class):
            item_sets.extend(self.group(i, resource_class).item_sets)
            item_sets.extend(self.group(resource_class, i).item_sets)
        return item_sets

    def items_of_resource_class(self, resource_class):
        items = list()
        for item_set in self.group(resource_class, resource_class).item_sets:
            items.extend(item_set.items)
        for i in range(1, resource_class):
            for item_set in self.group(i, resource_class).item_sets:
                items.extend(item_set.items)
            for item_set in self.group(resource_class, i).item_sets:
                items.extend(item_set.items)
        return items

    def groups_of_resource_class(self, resource_class):
        groups = [self.group(resource_class, resource_class)]
        for i in range(1, resource_class):
            groups.append(self.group(i, resource_class))
            groups.append(self.group(resource_class, i))
        return groups

    def remove_item_sets(self, item_sets, resource_class):
        groups = self.groups_of_resource_class(resource_class)
        for item_set in item_sets:
            removed = False
            for group in groups:
                if item_set in group.item_sets:
                    group.remove_item_set(item_set)
                    removed = True
                    break
            if not removed:
                raise Exception('ItemSet not found to be removed: {}'.format(item_set.dump()))


    def remove_item_set(self, item_set, resource_class):
        groups = [self.group(resource_class, resource_class)]
        for i in range(1, resource_class):
            groups.append(self.group(i, resource_class))
            groups.append(self.group(resource_class, i))

        for group in groups:
            if item_set in group.item_sets:
                group.remove_item_set(item_set)
                return

        raise Exception('ItemSet not found to be removed: {}'.format(item_set.dump()))


class Group:
    def __init__(self, cpu_class, mem_class):
        self.cpu_class = cpu_class
        self.mem_class = mem_class
        self.item_sets = list()

    def add_item_set(self, item_set):
        self.item_sets.append(item_set)

    def add_item_sets(self, item_sets):
        self.item_sets.extend(item_sets)

    def remove_item_set(self, item_set):
        self.item_sets.remove(item_set)

    def dump(self):
        return 'Group({})'.format(' '.join(i.to_s for i in self.item_sets))


class ItemSet:
    def __init__(self, item, cpu, mem):
        self.items = [item]
        self.cpu = cpu
        self.mem = mem

    def extend(self, other_item_set):
        self.items.extend(other_item_set.items)
        self.cpu += other_item_set.cpu
        self.mem += other_item_set.mem

    def dump(self):
        return 'ItemSet({}, {}): {}'.format(self.cpu,
                                            self.mem,
                                            ', '.join(i.dump() for i in self.items))

    def to_s(self):
        return 'ItemSet({}, {})'.format(self.cpu, self.mem)
