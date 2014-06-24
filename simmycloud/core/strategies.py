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


from core.vms_pool import PendingVMsPool
from core.virtual_machine import VirtualMachine
import logging

class Strategy:
    def set_config(self, config):
        self._config = config
        self._logger = self._config.getLogger(self)

    """ This method will be called before the simulation starts.
        Override this method if you want to configure something in your
        strategy.
        The self._config and self._logger objects will already be available. """
    def initialize(self):
        pass


class SchedulingStrategy(Strategy):

    def schedule_vms_strategy(func):
        def new_schedule_vms(self, *args, **kwargs):
            vms = [arg for arg in args if isinstance(arg, list)][0]
            func(self, *args, **kwargs)

            for vm in vms:
                server = self._config.resource_manager.get_server_of_vm(vm.name)
                if server is not None:
                    self._logger.debug('VM %s was allocated to server %s',
                                                       vm.name, server.name)

                elif vm not in self._config.vms_pool.get_ordered_list() \
                        and 'migration' not in self._config.simulation_info.scope:
                    self._config.vms_pool.add_vm(vm, PendingVMsPool.LOW_PRIORITY)
                    self._config.statistics.notify_event('vms_added_to_pending')
                    self._logger.debug('VM %s was added to pending', vm.name)

            return
        return new_schedule_vms

    """ Schedules a list of VirtualMachine in servers in which they fit.
        If no server can provide one VM's demands, nothing needs to be done. """
    @schedule_vms_strategy
    def schedule_vms(self, vms):
        raise NotImplementedError


class MigrationStrategy(Strategy):

    def list_of_vms_to_migrate_strategy(func):
        def new_list_of_vms_to_migrate(self, *args, **kwargs):
            vms_to_migrate = func(self, *args, **kwargs)
            self.__old_servers__ = dict()
            for vm in vms_to_migrate:
                self.__old_servers__[vm.name] = self._config.resource_manager.get_server_of_vm(vm.name)
                self._logger.debug('Should migrate VM %s. It is on server %s',
                                   vm.name,
                                   self.__old_servers__[vm.name].dump())
            return vms_to_migrate
        return new_list_of_vms_to_migrate

    def migrate_vms_strategy(func):
        def new_migrate_vms(self, *args, **kwargs):
            vms = [arg for arg in args if isinstance(arg, list)][0]

            if self._logger.level <= logging.DEBUG:
                for vm in vms:
                    self.__old_servers__[vm.name] = self._config.resource_manager.get_server_of_vm(vm.name)
                    self._logger.debug('Should migrate VM %s. It was on server %s',
                                       vm.name,
                                       self.__old_servers__[vm.name].dump())

            func(self, *args, **kwargs)

            for vm in vms:
                new_server = self._config.resource_manager.get_server_of_vm(vm.name)
                if new_server is None:
                    self._config.vms_pool.add_vm(vm, PendingVMsPool.HIGH_PRIORITY)
                    self._config.statistics.notify_event('vms_paused')
                    self._logger.debug('VM %s migration from server %s failed. Added to vms_pool with high priority.',
                                                       vm.name,
                                                       self.__old_servers__[vm.name].name)

                elif new_server == self.__old_servers__[vm.name]:
                    self._logger.debug('VM %s not migrated. It keeps on server %s.',
                                                       vm.name,
                                                       new_server.name)

                elif new_server != self.__old_servers__[vm.name]:
                    self._config.statistics.notify_event('vms_migrated')
                    self._logger.debug('VM %s migrated from server %s to server %s',
                                                       vm.name,
                                                       self.__old_servers__[vm.name].name,
                                                       new_server.name)
            return
        return new_migrate_vms

    """ Returns a list of virtual machines that should be migrated.
        This method is called after all updates in a timestamp.
        It MUST NOT free vm resources. """
    @list_of_vms_to_migrate_strategy
    def list_of_vms_to_migrate(self, list_of_online_servers):
        raise NotImplementedError

    """ Migrates all the virtual machines that didn't fit in theirs last servers. """
    @migrate_vms_strategy
    def migrate_vms(self, vms):
        raise NotImplementedError


class PredictionStrategy(Strategy):

    def predict_strategy(func):
        def new_predict(self, *args, **kwargs):
            vm = [arg for arg in args if isinstance(arg, VirtualMachine)][0]
            new_demands = func(self, *args, **kwargs)
            if new_demands is not None:
                self._logger.debug('Predicted demands for VM %s: %f, %f',
                                                   vm.name,
                                                   new_demands.cpu,
                                                   new_demands.mem)
                self._config.statistics.notify_event('new_demands')
            else:
                self._logger.debug('No predicted demands for VM %s', vm.name)
            return new_demands
        return new_predict

    def next_prediction_interval_strategy(func):
        def new_next_prediction_interval(self, *args, **kwargs):
            return func(self, *args, **kwargs)
        return new_next_prediction_interval

    """ Returns a new VirtualMachine with the predicted demand.
        Or None if no change should be made. """
    @predict_strategy
    def predict(self, vm):
        raise NotImplementedError

    """ Returns the next prediction interval. If you don't override this
        method, the 'prediction_interval' parameter in config will be returned. """
    @next_prediction_interval_strategy
    def next_prediction_interval(self):
        return int(self._config.params['prediction_interval'])


class PoweringOffStrategy(Strategy):

    def power_off_if_necessary_strategy(func):
        def new_power_off_if_necessary(self, *args, **kwargs):
            servers_list = [arg for arg in args if isinstance(arg, list)]
            servers_list = servers_list[0] if len(servers_list) > 0 else None
            if servers_list:
                self._logger.debug('Verifying servers that should be powered off: [%s]', ' '.join(s.name for s in servers_list))
            else:
                self._logger.debug('Verifying all servers that should be powered off.')
            func(self, *args, **kwargs)
        return new_power_off_if_necessary

    """ Returned value will not be used. Method should power off all servers
        that have to be powered off. The 'servers' argument can have one or
        more servers that had been someway updated (e.g. VMs resources were
        updated or freed. """
    @power_off_if_necessary_strategy
    def power_off_if_necessary(self, servers=None):
        raise NotImplementedError
