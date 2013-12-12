from core.statistics_manager import StatisticsManagerBuilder
from statistics_fields.base_fields import CounterField, EnvironmentField, MigrationStrategyField, OnlineServersField, SchedulingStrategyField, PoweringOffStrategyField, MeanUseOfServersField, OverloadedServersField, TimeIntervalSinceLastStatisticsField, SumOfUnallocatedVMsField

class StandardStatistics(StatisticsManagerBuilder):
    @classmethod
    def build(cls, statistics_manager):
        statistics_manager.add_field(EnvironmentField('environment'))
        statistics_manager.add_field(SchedulingStrategyField('scheduling'))
        statistics_manager.add_field(MigrationStrategyField('migration'))
        statistics_manager.add_field(PoweringOffStrategyField('powering_off'))
        statistics_manager.add_field(CounterField('submit_events'))
        statistics_manager.add_field(CounterField('update_events'))
        statistics_manager.add_field(CounterField('finish_events'))
        statistics_manager.add_field(OnlineServersField('online_servers'))
        statistics_manager.add_field(OverloadedServersField('overloaded_servers'))
        statistics_manager.add_field(CounterField('servers_turned_on'))
        statistics_manager.add_field(CounterField('servers_turned_off'))
        statistics_manager.add_field(CounterField('vms_not_allocated'))
        statistics_manager.add_field(CounterField('vms_migrated'))
        statistics_manager.add_field(CounterField('couldnot_reallocate'))
        statistics_manager.add_field(MeanUseOfServersField('mean_use_of_servers'))
        statistics_manager.add_field(SumOfUnallocatedVMsField('sum_vms_unallocated'))
        statistics_manager.add_field(TimeIntervalSinceLastStatisticsField('interval_time'))
