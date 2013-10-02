
class CloudSimulator:

    def __init__(self):
        self._server_of_vm = dict()
        self.environment = None
        self.schedule_strategy = None
        self.migration_strategy = None

        """ use a Config object to encapsulate the strategies

            see http://www.itmaybeahack.com/book/python-2.6/html/p03/p03c03_patterns.html#strategyb
            to understand a (probably) better way to implement strategies """

    def process_event(self, event):
        if event.is_submit():
            self.schedule_strategy.schedule(event.vm)
        elif event.is_update():
            server = self.environment.get_server_of_vm(event.vm.name)
            vm = server.update_vm(event.vm)
            self.migration_strategy.execute_over_updated_vm_and_server(vm, server)
        elif event.is_finish():
            server = self.environment.get_server_of_vm(event.vm.name)
            server.free_vm(event.vm.name)
            self.turn_off_strategy.execute_over_updated_server(server)
        else:
            #deu zica
            print 'Unknown event: %s'.format(event.to_str)
