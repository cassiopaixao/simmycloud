
class Server:

	def __init__(self, name, cpu=0.0, mem=0.0):
		self.name = name
		self.cpu = cpu
		self.mem = mem
		self.vm_dict = {}
		self.cpu_alloc = 0.0
		self.mem_alloc = 0.0

	def schedule_vm(self, vm):
		self.vm_dict[vm.name] = vm
		self.cpu_alloc += vm.cpu
		self.mem_alloc += vm.mem

	def free_vm(self, vm):
		vm = self.vm_dict.pop(vm.name)
		self.cpu_alloc -= vm.cpu
		self.mem_alloc -= vm.mem
		return vm

	def update_vm(self, vm):
		vm_allocated = self.vm_dict[vm.name]
		self.cpu_alloc += vm.cpu - vm_allocated.cpu
		self.mem_alloc += vm.mem - vm_allocated.mem
		vm_allocated.cpu = vm.cpu
		vm_allocated.mem = vm.mem
		return vm_allocated

	def vm_list(self):
		return self.vm_dict.values()

	def is_overloaded(self):
		return (self.cpu_alloc > self.cpu and self.mem_alloc > self.mem)

	def describe(self):
		return '{} ({}, {})'.format(self.name,
									self.cpu,
									self.mem
									)

	def dump(self):
		return '{} ({}/{}, {}/{}), {}'.format(self.name,
                                              self.cpu_alloc,
                                              self.cpu,
                                              self.mem_alloc,
                                              self.mem,
                                              ' '.join([vm.dump() for vm in self.vm_dict.values()])
                                              )