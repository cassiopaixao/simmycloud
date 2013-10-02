
class Server:

	def __init__(self, name, cpu=0.0, mem=0.0):
		self.name = name
		self.cpu = cpu
		self.mem = mem
		self.vm_dict = {}
		self.cpu_use = 0.0
		self.mem_use = 0.0

	def schedule_vm(self, vm):
		self.vm_dict[vm.name] = vm
		self.cpu_use += vm.cpu
		self.mem_use += vm.mem

	def free_vm(self, vm_name):
		vm = self.vm_dict.pop(vm_name)
		self.cpu_use -= vm.cpu
		self.mem_use -= vm.mem
		return vm

	def update_vm(self, vm):
		vm_allocated = self.vm_dict[vm.name]
		self.cpu_use += vm.cpu - vm_allocated.cpu
		self.mem_use += vm.mem - vm_allocated.mem
		vm_allocated.cpu = vm.cpu
		vm_allocated.mem = vm.mem
		return vm_allocated

