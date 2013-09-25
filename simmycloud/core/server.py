
class Server:

	def __init__(self, name, cpu=0.0, mem=0.0):
		self.name = name
		self.cpu = cpu
		self.mem = mem
		self.vm_dict = {}

	def schedule_vm(self, vm):
		self.vm_dict[vm.name] = vm

	def free_vm(self, vm_name):
		return self.vm_dict.pop(vm_name)

	def update_vm(self, vm):
		vm_allocated = self.vm_dict[vm.name]
		vm_allocated.cpu = vm.cpu
		vm_allocated.mem = vm.mem
