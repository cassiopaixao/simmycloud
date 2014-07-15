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


class Server:

	def __init__(self, name, cpu=0.0, mem=0.0):
		self.name = name
		self.cpu = cpu
		self.mem = mem
		self.vm_dict = {}
		self.cpu_alloc = 0.0
		self.mem_alloc = 0.0
		self.cpu_free = self.cpu
		self.mem_free = self.mem

	def schedule_vm(self, vm):
		self.vm_dict[vm.name] = vm
		self.cpu_alloc += vm.cpu
		self.mem_alloc += vm.mem
		self.cpu_free -= vm.cpu
		self.mem_free -= vm.mem

	def free_vm(self, vm):
		vm = self.vm_dict.pop(vm.name)
		self.cpu_alloc -= vm.cpu
		self.mem_alloc -= vm.mem
		self.cpu_free += vm.cpu
		self.mem_free += vm.mem
		return vm

	def update_vm(self, vm):
		vm_allocated = self.vm_dict[vm.name]
		self.cpu_alloc += vm.cpu - vm_allocated.cpu
		self.mem_alloc += vm.mem - vm_allocated.mem
		self.cpu_free += vm_allocated.cpu - vm.cpu
		self.mem_free += vm_allocated.mem - vm.mem
		vm_allocated.cpu = vm.cpu
		vm_allocated.mem = vm.mem
		return vm_allocated

	def vm_list(self):
		return self.vm_dict.values()

	def is_empty(self):
		return len(self.vm_dict) == 0

	def is_overloaded(self):
		return (self.cpu_free < 0 or self.mem_free < 0)

	def describe(self):
		return '{} ({}, {})'.format(self.name,
									self.cpu,
									self.mem
									)

	def dump(self):
		return '{} ({}/{}+{}, {}/{}+{}), {}'.format(self.name,
                                              self.cpu,
                                              self.cpu_alloc,
                                              self.cpu_free,
                                              self.mem,
                                              self.mem_alloc,
                                              self.mem_free,
                                              ' '.join(vm.dump() for vm in self.vm_dict.values())
                                              )