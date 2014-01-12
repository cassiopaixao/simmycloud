
class VirtualMachine:

    def __init__(self, name, cpu=0.0, mem=0.0):
        self.name = name
        self.cpu = cpu
        self.mem = mem

    def dump(self):
        return "{}({},{})".format(self.name,
                                  self.cpu,
                                  self.mem
                                  )
