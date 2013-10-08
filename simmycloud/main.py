#!/usr/bin/python
#

from core.cloud_simulator import CloudSimulator
from core.config import Config
from core.environment import Environment
from core.event import Event, EventType
from core.server import Server
from core.virtual_machine import VirtualMachine

config = Config()
environment = Environment()
event = Event(EventType.FINISH)
server = Server("name")
vm = VirtualMachine("name")
cloud_simulator = CloudSimulator(config)

print('ok')
