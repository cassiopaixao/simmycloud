from core.environment import EnvironmentBuilder
from core.server import Server

class FirstEnvironmentBuilder(EnvironmentBuilder):

    @staticmethod
    def build(environment):
        environment.add_servers_of_type(Server('', 1.0, 1.0))
        environment.add_servers_of_type(Server('', 0.5, 0.5))
