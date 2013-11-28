from core.environment import EnvironmentBuilder
from core.server import Server

class TestEnvironmentBuilder(EnvironmentBuilder):

    @staticmethod
    def build(environment):
        environment.add_servers_of_type(Server('', 1.0, 0.5), 15)
