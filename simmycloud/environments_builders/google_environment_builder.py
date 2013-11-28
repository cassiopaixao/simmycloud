from core.environment import EnvironmentBuilder
from core.server import Server

class GoogleEnvironmentBuilder(EnvironmentBuilder):

    # Heterogeneity and Dynamicity of Clouds at Scale: Google Trace Analysis
    # Charles Reiss, Alexey Tumanov, Gregory R. Ganger, Randy H. Katz, Machael A. Kozuch
    @staticmethod
    def build(environment):
        environment.add_servers_of_type(Server('', 0.50, 0.50), 6732)
        environment.add_servers_of_type(Server('', 0.50, 0.25), 3863)
        environment.add_servers_of_type(Server('', 0.50, 0.75), 1001)
        environment.add_servers_of_type(Server('', 1.00, 1.00), 795)
        environment.add_servers_of_type(Server('', 0.25, 0.25), 126)
        environment.add_servers_of_type(Server('', 0.50, 0.12), 52)
        environment.add_servers_of_type(Server('', 0.50, 0.03), 5)
        environment.add_servers_of_type(Server('', 0.50, 0.97), 5)
        environment.add_servers_of_type(Server('', 1.00, 0.50), 3)
        environment.add_servers_of_type(Server('', 0.50, 0.06), 1)
