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
