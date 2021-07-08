import datetime
from typing import List

from capitalgram.caip import ChainAddressTuple
from capitalgram.chain import ChainId
from capitalgram.client.http import HTTPTransport
from capitalgram.environment.base import Environment
from capitalgram.environment.jupyter import JupyterEnvironment
from capitalgram.pair import PairUniverse


class Capitalgram:
    """An API client for the Capitalgram quant service.

    """

    def __init__(self, env: Environment, transport: HTTPTransport):
        self.env = env
        self.transport = transport

    @classmethod
    def create_jupyter_client(cls) -> "Capitalgram":
        return Capitalgram(JupyterEnvironment(), HTTPTransport())

    def start(self):
        """Checks the API key validity and if the server is responsive.

        If no API key is avaiable open an interactive prompt to register (if available)
        """
        # TODO: do nothing

    def register_on_demand(self):
        """If there is not yet API key available, automatically register for one."""
