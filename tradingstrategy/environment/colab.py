from tradingstrategy.environment.base import Environment
from tradingstrategy.environment.jupyter import DefaultClientEnvironment


class ColabEnvironment(DefaultClientEnvironment):

    def start(self):
        pass