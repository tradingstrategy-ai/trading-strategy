from abc import ABC
from dataclasses import dataclass
from typing import Optional


@dataclass
class Configuration:
    api_key: Optional[str] = None


class Environment(ABC):
    """How Capitalgram client interacts with its environment.

    User interactions is different in Jupyter (graphical CLI),
    console, in-page browser and when running in oracle sandbox.
    """

    def discover_configuration(self) -> Optional[Configuration]:
        pass

    def save_configuration(self, config:Configuration):
        pass

    def interactive_setup(self):
        """Perform interactive user onbaording"""