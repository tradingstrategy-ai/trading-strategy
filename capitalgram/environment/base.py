from abc import ABC
from dataclasses import dataclass
from typing import Optional

from dataclasses_json import dataclass_json


@dataclass_json
@dataclass
class Configuration:
    """Configuration for Capitalgram client."""
    api_key: Optional[str] = None


class Environment(ABC):
    """Capitalgram interacts within different run-time environments.

    User interactions is different in Jupyter (graphical CLI),
    console, in-page browser and when running in oracle sandbox.
    """

    def discover_configuration(self) -> Optional[Configuration]:
        pass

    def save_configuration(self, config: Configuration):
        pass

    def interactive_setup(self):
        """Perform interactive user onbaording"""

