import os

from typing import Optional

from capitalgram.environment.base import Environment
from capitalgram.environment.config import Configuration
from capitalgram.environment.interactive_setup import run_interactive_setup


class JupyterEnvironment(Environment):
    """Define paths and setup processes when using Capitalgram from any local Jupyter Notebook installation"""

    def __init__(self, cache_path=None):
        if not cache_path:
            self.cache_path = os.path.expanduser("~/.cache/capitalgram")
        else:
            self.cache_path = cache_path

    def get_cache_path(self) -> str:
        return self.cache_path

    def get_settings_path(self) -> str:
        return os.path.expanduser("~/.capitalgram")

    def discover_configuration(self) -> Optional[Configuration]:
        spath = self.get_settings_path()
        settings_file = os.path.join(spath, "settings.json")
        if os.path.exists(settings_file):
            with open(settings_file, "rt") as inp:
                data = inp.read()
                return Configuration.from_json(data)
        return None

    def save_configuration(self, config: Configuration):
        spath = self.get_settings_path()

        os.makedirs(spath)

        with open(os.path.join(spath, "settings.json"), "wt") as out:
            data = config.to_json()
            out.write(data)

    def interactive_setup(self) -> Configuration:
        """Perform interactive user onbaording"""
        config = run_interactive_setup()
        self.save_configuration(config)
        return config

    def setup_on_demand(self) -> Configuration:
        """Check if we need to set up the environment."""
        config = self.discover_configuration()
        if not config:
            print(f"No existing Capitalgram configuration found in {self.get_settings_path()}/settings.json. Starting interactive setup.")
            config = self.interactive_setup()
        else:
            print(f"Using configuration found in {self.get_settings_path()}")
        return config


