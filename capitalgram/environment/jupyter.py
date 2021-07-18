import os

from typing import Optional

from capitalgram.environment.base import Environment, Configuration


class JupyterEnvironment(Environment):

    def get_cache_path(self) -> str:
        return os.path.expanduser("~/.cache/capitalgram")

    def get_settings_path(self) -> str:
        return os.path.expanduser("~/.capitalgram")

    def discover_configuration(self) -> Optional[Configuration]:
        spath = self.get_settings_path()
        if os.path.exists():
            with open(spath, "rt") as inp:
                data = inp.read()
                return Configuration.from_json(data)
        return None

    def save_configuration(self, config: Configuration):
        spath = self.get_settings_path()
        if os.path.exists():
            with open(spath, "wt") as out:
                data = config.to_json()
                out.write(data)

    def interactive_setup(self, config: Configuration):
        """Perform interactive user onbaording"""
        print("Using Capitalgram requires an API key.")
        print("Do you have an API key yet?")
        foo = input()

    def setup_on_demand(self) -> Configuration:
        """Check if we need to set up the environment."""
        config = self.discover_configuration()
        if not config:
            print(f"No existing Capitalgram configuration found in {self.get_settings_path()}. Starting interactive setup.")
            config = self.interactive_setup()
        else:
            print(f"Using configuration found in {self.get_settings_path()}")
        return config


