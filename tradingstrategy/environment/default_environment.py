import logging
import os
import platform
from pathlib import Path
from typing import Optional

from tqdm_loggable.auto import tqdm
from tradingstrategy.environment.base import Environment
from tradingstrategy.environment.config import Configuration
from tradingstrategy.environment.interactive_setup import (
    run_interactive_setup,
    run_non_interactive_setup,
)

# Legacy export
from tradingstrategy.transport.progress_enabled_download import download_with_tqdm_progress_bar

if platform.system() == 'Emscripten':
    # disable tqdm thread in pyodide - it doesn't have threading yet
    tqdm.monitor_interval = 0

logger = logging.getLogger(__name__)


#: Where we will store our settings file
#:
#: Store under user home
#:
DEFAULT_SETTINGS_PATH = Path(os.path.expanduser("~/.tradingstrategy"))



class SettingsDisabled(Exception):
    """Raised when the user tries to create/read settings file when it is purposefully disabled.

    Docker environments.
    """


class DefaultClientEnvironment(Environment):
    """Sets up an application cache and settings.

    - Use default `~/.cache` and `~/.tradingstrategy/settings.json` storage in your home folder.

    - Locations can be overwritten e.g. in unit testes to temp paths
    """

    def __init__(
        self,
        cache_path=None,
        settings_path=DEFAULT_SETTINGS_PATH,
    ):
        """CReate environment.

        :param cache_path:
            Where do we store downloaded datasets

        :param settings_path:
            Override the default settings path.

            Useful for unit tests.
        """
        if not cache_path:
            # TODO: Not use which Unix standard mandates ~/.cache
            self.cache_path = os.path.expanduser("~/.cache/tradingstrategy")
        else:
            self.cache_path = cache_path

        if settings_path:

            assert isinstance(settings_path, Path), f"Got {settings_path.__class__}"

        self.setting_path = settings_path

    def check_settings_enabled(self):
        if not self.setting_path:
            raise SettingsDisabled("Settings file is disabled and the code path tried to access it.")

    def get_cache_path(self) -> str:
        return self.cache_path

    def get_settings_path(self) -> str:
        return self.setting_path

    def discover_configuration(self) -> Optional[Configuration]:
        spath = self.get_settings_path()
        settings_file = os.path.join(spath, "settings.json")
        if os.path.exists(settings_file):
            with open(settings_file, "rt") as inp:
                data = inp.read()
                if data:
                    return Configuration.from_json(data)
        return None

    def save_configuration(self, config: Configuration):
        spath = self.get_settings_path()
        os.makedirs(spath, exist_ok=True)

        assert config, "API key not configured. Re-run the notebook to restart the API key configuration process."

        with open(os.path.join(spath, "settings.json"), "wt") as out:
            data = config.to_json()
            out.write(data)

    def clear_configuration(self):
        """Delete the saved config file (if any)"""
        self.check_settings_enabled()
        spath = self.get_settings_path()
        path = os.path.join(spath, "settings.json")
        if os.path.exists(path):
            os.remove(path)

    def interactive_setup(self) -> Configuration:
        """Perform interactive user onbaording"""
        self.check_settings_enabled()
        config = run_interactive_setup()
        self.save_configuration(config)
        return config

    def non_interactive_setup(self, **kwargs) -> Configuration:
        """Perform interactive user onbaording"""
        self.check_settings_enabled()
        config = run_non_interactive_setup(**kwargs)
        if config:
            self.save_configuration(config)
        return config

    def setup_on_demand(self, **kwargs) -> Configuration:
        """Check if we need to set up the environment."""
        self.check_settings_enabled()
        config = self.discover_configuration()
        if not config:
            if platform.system() == 'Emscripten':
                print(f"No existing Trading Strategy configuration found in {self.get_settings_path()}/settings.json. Making config from keyword parameters.")
                config = self.non_interactive_setup(**kwargs)
            else:
                print(f"No existing Trading Strategy configuration found in {self.get_settings_path()}/settings.json. Starting interactive setup.")
                config = self.interactive_setup()
        else:
            print(f"Started Trading Strategy in Jupyter notebook environment, configuration is stored in {self.get_settings_path()}")
        return config

