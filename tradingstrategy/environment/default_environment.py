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
    run_interactive_vault_setup,
    run_non_interactive_setup,
)
from tradingstrategy.vault_pro import VAULT_PRO_API_KEY_ENV_VAR

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

    def ensure_vault_pro_api_key(
        self,
        config: Configuration | None = None,
        vault_pro_api_key: str | None = None,
    ) -> Configuration:
        """Make sure a Vaults Pro (Creem) licence key is configured and persisted.

        Mirrors the base API key onboarding for the separately licensed vault
        datasets. Resolution order, highest priority first:

        1. An explicit ``vault_pro_api_key`` argument.
        2. The key already stored in ``settings.json``.
        3. The ``VAULT_PRO_API_KEY`` environment variable.
        4. An interactive prompt (skipped in non-interactive Pyodide builds).

        The resulting configuration is written back to ``settings.json`` whenever
        it differs from what is stored, so both the vault key and any updated base
        API key are reused by later runs without prompting again.

        :param config:
            Existing configuration to extend. Loaded from disk (or created
            empty) when not given.

        :param vault_pro_api_key:
            Key supplied by the caller, overriding both the stored config and
            the environment variable.
        """
        self.check_settings_enabled()

        stored = self.discover_configuration()
        if config is None:
            config = stored or Configuration()

        # Explicit argument always wins over the stored key and the environment.
        resolved = vault_pro_api_key or config.vault_pro_api_key or os.environ.get(VAULT_PRO_API_KEY_ENV_VAR)

        if not resolved:
            if platform.system() == 'Emscripten':
                # Cannot prompt inside a browser build; let the missing key fail
                # later with the vault client's actionable error.
                return config
            resolved = run_interactive_vault_setup()

        if resolved:
            config.vault_pro_api_key = resolved

        # Persist when the resulting config differs from disk. This also carries a
        # base API key supplied by the caller (via ``config``) into the settings
        # file, so a later keyless run does not fall back to a stale saved key.
        if stored is None or stored.api_key != config.api_key or stored.vault_pro_api_key != config.vault_pro_api_key:
            self.save_configuration(config)

        return config

    def setup_on_demand(self, needs_vault_data: bool = False, **kwargs) -> Configuration:
        """Check if we need to set up the environment.

        :param needs_vault_data:
            Also ensure a Vaults Pro (Creem) licence key is configured, prompting
            for it the same way as the base API key. See
            :py:meth:`ensure_vault_pro_api_key`.
        """
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

        if needs_vault_data:
            config = self.ensure_vault_pro_api_key(config)

        return config

