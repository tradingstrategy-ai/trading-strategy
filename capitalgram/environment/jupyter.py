import functools
import os
import shutil
from typing import Optional

from requests import Session
from tqdm.autonotebook import tqdm

import requests

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

        os.makedirs(spath, exist_ok=True)

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
            print(f"Started Capitalgram in Jupyter notebook environment, configuration is stored in {self.get_settings_path()}")
        return config


def download_with_progress_jupyter(session: Session, path: str, url: str, params: dict):
    """Use tqdm library to raw a graphical progress bar in notebooks for long downloads."""

    # https://stackoverflow.com/questions/37573483/progress-bar-while-download-file-over-http-with-requests
    # https://stackoverflow.com/questions/42212810/tqdm-in-jupyter-notebook-prints-new-progress-bars-repeatedly

    r = session.get(url, stream=True, allow_redirects=True, params=params)
    if r.status_code != 200:
        r.raise_for_status()  # Will only raise for 4xx codes, so...
        raise RuntimeError(f"Request to {url} returned status code {r.status_code}")
    file_size = int(r.headers.get('Content-Length', 0))

    desc = "(Unknown total file size)" if file_size == 0 else ""
    r.raw.read = functools.partial(r.raw.read, decode_content=True)  # Decompress if needed
    with tqdm.wrapattr(r.raw, "read", total=file_size, desc=desc) as r_raw:
        with open(path, "wb") as f:
            shutil.copyfileobj(r_raw, f)

    return path
