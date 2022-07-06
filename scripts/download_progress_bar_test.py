"""Manual test for checking download progress bar visualisation."""
import os

from tradingstrategy.client import Client
from tradingstrategy.environment.jupyter import download_with_tqdm_progress_bar

c = Client.create_live_client(os.environ["TRADING_STRATEGY_API_KEY"])

c.transport.download_func = download_with_tqdm_progress_bar

c.fetch_pair_universe()