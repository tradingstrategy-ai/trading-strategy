"""Manual test for checking download progress bar visualisation."""
import os
import logging

from tradingstrategy.client import Client
from tradingstrategy.environment.jupyter import download_with_tqdm_progress_bar
from tradingstrategy.timebucket import TimeBucket

logger = logging.getLogger()

logging.basicConfig(handlers=[logging.StreamHandler()])
logging.getLogger("matplotlib").disabled = True

c = Client.create_live_client(os.environ["TRADING_STRATEGY_API_KEY"])

c.transport.download_func = download_with_tqdm_progress_bar

c.fetch_pair_universe()
c.fetch_all_candles(TimeBucket.d1)