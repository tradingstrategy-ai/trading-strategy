"""An example script how to visualise real-time candles.

- This application is mostly useful for testing interactive
  user interface components and data feed APIs

- The OHLCV data is randomly generated. We generate two pairs: ETH-USD
  and AAVE-ETH.

- This app is not safe, but just an example for scripting purposes - see
  https://dash.plotly.com/sharing-data-between-callbacks

- You need to install `dash` to run this application

.. code-block:: shell

    pip install dash

- Then run the application:

.. code-block:: shell

    python scripts/synthetic-real-time-candles.py

"""
import logging
import os
import sys
import time
from threading import Thread

from typing import Tuple, List, Optional, Dict

import pandas as pd
from dash import html, Output, Input, Dash
from dash.dash_table import DataTable
from dash.dcc import Graph, Interval, Dropdown
from dash.html import Div, Label, H2
from plotly.subplots import make_subplots

from eth_defi.price_oracle.oracle import TrustedStablecoinOracle, FixedPriceOracle
from tradingstrategy.charting.candle_chart import visualise_ohlcv
from tradingstrategy.direct_feed.candle_feed import CandleFeed
from tradingstrategy.direct_feed.reorg_mon import MockChainAndReorganisationMonitor
from tradingstrategy.direct_feed.synthetic_feed import SyntheticTradeFeed
from tradingstrategy.direct_feed.timeframe import Timeframe
from tradingstrategy.direct_feed.trade_feed import TradeFeed
from tradingstrategy.direct_feed.warn import disable_pandas_warnings

#: Setup a mock blockchain with this block time
BLOCK_DURATION_SECONDS = 1.5

#: Which candles our app can render
CANDLE_OPTIONS = {
    "1 minute": Timeframe("1min"),
    "5 minutes": Timeframe("5min"),
    "1 hour": Timeframe("1h"),
}

logger: Optional[logging.Logger] = logging.getLogger()


def setup_fake_market_data_feeds() -> Tuple[MockChainAndReorganisationMonitor, Dict[str, CandleFeed], SyntheticTradeFeed]:
    """Create the synthetic blockchain and trading pairs.

    This will generate random candle data to display.
    """
    mock_chain = MockChainAndReorganisationMonitor(block_duration_seconds=BLOCK_DURATION_SECONDS)

    # Generate 5 min candles
    timeframe = Timeframe("5min")

    # Backfill the fake data.
    # We start with 1 hour generated data in the buffer.
    mock_chain.produce_blocks(int(3600 / BLOCK_DURATION_SECONDS))

    pairs = ["ETH-USD", "AAVE-ETH"]

    # Have two pairs
    trade_feed = SyntheticTradeFeed(
        pairs,
        {
            "ETH-USD": TrustedStablecoinOracle(),
            "AAVE-ETH": FixedPriceOracle(1600),
        },
        mock_chain,
        timeframe=timeframe,
        min_amount=-50,
        max_amount=50,
        prices = {
            "ETH-USD": 1600,
            "AAVE-ETH": 0.1,
        }
    )

    candle_feeds = {
        label: CandleFeed(pairs, timeframe=timeframe) for label, timeframe in CANDLE_OPTIONS.items()
    }
    return mock_chain, candle_feeds, trade_feed


def start_chain_thread(block_producer: MockChainAndReorganisationMonitor):
    """Our fake blockchain creates blocks on the background."""
    while True:
        block_producer.produce_blocks(1)
        logger.debug("Produced block %d", block_producer.get_last_block_live())
        time.sleep(BLOCK_DURATION_SECONDS)


def start_block_consumer_thread(trade_feed: TradeFeed, candle_feeds: Dict[str, CandleFeed]):
    """Consume blockchain data and update in-memory candles."""
    while True:
        # Read trades from the blockchain
        delta = trade_feed.perform_duty_cycle()
        for candle_feed in candle_feeds.values():
            candle_feed.apply_delta(delta)
            time.sleep(BLOCK_DURATION_SECONDS)


def setup_app(
        pairs: List[str],
        freq_seconds: float,
        trade_feed: TradeFeed,
        candle_feeds: Dict[str, CandleFeed],
) -> Dash:
    """Build a Dash application UI using its framework.

    Hook in the synthetic data feed production
    to Dash interval callback.
    """

    # Load CSS
    assets_folder = os.path.join(os.path.dirname(__file__), "dash-assets")
    app = Dash(__name__, assets_folder=assets_folder)
    block_producer: MockChainAndReorganisationMonitor = trade_feed.reorg_mon

    candle_labels = list(CANDLE_OPTIONS.keys())

    app.layout = html.Div([
        Div(
            id="controls",
            children=[
                Div([Label("Trading pair:"), Dropdown(pairs, pairs[0], id='pair-dropdown'),]),
                Div([Label("Candle time:"), Dropdown(candle_labels, candle_labels[0], id='candle-dropdown')]),
                Div([Label("Chain status:"), Div(id='chain-stats'),]),
            ],
        ),
        H2("Latest trades"),
        DataTable(id="trades"),
        H2("Price chart"),
        Graph(id='live-update-graph', responsive=True),
        # https://dash.plotly.com/live-updates
        Interval(
            id='interval-component',
            interval=freq_seconds * 1000,
            n_intervals=0
        ),
    ])

    # Update the chain status
    @app.callback(Output('chain-stats', "children"),
                  Input('interval-component', 'n_intervals'))
    def update_chain_stats(n):
        try:
            if not block_producer.has_data():
                return "No blocks produced yet"

            block_num = block_producer.get_last_block_read()
            timestamp = block_producer.get_block_timestamp_as_pandas(block_num)
            timestamp_fmt = timestamp.strftime("%Y-%m-%d, %H:%M:%S UTC")
            return f"""Current block: {block_num:,} at {timestamp_fmt}, loop {n}"""
        except Exception as e:
            logger.exception(e)
            raise

    # Get the raw trades and convert them to
    # human readable table format
    @app.callback(Output('trades', "data"),
                  [Input('interval-component', 'n_intervals'), Input('pair-dropdown', 'value')])
    def update_last_trades(n, pair):
        df = trade_feed.get_latest_trades(5, pair)
        df = df.sort_values("timestamp", ascending=False)
        quote_token = pair.split("-")[-1]
        output = pd.DataFrame()
        output["Block number"] = df["block_number"]
        output["Pair"] = df["pair"]
        output["Transaction"] = df["tx_hash"]
        output["Price USD"] = df["price"]
        if quote_token != "USD":
            output[f"Price {quote_token}"] = df["price"] / df["exchange_rate"]
            output[f"Echange rate USD/{quote_token}"] = df["exchange_rate"]
        return output.to_dict("records")

    # Update the candle charts for the currently selected pair
    @app.callback(Output('live-update-graph', 'figure'),
                  [Input('interval-component', 'n_intervals'), Input('pair-dropdown', 'value'), Input('candle-dropdown', 'value')])
    def update_ohlcv_chart_live(n, current_pair, current_candle_duration):
        try:
            candles = candle_feeds[current_candle_duration].get_candles_by_pair(current_pair)
            if len(candles) > 0:
                fig = visualise_ohlcv(candles, height=500)
            else:
                # Create empty figure as we do not have data yet
                fig = make_subplots(rows=1, cols=1)
            return fig
        except Exception as e:
            # Dash does not show errors in the console by default
            logger.exception(e)
            sys.exit(1)

    return app


def run_app():
    """Setup and run the dash app."""

    # Get rid of pesky Pandas FutureWarnings
    disable_pandas_warnings()

    # Setup logging
    logging.basicConfig(level=logging.DEBUG, handlers=[logging.StreamHandler()])

    # Setup the fake blockchain data generator
    mock_chain, candle_feed, trade_feed = setup_fake_market_data_feeds()
    pairs = trade_feed.pairs

    # Start the fake blockchain generating fake data
    chain_bg_thread = Thread(target=start_chain_thread, args=(mock_chain,))
    chain_bg_thread.start()

    # Start blockchain data processor bg thread
    candle_bg_thread = Thread(target=start_block_consumer_thread, args=(trade_feed, candle_feed))
    candle_bg_thread.start()

    # Create the Dash web UI and start the web server
    app = setup_app(pairs, BLOCK_DURATION_SECONDS, trade_feed, candle_feed)
    app.run_server(debug=True)


if __name__ == '__main__':
    run_app()