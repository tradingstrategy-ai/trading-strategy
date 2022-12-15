"""An example script how to visualise real-time candles.

- This application is mostly useful for testing interactive
  user interface components and data feed APIs

- The OHLCV data is randomly generated

- You need to install `dash` to run this application

.. code-block:: shell

    pip install dash

- Then run the application:

.. code-block:: shell

    python scripts/synthetic-real-time-candles.py

"""
import logging
import sys
import time
from threading import Thread

from typing import Tuple, List, Optional

import pandas as pd
import dash
from dash import html, dcc, Output, Input, Dash
from dash.dcc import Graph, Interval, Markdown, Dropdown
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


logger: Optional[logging.Logger] = logging.getLogger()


def setup_fake_market_data_feeds() -> Tuple[MockChainAndReorganisationMonitor, CandleFeed, SyntheticTradeFeed]:
    """Create the synthetic blockchain and trading pairs.

    This will generate random candle data to display.
    """
    mock_chain = MockChainAndReorganisationMonitor(block_duration_seconds=BLOCK_DURATION_SECONDS)

    # Generate 5 min candles
    timeframe = Timeframe("5min")

    # Start with 1 hour data
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

    candle_feed = CandleFeed(
        pairs,
        timeframe=timeframe,
    )
    return mock_chain, candle_feed, trade_feed


def start_chain_thread(block_producer: MockChainAndReorganisationMonitor):
    """Our fake blockchain creates blocks on the background."""
    while True:
        block_producer.produce_blocks(1)
        logger.debug("Produced block %d", block_producer.get_last_block_live())
        time.sleep(BLOCK_DURATION_SECONDS)


def setup_app(
        pairs: List[str],
        freq_seconds: float,
        trade_feed: TradeFeed,
        candle_feed: CandleFeed,
) -> Dash:
    """Build a Dash application UI using its framework.

    Hook in the synthetic data feed production
    to Dash interval callback.
    """

    app = Dash(__name__)

    block_producer: MockChainAndReorganisationMonitor = trade_feed.reorg_mon

    current_pair = pairs[0]

    app.layout = html.Div([
        Dropdown(pairs, pairs[0], id='pair-dropdown'),
        Markdown(id='chain-stats'),
        Graph(id='live-update-graph'),
        # https://dash.plotly.com/live-updates
        Interval(
            id='interval-component',
            interval=freq_seconds * 1000,
            n_intervals=0
        )
    ])

    # Select the current pair
    @app.callback(
        Input('pair-dropdown', 'value'),
    )
    def update_output(value):
        nonlocal current_pair
        current_pair = value
        return f'You have selected {value}'

    @app.callback(Output('chain-stats', "markdown"),
                  Input('interval-component', 'n_intervals'))
    def update_chain_stats(n):
        block_num = block_producer.get_last_block_live()
        timestamp = block_producer.get_block_timestamp_as_pandas(block_num)
        return f"""Current block: {block_num:,} at {timestamp}"""

    # Update the candle chart
    @app.callback(Output('live-update-graph', 'figure'),
                  Input('interval-component', 'n_intervals'))
    def update_ohlcv_chart_live(n):
        try:
            delta = trade_feed.perform_duty_cycle()
            candle_feed.apply_delta(delta)
            candles = candle_feed.get_candles_by_pair(current_pair)
            if len(candles) > 0:
                fig = visualise_ohlcv(candles)
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
    global logger

    # Get rid of pesky Pandas FutureWarnings
    disable_pandas_warnings()

    # Setup logging
    logging.basicConfig(level=logging.DEBUG, handlers=[logging.StreamHandler()])

    mock_chain, candle_feed, trade_feed = setup_fake_market_data_feeds()
    pairs = trade_feed.pairs

    # Start the fake blockchain generating fake data
    bg_thread = Thread(target=start_chain_thread, args=(mock_chain,))
    bg_thread.start()

    app = setup_app(pairs, BLOCK_DURATION_SECONDS, trade_feed, candle_feed)
    app.run_server(debug=True)


if __name__ == '__main__':
    run_app()