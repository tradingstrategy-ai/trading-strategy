"""An example script how to visualise real-time candles from any Uniswap v2 compatible exchange.

- This application is shows real-time OHLCV candles from Uniswap v2 compatible
  exchanges

- It is a demo and interactive test applications for manual testing

"""
import logging
import os
import sys
import time
from threading import Thread

from typing import Tuple, List, Optional, Dict

import pandas as pd
import typer
from dash import html, Output, Input, Dash
from dash.dash_table import DataTable
from dash.dcc import Graph, Interval, Dropdown
from dash.html import Div, Label, H2
from plotly.subplots import make_subplots
from tqdm import tqdm
from web3 import Web3, HTTPProvider

from eth_defi.event_reader.block_time import measure_block_time
from eth_defi.event_reader.web3factory import TunedWeb3Factory
from eth_defi.price_oracle.oracle import TrustedStablecoinOracle
from eth_defi.uniswap_v2.pair import fetch_pair_details
from tradingstrategy.charting.candle_chart import visualise_ohlcv
from tradingstrategy.direct_feed.candle_feed import CandleFeed
from tradingstrategy.direct_feed.reorg_mon import MockChainAndReorganisationMonitor, JSONRPCReorganisationMonitor
from tradingstrategy.direct_feed.store import load_trade_feed
from tradingstrategy.direct_feed.timeframe import Timeframe
from tradingstrategy.direct_feed.trade_feed import TradeFeed
from tradingstrategy.direct_feed.uniswap_v2 import UniswapV2TradeFeed
from tradingstrategy.direct_feed.warn import disable_pandas_warnings

#: Which candles our app can render
CANDLE_OPTIONS = {
    "1 minute": Timeframe("1min"),
    "5 minutes": Timeframe("5min"),
    "1 hour": Timeframe("1h"),
}


#: Store 100,000 blocks per Parquet dataset file
DATASET_PARTITION_SIZE = 100_000


logger: Optional[logging.Logger] = logging.getLogger()


def setup_uniswap_v2_market_data_feeds(
        json_rpc_url: str,
        pair_address: str,
        candle_options: Dict[str, Timeframe],
) -> Tuple[float, Dict[str, CandleFeed], UniswapV2TradeFeed]:
    """Create the synthetic blockchain and trading pairs.

    This will generate random candle data to display.
    """

    web3 = Web3(HTTPProvider(json_rpc_url))

    data_refresh_requency = measure_block_time(web3)

    reorg_mon = JSONRPCReorganisationMonitor(web3)

    pair_details = fetch_pair_details(web3, pair_address)

    web3_factory = TunedWeb3Factory(json_rpc_url)

    max_timeframe = list(candle_options.values())[-1]

    pairs = [pair_details]

    oracles = {}
    for p in pairs:
        oracles[p] = TrustedStablecoinOracle()

    # Have two pairs
    trade_feed = UniswapV2TradeFeed(
        pairs=pairs,
        reorg_mon=reorg_mon,
        web3_factory=web3_factory,
        timeframe=max_timeframe,
        oracles=oracles,
    )

    candle_feeds = {
        label: CandleFeed(pairs, timeframe=timeframe) for label, timeframe in CANDLE_OPTIONS.items()
    }
    return data_refresh_requency, candle_feeds, trade_feed


def start_block_consumer_thread(
        data_refresh_frequency,
        trade_feed: TradeFeed,
        candle_feeds: Dict[str, CandleFeed]):
    """Consume blockchain data and update in-memory candles."""
    while True:
        # Read trades from the blockchain
        delta = trade_feed.perform_duty_cycle()
        for candle_feed in candle_feeds.values():
            candle_feed.apply_delta(delta)
            time.sleep(data_refresh_frequency)


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


app = typer.Typer()


@app.command()
def main(
        json_rpc_url: str = typer.Option(None, help="Connect to EVM blockchain using this JSON-RPC node"),
        pair_address: str= typer.Option(None, help="Address of Uniswap v2 compatible pair contract"),
):
    """Setup and run the dash app."""

    # Get rid of pesky Pandas FutureWarnings
    disable_pandas_warnings()

    # Setup logging
    logging.basicConfig(level=logging.DEBUG, handlers=[logging.StreamHandler()])

    # Setup the fake blockchain data generator
    data_refresh_frequency, candle_feed, trade_feed = setup_uniswap_v2_market_data_feeds()
    pairs = trade_feed.pairs

    cache_path = os.path.expanduser("~/.cache/uniswap-v2-candle-demo")

    if load_trade_feed(trade_feed, candle_feed, DATASET_PARTITION_SIZE):
        logger.info("Loaded old data from %s", cache_path)
    else:
        logger.info("First run, cache is empty %s", cache_path)

    # Buffer the block data before starting the GUI application.
    # Display interactive tqdm progress bar.
    buffer_hours = 3
    blocks_needed = int(buffer_hours * 3600 // data_refresh_frequency) + 1
    trade_feed.backfill_buffer(blocks_needed, tqdm)

    # Start blockchain data processor bg thread
    candle_bg_thread = Thread(target=start_block_consumer_thread, args=(data_refresh_frequency, trade_feed, candle_feed))
    candle_bg_thread.start()

    # Create the Dash web UI and start the web server
    app = setup_app(pairs, data_refresh_frequency, trade_feed, candle_feed)
    app.run_server(debug=True)


if __name__ == '__main__':
    main()