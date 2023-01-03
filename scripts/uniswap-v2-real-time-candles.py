"""An example script how to visualise real-time candles from any Uniswap v2 compatible exchange.

- This application is shows real-time OHLCV candles from Uniswap v2 compatible
  exchanges

- It is a demo and interactive test applications for manual testing

- TODO: Non-USD pairs are not yet supported

Example of viewing MATIC/USDC on QuickSwap:

.. code-block:: shell

    export JSON_RPC_POLYGON="https://polygon-rpc.com"
    python scripts/uniswap-v2-real-time-candles.py \
        --json-rpc-url=$JSON_RPC_POLYGON \
        --pair-address=0x6e7a5fafcec6bb1e78bae2a1f0b612012bf14827


Example of viewing BNB/BUSD on PancakeSwap:

.. code-block:: shell

    python scripts/uniswap-v2-real-time-candles.py \
        --json-rpc-url=$BNB_CHAIN_JSON_RPC \
        --pair-address=0x58f876857a02d6762e0101bb5c46a8c1ed44dc16

"""
import datetime
import logging
import os
import shutil
import sys
import time
from pathlib import Path
from threading import Thread

from typing import Tuple, Optional, Dict
from urllib.parse import urljoin

import coloredlogs
import pandas as pd
import typer
from dash import html, Output, Input, Dash
from dash.dash_table import DataTable
from dash.dcc import Graph, Interval, Dropdown
from dash.html import Div, Label, H2
from plotly.subplots import make_subplots
from tqdm import tqdm
from web3 import Web3

from eth_defi.chain import has_graphql_support
from eth_defi.event_reader.block_time import measure_block_time
from eth_defi.event_reader.web3factory import TunedWeb3Factory
from eth_defi.price_oracle.oracle import TrustedStablecoinOracle
from eth_defi.uniswap_v2.pair import fetch_pair_details, PairDetails
from eth_defi.event_reader.reorganisation_monitor import MockChainAndReorganisationMonitor, \
    JSONRPCReorganisationMonitor, ReorganisationMonitor, GraphQLReorganisationMonitor
from tradingstrategy.chain import ChainId

from tradingstrategy.charting.candle_chart import visualise_ohlcv, make_candle_labels
from tradingstrategy.direct_feed.candle_feed import CandleFeed
from tradingstrategy.direct_feed.store import DirectFeedStore
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

#: How many hours worth of event data we buffer for the chart
BUFFER_HOURS = 24

logger: Optional[logging.Logger] = logging.getLogger()


def setup_uniswap_v2_market_data_feeds(
        json_rpc_url: str,
        pair_address: str,
        candle_options: Dict[str, Timeframe],
) -> Tuple[ChainId, float, Dict[str, CandleFeed], UniswapV2TradeFeed]:
    """Create the synthetic blockchain and trading pairs.

    This will generate random candle data to display.
    """

    assert json_rpc_url.startswith("https://"), f"Got URL {json_rpc_url}"

    # Create connection factory and the default web3 instance
    web3_factory = TunedWeb3Factory(json_rpc_url)
    web3 = web3_factory(None)

    # Allow chain reorgs up to 10 blocks
    check_depth = 10

    if has_graphql_support(web3.provider):
        # 10x faster /graphql implementation,
        # not provided by public nodes
        reorg_mon = GraphQLReorganisationMonitor(
            graphql_url=urljoin(json_rpc_url, "/graphql"),
            check_depth=check_depth)
    else:
        # Default slow implementation
        logger.warning("The node does not support /graphql interface. Downloading block headers and timestamps will be extremely slow.")
        reorg_mon = JSONRPCReorganisationMonitor(web3, check_depth=check_depth)

    data_refresh_requency = measure_block_time(web3)

    pair_address = Web3.toChecksumAddress(pair_address)

    pair_details = fetch_pair_details(web3, pair_address, reverse_token_order=False)
    max_timeframe = list(candle_options.values())[-1]

    pairs = [pair_details]

    for p in pairs:
        logger.info("Setting up market data feeds for %s", p)

    oracles = {}
    for p in pairs:
        oracles[p.checksum_free_address] = TrustedStablecoinOracle()

    # Have two pairs
    trade_feed = UniswapV2TradeFeed(
        pairs=pairs,
        reorg_mon=reorg_mon,
        web3_factory=web3_factory,
        timeframe=max_timeframe,
        oracles=oracles,
    )

    pair_addresses = [p.checksum_free_address for p in pairs]
    candle_feeds = {
        label: CandleFeed(pair_addresses, timeframe=timeframe) for label, timeframe in CANDLE_OPTIONS.items()
    }

    chain_id = ChainId(web3.eth.chain_id)

    return chain_id, data_refresh_requency, candle_feeds, trade_feed


def start_block_consumer_thread(
        data_refresh_frequency,
        pair: PairDetails,
        store: DirectFeedStore,
        trade_feed: TradeFeed,
        candle_feeds: Dict[str, CandleFeed]):
    """Consume blockchain data and update in-memory candles."""

    last_save = 0
    save_freq = 15

    try:

        while True:
            # Read trades from the blockchain
            start = time.time()
            next_block = time.time() + data_refresh_frequency

            delta = trade_feed.perform_duty_cycle()
            logger.info(f"Block {delta.unadjusted_start_block} - {delta.end_block} has total {len(delta.trades)} for candles")

            # Internal sanity check
            trade_feed.check_duplicates()

            for candle_feed in candle_feeds.values():
                candle_feed.apply_delta(delta)
                if time.time() - last_save > save_freq:
                    logger.info("Saving data")
                    store.save_trade_feed(trade_feed)
                    last_save = time.time()

                make_candle_labels(
                    candle_feed.candle_df,
                    dollar_prices=False,
                    base_token_name=pair.get_base_token().symbol,
                    quote_token_name=pair.get_quote_token().symbol,
                )

            duration = time.time() - start
            logger.info("Block processing loop took %f seconds", duration)

            left_to_sleep = next_block - time.time()
            if left_to_sleep > 0:
                time.sleep(left_to_sleep)

    except Exception as e:
        logger.error("Reader thread died: %s", e)
        logger.exception(e)
        sys.exit(1)


def setup_app(
        chain_id: ChainId,
        pair: PairDetails,
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

    reorg_mon: ReorganisationMonitor = trade_feed.reorg_mon

    candle_labels = list(CANDLE_OPTIONS.keys())

    quote_token = pair.get_quote_token().symbol

    if quote_token not in ("BUSD", "USDC", "USDT"):
        extra_columns = [
            {"id": f"Price {quote_token}"},
            {"id": f"Exchange rate USD/{quote_token}"},
        ]
    else:
        extra_columns = []

    app.layout = html.Div([
        Div(
            id="controls",
            children=[
                Div([Label("Candle time:"), Dropdown(candle_labels, candle_labels[0], id='candle-dropdown')]),
                Div([Label("Chain status:"), Div(id='chain-stats'),]),
                Div([Label("Data status:"), Div(id='data-stats'), ]),
            ],
        ),
        H2("Latest trades"),
        DataTable(
            id="trades",
            columns=[
                {"id": "Block number", "name": "Block number"},
                {"id": "Pair", "name": "Pair", "presentation": "markdown"},
                {"id": "Transaction", "name": "Transaction", "presentation": "markdown"},
                {"id": "Price USD", "name": "Price USD"},
                {"id": "Amount USD", "name": "Amount USD"},
            ] + extra_columns,
        ),
        H2(f"{pair.get_base_token().symbol}-{pair.get_quote_token().symbol} price chart"),
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
        logger.debug("update_chain_stats(%d)", n)
        try:
            if not reorg_mon.has_data():
                return "No blocks produced yet"

            block_num = reorg_mon.get_last_block_read()
            timestamp = reorg_mon.get_block_timestamp_as_pandas(block_num)
            block_header_count = len(reorg_mon.block_map)
            ago = datetime.datetime.utcnow() - timestamp.to_pydatetime()
            ago_seconds = ago.total_seconds()
            return f"""Current block: {block_num:,} {ago_seconds} seconds ago, block headers cached: {block_header_count:,}"""
        except Exception as e:
            logger.exception(e)
            raise

    # Update the status of candle data
    @app.callback(Output('data-stats', "children"),
                  Input('interval-component', 'n_intervals'), Input('candle-dropdown', 'value'))
    def update_data_stats(n, current_candle_duration):
        logger.debug("update_data_stats(%d)", n)
        try:
            candles = candle_feeds[current_candle_duration].get_candles_by_pair(pair.address.lower())
            trade_count = len(trade_feed.trades_df)
            last_candle = candles.iloc[-1]
            timestamp_fmt = last_candle.timestamp.strftime("%Y-%m-%d, %H:%M:%S UTC")

            ago = datetime.datetime.utcnow() - last_candle.timestamp.to_pydatetime()
            ago_seconds = ago.total_seconds()

            trade_data_duration = candles.iloc[-1].timestamp - candles.iloc[0].timestamp

            return f"""Candles: {len(candles):,} last at {timestamp_fmt}, {ago_seconds}s ago, trades cached: {trade_count:,}, trade data availability: {trade_data_duration}"""
        except Exception as e:
            logger.exception(e)
            raise

    # Get the raw trades and convert them to
    # human readable table format
    @app.callback(Output('trades', "data"),
                  Input('interval-component', 'n_intervals'))
    def update_last_trades(n):
        logger.debug("update_last_trades(%d)", n)

        def get_pair_markdown(pair_id: str) -> str:
            # TODO: We are hardcoded to a single pair here
            pair_name = f"{pair.get_base_token().symbol} - {pair.get_quote_token().symbol}"
            pair_link = f"https://tradingstrategy.ai/search?q={pair.address}"
            return f"[{pair_name}]({pair_link})"

        try:
            df = trade_feed.get_latest_trades(5, pair.address.lower())
            df = df.sort_values("timestamp", ascending=False)
            quote_token = pair.get_quote_token().symbol
            output = pd.DataFrame()
            output["Block number"] = df["block_number"]
            output["Pair"] = df["pair"].apply(get_pair_markdown)
            #output["Transaction"] =
            output["Transaction"] = df["tx_hash"].apply(lambda tx_hash: f"[{tx_hash}]({chain_id.get_tx_link(tx_hash)})")
            # TODO: Check values here for non-stablecoin nominated tokens
            output["Price USD"] = df["price"]
            output["Amount USD"] = df["amount"]
            if quote_token not in ("BUSD", "USDC", "USDT"):
                output[f"Price {quote_token}"] = df["price"] / df["exchange_rate"]
                output[f"Exchange rate USD/{quote_token}"] = df["exchange_rate"]

            price = trade_feed.get_latest_price(pair.address.lower())
            logger.info("Current price is: %s %s/%s", price, pair.get_quote_token().symbol, pair.get_base_token().symbol)

            return output.to_dict("records")
        except Exception as e:
            logger.error("update_last_trades() error: %s", e)
            logger.exception(e)
            sys.exit(1)

    # Update the candle charts for the currently selected pair
    @app.callback(Output('live-update-graph', 'figure'),
                  [Input('interval-component', 'n_intervals'), Input('candle-dropdown', 'value')])
    def update_ohlcv_chart_live(n, current_candle_duration):
        logger.debug("update_ohlcv_chart_live(%s)", n)
        try:
            candles = candle_feeds[current_candle_duration].get_candles_by_pair(pair.address.lower())
            if len(candles) > 0:
                logger.info("Drawing %d candles", len(candles))
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


def setup_logging(log_level: str):
    """Setup logging.

    Use colored logs for nicer readability.
    """
    level = logging.getLevelName(log_level.upper())

    # Set log format to dislay the logger name to hunt down verbose logging modules
    fmt = "%(asctime)s %(name)-50s %(levelname)-8s %(message)s"

    # Use colored logging output for console
    coloredlogs.install(level=level, fmt=fmt, logger=logger)

    # Silencio!
    logging.getLogger("gql").setLevel(logging.WARNING)
    logging.getLogger("futureproof.executors").setLevel(logging.WARNING)


# https://github.com/tiangolo/typer/issues/511#issuecomment-1331692007
app = typer.Typer(context_settings={
    "max_content_width": shutil.get_terminal_size().columns
})

@app.command()
def main(
        json_rpc_url: str = typer.Option(..., help="Connect to EVM blockchain using this JSON-RPC node URL"),
        pair_address: str = typer.Option(..., help="Address of Uniswap v2 compatible pair contract"),
        log_level: str = typer.Option("info", help="Python logging level"),
        clear_cache: bool = typer.Option(False, is_flag=True, help="Clear the block header and trade disk cache at start"),
):
    """Render real-time price chart for Uniswap v2 compatible DEX.

    Show a price chart of one trading pair with different candle durations.

    By default, the example loads 24 hours worth of data over JSON-RPC connection.
    It may take several minutes depending on your blockchain node setup.
    The script can automatically resume downloading data if it crashes or is shut down.
    """

    # Get rid of pesky Pandas FutureWarnings
    disable_pandas_warnings()

    setup_logging(log_level)

    # No Ethereum checksum addresses
    pair_address = pair_address.lower()

    # Setup the fake blockchain data generator
    chain_id, data_refresh_frequency, candle_feeds, trade_feed = setup_uniswap_v2_market_data_feeds(
        json_rpc_url,
        pair_address,
        CANDLE_OPTIONS,
    )
    pairs = trade_feed.pairs

    cache_path = os.path.expanduser("~/.cache/uniswap-v2-candle-demo")

    store = DirectFeedStore(Path(cache_path), DATASET_PARTITION_SIZE)

    if clear_cache:
        logger.info("Clearing the cache: %s", cache_path)
        store.clear()
    else:
        if store.load_trade_feed(trade_feed):
            logger.info("Loaded old data from %s", cache_path)
        else:
            logger.info("First run, cache is empty %s", cache_path)

    # Buffer the block data before starting the GUI application.
    # Display interactive tqdm progress bar.
    blocks_needed = int(BUFFER_HOURS * 3600 // data_refresh_frequency) + 1

    last_save = 0
    save_frequency = 10

    def save_hook() -> Tuple[int, int]:
        nonlocal last_save
        nonlocal save_frequency
        if time.time() - last_save > save_frequency:
            last_saved_tuple = store.save_trade_feed(trade_feed)
            last_save = time.time()
            return last_saved_tuple
        return 0, 0

    pair: PairDetails = pairs[0]
    pair_details = trade_feed.get_pair_details(pair)

    # Fill the trade buffer with data
    # and create the initial candles
    logger.info("Backfilling blockchain data buffer for %f hours, %d blocks", BUFFER_HOURS, blocks_needed)
    delta = trade_feed.backfill_buffer(blocks_needed, tqdm, save_hook)
    for feed in candle_feeds.values():
        feed.apply_delta(delta)
        for df in feed.iterate_pairs():
            make_candle_labels(
                df,
                dollar_prices=False,
                base_token_name=pair_details.get_base_token().symbol,
                quote_token_name=pair_details.get_quote_token().symbol,
            )

    # Save that we do not need to backfill again
    store.save_trade_feed(trade_feed)

    price = trade_feed.get_latest_price(pair)
    logger.info("Current price is: %s %s/%s", price, pair_details.get_quote_token().symbol, pair_details.get_base_token().symbol)

    # Start blockchain data processor bg thread
    logger.info("Starting blockchain data consumer, block time is %f seconds", data_refresh_frequency)
    candle_bg_thread = Thread(
        target=start_block_consumer_thread,
        args=(data_refresh_frequency, pair_details, store, trade_feed, candle_feeds))
    candle_bg_thread.start()

    # Create the Dash web UI and start the web server
    app = setup_app(
        chain_id,
        pair_details,
        data_refresh_frequency,
        trade_feed,
        candle_feeds)
    app.run_server(debug=True)


if __name__ == '__main__':
    app()
