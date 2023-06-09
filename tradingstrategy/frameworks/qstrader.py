""""Qstrader integration (legacy).

.. warning::

    Deprecated. Do not use anymore. Use `trade-executor` framework instead.
"""
import logging
import functools
from typing import List, Dict

import pytz
import pandas as pd
import numpy as np

from tradingstrategy.analysis.portfolioanalyzer import PortfolioAnalyzer, PortfolioSnapshot, AssetSnapshot
from tradingstrategy.analysis.tradeanalyzer import AssetTradeHistory, SpotTrade, TradeAnalyzer
from qstrader import settings
from qstrader.asset.asset import Asset


from tradingstrategy.candle import GroupedCandleUniverse
from tradingstrategy.exchange import ExchangeUniverse
from tradingstrategy.pair import DEXPair, LegacyPairUniverse, PandasPairUniverse
from qstrader.broker.portfolio.portfolio_event import PortfolioEvent
from qstrader.broker.transaction.transaction import Transaction
from tradingstrategy.timebucket import TimeBucket
from tradingstrategy.types import PrimaryKey

logger = logging.getLogger(__name__)


class DEXAsset(Asset):

    def __init__(
        self,
        pair_info: DEXPair,
    ):
        self.cash_like = False
        self.pair_info = pair_info

    def __repr__(self):
        """
        String representation of the Equity Asset.
        """
        return f"<DEXAsset {self.pair_info}>"


def prepare_candles_for_qstrader(candles: pd.DataFrame) -> pd.DataFrame:
    """Prepare DataFrame format so that QSTrader strategy can read it.

    QSTrader assumes Yahoo Finance CSV file format with the following columns:

    `Date,Open,High,Low,Close,Adj Close,Volume`
    """
    candles = candles.rename(columns={
        "open": "Open",
        "close": "Close",
        "high": "High",
        "low": "Low",
        "timestamp": "Date",
    })

    # Our index must be the timestamp
    candles = candles.set_index(pd.DatetimeIndex(candles["Date"]))

    # Create volume column
    candles["Volume"] = candles["buy_volume"] + candles["sell_volume"]
    candles["Adj Close"] = candles["Close"]

    return candles



class TradingStrategyDataSource:
    """QSTrader daily price integration for Capitalgram dataframe object."""

    def __init__(self,
                exchange_universe: ExchangeUniverse,
                pair_universe: PandasPairUniverse,
                candle_universe: GroupedCandleUniverse,
                price_look_back_candles=5):
        """

        :param exchange_universe:
        :param pair_universe:
        :param candle_universe:
        :param price_look_back_candles: For low liquidity assets that may have no data at the certain data point, how many candles look back t
        """

        # These are column names that QSTrader expects
        assert "Date" in candle_universe.get_columns()
        assert "Open" in candle_universe.get_columns()
        assert "Close" in candle_universe.get_columns()

        self.exchange_universe = exchange_universe
        self.pair_universe = pair_universe
        self.candle_universe = candle_universe
        self.asset_bar_frames = {pair_id: df for pair_id, df in candle_universe.get_all_pairs()}
        self.asset_type = DEXAsset
        self.adjust_prices = False
        self.asset_bid_ask_frames = self._convert_bars_into_bid_ask_dfs()
        # self.asset_bid_ask_frames = self._convert_bars_into_bid_ask_dfs()

        # For low liquidt y
        self.price_look_back_candles = price_look_back_candles

    def _convert_bar_frame_into_bid_ask_df(self, bar_df):
        """
        Converts the DataFrame from daily OHLCV 'bars' into a DataFrame
        of open and closing price timestamps.

        Optionally adjusts the open/close prices for corporate actions
        using any provided 'Adjusted Close' column.

        Parameters
        ----------
        `pd.DataFrame`
            The daily 'bar' OHLCV DataFrame.

        Returns
        -------
        `pd.DataFrame`
            The individually-timestamped open/closing prices, optionally
            adjusted for corporate actions.
        """
        bar_df = bar_df.sort_index()
        if self.adjust_prices:
            if 'Adj Close' not in bar_df.columns:
                raise ValueError(
                    "Unable to locate Adjusted Close pricing column in CSV data file. "
                    "Prices cannot be adjusted. Exiting."
                )

            # Restrict solely to the open/closing prices
            oc_df = bar_df.loc[:, ['Open', 'Close', 'Adj Close']]

            # Adjust opening prices
            oc_df['Adj Open'] = (oc_df['Adj Close'] / oc_df['Close']) * oc_df['Open']
            oc_df = oc_df.loc[:, ['Adj Open', 'Adj Close']]
            oc_df.columns = ['Open', 'Close']
        else:
            oc_df = bar_df.loc[:, ['Open', 'Close']]

        # Convert bars into separate rows for open/close prices
        # appropriately timestamped
        seq_oc_df = oc_df.T.unstack(level=0).reset_index()
        seq_oc_df.columns = ['Date', 'Market', 'Price']
        seq_oc_df.loc[seq_oc_df['Market'] == 'Open', 'Date'] += pd.Timedelta(hours=14, minutes=30)
        seq_oc_df.loc[seq_oc_df['Market'] == 'Close', 'Date'] += pd.Timedelta(hours=21, minutes=00)

        # TODO: Make this more DEX compatible, now assume
        # Close price on everything
        dp_df = seq_oc_df[['Date', 'Price']]
        #import ipdb ; ipdb.set_trace()
        dp_df['Bid'] = dp_df['Price']
        dp_df['Ask'] = dp_df['Price']
        dp_df = dp_df.loc[:, ['Date', 'Bid', 'Ask']].fillna(method='ffill').set_index('Date').sort_index()
        return dp_df

    def _convert_bars_into_bid_ask_dfs(self):
        """
        Convert all of the daily OHLCV 'bar' based DataFrames into
        individually-timestamped open/closing price DataFrames.

        Returns
        -------
        `dict{pd.DataFrame}`
            The converted DataFrames.
        """
        if settings.PRINT_EVENTS:
            logger.debug("Adjusting pricing in CSV files...")
        asset_bid_ask_frames = {}
        for asset_symbol, bar_df in self.asset_bar_frames.items():
            if settings.PRINT_EVENTS:
                # logger.debug("Adjusting CSV file for symbol '%s'...", asset_symbol)
                pass
            asset_bid_ask_frames[asset_symbol] = self._convert_bar_frame_into_bid_ask_df(bar_df)
        return asset_bid_ask_frames

    def get_price(self, dt: pd.Timestamp, pair_id: PrimaryKey, ohlc="Open", complain=False) -> float:
        """Get a price for a trading pair base pair from candle data.

        If there is no candle (no trades at the day), look for a previous day.
        """
        assert complain, "Get rid of bad data accesses"

        dt = dt.replace(hour=0, minute=0)
        pair = self.pair_universe.get_pair_by_id(pair_id)
        if not pair:
            raise RuntimeError(f"Tried to access unknown pair {pair_id}")
        candles = self.candle_universe.get_candles_by_pair(pair_id)

        if len(candles) == 0:
            raise RuntimeError(f"Pair has no candles {pair}")

        ohlc_value = candles[ohlc]

        first_attempt_ts = dt
        for attempt in range(self.price_look_back_candles):
            try:
                val = ohlc_value[dt]
                return val
            except KeyError:
                # Try candle at previous timestamp
                bucket: TimeBucket = self.candle_universe.time_bucket
                dt -= bucket.to_timedelta()

        if complain:
            raise RuntimeError(f"Pair {pair} has no price using candles at {first_attempt_ts}, tried range {dt} - {first_attempt_ts}")

        return np.NaN

    @functools.lru_cache(maxsize=1024 * 1024)
    def get_bid(self, dt: pd.Timestamp, pair_id: PrimaryKey, complain=False) -> float:
        """Get a bid price for an asset at a certain timestamp.

        LIMITATIONS:
        - Assume using daily bars
        - Use opening price of each candle

        :param complain: Do not fail silently on data gaps
        """
        return self.get_price(dt, pair_id, "Open", complain)

    @functools.lru_cache(maxsize=1024 * 1024)
    def get_ask(self, dt: pd.Timestamp, pair_id: PrimaryKey, complain=False) -> float:
        return self.get_price(dt, pair_id, "Open", complain)

    def get_assets_historical_closes(self, start_dt, end_dt, assets):
        """
        Obtain a multi-asset historical range of closing prices as a DataFrame,
        indexed by timestamp with asset symbols as columns.

        Parameters
        ----------
        start_dt : `pd.Timestamp`
            The starting datetime of the range to obtain.
        end_dt : `pd.Timestamp`
            The ending datetime of the range to obtain.
        assets : `list[str]`
            The list of asset symbols to obtain closing prices for.

        Returns
        -------
        `pd.DataFrame`
            The multi-asset closing prices DataFrame.
        """
        close_series = []
        for asset in assets:
            if asset in self.asset_bar_frames.keys():
                asset_close_prices = self.asset_bar_frames[asset][['Close']]
                asset_close_prices.columns = [asset]
                close_series.append(asset_close_prices)

        prices_df = pd.concat(close_series, axis=1).dropna(how='all')
        prices_df = prices_df.loc[start_dt:end_dt]
        return prices_df


def create_portfolio_snapshot(state_details: Dict) -> PortfolioSnapshot:
    """Convert QSTrader internal debug_details trace to a portfolio snapshot."""
    assert state_details

    portfolios = state_details["broker"]["portfolios"]
    assert len(portfolios) == 1, "We support analysing only 1 portfolio runs for now"

    asset_snapshots = {}

    portfolio = portfolios["000001"]
    for pair_id, asset_data in portfolio["assets"].items():
        asset_snapshots[pair_id] = AssetSnapshot(
            quantity=asset_data["quantity"],
            market_value=asset_data["market_value"],
            realised_pnl=float(asset_data["realised_pnl"]),   # Convert from numpy.float64
            unrealised_pnl=float(asset_data["unrealised_pnl"]),
            total_pnl=float(asset_data["total_pnl"]),
        )

    assert portfolio["currency"] == "USD", "Supporting USD only for now"
    cash_balances = {
        "USD": portfolio["cash"]
    }

    return PortfolioSnapshot(
        tick=state_details["event_index"],
        cash_balances=cash_balances,
        asset_snapshots=asset_snapshots,
        state_details=state_details,
    )


def analyse_trade_history(events: List[PortfolioEvent]) -> TradeAnalyzer:
    """Build algorithm performance analyzers from QSTrader backtesting events."""

    histories = {}
    snapshots = {}

    trade_id = 1

    for e in events:

        txn: Transaction = e.txn

        # Portfolio generates multiple prevents, but transaction is only present in buys and sells.
        # QSTrader PortfolioEvents are separated by event.description string
        if txn:

            # Build the trading history and different positions
            debug_details: Dict = txn.debug_details
            assert debug_details

            pair_id = txn.asset
            assert type(pair_id) == int
            history = histories.get(pair_id)
            if not history:
                history = histories[pair_id] = AssetTradeHistory()

            trade = SpotTrade(
                pair_id=pair_id,
                trade_id=trade_id,
                timestamp=txn.dt,
                price=txn.price,
                quantity=txn.quantity,
                commission=0,
                slippage=0,
                state_details=debug_details,
            )
            assert txn.quantity
            assert txn.price
            history.add_trade(trade)
            trade_id += 1

    trade_analyzer = TradeAnalyzer(asset_histories=histories)

    return trade_analyzer


def analyse_portfolio_development(events: List[dict]) -> PortfolioAnalyzer:
    """Build algorithm performance analyzers from QSTrader backtesting events."""

    snapshots = {}

    for e in events:
        # Add the portfolio snapshot to the histories if we do not have it yet.
        # Because we can have multiple transactions per day, we just take the snapshot from the first transaction.
        event_ts = e["timestamp"]
        snapshots[event_ts] = create_portfolio_snapshot(e)

    portfolio_analyzer = PortfolioAnalyzer(snapshots=snapshots)

    return portfolio_analyzer
