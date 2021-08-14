import logging
import functools

import pytz
import pandas as pd
import numpy as np
from qstrader import settings
from qstrader.asset.asset import Asset


from capitalgram.candle import GroupedCandleUniverse
from capitalgram.exchange import ExchangeUniverse
from capitalgram.pair import DEXPair, PairUniverse, PandasPairUniverse

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



class CapitalgramDataSource:
    """QSTrader daily price integration for Capitalgram dataframe object."""

    def __init__(self,
                exchange_universe: ExchangeUniverse,
                pair_universe: PandasPairUniverse,
                candle_universe: GroupedCandleUniverse):

        # These are column names that QSTrader expects
        assert "Date" in candle_universe.get_columns()
        assert "Open" in candle_universe.get_columns()
        assert "Close" in candle_universe.get_columns()

        self.exchange_universe = exchange_universe
        self.pair_universe = pair_universe
        self.asset_bar_frames = {pair_id: df for pair_id, df in candle_universe.get_all_pairs()}
        self.asset_type = DEXAsset
        self.adjust_prices = False
        self.asset_bid_ask_frames = self._convert_bars_into_bid_ask_dfs()
        # self.asset_bid_ask_frames = self._convert_bars_into_bid_ask_dfs()

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

        # TODO: Unable to distinguish between Bid/Ask, implement later
        dp_df = seq_oc_df[['Date', 'Price']]
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
                logger.debug("Adjusting CSV file for symbol '%s'...", asset_symbol)
            asset_bid_ask_frames[asset_symbol] = \
                self._convert_bar_frame_into_bid_ask_df(bar_df)
        return asset_bid_ask_frames

    @functools.lru_cache(maxsize=1024 * 1024)
    def get_bid(self, dt, asset):
        """
        Obtain the bid price of an asset at the provided timestamp.

        Parameters
        ----------
        dt : `pd.Timestamp`
            When to obtain the bid price for.
        asset : `str`
            The asset symbol to obtain the bid price for.

        Returns
        -------
        `float`
            The bid price.
        """
        bid_ask_df = self.asset_bid_ask_frames[asset]
        try:
            bid = bid_ask_df.iloc[bid_ask_df.index.get_loc(dt, method='pad')]['Bid']
        except KeyError:  # Before start date
            return np.NaN
        return bid

    @functools.lru_cache(maxsize=1024 * 1024)
    def get_ask(self, dt: pd.Timestamp, asset: int):
        """
        Obtain the ask price of an asset at the provided timestamp.

        Parameters
        ----------
        dt : `pd.Timestamp`
            When to obtain the ask price for.
        asset : `str`
            The asset symbol to obtain the ask price for.

        Returns
        -------
        `float`
            The ask price.
        """

        assert type(dt.tz) != pytz.UTC, "Trying to get rid of timestamps in the processing as it is slowing down"

        bid_ask_df = self.asset_bid_ask_frames[asset]
        try:
            # This tries to interpolate missing data? It is veery slow
            # ask = bid_ask_df.iloc[bid_ask_df.index.get_loc(dt, method='pad')]['Ask']
            ask = bid_ask_df["Ask"][dt]
        except KeyError:  # Before start date
            pair_info = self.pair_universe.get_pair_by_id(asset)
            asset_name = pair_info.get_friendly_name(self.exchange_universe)
            raise RuntimeError(f"Tried to get price for an asset that does not have a price yet: {asset_name} at {dt}")
            # return np.NaN
        return ask

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

