"""Analyse the portfolio development over the time.

- Portfolio situation at the start of the each tick
- Currently held assets
- Net asset value (NAV)
- Asset valuation change
"""

from dataclasses import dataclass, field
from typing import List, Dict, Iterable, Optional, Tuple, Callable

import numpy as np
import pandas as pd
from pandas.io.formats.style import Styler

from tradingstrategy.exchange import ExchangeUniverse
from tradingstrategy.pair import DEXPair, PairUniverse
from tradingstrategy.types import USDollarAmount, PrimaryKey


@dataclass
class AssetSnapshot:
    """Asset holdings at a specific timepoint."""

    #: How many tokens of the asset we had
    quantity: float

    #: Daily market value of the tokens
    market_value: USDollarAmount

    realised_pnl: USDollarAmount

    unrealised_pnl: USDollarAmount

    total_pnl: USDollarAmount

    # https://docs.python.org/3/library/dataclasses.html#post-init-processing
    def __post_init__(self):
        assert self.quantity > 0, f"Bad quantity {self.quantity}"
        assert self.market_value > 0, f"Bad market value {self.market_value}"
        assert type(self.total_pnl) == float
        assert type(self.realised_pnl) == float
        assert type(self.unrealised_pnl) == float


@dataclass
class PortfolioSnapshot:
    """Represents the portfolio status at the start of the day/candle snapshot"""

    #: A running counter where the first backtest simulated event is tick 1, the next one tick 2
    tick: int

    #: What reserve currencies we have.
    #: E.g. USD: 10_000
    #: Resever currenecies expressed as strings for the backwards compatibiltiy.
    cash_balances: Dict[str, float]

    #: What reserve currencies we have.
    #: E.g. USD: 10_000
    #: Resever currenecies expressed as strings for the backwards compatibiltiy.
    asset_snapshots: Dict[PrimaryKey, AssetSnapshot]

    #: Internal state dump of the algorithm when this trade was made.
    #: This is mostly useful when doing the trade analysis try to understand
    #: why some trades were made.
    #: It also allows you to reconstruct the portfolio state over the time.
    state_details: Optional[Dict] = None

    def get_ordered_assets_stable(self) -> List[Tuple[PrimaryKey, AssetSnapshot]]:
        """Return asset snapshots in a stable order between days.
        """
        assets = [(pair_id, s) for pair_id, s in self.asset_snapshots.items()]
        assets.sort(key=lambda a: a[0])
        return assets

    def get_ordered_assets_by_weight(self) -> List[Tuple[PrimaryKey, AssetSnapshot]]:
        """Return asset snapshots in an order where the heaviest asset is first.
        """
        assets = [(pair_id, s) for pair_id, s in self.asset_snapshots.items()]
        assets.sort(key=lambda a: a[1].market_value, reverse=True)
        return assets


@dataclass
class PortfolioAnalyzer:
    """Represents the portfolio analysis over the backtest period."""

    snapshots: Dict[pd.Timestamp, PortfolioSnapshot]

    def get_max_assets_held_once(self) -> int:
        """Find out what was the max number of assets the strategy was holding at the same time."""
        max_hold = 0
        for s in self.snapshots.values():
            max_hold = max(max_hold, len(s.asset_snapshots))
        return max_hold


def expand_snapshot_to_row(
        exchange_universe: ExchangeUniverse,
        pair_universe: PairUniverse,
        ts: pd.Timestamp,
        snapshot: PortfolioSnapshot,
        max_assets: int,
        timestamp_format: str) -> dict:
    """Create DataFrame rows from each portfolio snapshot."""

    # timestamp = row.name  # ???
    assert max_assets

    r = {
        # "timestamp": timestamp,
        "Id": snapshot.tick,
        "Holdings at": ts.strftime(timestamp_format),
        "NAV USD": 0,
        "Cash USD": f"{snapshot.cash_balances['USD']:,.0f}",
    }

    assets = snapshot.get_ordered_assets_by_weight()

    # Initialize empty asset columns
    for i in range(max_assets):
        idx = i + 1
        r[f"#{idx} asset"] = pd.NA
        r[f"#{idx} value"] = pd.NA
        r[f"#{idx} weight %"] = pd.NA
        r[f"#{idx} PnL"] = pd.NA
        r[f"#{idx} PnL raw"] = 0

    total_asset_value = 0
    for i, asset_tuple in enumerate(assets):
        idx = i + 1
        pair_id, asset_snapshot = asset_tuple
        pair = pair_universe.get_pair_by_id(pair_id)
        r[f"#{idx} asset"] = pair.base_token_symbol[0:8]  # Cut long ticker names
        r[f"#{idx} value"] = f"{asset_snapshot.market_value:,.0f}"
        r[f"#{idx} PnL"] = f"{asset_snapshot.total_pnl:,.2f}"
        r[f"#{idx} PnL raw"] = asset_snapshot.total_pnl
        assert type(asset_snapshot.total_pnl) == float
        total_asset_value += asset_snapshot.market_value

    for i, asset_tuple in enumerate(assets):
        idx = i + 1
        pair_id, asset_snapshot = asset_tuple
        value = r[f"#{idx} value"]
        if value:
            r[f"#{idx} weight %"] = f"{asset_snapshot.market_value / total_asset_value * 100:.0f}"

    r["NAV USD"] = f"{snapshot.cash_balances['USD'] + total_asset_value:,.2f}"

    return r


def expand_timeline(
        exchange_universe: ExchangeUniverse,
        pair_universe: PairUniverse,
        analyzer: PortfolioAnalyzer,
        create_html_styles=True,
        vmin=-0.3,
        vmax=0.2,
        timestamp_format="%Y-%m-%d",
) -> pd.DataFrame:
    """Console output for the portfolio development over the time.

    Each row presents the portfolio status at the end of the day/candle.

    The outputted data frame is intented to be human readable and not for programmatic manipulation.

    :param create_html_styles: Create a formatter function that can be applied to hide and recolour columns.

    :param vmax: Trade success % to have the extreme green color.

    :param vmin: The % of lost capital on the trade to have the extreme red color.

    :param timestamp_format: How to format Opened at column, as passed to `strftime()`

    :return: pd.Dataframe rendering the portfolio development over the time
    """

    asset_column_count = analyzer.get_max_assets_held_once()

    raw_output = [expand_snapshot_to_row(exchange_universe, pair_universe, ts, s, asset_column_count, timestamp_format) for ts, s in analyzer.snapshots.items()]

    applied_df = pd.DataFrame(raw_output) # timeline.apply(expander, axis='columns', result_type='expand')

    # Sort portfoli snapshots by backtest tick events
    # https://stackoverflow.com/a/52720936/315168
    applied_df\
        .sort_values(by=['Id'], ascending=[True], inplace=True)

    # Get rid of NaN labels
    # https://stackoverflow.com/a/28390992/315168
    applied_df.fillna('', inplace=True)

    def apply_styles(df: pd.DataFrame):
        # Create a Pandas Styler with multiple styling options applied
        # https://www.geeksforgeeks.org/make-a-gradient-color-mapping-on-a-specified-column-in-pandas/
        # Dynamically color the background of trade outcome coluns # https://pandas.pydata.org/docs/reference/api/pandas.io.formats.style.Styler.background_gradient.html
        styles = df.style.hide_index()
        hidden_columns = []
        asset_colums = []
        for i in range(asset_column_count):
            # Add a background color for each asset column group
            idx = i + 1
            styles = styles.background_gradient(
                axis=0,
                subset=[f"#{idx} asset", f"#{idx} value", f"#{idx} PnL", f"#{idx} weight %"],
                gmap=applied_df[f"#{idx} PnL raw"],
                cmap='RdYlGn',
                vmin=vmin,  # We can only lose 100% of our money on position
                vmax=vmax)  # 50% profit is 21.5 position. Assume this is the max success color we can hit over

            # Add more border between assets
            # https://coderzcolumn.com/tutorials/python/simple-guide-to-style-display-of-pandas-dataframes
            # https://pandas.pydata.org/docs/reference/api/pandas.io.formats.style.Styler.set_table_styles.html
            hidden_columns.append(f"#{idx} PnL raw")
            asset_colums.append(f"#{idx} asset")

        # Build table col styles

        styles_dict = {
            # Don't break timestamp value to multiple lines
            "Holdings at": [{'selector': 'td', 'props': [('white-space', 'nowrap')]}],
        }

        # Format asset column groups
        for col in asset_colums:
            styles_dict[col] = [{'selector': 'td', 'props': [('border-left', '3px solid #888'), ("font-weight", "bold"), ("text-align", "left")]}]

        styles = styles.set_table_styles(styles_dict)
        styles = styles.hide_columns(hidden_columns)
        return styles

    if create_html_styles:
        return applied_df, apply_styles
    else:
        # Format for console
        for i in range(asset_column_count):
            idx = i + 1
            del applied_df[f"#{idx} PnL raw"]
        return applied_df,  None
