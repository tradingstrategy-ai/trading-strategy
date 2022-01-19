"""Analyse the portfolio development over the time."""

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

    def get_ordered_assets(self) -> List[Tuple[PrimaryKey, AssetSnapshot]]:
        """Return asset snapshots in a stable order between days.
        """
        assets = [(pair_id, s) for pair_id, s in self.asset_snapshots.items()]
        assets.sort(key=lambda a: a[0])
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
        max_assets: int) -> dict:
    """Create DataFrame rows from each portfolio snapshot."""

    # timestamp = row.name  # ???
    assert max_assets

    r = {
        # "timestamp": timestamp,
        "Id": snapshot.tick,
        "At": ts,
        "NAV": 0,
        "Cash USD": snapshot.cash_balances["USD"],
    }

    # Initialize empty asset columns
    for i in range(max_assets):
        idx = i + 1
        r[f"#{idx} asset"] = pd.NA
        r[f"#{idx} value"] = pd.NA
        r[f"#{idx} PnL"] = pd.NA

    total_asset_value = 0
    for i, asset_tuple in enumerate(snapshot.get_ordered_assets()):
        idx = i + 1
        pair_id, asset_snapshot = asset_tuple
        pair = pair_universe.get_pair_by_id(pair_id)
        r[f"#{idx} asset"] = pair.base_token_symbol
        r[f"#{idx} value"] = asset_snapshot.market_value
        r[f"#{idx} PnL"] = asset_snapshot.total_pnl
        total_asset_value += asset_snapshot.market_value

    r["NAV"] = f"{snapshot.cash_balances['USD'] + total_asset_value:,.2f}"

    return r


def expand_timeline(
        exchange_universe: ExchangeUniverse,
        pair_universe: PairUniverse,
        analyzer: PortfolioAnalyzer,
        vmin=-0.3,
        vmax=0.2,
) -> pd.DataFrame:
    """Console output for the portfolio development over the time.

    :return: pd.Dataframe rendering the portfolio development over the time
    """

    asset_column_count = analyzer.get_max_assets_held_once()

    raw_output = [expand_snapshot_to_row(exchange_universe, pair_universe, ts, s, asset_column_count) for ts, s in analyzer.snapshots.items()]

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
        for i in range(asset_column_count):
            # Add a background color for each asset column group
            idx = i + 1
            styles = styles.background_gradient(
                axis=0,
                subset=[f"#{idx} asset", f"#{idx} value", f"#{idx} PnL"],
                gmap=applied_df[f"#{idx} PnL"],
                cmap='RdYlGn',
                vmin=vmin,  # We can only lose 100% of our money on position
                vmax=vmax)  # 50% profit is 21.5 position. Assume this is the max success color we can hit over
        return styles

    return applied_df, apply_styles
