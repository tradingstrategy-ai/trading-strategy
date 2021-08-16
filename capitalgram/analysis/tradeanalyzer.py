"""Analyze spot trades: figure out how we won or lost trades.

A simplified trade analysis that only understands spots buys and sells, not margined trading or short positions.
Unlike Backtrader, this one is good for multiasset portfolio analysis.
"""

import datetime
import enum
from dataclasses import dataclass, field
from typing import List, Dict, Iterable, Optional

import pandas as pd

from capitalgram.exchange import ExchangeUniverse
from capitalgram.pair import PairUniverse, PandasPairUniverse
from capitalgram.types import PrimaryKey, USDollarAmount


@dataclass
class SpotTrade:
    """Track spot trades to construct position performance.

    For sells, quantity is negative.
    """
    timestamp: pd.Timestamp
    price: USDollarAmount
    quantity: float
    commission: USDollarAmount
    slippage: USDollarAmount

    def is_buy(self):
        return self.quantity > 0

    def is_sell(self):
        return self.quantity < 0

    @property
    def value(self) -> USDollarAmount:
        return  abs(self.price * self.quantity)


@dataclass
class TradePosition:
    """How a particular asset traded.

    Each asset can have multiple entries (buys) and exits (sells)

    For a simple strategies there can be only one or two trades per position.

    * Enter (buy)

    * Exit (sell optionally)
    """
    trades: List[SpotTrade] = field(default_factory=list)

    #: Closing the position could be deducted from the trades themselves,
    #: but we cache it by hand to speed up processing
    closed_at: Optional[pd.Timestamp] = None

    def is_open(self):
        return self.closed_at is None

    def is_closed(self):
        return not self.is_open()

    @property
    def opened_at(self) -> pd.Timestamp:
        return self.trades[0].timestamp

    @property
    def open_quantity(self) -> float:
        return sum([t.quantity for t in self.trades])

    @property
    def open_value(self) -> float:
        return sum([t.value for t in self.trades])

    @property
    def open_price(self) -> float:
        """At what price we opened this position.

        Supports only simple enter/exit positions.
        """
        buys = list(self.buys)
        assert len(buys) == 1
        return buys[0].price

    @property
    def close_price(self) -> float:
        """At what price we opened this position.

        Supports only simple enter/exit positions.
        """
        sells = list(self.sells)
        assert len(sells) == 1
        return sells[0].price

    @property
    def buys(self) -> Iterable[SpotTrade]:
        return [t for t in self.trades if t.is_buy()]

    @property
    def sells(self) -> Iterable[SpotTrade]:
        return [t for t in self.trades if t.is_sell()]

    @property
    def buy_value(self) -> USDollarAmount:
        return sum([t.value for t in self.trades if t.is_buy()])

    @property
    def sell_value(self) -> USDollarAmount:
        return sum([t.value for t in self.trades if t.is_sell()])

    @property
    def realised_profit(self) -> USDollarAmount:
        """Calculated life-time profit over this position."""
        assert not self.is_open()
        return -sum([t.quantity * t.price for t in self.trades])

    @property
    def realised_profit_percent(self) -> float:
        """Calculated life-time profit over this position."""
        assert not self.is_open()
        buy_value = self.buy_value
        sell_value = self.sell_value
        return sell_value / buy_value - 1

    def is_win(self):
        """Did we win this trade."""
        assert not self.is_open()
        return self.realised_profit > 0

    def is_lose(self):
        assert not self.is_open()
        return self.realised_profit < 0

    def add_trade(self, t):
        if self.trades:
            last_trade = self.trades[-1]
            assert t.timestamp > last_trade.timestamp, f"Tried to do trades in wrong order. Last: {last_trade}, got {t}"
        self.trades.append(t)

    def can_trade_close_position(self, t: SpotTrade):
        assert self.is_open()
        if not t.is_sell():
            return False
        open_quantity = self.open_quantity
        closing_quantity = -t.quantity
        assert closing_quantity <= open_quantity, "Cannot sell more than we have in balance sheet"
        return closing_quantity == open_quantity


@dataclass
class AssetTradeHistory:
    """How a particular asset traded.

    Each position can have increments or decrements.
    When position is decreased to zero, it is considered closed, and a new buy open a new position.
    """
    positions: List[TradePosition] = field(default_factory=list)

    def get_first_opened_at(self) -> Optional[pd.Timestamp]:
        if self.positions:
            return self.positions[0].opened_at
        return None

    def get_last_closed_at(self) -> Optional[pd.Timestamp]:
        for position in reversed(self.positions):
            if not position.is_open():
                return position.closed_at

        return None

    def add_trade(self, t: SpotTrade):
        """Adds a new trade to the asset history.

        If there is an open position the trade is added against this,
        otherwise a new position is opened for tracking.
        """
        current_position = None
        if self.positions:
            if self.positions[-1].is_open():
                current_position = self.positions[-1]

        if current_position:
            if current_position.can_trade_close_position(t):
                # Close the existing position
                current_position.closed_at = t.timestamp
                current_position.add_trade(t)
                assert current_position.open_quantity == 0
            else:
                # Add to the existing position
                current_position.add_trade(t)
        else:
            # Open new position
            new_position = TradePosition()
            new_position.add_trade(t)
            self.positions.append(new_position)


@dataclass
class TradeSummary:
    """Some generic statistics over all the trades"""
    won: int
    lost: int
    zero_loss: int
    undecided: int
    realised_profit: USDollarAmount



class TimelineEventType(enum.Enum):
    """Currently supporting only spot open and close, no incremental events."""
    open = "open"
    close = "cloes"


@dataclass
class TimelineEvent:
    pair_id: PrimaryKey
    position: TradePosition
    type: TimelineEventType

    @property
    def price(self):
        """The price on which this event happened.

        If open (enter) then the buy price. If close (exit) then the sell price.
        """
        if self.type == TimelineEventType.open:
            return self.position.open_price
        else:
            return self.position.close_price


@dataclass
class TradeAnalyzer:
    """Analysis of trades in a portfolio."""

    #: How a particular asset traded. Asset id -> Asset history mapping
    asset_histories: Dict[object, AssetTradeHistory] = field(default_factory=dict)

    def get_first_opened_at(self) -> Optional[pd.Timestamp]:
        def all_opens():
            for history in self.asset_histories.values():
                yield history.get_first_opened_at()

        return min(all_opens())

    def get_last_closed_at(self) -> Optional[pd.Timestamp]:
        def all_closes():
            for history in self.asset_histories.values():
                closed = history.get_last_closed_at()
                if closed:
                    yield closed
        return max(all_closes())

    def get_all_positions(self) -> Iterable[TradePosition]:
        """Return open and closed positions over all traded assets."""
        for history in self.asset_histories.values():
            yield from history.positions

    def calculate_summary_statistics(self) -> TradeSummary:
        """Calculate some statistics how our trades went.
        """
        won = lost = zero_loss = undecided = 0
        profit: USDollarAmount = 0
        for position in self.get_all_positions():
            if position.is_open():
                undecided += 1
                continue

            if position.is_win():
                won += 1
            elif position.is_lose():
                lost += 1
            else:
                # Any profit exactly balances out loss in slippage and commission
                zero_loss += 1

            profit += position.realised_profit

        return TradeSummary(
            won=won,
            lost=lost,
            zero_loss=zero_loss,
            undecided=undecided,
            realised_profit=profit,
        )

    def create_timeline(self) -> pd.DataFrame:
        """Create a timeline feed how we traded over a course of time.

        Note: We assume each position has only one enter and exit event, not position increases over the lifetime.

        :return: DataFrame with timestamp and timeline_event columns
        """

        # https://stackoverflow.com/questions/42999332/fastest-way-to-convert-python-iterator-output-to-pandas-dataframe
        def gen_events():
            """Generate data for the dataframe.

            Use Python generators to dynamically fill Pandas dataframe.
            Each dataframe gets timestamp, timeline_event columns.
            """
            for pair_id, history in self.asset_histories.items():
                for position in history.positions:
                    open_event = TimelineEvent(
                        pair_id=pair_id,
                        position=position,
                        type=TimelineEventType.open,
                    )
                    yield (position.opened_at, open_event)

                    # If position is closed generated two events
                    if position.is_closed():
                        close_event = TimelineEvent(
                            pair_id=pair_id,
                            position=position,
                            type=TimelineEventType.close,
                        )
                        yield (position.closed_at, close_event)

        df = pd.DataFrame(gen_events(), columns=["timestamp", "timeline_event"])
        df = df.set_index(["timestamp"])
        return df


def expand_timeline(exchange_universe: ExchangeUniverse, pair_universe: PandasPairUniverse, timeline: pd.DataFrame) -> pd.DataFrame:
    """Expand trade history timeline to human readable table.

    This will the outputting much easier in Python Notebooks.

    Currently does not incrementing/decreasing positions gradually.

    :return: DataFrame with human readable position win/loss information, having DF indexed by timestamps
    """

    # https://stackoverflow.com/a/52363890/315168
    def expander(row):
        tle: TimelineEvent = row["timeline_event"]
        # timestamp = row.name  # ???
        pair_id = tle.pair_id
        pair_info = pair_universe.get_pair_by_id(pair_id)
        exchange = exchange_universe.get_by_id(pair_info.exchange_id)
        r = {
            # "timestamp": timestamp,
            "exchange": exchange.name,
            "base": pair_info.base_token_symbol,
            "quote": pair_info.quote_token_symbol,
            "price": tle.price,
        }
        if tle.type == TimelineEventType.close:
            r["event"] = "Closed"
            r["profit"] = tle.position.realised_profit
            r["profit_pct"] = tle.position.realised_profit_percent
            r["closed_value"] = tle.position.sell_value
            r["price"] = tle.position.close_price
            if tle.position.is_win():
                r["won"] = 1
            else:
                r["lost"] = 1
        else:
            r["event"] = "Opened"
            r["opened_value"] = tle.position.buy_value
            r["price"] = tle.position.open_price
        return r

    applied_df = timeline.apply(expander, axis='columns', result_type='expand')

    # https://stackoverflow.com/a/52720936/315168
    applied_df\
        .rename_axis('timestamp')\
        .sort_values(by=['timestamp', 'event'], ascending=[True, True], inplace=True)
    return applied_df







