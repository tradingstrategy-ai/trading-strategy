"""Price impact calculations.


"""
import enum

import pandas as pd

from dataclasses import dataclass, asdict

from tradingstrategy.liquidity import GroupedLiquidityUniverse
from tradingstrategy.types import PrimaryKey, USDollarAmount


class LiquidityDataMissing(Exception):
    """We try to get a price impact for a pair for which we have no data."""


class NoTradingPair(LiquidityDataMissing):
    """Trading pair is missing."""


class SampleTooFarOff(LiquidityDataMissing):
    """There are no samples in this timeset for a specific timepoint."""



class LiquiditySampleMeasure(enum.Enum):
    """For liquidity samples, which measurement we use for a price impact."""
    open = "open"
    close = "close"
    high = "high"
    low = "low"


@dataclass
class PriceImpact:
    """How much price impact a potential trade would have.

    Because how Uniswap v2 like DEXes operate, liquidity provider
    and protocol fees are included within the price impact calculations.
    Depending on the example these fees are not included in the calculations.

    This `PriceImpact` data also includes separate liquidity provider and protocol fees,
    as for trading companies these fees might be tax deductible.

    TODO: These fees are not yet confirmed with a live exchange.
    """

    #: Liquidity that was used for the price impact calculation, as expressed US dollars of one sided liquidity, see :py:class:`XYLiquidity`.
    available_liquidity: USDollarAmount

    #: How much % worse execution you get because due to in availability of the liquidity.
    price_impact: float

    #: How much worth of tokens you actually get for trade amount, expressed as US dollar
    delivered: USDollarAmount

    #: How much LP fees are paid in this transaction
    lp_fees_paid: USDollarAmount

    #: How much protocol fees are paid in this transaction
    protocol_fees_paid: USDollarAmount

    #: How much the trade cost you totally (trade amount - delivered)
    #: This includes LP fees paid, protocol fees paid and any loss because of limited liquidity.
    #: This does not include gas fees (tx fees) for the network.
    cost_of_trade: USDollarAmount


def estimate_xyk_price_impact(liquidity: float, trade_amount: float, lp_fee: float, protocol_fee: float) -> PriceImpact:
    """Calculates XY liquidity model slippage.

    Works for all Uniswap v2 style DEXes.

    For us, price impacts are easier because we operate solely in the US dollar space.

    Some price impact calculation examples to study:

    https://help.sushidocs.com/products/sushiswap-exchange

    https://ethereum.stackexchange.com/a/111334/620

    https://dailydefi.org/articles/price-impact-and-how-to-calculate/

    https://ethereum.stackexchange.com/a/111334/620

    TODO: Check that calculations are consistent with SushiSwap.

    :param liquidity: Liquidity expressed as USD :term:`XY liquidity model` single side liquidity.
    :param trade_amount: How much buy/sell you are doing
    :param lp_fee: Liquidity provider fee set for the pool as %. E.g. 0.0035 for Sushi.
    :param protocol_fee: Fees paid to the protocol, e.g. xSushi stakers
    """

    #price_impact = amount_in_with_fee / (liquidity + amount_in_with_fee)

    # We value both halves of the liquidity pool with
    # our precalculated US dollar reference exchange rate,
    # and thus skip a lot of uint256 bit issues what comes when dealing with raw
    # tokens amounts.
    total_fees = protocol_fee + lp_fee
    reserve_a_initial = reserve_b_initial = liquidity
    amount_in_with_fee = trade_amount * (1 - total_fees)

    lp_fees_paid = trade_amount * lp_fee
    protocol_fees_paid = trade_amount * protocol_fee

    constant_product = reserve_a_initial * reserve_b_initial
    reserve_b_after_execution = constant_product / (reserve_a_initial + amount_in_with_fee)
    amount_out = reserve_b_initial - reserve_b_after_execution
    market_price = amount_in_with_fee / amount_out
    mid_price = reserve_a_initial / reserve_b_initial
    price_impact = 1 - (mid_price / market_price)
    price_impact = abs(price_impact)
    cost_of_trade = trade_amount - amount_out

    return PriceImpact(
        available_liquidity=liquidity,
        delivered=amount_out,
        price_impact=price_impact,
        lp_fees_paid=lp_fees_paid,
        protocol_fees_paid=protocol_fees_paid,
        cost_of_trade=cost_of_trade,
    )


class HistoricalXYPriceImpactCalculator:
    """Calculates the Uniswap slippage, old price and new price based on :term:`XY liquidity model`.

    Used for backtesting and historical price impact calculations.

    The price impact model here is naive. It assumes that the trade would only use assets in a single pool.
    However, for any real DEX this is not the case. All DEXes implement the equivalent of
    `Uniswap auto router <https://uniswap.org/blog/auto-router>`_ also known as smart order routing (SOR).
    Routing finds the optimal path for the swaps between different tokens and can include three hop
    trades or even four hop trades to find the best price for the swapper. Thus, in real life
    the price impact might be less than what this model gives to you.

    TODO: These fees are not yet confirmed with a live exchange.

    .. note ::

        Currently we do not have dynamic liquidity provider `lp_fees` data for all the pairs, as it may vary
        pair by pair. Thus, in your model you need to manually confirm you are using the correct `lp_fees` value.
    """

    def __init__(self, liquidity_universe: GroupedLiquidityUniverse, lp_fee=0.0030, protocol_fee=0):
        """
        :param lp_fees: Liquidity provider fees as 0...1 % number
        """
        self.liquidity_universe = liquidity_universe
        # TODO: Later, pull this data dynamicalyl from the exchanges
        self.lp_fee = lp_fee
        self.protocol_fee = protocol_fee

    def calculate_price_impact(self, when: pd.Timestamp, pair_id: PrimaryKey, trade_amount: USDollarAmount, measurement: LiquiditySampleMeasure=LiquiditySampleMeasure.open, max_distance: pd.Timedelta=pd.Timedelta(days=1)) -> PriceImpact:
        """What would have been a price impact if a Uniswap-style trade were executed in the past.

        :param measurement: By default, we check the liquidity based on the liquidity available at the sample openining time.

        :param max_distance: If the sample is too far off, then abort because of gaps in data. Depending on the candle time frame you operate, you might need to adjust `max_distance`  to account the sample timestamp skew. For example, if you are using weekly candles, you need to set this to seven days.

        """

        liquidity_samples = self.liquidity_universe.get_liquidity_samples_by_pair(pair_id)
        if liquidity_samples is None:
            raise NoTradingPair(f"The universe does not contain liquidity data for pair {pair_id}")

        # measurement_samples = liquidity_samples[when:]

        ranged_samples = liquidity_samples[when:]

        if len(ranged_samples) == 0:
            raise SampleTooFarOff(f"Pair {pair_id} has no liquidity samples before {when}")

        # pair_id                        74846
        # timestamp        2021-06-07 00:00:00
        # exchange_rate            2489.434326
        # open                     259676608.0
        # close                    191939088.0
        # high                     267280096.0
        # low                      172410688.0
        # adds                             249
        # removes                          154
        # syncs                           7824
        # add_volume                2689543.75
        # remove_volume             30361768.0
        # start_block                 12584093
        # end_block                   12629257
        # Name: 2021-06-07 00:00:00, dtype: object

        first_sample = ranged_samples.iloc[0]

        ts = first_sample.timestamp

        distance = abs(ts - when)
        if distance > max_distance:
            raise SampleTooFarOff(f"Pair {pair_id} has liquidity samples, but the sample we got at {ts} is too far off from {when}. Distance is {distance} when we want at least {max_distance}")

        # "open", "close", etc.
        liquidity_at_sample = first_sample[measurement.value]

        return estimate_xyk_price_impact(liquidity_at_sample, trade_amount, self.lp_fee, self.protocol_fee)





