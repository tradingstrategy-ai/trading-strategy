"""Price impact calculations.


"""
from dataclasses import dataclass

from tradingstrategy.types import PrimaryKey, USDollarAmount


@dataclass
class PriceImpact:
    """How much slippage a potential trade would have.

    Because how Uniswap v2 operates, liquidity provider
    fees are included within the price impact.
    """

    #: Slippage in the terms of trade amout 0...1
    price_impact: float

    #: Slippage in the terms of trade currency (USD)
    slippage_amount: float

    # How much worth of tokens you actually get for trade amount
    delivered: float

    # How much LP fees are paid in this transaction
    lp_fees_paid: float


def estimate_xyk_price_impact(liquidity: float, trade_amount: float, lp_fee: float) -> PriceImpact:
    """Calculates XY liquidity model slippage.

    Works for all Uniswap v2 style DEXes.

    For us, price impacts are easier because we operate solely in the US dollar space.

    Some price impact calculation examples to study:

    https://ethereum.stackexchange.com/a/111334/620

    https://dailydefi.org/articles/price-impact-and-how-to-calculate/

    https://ethereum.stackexchange.com/a/111334/620

    :param liquidity: Liquidity expressed as USD :term:`XY liquidity model` single side liquidity.
    :param trade_amount: How much buy/sell you are doing
    :param lp_fee: Liquidity provider fee set for the pool as %. E.g. 0.0035 for Sushi.
    """

    # We value both halves of the liquidity pool with
    # our precalculated US dollar reference price rate
    reserve_a_initial = reserve_b_initial = liquidity

    amount_in_with_fee = trade_amount * (1 - lp_fee)
    lp_fees_paid = trade_amount * lp_fee
    #price_impact = amount_in_with_fee / (liquidity + amount_in_with_fee)

    constant_product = reserve_a_initial * reserve_b_initial;
    reserve_b_after_execution = constant_product / (reserve_a_initial + amount_in_with_fee);
    amount_out = reserve_b_initial - reserve_b_after_execution;
    market_price = amount_in_with_fee / amount_out
    mid_price = reserve_a_initial / reserve_b_initial
    price_impact = 1 - (mid_price / market_price)

    return PriceImpact(
        delivered=amount_out,
        price_impact=price_impact,
        slippage_amount=trade_amount - amount_out,
        lp_fees_paid=lp_fees_paid,
    )


class HistoricalXYLiquiditySlippageCalculator:
    """Calculates the Uniswap slippage, old price and new price based on :term:`XY liquidity model`.

    Used for backtesting and historical price impact calculations.
    """

    def __init__(self):
        pass

    def calculate_price_impact(self, pair_id: PrimaryKey, trade_amount: USDollarAmount) -> PriceImpact:
        pass