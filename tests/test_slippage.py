"""Slippage calculation test suite."""
import pytest

from tradingstrategy.priceimpact import estimate_xyk_price_impact


def test_calculate_slippage_simple():
    """Simple slippage calculation example.

    https://dailydefi.org/articles/price-impact-and-how-to-calculate/

    Pool info::

        USDC = 2,000,000
        ETH = 1,000
        Constant Product = 2,000,000,000
        Market Price = 2,000

    We sell 100,000 USDC worth of ETH.
    """

    # 30 bps
    uniswap_v2_lp_fee = 0.0030
    impact = estimate_xyk_price_impact(2_000_000, 100_000, uniswap_v2_lp_fee)
    assert impact.delivered == pytest.approx(94965.94751631189)
    assert impact.lp_fees_paid == pytest.approx(300.0)
    assert impact.slippage_amount == pytest.approx(5034.052483688109)
