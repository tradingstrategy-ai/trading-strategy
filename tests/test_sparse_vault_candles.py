"""Regression coverage for vaults whose price scans are sparser than candle buckets."""

import pandas as pd
import pytest

from tradingstrategy.alternative_data.vault import convert_vault_prices_to_candles
from tradingstrategy.utils.forward_fill import resample_candles_multiple_pairs


def test_sparse_vault_gap_uses_last_close() -> None:
    """Forward-fill a missed day without replaying yesterday's price or TVL range.

    1. Build changing four-hour price/TVL observations with one missing day.
    2. Convert to daily candles and check the gap is flat at the previous close.
    3. Resample again and verify the synthetic marker is retained.
    """
    # 1. A real day has a non-flat range, followed by a missing scanner day.
    raw = pd.DataFrame({
        "timestamp": pd.to_datetime(["2026-09-19 01:30", "2026-09-19 05:30", "2026-09-21 01:30"]),
        "chain": 9999,
        "address": "0x0000000000000000000000000000000000000001",
        "share_price": [1.0, 1.2, 1.3],
        "total_assets": [1000.0, 1200.0, 1300.0],
    })
    # 2. A filled day must not repeat the prior day's low/open.
    prices, tvl = convert_vault_prices_to_candles(raw, "1d")
    for frame, expected in [(prices, 1.2), (tvl, 1200.0)]:
        gap = frame.loc[frame.timestamp == pd.Timestamp("2026-09-20")].iloc[0]
        for column in ("open", "high", "low", "close"):
            assert gap[column] == pytest.approx(expected)
        assert gap.forward_filled
        assert frame.timestamp.max() == pd.Timestamp("2026-09-21")
    # 3. Later aggregation must not relabel a synthetic observation as real.
    again = resample_candles_multiple_pairs(prices.reset_index(drop=True), "1d")
    assert again.loc[pd.Timestamp("2026-09-20"), "forward_filled"]
