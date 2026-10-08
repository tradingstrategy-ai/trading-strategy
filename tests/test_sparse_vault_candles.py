"""Regression coverage for vaults whose price scans are sparser than candle buckets."""

import numpy as np
import pandas as pd
import pytest

from tradingstrategy.alternative_data.vault import convert_vault_prices_to_candles
from tradingstrategy.utils.forward_fill import resample_candles_multiple_pairs
from tradingstrategy.vault import _derive_pair_id_from_address


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


def _resample_one_value_per_vault(raw: pd.DataFrame, value_column: str, frequency: str) -> pd.DataFrame:
    """Resample one value column with the per-vault resampler as the reference candles."""
    df = raw.copy()
    for column in ("open", "high", "low", "close"):
        df[column] = df[value_column]
    df["volume"] = 0
    df["pair_id"] = df["address"].apply(_derive_pair_id_from_address)
    return resample_candles_multiple_pairs(df, frequency)


def test_grouped_vault_candles_match_per_vault_resampler() -> None:
    """Build exactly the candles of the per-vault resampler with one grouped pass.

    Vault candle conversion replaced two per-vault ``resample()`` loops with a
    single groupby, as the loops took about 10 seconds of every live universe
    construction. Any difference would change prices and TVL seen by strategies.

    1. Build sparse histories with gaps, duplicate clocks, NaN values, a single sample vault and millisecond timestamps.
    2. Convert them for daily and hourly buckets, from a timestamp column, a DatetimeIndex and with forward-filled markers.
    3. Verify price and TVL candles equal the per-vault resampler output, including order, dtypes and markers.
    """
    # 1. Build sparse histories with gaps, duplicate clocks, NaN values, a single sample vault and millisecond timestamps.
    rng = np.random.default_rng(42)
    rows = []
    for vault_index, sample_count in enumerate((1, 6, 40, 80)):
        address = f"0x{vault_index + 1:040x}"
        clocks = pd.Timestamp("2026-09-01") + pd.to_timedelta(np.sort(rng.integers(0, 60 * 24 * 12, sample_count)), unit="min")
        for clock in clocks:
            rows.append({"chain": 9999, "address": address, "timestamp": clock, "share_price": rng.uniform(0.5, 2), "total_assets": rng.uniform(0, 1e6)})
    raw = pd.DataFrame(rows)
    # Repeat clocks so first and last depend on row order within a bucket
    raw = pd.concat([raw, raw.iloc[::7].assign(share_price=1.5, total_assets=7.0)], ignore_index=True)
    raw.loc[raw.index % 11 == 3, "share_price"] = np.nan
    raw.loc[raw.index % 13 == 5, "total_assets"] = np.nan
    raw["timestamp"] = raw["timestamp"].astype("datetime64[ms]")

    marked = raw.assign(forward_filled=raw.index % 5 == 0)
    indexed = raw.set_index("timestamp", drop=False)
    for frequency in ("1d", "1h"):
        for source in (raw, marked, indexed):
            # 2. Convert them for daily and hourly buckets, from a timestamp column, a DatetimeIndex and with forward-filled markers.
            prices, tvl = convert_vault_prices_to_candles(source.copy(), frequency)

            # 3. Verify price and TVL candles equal the per-vault resampler output, including order, dtypes and markers.
            pd.testing.assert_frame_equal(prices, _resample_one_value_per_vault(source, "share_price", frequency), check_freq=False)
            pd.testing.assert_frame_equal(tvl, _resample_one_value_per_vault(source, "total_assets", frequency), check_freq=False)
