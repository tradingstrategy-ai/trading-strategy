"""Grouped multi-pair forward fill must match the per-pair resample and fill loop."""

import warnings

import numpy as np
import pandas as pd

from tradingstrategy.utils.forward_fill import forward_fill_ohlcv_single_pair, resample_candles, resample_candles_multiple_pairs


def _forward_fill_per_pair(df: pd.DataFrame, frequency: str, forward_fill_until: pd.Timestamp) -> pd.DataFrame:
    """Forward fill pair by pair, as resample_candles_multiple_pairs() does without its grouped path."""
    segments = []
    for pair_id, pair_df in df.groupby("pair_id"):
        segment = resample_candles(pair_df, frequency)
        segment["pair_id"] = pair_df.iloc[0]["pair_id"]
        segments.append(forward_fill_ohlcv_single_pair(segment, freq=frequency, forward_fill_until=forward_fill_until, pair_id=pair_id))
    return pd.concat(segments)


def test_grouped_forward_fill_matches_per_pair_loop() -> None:
    """Forward fill multi-pair candles in one grouped pass with the per-pair loop's exact output.

    Live universe construction forward fills every pair up to the current time.
    The per-pair loop took about 8 seconds for 520 vaults, so a grouped path
    replaces it. Candle frames are also recorded and hashed as live strategy
    inputs, so even index resolution and column order must not change.

    1. Build unordered candles with gaps, duplicate clocks, NaN prices and millisecond timestamps.
    2. Forward fill daily and hourly candles, with and without forward_filled markers, padded and not padded.
    3. Verify the grouped output equals the per-pair loop, including index name and resolution.
    """
    # 1. Build unordered candles with gaps, duplicate clocks, NaN prices and millisecond timestamps.
    rng = np.random.default_rng(7)
    frames = []
    for pair_id, sample_count in ((3, 1), (5, 8), (11, 30)):
        clocks = pd.Timestamp("2026-09-01") + pd.to_timedelta(rng.integers(0, 24 * 20, sample_count), unit="h")
        prices = rng.uniform(1, 2, sample_count)
        prices[rng.random(sample_count) < 0.1] = np.nan
        frames.append(pd.DataFrame({"timestamp": clocks, "pair_id": pair_id, "open": prices, "high": prices * 1.1, "low": prices * 0.9, "close": prices, "volume": rng.integers(0, 100, sample_count)}))
    candles = pd.concat(frames, ignore_index=True)
    candles = pd.concat([candles, candles.iloc[::4]], ignore_index=True).sample(frac=1, random_state=7)
    candles["timestamp"] = candles["timestamp"].astype("datetime64[ms]")
    candles = candles.set_index("timestamp", drop=False)
    marked = candles.assign(forward_filled=rng.random(len(candles)) < 0.2)
    last = candles["timestamp"].max()

    for frequency in ("1d", "1h"):
        for source in (candles, marked):
            for forward_fill_until in (last, last + pd.Timedelta(days=3, minutes=7)):
                # 2. Forward fill daily and hourly candles, with and without forward_filled markers, padded and not padded.
                grouped = resample_candles_multiple_pairs(source.copy(), frequency, forward_fill_until=forward_fill_until)
                with warnings.catch_warnings():
                    # The per-pair reference repeats the loop's own pandas deprecation warnings
                    warnings.simplefilter("ignore", FutureWarning)
                    expected = _forward_fill_per_pair(source.copy(), frequency, forward_fill_until)

                # 3. Verify the grouped output equals the per-pair loop, including index name and resolution.
                pd.testing.assert_frame_equal(grouped, expected, check_freq=False)
                assert grouped.index.dtype == expected.index.dtype
                assert grouped.attrs["forward_filled_until"] == forward_fill_until
