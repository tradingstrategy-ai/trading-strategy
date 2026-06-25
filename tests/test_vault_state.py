"""Tests for per-(vault, timestamp) deposit/redemption availability state.

Covers :py:func:`convert_vault_prices_to_vault_state` and the schema-tolerant column
handling in :py:func:`read_vault_price_history_parquet`. Uses synthetic data so the tests
do not depend on the downloaded vault price bundle.
"""
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tradingstrategy.alternative_data.vault import (
    convert_vault_prices_to_vault_state,
    read_vault_price_history_parquet,
    _normalise_bool_like,
    VAULT_STATE_COLUMNS,
)

ADDR = "0x04c8e82bbd0a33d3179e767c262223f50e74f7d2"


def _make_raw_hourly() -> pd.DataFrame:
    """One vault, two days of hourly rows with an intra-day open->closed transition."""
    rows = []
    # Day 1: open at 00:00/06:00, then closed at 12:00/18:00 — last value within bucket is closed.
    for hour, do, reason, max_dep in [
        (0, "true", None, 1000.0),
        (6, "true", None, 1000.0),
        (12, "false", "Vault deposits disabled by leader", 0.0),
        (18, "false", "Vault deposits disabled by leader", 0.0),
    ]:
        rows.append(_row("2026-03-05", hour, do, reason, max_dep))
    # Day 2: open all day.
    for hour in (0, 6, 12, 18):
        rows.append(_row("2026-03-06", hour, "true", None, 1000.0))
    return pd.DataFrame(rows)


def _row(day: str, hour: int, deposits_open, reason, max_deposit) -> dict:
    return {
        "chain": 9999,
        "address": ADDR,
        "timestamp": pd.Timestamp(f"{day} {hour:02d}:00"),
        "share_price": 1.0,
        "total_assets": 1_000_000.0,
        "deposits_open": deposits_open,
        "redemption_open": pd.NA,
        "deposit_closed_reason": reason,
        "max_deposit": max_deposit,
        "max_redeem": 500.0,
    }


def test_normalise_bool_like():
    s = pd.Series(["true", "false", "TRUE", "False", None, "weird", pd.NA])
    out = _normalise_bool_like(s)
    assert out.dtype == "boolean"
    assert list(out[:4]) == [True, False, True, False]
    assert out[4] is pd.NA
    assert out[5] is pd.NA
    assert out[6] is pd.NA


def test_convert_vault_state_daily_last_known_value():
    """Daily resample takes the last value within each bucket (consistent with TVL candles)."""
    state = convert_vault_prices_to_vault_state(_make_raw_hourly(), "1d")
    assert state is not None
    state = state.set_index("timestamp").sort_index()

    # Two daily buckets.
    assert len(state) == 2
    assert state["deposits_open"].dtype == "boolean"

    day1 = state.loc[pd.Timestamp("2026-03-05")]
    day2 = state.loc[pd.Timestamp("2026-03-06")]

    # Day 1: last value in the bucket is "closed".
    assert day1["deposits_open"] == False  # noqa: E712
    assert day1["deposit_closed_reason"] == "Vault deposits disabled by leader"
    assert day1["max_deposit"] == 0.0

    # Day 2: open.
    assert day2["deposits_open"] == True  # noqa: E712
    assert day2["max_deposit"] == 1000.0

    # redemption_open is unknown everywhere in the source -> NA, never False.
    assert state["redemption_open"].isna().all()

    # pair_id is derived and stable per address.
    assert state["pair_id"].nunique() == 1


def test_convert_vault_state_returns_none_without_state_columns():
    raw = _make_raw_hourly().drop(columns=VAULT_STATE_COLUMNS, errors="ignore")
    assert convert_vault_prices_to_vault_state(raw, "1d") is None


def test_read_parquet_tolerates_missing_state_columns(tmp_path):
    """Requesting state columns from a file that lacks them must not raise."""
    path = tmp_path / "no-state.parquet"
    df = pd.DataFrame(
        {
            "chain": [9999, 9999],
            "address": [ADDR, ADDR],
            "timestamp": [pd.Timestamp("2026-03-05"), pd.Timestamp("2026-03-06")],
            "share_price": [1.0, 1.01],
            "total_assets": [1_000_000.0, 1_010_000.0],
        }
    )
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, path)

    pairs_df = pd.DataFrame([{"chain_id": 9999, "address": ADDR}])
    requested = ["chain", "address", "timestamp", "share_price", "total_assets", *VAULT_STATE_COLUMNS]
    out = read_vault_price_history_parquet(path, vault_pairs_df=pairs_df, columns=requested)

    assert len(out) == 2
    # State columns were silently dropped because the file does not carry them.
    assert not any(c in out.columns for c in VAULT_STATE_COLUMNS)


def test_convert_vault_state_takes_whole_last_row():
    """The latest sample in a bucket wins as a whole row, including its nulls.

    Regression: a per-column ``Resampler.last()`` would keep a stale close reason from earlier in
    the bucket alongside a freshly re-opened ``deposits_open=True``.
    """
    rows = [
        _row("2026-03-05", 0, "true", None, 1000.0),
        _row("2026-03-05", 12, "false", "Vault deposits disabled by leader", 0.0),
        _row("2026-03-05", 18, "true", None, 1000.0),  # reopened, reason cleared
    ]
    state = convert_vault_prices_to_vault_state(pd.DataFrame(rows), "1d").set_index("timestamp")
    day = state.loc[pd.Timestamp("2026-03-05")]
    assert day["deposits_open"] == True  # noqa: E712 — last row reopened
    assert pd.isna(day["deposit_closed_reason"])  # stale noon reason must NOT carry over
    assert day["max_deposit"] == 1000.0  # last row's cap, not the noon 0.0


def test_convert_vault_state_latest_unknown_wins():
    """A latest unknown (NA) sample is not overwritten by an earlier non-null value."""
    rows = [
        _row("2026-03-05", 0, "false", "Vault deposits disabled by leader", 0.0),
        _row("2026-03-05", 18, None, None, float("nan")),  # latest sample is unknown
    ]
    state = convert_vault_prices_to_vault_state(pd.DataFrame(rows), "1d").set_index("timestamp")
    day = state.loc[pd.Timestamp("2026-03-05")]
    assert pd.isna(day["deposits_open"])  # latest unknown wins, not the earlier False
    assert pd.isna(day["deposit_closed_reason"])


def test_read_parquet_errors_on_missing_non_state_column(tmp_path):
    """A missing NON-state requested column still fails fast (not silently dropped)."""
    path = tmp_path / "no-state.parquet"
    df = pd.DataFrame(
        {
            "chain": [9999, 9999],
            "address": [ADDR, ADDR],
            "timestamp": [pd.Timestamp("2026-03-05"), pd.Timestamp("2026-03-06")],
            "share_price": [1.0, 1.01],
            "total_assets": [1_000_000.0, 1_010_000.0],
        }
    )
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), path)
    pairs_df = pd.DataFrame([{"chain_id": 9999, "address": ADDR}])
    with pytest.raises(Exception):
        # `share_priec` is a typo, not an optional state column -> must surface, not be dropped.
        read_vault_price_history_parquet(
            path,
            vault_pairs_df=pairs_df,
            columns=["chain", "address", "timestamp", "share_priec"],
        )
