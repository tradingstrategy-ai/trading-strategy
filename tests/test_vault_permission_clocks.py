"""Regression coverage for HyperCore permission clocks and independent sidecars."""
from pathlib import Path

import pandas as pd
import pytest

from tradingstrategy.alternative_data.vault import (
    VAULT_STATE_COLUMNS,
    VAULT_STATE_METADATA_COLUMNS,
    convert_vault_prices_to_vault_state,
    read_vault_permission_history_parquet,
    read_vault_price_history_parquet,
)


ADDRESS = "0xcae0d1558b70b92ee9fd0acb20cb639c8c28ae69"


def _price(at: str, opened: bool | None, **metadata) -> dict:
    """Create a synthetic price projection without contacting the venue."""
    return {
        "chain": 9999, "address": ADDRESS, "timestamp": pd.Timestamp(at),
        "share_price": 1.0, "total_assets": 1000.0, "deposits_open": opened,
        "written_at": pd.Timestamp("2026-10-05"), "max_deposit": float("nan"),
        **metadata,
    }


def _receipt(at: str, closed: bool | None, allowed: bool | None, identity: str, **metadata) -> dict:
    """Create the producer's exact sidecar schema without network dependencies."""
    return {
        "vault_address": ADDRESS, "permission_observed_at": pd.Timestamp(at),
        "is_closed": closed, "allow_deposits": allowed,
        "observation_id": identity, "provenance": "observed",
        "record_kind": "observation", "written_at": pd.Timestamp("2026-10-05"),
        "capacity_observed_at": pd.Timestamp(at), "leader_fraction": 0.1,
        "relationship_type": "normal", "source_endpoint": "POST /info vaultDetails",
        "evidence_available_at": pd.NaT, "effective_from": pd.NaT,
        "effective_to": pd.NaT, "reason": None, **metadata,
    }


def test_recovered_doezoe_uses_original_price_clock(tmp_path: Path) -> None:
    """Recover September availability despite October publication, preserving quality.

    1. Write recovered Open and disabled legacy flags with a later write time.
    2. Read optional metadata and convert to daily state.
    3. Verify the September transition and retain the recorded cap without inventing a clock.
    """
    # 1. Publication is deliberately later than the retained price keys.
    prices = pd.DataFrame([
        _price("2026-09-17 12:00", True),
        _price("2026-09-20 12:00", True),
        _price("2026-09-21 11:48:00.756", False, max_deposit=0.0),
    ])
    path = tmp_path / "prices.parquet"
    prices.to_parquet(path, index=False)

    # 2. Missing metadata is optional for legacy bundles.
    read = read_vault_price_history_parquet(path, columns=["chain", "address", "timestamp", *VAULT_STATE_COLUMNS, *VAULT_STATE_METADATA_COLUMNS])
    state = convert_vault_prices_to_vault_state(read).set_index("timestamp")
    without_sidecar = convert_vault_prices_to_vault_state(read, permission_history_df=pd.DataFrame()).set_index("timestamp")

    # 3. This is a recovery approximation, without a fabricated capacity clock.
    assert bool(state.loc[pd.Timestamp("2026-09-18"), "deposits_open"])
    assert bool(state.loc[pd.Timestamp("2026-09-21"), "deposits_open"])
    assert not bool(state.loc[pd.Timestamp("2026-09-22"), "deposits_open"])
    assert state.loc[pd.Timestamp("2026-09-22"), "permission_observed_at"] == pd.Timestamp("2026-09-21 11:48:00.756")
    assert state["permission_provenance"].eq("legacy_price_timestamp").all()
    assert state["capacity_observed_at"].isna().all()
    assert pd.isna(state.loc[pd.Timestamp("2026-09-18"), "max_deposit"])
    assert pd.isna(state.loc[pd.Timestamp("2026-09-21"), "max_deposit"])
    assert state.loc[pd.Timestamp("2026-09-22"), "max_deposit"] == pytest.approx(0.0)
    pd.testing.assert_frame_equal(state, without_sidecar)


@pytest.mark.parametrize("unit", ["us", "ns"])
def test_sidecar_unknown_conflicts_and_rounding(tmp_path: Path, unit: str) -> None:
    """Select whole responses after the newest price without allowing inferred overrides.

    1. Write microsecond/nanosecond receipts, including unknown and conflicting flags.
    2. Convert with legacy prices whose later inferred Open flags cannot win.
    3. Verify bucket order, original clocks, explicit unknowns and sparse prices.
    4. Reject an Open flag claiming deny-only archive provenance.
    """
    # 1. The last response in a bucket must win with its own null fields.
    receipts = pd.DataFrame([
        _receipt("2026-09-17 02:00", False, True, "open"),
        _receipt("2026-09-17 03:00", None, None, "unknown", provenance="observed_unknown", leader_fraction=None, capacity_observed_at=pd.NaT),
        _receipt("2026-09-18 01:00", False, True, "conflict-open"),
        _receipt("2026-09-18 01:00", True, False, "conflict-closed"),
        _receipt("2026-09-19 01:00", False, True, "new-open"),
        _receipt("2026-09-20 01:00", False, False, "disabled"),
        _receipt("2026-09-20 01:00", True, True, "closed"),
        _receipt("2026-09-21 01:00", False, None, "hlp", relationship_type="parent", leader_fraction=0.01),
    ])
    for name in ("permission_observed_at", "capacity_observed_at", "written_at", "effective_from", "effective_to", "evidence_available_at"):
        receipts[name] = pd.to_datetime(receipts[name]).astype(f"datetime64[{unit}]")
    path = tmp_path / "permissions.parquet"
    receipts.to_parquet(path, index=False)
    pairs = pd.DataFrame([{"chain_id": 9999, "address": ADDRESS}])
    sidecar = read_vault_permission_history_parquet(path, pairs)

    # 2. Later inferred rows must not overwrite a genuine unknown response.
    other_address = "0xcae0d1558b70b92ee9fd0acb20cb639c8c28ae60"
    prices = pd.DataFrame([
        _price("2026-09-16", True), _price("2026-09-17 22:00", True),
        _price("2026-09-17 03:00", True, address=other_address,
               permission_observed_at=pd.Timestamp("2026-09-17 03:00"),
               permission_provenance="observed", permission_observation_id="unknown"),
    ])
    original = prices.copy(deep=True)
    combined = convert_vault_prices_to_vault_state(prices, permission_history_df=sidecar)
    state = combined.loc[combined.address.eq(ADDRESS)].set_index("timestamp")

    # 3. Ceiling precedes selection; later responses do not create prices.
    unknown = state.loc[pd.Timestamp("2026-09-18")]
    assert pd.isna(unknown["deposits_open"])
    assert unknown["permission_observation_id"] == "unknown"
    assert unknown["permission_observed_at"] == pd.Timestamp("2026-09-17 03:00")
    assert pd.isna(unknown["max_deposit"])
    assert pd.isna(state.loc[pd.Timestamp("2026-09-19"), "deposits_open"])
    assert state.loc[pd.Timestamp("2026-09-19"), "permission_provenance"] == "observed_unknown"
    assert bool(state.loc[pd.Timestamp("2026-09-20"), "deposits_open"])

    # 4. Deny-only archive bounds must never authenticate an Open flag.
    bounded_open = _receipt("2026-09-17", False, True, "invalid-bound", provenance="legacy_closure_bounded", permission_observed_at=pd.NaT, evidence_available_at=pd.Timestamp("2026-09-17"))
    rejected = convert_vault_prices_to_vault_state(
        pd.DataFrame([_price("2026-09-17", None, permission_provenance="corrupted_unknown")]),
        permission_history_df=pd.DataFrame([bounded_open]),
    )
    assert rejected["deposits_open"].isna().all()
    # Conflicting raw inputs remain unknown even if both derive Closed.
    assert pd.isna(state.loc[pd.Timestamp("2026-09-21"), "deposits_open"])
    # HLP parents ignore the leader deposit flag and leader-share capacity policy.
    assert bool(state.loc[pd.Timestamp("2026-09-22"), "deposits_open"])
    assert pd.isna(state.loc[pd.Timestamp("2026-09-22"), "max_deposit"])
    # Receipt IDs are scoped by vault; a sidecar must not erase another vault.
    assert combined.loc[combined.address.eq(other_address), "deposits_open"].all()
    pd.testing.assert_frame_equal(prices, original)


def test_corrupt_flags_and_retrospective_uncertainty() -> None:
    """Keep corrupt flags unknown and apply migration boundaries at historical time.

    1. Supply a genuine Open receipt and a later retrospective uncertainty interval.
    2. Add explicitly corrupted projected flags and convert the combined history.
    3. Verify the interval blocks old evidence, then allows a genuine new receipt.
    """
    # 1. Publication time cannot move the historical uncertainty interval.
    boundary = _receipt("2026-10-05", None, None, "boundary", record_kind="uncertainty_boundary", provenance="corrupted_unknown", permission_observed_at=pd.NaT, effective_from=pd.Timestamp("2026-09-18"), effective_to=pd.Timestamp("2026-09-20"))
    receipts = pd.DataFrame([_receipt("2026-09-17", False, True, "open"), boundary, _receipt("2026-09-19 02:00", False, True, "reopened")])

    # 2. Clock fallback must never authenticate explicitly corrupted flags.
    prices = pd.DataFrame([_price("2026-09-18", True, permission_provenance="corrupted_unknown")])
    state = convert_vault_prices_to_vault_state(prices, permission_history_df=receipts).set_index("timestamp")

    # 3. Unknown starts at the boundary, irrespective of the October write.
    assert pd.isna(state.loc[pd.Timestamp("2026-09-18"), "deposits_open"])
    assert state.loc[pd.Timestamp("2026-09-18"), "permission_provenance"] == "corrupted_unknown"
    assert bool(state.loc[pd.Timestamp("2026-09-20"), "deposits_open"])


def test_uncertainty_and_expiry_use_raw_clocks() -> None:
    """Keep rounded availability from bridging an uncertainty start or refreshing age.

    1. Place a receipt before an intraday uncertainty start in the same bucket.
    2. Convert the boundary and a separately ageing inferred legacy snapshot.
    3. Verify unknown state with the retained original receipt and recovery clocks.
    """
    # 1. The receipt's ceiling falls after the start, but its actual clock does not.
    receipts = pd.DataFrame([
        _receipt("2026-09-17 11:00", False, True, "before-gap"),
        _receipt("2026-10-05", None, None, "boundary", record_kind="uncertainty_boundary", permission_observed_at=pd.NaT, effective_from=pd.Timestamp("2026-09-17 12:00"), effective_to=pd.Timestamp("2026-09-20")),
    ])

    # 2. A later price supplies no fresh permission and cannot refresh the old clock.
    genuine = convert_vault_prices_to_vault_state(pd.DataFrame([_price("2026-09-16", True)]), permission_history_df=receipts).set_index("timestamp")
    legacy_prices = pd.DataFrame([
        _price("2026-09-17 12:00", True, permission_provenance="legacy_price_timestamp", permission_observed_at=pd.Timestamp("2026-09-17 12:00"), permission_observation_id="inferred"),
        _price("2026-09-20", True, permission_provenance="legacy_price_timestamp", permission_observed_at=pd.Timestamp("2026-09-17 12:00"), permission_observation_id="inferred"),
    ])
    legacy = convert_vault_prices_to_vault_state(legacy_prices).set_index("timestamp")

    # 3. Original clocks and inferred quality survive the Unknown decision.
    assert pd.isna(genuine.loc[pd.Timestamp("2026-09-18"), "deposits_open"])
    assert genuine.loc[pd.Timestamp("2026-09-18"), "permission_provenance"] == "corrupted_unknown"
    assert pd.isna(legacy.loc[pd.Timestamp("2026-09-20"), "deposits_open"])
    assert legacy.loc[pd.Timestamp("2026-09-20"), "permission_provenance"] == "legacy_price_timestamp"
    assert legacy.loc[pd.Timestamp("2026-09-20"), "permission_observed_at"] == pd.Timestamp("2026-09-17 12:00")


def test_newer_archive_closure_denies_inferred_open() -> None:
    """Use a later deny-only archive bound without replacing genuine responses.

    1. Supply inferred Open and later Closed archive bounds without receipt clocks.
    2. Convert daily decisions with and without a genuine unknown response.
    3. Verify the closure keeps its bound and genuine unknown retains precedence.
    """
    # 1. An archive bound authenticates denial only, not measured permission or capacity.
    prices = pd.DataFrame([_price("2026-09-17", True)])
    bound = _receipt(
        "2026-10-05", False, False, "z-old-bound",
        provenance="legacy_closure_bounded", permission_observed_at=pd.NaT,
        capacity_observed_at=pd.NaT,
        evidence_available_at=pd.Timestamp("2026-09-17 12:00"),
    )
    later_bound = dict(bound, observation_id="a-new-bound", evidence_available_at=pd.Timestamp("2026-09-17 18:00"))

    # 2. No network is needed: these are the exact nullable sidecar inputs.
    control = convert_vault_prices_to_vault_state(prices).set_index("timestamp")
    inferred = control.loc[:pd.Timestamp("2026-09-18")].iloc[-1]
    assert bool(inferred.deposits_open)
    assert inferred.permission_provenance == "legacy_price_timestamp"
    assert pd.notna(inferred.permission_observation_id)
    assert inferred.permission_observed_at == pd.Timestamp("2026-09-17")
    closed = convert_vault_prices_to_vault_state(prices, permission_history_df=pd.DataFrame([later_bound, bound])).set_index("timestamp")
    unknown = convert_vault_prices_to_vault_state(prices, permission_history_df=pd.DataFrame([
        bound, _receipt("2026-09-17 13:00", None, None, "unknown", provenance="observed_unknown"),
    ])).set_index("timestamp")

    # 3. Whole snapshots retain their original clocks and quality.
    selected = closed.loc[pd.Timestamp("2026-09-18")]
    assert not bool(selected.deposits_open)
    assert selected.permission_provenance == "legacy_closure_bounded"
    assert selected.permission_observation_id == "a-new-bound"
    assert pd.isna(selected.permission_observed_at)
    assert selected.evidence_available_at == pd.Timestamp("2026-09-17 18:00")
    assert pd.isna(selected.capacity_observed_at)
    assert selected.max_deposit == pytest.approx(0.0)
    assert pd.isna(unknown.loc[pd.Timestamp("2026-09-18"), "deposits_open"])
    assert unknown.loc[pd.Timestamp("2026-09-18"), "permission_observation_id"] == "unknown"


@pytest.mark.parametrize("frequency", ["1d", "1h"])
def test_legacy_gucky_policy_survives_missing_capacity_clock(tmp_path: Path, frequency: str) -> None:
    """Retain Gucky's recorded low-share cap through daily and HF price projections.

    1. Write the ticket's archived share and zero cap with repeated source metadata.
    2. Load and resample with and without the matching repaired permission sidecar.
    3. Verify exact values, original clocks and unchanged economic/write data.
    """
    # 1. These local records reproduce the reported schema without network access.
    clock = pd.Timestamp("2026-04-11 04:22:05.613")
    address = "0x3a6747c8e913085e243a2c22d188dafa8c6a612a"
    fraction = 0.05000280943338046
    prices = pd.DataFrame([
        _price(at, True, address=address, leader_fraction=fraction, max_deposit=0.0,
               permission_observed_at=clock, permission_provenance="legacy_price_timestamp",
               permission_observation_id="gucky-source", capacity_observed_at=pd.NaT,
               written_at=pd.Timestamp("2026-04-11 04:22:06.999"), share_price=1.234)
        for at in (str(clock), "2026-04-12 12:00", "2026-04-14 12:00")
    ])
    path = tmp_path / "prices.parquet"
    prices.to_parquet(path, index=False)
    sidecar = pd.DataFrame([_receipt(
        str(clock), False, True, "gucky-source", vault_address=address,
        provenance="legacy_price_timestamp", leader_fraction=fraction,
        max_deposit=0.0, capacity_observed_at=pd.NaT,
    )])

    # 2. Loading and whole-snapshot selection must not discard legacy policy inputs.
    read = read_vault_price_history_parquet(path)
    before = read.copy(deep=True)
    for history in (None, sidecar, sidecar.drop(columns="max_deposit")):
        state = convert_vault_prices_to_vault_state(read, frequency, history)

        # 3. Publication and repeated prices never supply a fresh measurement clock.
        assert state["leader_fraction"].eq(fraction).all()
        assert state["max_deposit"].eq(0.0).all()
        assert state["capacity_observed_at"].isna().all()
        assert state["permission_observed_at"].eq(clock).all()
        assert state.iloc[0]["timestamp"] == clock.ceil(frequency)
        assert pd.isna(state.iloc[-1]["deposits_open"])
    pd.testing.assert_frame_equal(read, before)
    pd.testing.assert_frame_equal(read, prices)

    # 3. The original schema falls back to its price key, never its later write time.
    original = read.iloc[:1].drop(columns=["permission_observed_at", "permission_provenance", "permission_observation_id"])
    legacy = convert_vault_prices_to_vault_state(original, frequency).iloc[0]
    assert legacy.leader_fraction == fraction
    assert legacy.max_deposit == pytest.approx(0.0)
    assert legacy.permission_observed_at == clock
    assert pd.isna(legacy.capacity_observed_at)

    # 3. A recorded independent clock also survives legacy provenance unchanged.
    recorded = read.assign(capacity_observed_at=clock - pd.Timedelta(hours=1))
    state = convert_vault_prices_to_vault_state(recorded, frequency)
    assert state["capacity_observed_at"].eq(clock - pd.Timedelta(hours=1)).all()


@pytest.mark.parametrize("frequency", ["1d", "1h"])
def test_new_policy_snapshots_clear_legacy_caps_and_keep_nulls(frequency: str) -> None:
    """Keep recorded NULLs and supersede older caps with coherent new responses.

    1. Supply a legacy zero cap and newer sufficient-share, unknown and closed responses.
    2. Select daily and HF snapshots despite later inferred price projections.
    3. Verify share/cap clearing, explicit NULLs and separate permission meanings.
    """
    # 1. The nullable cap column is present in the repaired sidecar contract.
    prices = pd.DataFrame([
        _price("2026-04-11 04:22", True, leader_fraction=0.05, max_deposit=0.0),
        _price("2026-04-16", True, leader_fraction=0.05, max_deposit=0.0),
    ])
    sidecar = pd.DataFrame([
        _receipt("2026-04-12 04:22", False, True, "sufficient", leader_fraction=0.1, max_deposit=None, capacity_observed_at=pd.NaT),
        _receipt("2026-04-13 04:22", None, None, "unknown", leader_fraction=None, max_deposit=None, capacity_observed_at=pd.NaT, provenance="observed_unknown"),
        _receipt("2026-04-14 04:22", False, True, "no-recorded-cap", leader_fraction=0.05, max_deposit=None, capacity_observed_at=pd.NaT),
        _receipt("2026-04-15 04:22", True, False, "closed", leader_fraction=None, max_deposit=None, capacity_observed_at=pd.NaT),
        _receipt("2026-04-17 04:22", False, True, "positive-cap", leader_fraction=0.1, max_deposit=123.0, capacity_observed_at=pd.NaT),
    ])

    # 2. The actual converter selects whole snapshots, without per-field filling.
    state = convert_vault_prices_to_vault_state(prices, frequency, sidecar).set_index("timestamp")

    # 3. An old zero never survives a new snapshot that has no recorded cap.
    initial = state.loc[pd.Timestamp("2026-04-11 04:22").ceil(frequency)]
    assert initial.max_deposit == pytest.approx(0.0)
    for receipt in sidecar.itertuples():
        selected = state.loc[receipt.permission_observed_at.ceil(frequency)]
        if receipt.observation_id == "positive-cap":
            assert selected.max_deposit == pytest.approx(123.0)
        else:
            assert pd.isna(selected.max_deposit)
        assert selected.permission_observation_id == receipt.observation_id
        if receipt.observation_id == "unknown":
            assert pd.isna(selected.leader_fraction)
            assert pd.isna(selected.deposits_open)
        elif receipt.observation_id == "closed":
            assert pd.isna(selected.leader_fraction)
            assert not bool(selected.deposits_open)
        else:
            assert selected.leader_fraction == pytest.approx(receipt.leader_fraction)
            assert bool(selected.deposits_open)
