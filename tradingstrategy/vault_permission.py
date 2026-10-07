"""Point-in-time HyperCore permission selection.

This selector follows the separate eth-defi permission sidecar contract.
Publication and repeated price projections never refresh an existing snapshot.
Clockless recovered snapshots retain recorded policy inputs and inferred price clocks.
"""
import datetime

import pandas as pd

STATE_FIELDS = ["deposits_open", "redemption_open", "deposit_closed_reason", "redemption_closed_reason", "max_deposit", "max_redeem"]

POLICY_FIELDS = ["is_closed", "allow_deposits", "relationship_type", "leader_fraction"]


def select_permission_state(
    observations: pd.DataFrame,
    decisions: pd.DataFrame,
    frequency: str | None = None,
    *,
    legacy_max_age: datetime.timedelta = datetime.timedelta(days=2),
) -> pd.DataFrame:
    """Select coherent permission snapshots available at each decision.

    Round availability upwards before selection when a decision frequency is
    supplied. Retrospective uncertainty intervals override earlier open state
    irrespective of migration publication time. Explicit unknown responses
    remain in selection, preventing per-field filling across source snapshots.
    When no eligible genuine snapshot exists, recovered backup flags may use
    their price clock with ``legacy_price_timestamp`` provenance. They supply
    recorded leader shares and caps without inventing a capacity receipt clock,
    and never override a genuine unknown response.

    :param observations: Exact observation table with nullable flags and clocks.
    :param decisions: Frame with ``vault_address`` (string) and ``timestamp`` (naive UTC).
    :param frequency: Optional Pandas decision frequency such as ``4h``.
    :param legacy_max_age: Maximum carry-forward age of an inferred backup flag, measured from its original price clock.
    :return: Decision frame plus snapshot values, provenance and original clocks.
    """
    output = decisions.reset_index(drop=True).copy()
    output["_decision_order"] = range(len(output))
    # Arrow sidecars use microseconds while decision grids commonly use
    # nanoseconds. Pandas as-of joins require identical datetime units.
    output["timestamp"] = pd.to_datetime(output["timestamp"]).astype("datetime64[ns]")
    fields = ["observation_id", "permission_observed_at", "evidence_available_at", "capacity_observed_at", "provenance", "reason", "available_at", "source_order", *STATE_FIELDS, *POLICY_FIELDS]
    for name in fields:
        output[name] = pd.NaT if name.endswith("_at") else None
    if observations.empty or output.empty:
        return output.drop(columns="_decision_order")
    snapshots = observations[observations["record_kind"] == "observation"].copy()
    for name in ("permission_observed_at", "capacity_observed_at", "evidence_available_at"):
        snapshots[name] = pd.to_datetime(snapshots[name]).astype("datetime64[ns]")
    snapshots["available_at"] = pd.to_datetime(snapshots["permission_observed_at"]).astype("datetime64[ns]")
    bounded = snapshots["provenance"] == "legacy_closure_bounded"
    snapshots.loc[bounded, "available_at"] = pd.to_datetime(snapshots.loc[bounded, "evidence_available_at"]).astype("datetime64[ns]")
    snapshots = snapshots[snapshots["available_at"].notna() & snapshots["provenance"].isin(("observed", "observed_unknown", "restored", "legacy_closure_bounded", "legacy_price_timestamp"))]
    snapshots["deposits_open"] = snapshots["deposits_open"].astype("boolean")
    archive_closures = snapshots[snapshots["provenance"].eq("legacy_closure_bounded") & snapshots["deposits_open"].eq(False)].copy()
    inferred = snapshots[snapshots["provenance"].eq("legacy_price_timestamp")].copy()
    # At identical source clocks the finer scanner has priority over the daily
    # compatibility sample, matching the price export's source precedence.
    inferred["_source_priority"] = inferred["source_endpoint"].eq("archive price rows/vault_high_freq_prices").astype(int)
    inferred = inferred.sort_values(["available_at", "_source_priority", "source_order"]).drop_duplicates(["vault_address", "available_at"], keep="last")
    inferred["_original_available_at"] = inferred["available_at"]
    if frequency:
        inferred["available_at"] = inferred["available_at"].dt.ceil(frequency)
    inferred = inferred.sort_values(["available_at", "_original_available_at", "source_order"])
    inferred_groups = dict(iter(inferred.groupby("vault_address", sort=False)))
    snapshots = snapshots[~snapshots["provenance"].isin(("legacy_closure_bounded", "legacy_price_timestamp"))]
    # Equal receipt clocks with conflicting coherent inputs have no reliable
    # precedence. Preserve both raw records but select an explicit unknown.
    snapshot_fields = [*STATE_FIELDS, *POLICY_FIELDS, "capacity_observed_at"]
    snapshots["_state_hash"] = pd.util.hash_pandas_object(snapshots[snapshot_fields], index=False)
    conflicting = snapshots.groupby(["vault_address", "available_at"])["_state_hash"].transform("nunique").gt(1)
    snapshots.loc[conflicting, snapshot_fields] = None
    snapshots.loc[conflicting, "provenance"] = "observed_unknown"
    snapshots.loc[conflicting, "reason"] = "Conflicting permission snapshots at the same receipt time"
    snapshots["_original_available_at"] = snapshots["available_at"]
    if frequency:
        snapshots["available_at"] = snapshots["available_at"].dt.ceil(frequency)
    snapshots = snapshots.sort_values(["available_at", "_original_available_at", "source_order"])
    if frequency:
        archive_closures["available_at"] = archive_closures["available_at"].dt.ceil(frequency)
    archive_closures = archive_closures.sort_values(["available_at", "evidence_available_at", "source_order", "observation_id"])
    archive_groups = dict(iter(archive_closures.groupby("vault_address", sort=False)))
    snapshot_groups = dict(iter(snapshots.groupby("vault_address", sort=False)))
    boundary_groups = dict(iter(observations[observations["record_kind"] == "uncertainty_boundary"].groupby("vault_address", sort=False)))
    parts = []
    for address, group in output.groupby("vault_address", sort=False):
        group = group.sort_values("timestamp").reset_index(drop=True)
        candidates = snapshot_groups.get(address, snapshots.iloc[:0])
        if not candidates.empty:
            group = pd.merge_asof(group.drop(columns=fields).sort_values("timestamp"), candidates[fields], left_on="timestamp", right_on="available_at", direction="backward")
        boundaries = boundary_groups.get(address, observations.iloc[:0])
        for boundary in boundaries.itertuples():
            start = pd.Timestamp(boundary.effective_from)
            end = pd.Timestamp(boundary.effective_to) if pd.notna(boundary.effective_to) else pd.Timestamp.max
            in_gap = (group["timestamp"] >= start) & (group["timestamp"] < end)
            measured = pd.to_datetime(group["permission_observed_at"])
            rounded = measured.dt.ceil(frequency) if frequency else measured
            fresh = measured.notna() & (measured >= start) & (rounded <= group["timestamp"])
            mask = in_gap & ~fresh
            for name in (*STATE_FIELDS, *POLICY_FIELDS):
                group.loc[mask, name] = None
            group.loc[mask, "provenance"] = "corrupted_unknown"
            group.loc[mask, "reason"] = boundary.reason
        # Legacy evidence cannot replace genuine responses. A newer archive
        # closure may deny an older inferred flag, but never establish Open.
        for fallback, check_gaps in (
            (inferred_groups.get(address, inferred.iloc[:0]), True),
            (archive_groups.get(address, archive_closures.iloc[:0]), False),
        ):
            if fallback.empty:
                continue
            recovered = pd.merge_asof(group[["timestamp"]].sort_values("timestamp"), fallback[fields], left_on="timestamp", right_on="available_at", direction="backward")
            missing = group["observation_id"].isna().to_numpy() & recovered["observation_id"].notna().to_numpy()
            if not check_gaps:
                newer_closure = group["provenance"].eq("legacy_price_timestamp") & recovered["evidence_available_at"].gt(group["permission_observed_at"])
                missing |= newer_closure.to_numpy()
            if check_gaps:
                for boundary in boundaries.itertuples():
                    start = pd.Timestamp(boundary.effective_from)
                    end = pd.Timestamp(boundary.effective_to) if pd.notna(boundary.effective_to) else pd.Timestamp.max
                    # An older inferred flag cannot bridge an uncertainty gap.
                    in_gap = (group["timestamp"] >= start) & (group["timestamp"] < end)
                    missing &= ~(in_gap & (recovered["permission_observed_at"] < start)).to_numpy()
            for name in fields:
                group.loc[missing, name] = recovered.loc[missing, name].to_numpy()
        # Keep inferred clock quality visible even when that evidence has aged
        # out. Unknown measurement age does not erase a recorded policy cap.
        inferred_age = group["timestamp"] - pd.to_datetime(group["permission_observed_at"])
        stale = group["provenance"].eq("legacy_price_timestamp") & inferred_age.gt(pd.Timedelta(legacy_max_age))
        for name in STATE_FIELDS:
            if name != "max_deposit":
                group.loc[stale, name] = None
        parts.append(group)
    result = pd.concat(parts).sort_values("_decision_order").drop(columns="_decision_order").reset_index(drop=True)
    return result
