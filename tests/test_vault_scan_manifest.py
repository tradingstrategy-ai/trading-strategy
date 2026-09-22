"""Validate the wire receipt before live polling trusts scanner provenance."""

from copy import deepcopy

import pytest

from tradingstrategy.vault_scan_manifest import validate_vault_scan_manifest


def test_manifest_nulls_and_contract_rejections() -> None:
    """Distinguish valid unknown freshness from malformed or future-dated data.

    1. Validate a receipt with unknown chain timestamps and additive fields.
    2. Mutate required types, version, ETag and timestamps independently.
    3. Verify every malformed receipt fails instead of becoming 'not ready'.
    """
    # 1. Unknown provenance is allowed for chains not scanned yet.
    document = {
        "schema_version": 1,
        "published_at": "2026-09-22T04:00:00Z",
        "price_file": {"key": "cleaned.parquet", "etag": "v1"},
        "chains": {"9999": {"name": "Hypercore", "last_successful_price_scan_ended_at": None, "last_candle_at": None}},
        "future_optional_field": True,
    }
    assert validate_vault_scan_manifest(document) == document

    # 2. Each case targets a separate wire invariant.
    invalid = []
    for version in [True, 2, "1"]:
        invalid.append({**document, "schema_version": version})
    invalid.append({**document, "published_at": "2026-09-22T04:00:00+00:00"})
    invalid.append({**document, "price_file": {"key": "cleaned.parquet", "etag": 'W/"v1"'}})
    invalid.append({**document, "chains": {"not-a-chain": document["chains"]["9999"]}})
    missing = deepcopy(document)
    del missing["chains"]["9999"]["last_candle_at"]
    invalid.append(missing)
    future = deepcopy(document)
    future["chains"]["9999"]["last_candle_at"] = "2026-09-22T04:00:01Z"
    invalid.append(future)

    # 3. A malformed receipt is a deployment/data-contract failure.
    for receipt in invalid:
        with pytest.raises(ValueError):
            validate_vault_scan_manifest(receipt)
