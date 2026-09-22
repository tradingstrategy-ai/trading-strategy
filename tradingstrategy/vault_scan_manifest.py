"""Typed representation and validation for the vault scan readiness manifest.

The vault scanner publishes this small JSON document after uploading a cleaned
price file. Live executors use it as a cheap readiness probe: polling this
document must not download parquet data or populate the ordinary dataset cache.
The wire types live here so the producer and consumer can validate the same
field meanings without sharing a runtime package.

The authenticated endpoint is ``GET /vaults/datasets/download/vault-scan-manifest``
on the vault dataset service. It is backed by the private R2 object
``vault-scan-manifest.json`` under the configured upload prefix, served with
``Cache-Control: private, no-store``. The client uses 15-second connect and
60-second read-inactivity timeouts within a five-minute elapsed budget, capped
by the caller's remaining readiness window. It sends ``Cache-Control: no-cache``
and never uses the ordinary 12-hour parquet cache for this request.
"""

import datetime
import re
from typing import Literal, TypedDict


class VaultPriceFileManifest(TypedDict):
    """Identify the uploaded cleaned price object represented by the manifest."""

    #: Exact private R2 object key, retained for audit only.
    key: str

    #: Strong opaque ETag without HTTP quote characters.
    etag: str


class VaultChainScanManifest(TypedDict):
    """Published price freshness information for one numeric chain ID."""

    #: Human-readable label; consumers select chains by numeric key.
    name: str

    #: Completion time of the last successful price fetch, or ``None`` if unknown.
    last_successful_price_scan_ended_at: str | None

    #: Maximum timestamp in the published cleaned file, including hourly bucket
    #: labels from sparse observations. This is not per-vault completeness.
    last_candle_at: str | None


class VaultScanManifest(TypedDict):
    """Small readiness receipt published after a cleaned price upload."""

    #: Wire schema version; unsupported versions are rejected.
    schema_version: Literal[1]

    #: UTC publication time after the referenced price upload.
    published_at: str

    #: Identity and source version of the referenced cleaned price object.
    price_file: VaultPriceFileManifest

    #: Decimal chain-ID keys and their published freshness metadata.
    chains: dict[str, VaultChainScanManifest]


_CHAIN_ID_RE = re.compile(r"^[1-9][0-9]*$")
_TIMESTAMP_RE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?Z")
_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S"


def parse_manifest_timestamp(value: str, field_name: str) -> datetime.datetime:
    """Parse one canonical UTC timestamp from the manifest.

    :param value:
        Timestamp using ``YYYY-MM-DDTHH:MM:SS[.ffffff]Z``.
    :param field_name:
        Field name included in validation errors.
    :return:
        Naive UTC datetime used by the rest of the trading-strategy package.
    :raises ValueError:
        If the wire value is not canonical UTC.
    """

    if not isinstance(value, str) or not _TIMESTAMP_RE.fullmatch(value):
        raise ValueError(f"{field_name} must be a canonical UTC timestamp ending in Z")
    raw = value[:-1]
    try:
        parsed = datetime.datetime.strptime(raw, _TIMESTAMP_FORMAT + ".%f" if "." in raw else _TIMESTAMP_FORMAT)
    except ValueError as exc:
        raise ValueError(f"{field_name} is not a canonical UTC timestamp: {value!r}") from exc
    return parsed


def validate_vault_scan_manifest(document: object) -> VaultScanManifest:
    """Validate and return a decoded vault scan manifest.

    The validator rejects missing required fields and malformed provenance so a
    caller cannot accidentally treat an invalid document as a not-ready result.
    A missing HyperCore entry or a null per-chain timestamp is valid structure,
    but is interpreted as not ready by the scheduling layer.

    :param document:
        JSON-decoded Python object.
    :return:
        The validated manifest mapping.
    :raises ValueError:
        If the document does not satisfy schema version 1.
    """

    if not isinstance(document, dict):
        raise ValueError("Vault scan manifest must be a JSON object")
    if type(document.get("schema_version")) is not int or document["schema_version"] != 1:
        raise ValueError("Vault scan manifest schema_version must be integer 1")
    for field in ("published_at", "price_file", "chains"):
        if field not in document:
            raise ValueError(f"Vault scan manifest is missing {field!r}")

    published_dt = parse_manifest_timestamp(document["published_at"], "published_at")
    price_file = document["price_file"]
    if not isinstance(price_file, dict) or not isinstance(price_file.get("key"), str) or not price_file["key"]:
        raise ValueError("price_file.key must be a non-empty string")
    etag = price_file.get("etag")
    if not isinstance(etag, str) or not etag or etag.startswith("W/") or '"' in etag:
        raise ValueError("price_file.etag must be a strong non-empty ETag without HTTP quotes")

    chains = document["chains"]
    if not isinstance(chains, dict):
        raise ValueError("chains must be a JSON object")
    for chain_id, chain in chains.items():
        if not isinstance(chain_id, str) or not _CHAIN_ID_RE.fullmatch(chain_id):
            raise ValueError(f"Invalid chain ID key: {chain_id!r}")
        if not isinstance(chain, dict) or not isinstance(chain.get("name"), str):
            raise ValueError(f"chains[{chain_id!r}].name must be a string")
        for field in ("last_successful_price_scan_ended_at", "last_candle_at"):
            if field not in chain:
                raise ValueError(f"chains[{chain_id!r}] is missing {field!r}")
            value = chain.get(field)
            if value is not None:
                parsed = parse_manifest_timestamp(value, f"chains[{chain_id!r}].{field}")
                if parsed > published_dt:
                    raise ValueError(f"chains[{chain_id!r}].{field} is after published_at")

    return document  # type: ignore[return-value]
