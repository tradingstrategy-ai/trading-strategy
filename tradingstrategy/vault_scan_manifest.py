"""Vault scan readiness receipts: wire schema, validation and live usage.

The wire types live here so the eth-defi producer and trading-strategy consumer
can agree on field meanings without a cross-package runtime dependency.
Use :func:`validate_vault_scan_manifest` to validate a received JSON document.

Live strategies can poll
``tradingstrategy.vault_data_client.VaultDataClient.fetch_vault_scan_manifest()``
before downloading the large price history. Every call fetches the authenticated
``/vaults/datasets/download/vault-scan-manifest`` JSON endpoint with no local
cache. The same ``VAULT_PRO_API_KEY`` used for the price dataset is required.

The wire types and validation are defined in
``tradingstrategy.vault_scan_manifest``. The eth-defi scanner publishes
``vault-scan-manifest.json`` after the cleaned price upload, in the private R2
bucket selected by ``R2_ALTERNATIVE_VAULT_METADATA_BUCKET_NAME`` and the existing
``UPLOAD_PREFIX``. The serving worker must map the dataset route to this object,
return ``Cache-Control: private, no-store``, bypass CDN caches, and preserve the
price object's strong ETag on price downloads. A frontend proxy alone does not
establish those deployment properties.

A polling caller can inspect freshness without touching the parquet cache::

    from tradingstrategy.vault_data_client import VaultDataClient

    client = VaultDataClient()  # Reads VAULT_PRO_API_KEY from the environment.
    receipt = client.fetch_vault_scan_manifest(request_budget=300)
    hypercore = receipt["chains"].get("9999")

This is one request, not a scheduler. The executor owns the 15-minute polling
cadence, slot deadline and readiness comparisons. Missing chain entries or null
timestamps are valid unknown freshness; missing required fields or malformed
timestamps are contract errors. Network failures, expired request budgets and
temporary HTTP failures raise ``VaultManifestUnavailable`` for the poller to
retry. Authentication failures and HTTP 404 raise distinct fatal errors.

For HyperCore chain ``9999``, the executor requires both successful price-scan
completion and the cleaned price timestamp to reach the logical decision
midnight. Null timestamps or a missing chain mean not ready. This is a
chain-level freshness heuristic, not a count of hourly observations: four-hour
source samples and occasional missed samples are supported.

Manifest reads default to a five-minute elapsed budget, configurable through
``request_budget``. The executor caps it by the remaining eight-hour window.
Connect/read-inactivity timeouts default to 15/60 seconds. The streamed body is
limited to 1 MiB. A blocked read may last until its socket timeout, but responses
beyond the elapsed budget are rejected. This budget does not constrain the
large parquet transfer.

After readiness, use ``download(..., expected_etag=..., destination=...)`` with
a private destination owned by the caller. The returned file must be passed
directly to universe construction. Mismatching headers raise
``VaultDataVersionMismatch`` before reading price bytes; the executor can then
recheck the manifest once. A verified file must not be re-resolved through the
shared 12-hour cache. Ordinary dataset downloads retain that cache behaviour.
Missing or weak price ETags instead raise ``VaultDataDeploymentError``: waiting
for another scanner cycle cannot repair a route that strips source versions.
Price-download network failures abort the current attempt with a redacted
``RuntimeError``; only the JSON probe has the poller's retryable exception.

Historical deposit availability is separate from receipt freshness. For
HyperCore rows, state becomes queryable no earlier than both the price timestamp
and the scanner's ``written_at`` timestamp, rounded up to the decision bucket.
Rows without ``written_at`` cannot establish point-in-time state. The executor
assumes deposits open before ``HYPERCORE_DEPOSIT_STATE_CUTOFF`` (11 April 2026)
and applies its missing/stale-state policy after that date; a current manifest
does not retrospectively certify historical availability.
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
