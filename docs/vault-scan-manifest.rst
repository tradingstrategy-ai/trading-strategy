Vault scan readiness receipts
============================

Live strategies can poll
:py:meth:`tradingstrategy.vault_data_client.VaultDataClient.fetch_vault_scan_manifest`
before downloading the large price history. Every call fetches the authenticated
``/vaults/datasets/download/vault-scan-manifest`` JSON endpoint with no local
cache. The same ``VAULT_PRO_API_KEY`` used for the price dataset is required.

The wire types and validation are defined in
:py:mod:`tradingstrategy.vault_scan_manifest`. The eth-defi scanner publishes
``vault-scan-manifest.json`` after the cleaned price upload, in the private R2
bucket selected by ``R2_ALTERNATIVE_VAULT_METADATA_BUCKET_NAME`` and the existing
``UPLOAD_PREFIX``. The serving worker must map the dataset route to this object,
return ``Cache-Control: private, no-store``, bypass CDN caches, and preserve the
price object's strong ETag on price downloads. A frontend proxy alone does not
establish those deployment properties.

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
