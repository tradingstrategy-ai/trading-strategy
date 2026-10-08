"""Vault data sideloading.

To repackage the vault bundle:

.. code-block:: shell

    # Copy scanned vault bundles to Python package data
    ./scripts/repackage-vault-data.sh


"""

import datetime
import logging
from enum import Enum
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import zstandard

from tradingstrategy.vault_permission import select_permission_state, STATE_FIELDS, POLICY_FIELDS
from tradingstrategy.chain import ChainId
from tradingstrategy.utils.flexible_pickle import flexible_load, filter_broken_enum_values
from tradingstrategy.exchange import Exchange
from tradingstrategy.types import NonChecksummedAddress
from tradingstrategy.vault import VaultUniverse, Vault, VaultMetadata, VaultDepositPermission, VaultDepositStatus, VaultRedemptionStatus, _derive_pair_id_from_address

logger = logging.getLogger(__name__)

#: Path to the bundled vault database
#:
#: To regenerate the bundle: `zstd -f -o tradingstrategy/alternative_data/vault-db.pickle.zstd ~/.tradingstrategy/vaults/vault-db.pickle`
DEFAULT_VAULT_BUNDLE = Path(__file__).parent / ".." / "data_bundles" / "vault-metadata-db.pickle.zstd"

#: Path to the example vault price data
DEFAULT_VAULT_PRICE_BUNDLE = Path(__file__).parent / ".." / "data_bundles" / "vault-prices.parquet"

#: Optional per-(vault, timestamp) availability columns in the cleaned vault price parquet.
#:
#: These describe whether a vault accepted new deposits / allowed redemptions at a given
#: historical timestamp, plus any recorded caps. Repaired daily and HF exports can
#: include these fields; older bundles may lack them. The protocol-specific backtest
#: consumer decides whether missing / unknown values are allowed.
VAULT_STATE_COLUMNS = STATE_FIELDS.copy()

#: Optional original observation clocks and provenance, projected with the whole state.
VAULT_STATE_METADATA_COLUMNS = [
    "permission_observed_at", "permission_provenance", "permission_observation_id",
    "capacity_observed_at", "evidence_available_at", "leader_fraction",
    "relationship_type", "is_closed", "allow_deposits", "written_at", "source_order",
]


def _derive_pair_ids(addresses: pd.Series) -> pd.Series:
    """Derive vault pair ids for an address column, once per distinct address.

    Gives the same result as applying
    :py:func:`tradingstrategy.vault._derive_pair_id_from_address` row by row.
    """
    # A live vault history repeats a few hundred addresses over 1.5M+ rows.
    # A row by row Series.apply() spent about a second per call parsing the
    # same hex strings again, and the vault history pipeline calls this several times.
    codes, uniques = pd.factorize(addresses)
    if len(addresses) == 0 or (codes < 0).any():
        # Keep the row by row behaviour, including its result dtype and its
        # failure on a missing address, for the cases the fast path cannot express
        return addresses.apply(_derive_pair_id_from_address)
    pair_ids = np.fromiter((_derive_pair_id_from_address(address) for address in uniques), dtype=np.int64, count=len(uniques))
    return pd.Series(pair_ids[codes], index=addresses.index, name=addresses.name)


def read_vault_permission_history_parquet(
    path: Path,
    vault_pairs_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Read exact sidecar observations, retaining pre-window evidence and uncertainty.

    Price-window predicates must never discard an earlier permission receipt.
    The sidecar has no price rows and does not create candles.
    """
    dataset = ds.dataset(str(path), format="parquet")
    expression = None
    if vault_pairs_df is not None:
        addresses = vault_pairs_df.loc[vault_pairs_df["chain_id"].astype(int).eq(ChainId.hypercore.value), "address"]
        expression = pc.is_in(pc.utf8_lower(ds.field("vault_address")), value_set=pa.array(addresses.str.lower().tolist(), type=pa.string()))
    return dataset.to_table(filter=expression).to_pandas(ignore_metadata=True)


# HyperCore availability fields became complete enough for historical admission decisions at
# this daily boundary. Before it, backtests deliberately assume deposits were open.
HYPERCORE_DEPOSIT_STATE_CUTOFF = datetime.datetime(2026, 4, 11)


#: Cached loaded vault universe from our defaut bundle
_cached_vault_universe: dict[Path, VaultUniverse] = {}


def load_vault_database(
    path: Path | None = None,
    filter_bad_entries: bool = True,
) -> VaultUniverse:
    """Load pickled vault metadata database generated with an offline script.

    - For sideloading vault data

    - Normalises vault data in a good documented format

    - For the generation `see this tutorial <https://web3-ethereum-defi.readthedocs.io/tutorials/erc-4626-scan-prices.html>`__

    :param path:
        Path to the pickle file.

        If not given use the default location.

        Can be zstd compressed with .zstd suffix.
    """

    from eth_defi.vault.vaultdb import VaultDatabase

    if path is None:
        path = DEFAULT_VAULT_BUNDLE

    assert path.exists(), f"No vault file: {path}"

    existing = _cached_vault_universe.get(path)
    if existing is not None:
        return existing


    vault_db: VaultDatabase

    if path.suffix == ".zstd":
        with zstandard.open(path, "rb") as inp:
            vault_db = flexible_load(inp)
    else:
        # Normal pickle
        with path.open("rb") as f:
            vault_db = flexible_load(f)

    vaults = []

    #         data = {
    #             "Symbol": vault.symbol,
    #             "Name": vault.name,
    #             "Address": detection.address,
    #             "Denomination": vault.denomination_token.symbol if vault.denomination_token else None,
    #             "NAV": total_assets,
    #             "Protocol": get_vault_protocol_name(detection.features),
    #             "Mgmt fee": management_fee,
    #             "Perf fee": performance_fee,
    #             "Shares": total_supply,
    #             "First seen": detection.first_seen_at,
    #             "_detection_data": detection,
    #             "_denomination_token": denomination_token,
    #             "_share_token": vault.share_token.export() if vault.share_token else None,
    #         }

    def _safe_get(d: dict, key: str, key2: str, default=None):
        try:
            return (d.get(key, {}) or {}).get(key2, default)
        except Exception:
            return default

    for address, entry in vault_db.items():
        try:
            detection: "eth_defi.erc_4626.core.ERC4262VaultDetection" = entry["_detection_data"]

            if filter_bad_entries:
                if (not entry["Name"]) or (not entry["Denomination"]):
                    # Skip invalid entries as all other required data is missing
                    continue

                if "unknown" in entry["Name"]:
                    # Skip nameless / broken entries
                    continue

                if detection.chain < 0:
                    # Skip negative chain IDs (placeholder/sentinel values)
                    continue

            try:
                chain_id = ChainId(detection.chain)
            except ValueError:
                if filter_bad_entries:
                    # Skip vaults with unknown chain IDs
                    continue
                raise

            protocol_slug = entry["Protocol"].lower().replace(" ", "-")

            vault = Vault(
                chain_id=chain_id,
                name=entry.get("Name") or "<unknown>",
                token_symbol=entry["Symbol"],
                vault_address=entry["Address"],
                denomination_token_address=_safe_get(entry,"_denomination_token", "address"),
                denomination_token_symbol=_safe_get(entry,"_denomination_token", "symbol"),
                denomination_token_decimals=_safe_get(entry,"_denomination_token", "decimals"),
                share_token_address=_safe_get(entry,"_share_token", "address") or entry["Address"],
                share_token_symbol=_safe_get(entry,"_share_token", "symbol") or entry["Symbol"],
                share_token_decimals=_safe_get(entry,"_share_token", "decimals") or 18,
                protocol_name=entry["Protocol"],
                protocol_slug=protocol_slug,
                performance_fee=entry["Perf fee"],
                management_fee=entry["Mgmt fee"],
                deployed_at=detection.first_seen_at,
                features=filter_broken_enum_values(detection.features),
                denormalised_data_updated_at=detection.updated_at,
                tvl=entry["NAV"],
                issued_shares=entry["Shares"],
            )
        except Exception as e:
            raise RuntimeError(f"Could not decode entry: {entry}") from e

        vaults.append(vault)

    vault_universe = VaultUniverse(vaults)
    _cached_vault_universe[path] = vault_universe
    return vault_universe


def convert_vaults_to_trading_pairs(
    vaults: Iterable[Vault]
) -> tuple[list[Exchange], pd.DataFrame]:
    """Create a dataframe that contains vaults as trading pairs to be included alongside real trading pairs.

    - Generates :py:class:`tradingstrategy.pair.PandasPairUniverse` compatible dataframe for all vaults
    - Adds

    :return:
        Exchange data, pair dataframe tuple
    """

    exchanges = list(Exchange(**v.export_as_exchange()) for v in vaults)
    rows = [v.export_as_trading_pair() for v in vaults]
    pairs_df = pd.DataFrame(rows).astype(Vault.get_pandas_schema())
    return exchanges, pairs_df


def load_single_vault(
    chain_id: ChainId,
    vault_address: str,
    path=DEFAULT_VAULT_BUNDLE,
) -> tuple[list[Exchange], pd.DataFrame]:
    """Load a single bundled vault entry and return as pairs data.

    Example:

    .. code-block:: python

        vault_exchanges, vault_pairs_df = load_single_vault(ChainId.base, "0x45aa96f0b3188d47a1dafdbefce1db6b37f58216")
        exchange_universe.add(vault_exchanges)
        pairs_df = pd.concat([pairs_df, vault_pairs_df])

    """
    vault_universe = load_vault_database(path)
    vault_universe = vault_universe.limit_to_single(chain_id, vault_address)
    return convert_vaults_to_trading_pairs(vault_universe.export_all_vaults())


def load_multiple_vaults(
    vaults: list[tuple[ChainId, NonChecksummedAddress]] | VaultUniverse,
    path=DEFAULT_VAULT_BUNDLE,
    check_all_vaults_found: bool = True,
) -> tuple[list[Exchange], pd.DataFrame]:
    """Load a single bundled vault entry and return as pairs data.

    Example:

    .. code-block:: python

        vault_exchanges, vault_pairs_df = load_multiple_vaults([ChainId.base, "0x45aa96f0b3188d47a1dafdbefce1db6b37f58216"])
        exchange_universe.add(vault_exchanges)
        pairs_df = pd.concat([pairs_df, vault_pairs_df])

    """
    if isinstance(vaults, VaultUniverse):
        vault_universe = vaults
    else:
        vault_universe = load_vault_database(path)
        vault_universe = vault_universe.limit_to_vaults(vaults, check_all_vaults_found=check_all_vaults_found)
    return convert_vaults_to_trading_pairs(vault_universe.export_all_vaults())



def create_vault_universe(
    vaults: list[tuple[ChainId, NonChecksummedAddress]],
    path=DEFAULT_VAULT_BUNDLE,
) -> VaultUniverse:
    """Load a single bundled vault entry and return as pairs data.

    Example:

    .. code-block:: python

        vault_exchanges, vault_pairs_df = load_multiple_vaults([ChainId.base, "0x45aa96f0b3188d47a1dafdbefce1db6b37f58216"])
        exchange_universe.add(vault_exchanges)
        pairs_df = pd.concat([pairs_df, vault_pairs_df])

    """
    vault_universe = load_vault_database(path)
    vault_universe.limit_to_vaults(vaults)
    return convert_vaults_to_trading_pairs(vault_universe.export_all_vaults())



def load_vault_price_data(
    pairs_df: pd.DataFrame,
    prices_path: Path=DEFAULT_VAULT_PRICE_BUNDLE,
) -> pd.DataFrame:
    """Sideload price data for vaults.

    Uses Arrow-native filtering before pandas conversion to avoid
    converting the entire Parquet file to pandas when only a small
    subset of vaults is needed.

    Schema sample:

    .. code-block:: plain

        schema = pa.schema([
            ("chain", pa.uint32()),
            ("address", pa.string()),  # Lowercase
            ("block_number", pa.uint32()),
            ("timestamp", pa.timestamp("ms")),  # s accuracy does not seem to work on rewrite
            ("share_price", pa.float64()),
            ("total_assets", pa.float64()),
            ("total_supply", pa.float64()),
            ("performance_fee", pa.float32()),
            ("management_fee", pa.float32()),
            ("errors", pa.string()),
        ])

    :param pairs_df:
        Vaults in DataFrame format as exported functions in this module.

    :param path:
        Load vault prices file.

        If not given use the default hardcoded sample bundle.

    :return:
        DataFrame with the columns as defined in the schema above.

    """
    assert isinstance(pairs_df, pd.DataFrame)

    assert prices_path.exists(), f"Vault price file does not exist: {prices_path}"
    chain_values = pairs_df["chain_id"].astype(int)
    address_values = pairs_df["address"].astype(str).str.lower()
    vaults_to_match = set(zip(chain_values, address_values, strict=False))

    assert len(vaults_to_match) < 3000, f"The vaults to load number looks too high: {len(vaults_to_match)}"

    # Push the coarse chain/address predicate into the Parquet scanner so Arrow
    # can skip row groups before materialising a table.
    unique_chains = pa.array(sorted({int(c) for c, _ in vaults_to_match}), type=pa.uint32())
    unique_addresses = pa.array(sorted({a for _, a in vaults_to_match}))
    dataset = ds.dataset(str(prices_path), format="parquet")
    table = dataset.to_table(
        filter=(
            pc.is_in(ds.field("chain"), value_set=unique_chains)
            & pc.is_in(pc.utf8_lower(ds.field("address")), value_set=unique_addresses)
        ),
    )

    # Convert only the filtered rows to pandas
    df = table.to_pandas()
    return filter_vault_price_history(df, pairs_df)


def read_vault_price_history_parquet(
    prices_path: Path,
    vault_pairs_df: pd.DataFrame | None = None,
    start_at: datetime.datetime | None = None,
    end_at: datetime.datetime | None = None,
    columns: list[str] | None = None,
) -> pd.DataFrame:
    """Read cleaned vault price history parquet with optional Arrow pushdown.

    The live vault history parquet contains millions of rows across all vaults.
    Strategies usually need a small vault subset and a bounded time range, so
    pushing the coarse predicate into Arrow avoids materialising the full file
    as pandas before filtering.
    """
    assert prices_path.exists(), f"Vault price file does not exist: {prices_path}"

    dataset = ds.dataset(str(prices_path), format="parquet")
    schema_names = set(dataset.schema.names)
    if "timestamp" in schema_names:
        timestamp_column = "timestamp"
    elif "__index_level_0__" in schema_names:
        timestamp_column = "__index_level_0__"
    else:
        raise AssertionError(f"Vault price file does not contain a timestamp column: {dataset.schema.names}")

    timestamp_type = dataset.schema.field(timestamp_column).type
    assert pa.types.is_timestamp(timestamp_type), f"Vault price timestamp column must be timestamp typed, got {timestamp_type}"
    expression = None

    if vault_pairs_df is not None:
        chain_values = vault_pairs_df["chain_id"].astype(int)
        address_values = vault_pairs_df["address"].astype(str).str.lower()
        unique_chains = pa.array(sorted(set(chain_values)), type=pa.uint32())
        unique_addresses = pa.array(sorted(set(address_values)))
        expression = (
            pc.is_in(ds.field("chain"), value_set=unique_chains)
            & pc.is_in(pc.utf8_lower(ds.field("address")), value_set=unique_addresses)
        )

    if start_at is not None:
        start_filter = ds.field(timestamp_column) >= _make_timestamp_scalar(start_at, timestamp_type)
        expression = start_filter if expression is None else expression & start_filter

    if end_at is not None:
        end_filter = ds.field(timestamp_column) <= _make_timestamp_scalar(end_at, timestamp_type)
        expression = end_filter if expression is None else expression & end_filter

    if columns is not None:
        requested_columns = [timestamp_column if c == "timestamp" else c for c in columns]
        # Drop only the *optional* vault-history columns when they are absent from this parquet
        # schema, so callers can opt in to them without breaking on older files (e.g. the daily
        # price bundle). Any other missing requested column is kept so the read still fails fast
        # on a genuine schema mismatch (e.g. a misspelled `share_price`).
        optional_columns = set(VAULT_STATE_COLUMNS) | set(VAULT_STATE_METADATA_COLUMNS)
        requested_columns = [c for c in requested_columns if c in schema_names or c not in optional_columns]
        required_columns = {timestamp_column}
        if vault_pairs_df is not None:
            required_columns.update({"chain", "address"})
        columns = list(dict.fromkeys([*requested_columns, *required_columns]))

    table = dataset.to_table(filter=expression, columns=columns)
    df = table.to_pandas(ignore_metadata=True)
    if timestamp_column != "timestamp" and timestamp_column in df.columns:
        df = df.rename(columns={timestamp_column: "timestamp"})

    if vault_pairs_df is not None:
        return filter_vault_price_history(df, vault_pairs_df, start_at=start_at, end_at=end_at)

    if "timestamp" in df.columns:
        _normalise_timestamp_column(df)

    return df


def _make_timestamp_scalar(
    value: datetime.datetime,
    timestamp_type: pa.DataType,
) -> pa.Scalar:
    """Build an Arrow timestamp scalar matching the parquet timestamp field."""
    timestamp = pd.Timestamp(value)
    if pa.types.is_timestamp(timestamp_type):
        if timestamp_type.tz is None and timestamp.tzinfo is not None:
            timestamp = timestamp.tz_convert(None)
        elif timestamp_type.tz is not None and timestamp.tzinfo is None:
            timestamp = timestamp.tz_localize("UTC")

    return pa.scalar(timestamp.to_pydatetime(), type=timestamp_type)


def filter_vault_price_history(
    vault_prices_df: pd.DataFrame,
    vault_pairs_df: pd.DataFrame,
    start_at: datetime.datetime | None = None,
    end_at: datetime.datetime | None = None,
) -> pd.DataFrame:
    """Filter vault history to the requested vaults and optional date window.

    This helper centralises vault-history filtering so bundled parquet reads and
    Trading Strategy website downloads expose the same caller-facing behaviour.

    The returned DataFrame is normalised as follows:

    - only exact ``(chain_id, address)`` tuples from ``vault_pairs_df`` remain
    - ``address`` values are lowercased
    - ``timestamp`` is materialised as a pandas datetime column
    - optional ``start_at`` / ``end_at`` clipping is applied after tuple
      filtering

    The helper accepts vault history where ``timestamp`` is already a column or
    where parquet round-tripping has placed it in the DataFrame index.
    """
    assert "chain" in vault_prices_df.columns, f"Got {vault_prices_df.columns}"
    assert "address" in vault_prices_df.columns, f"Got {vault_prices_df.columns}"

    if "timestamp" not in vault_prices_df.columns and vault_prices_df.index.name == "timestamp":
        vault_prices_df = vault_prices_df.reset_index()

    assert "timestamp" in vault_prices_df.columns, f"Got {vault_prices_df.columns}"

    vaults_to_match = set(
        zip(
            vault_pairs_df["chain_id"].astype(int),
            vault_pairs_df["address"].astype(str).str.lower(),
            strict=False,
        )
    )
    addresses = vault_prices_df["address"].astype(str).str.lower()
    mask = pd.MultiIndex.from_arrays([vault_prices_df["chain"], addresses]).isin(vaults_to_match)

    # read_vault_price_history_parquet() has already pushed the vault and time
    # predicates into Arrow, so on that path every row usually matches. Each
    # boolean .loc[] take copies all columns of a 1.5M+ row frame, so skip the
    # takes that would keep every row and do the copy only once.
    if mask.all():
        filtered_df = vault_prices_df.copy()
    else:
        filtered_df = vault_prices_df.loc[mask].copy()

    # Reuse the lowercased addresses from matching instead of lowercasing again
    filtered_df["address"] = addresses.to_numpy()[mask]
    _normalise_timestamp_column(filtered_df)

    # One combined window mask instead of a separate take per bound
    in_window = np.ones(len(filtered_df), dtype=bool)
    if start_at is not None:
        in_window &= (filtered_df["timestamp"] >= pd.Timestamp(start_at)).to_numpy()

    if end_at is not None:
        in_window &= (filtered_df["timestamp"] <= pd.Timestamp(end_at)).to_numpy()

    if not in_window.all():
        filtered_df = filtered_df.loc[in_window]

    return filtered_df


def _normalise_timestamp_column(df: pd.DataFrame) -> None:
    """Normalise timestamp column to naive UTC pandas datetimes in-place."""
    if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
        df["timestamp"] = pd.to_datetime(df["timestamp"])

    if isinstance(df["timestamp"].dtype, pd.DatetimeTZDtype):
        df["timestamp"] = df["timestamp"].dt.tz_convert(None)



def convert_vault_prices_to_candles(
    raw_prices_df: pd.DataFrame,
    frequency: str = "1d",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Convert vault price data to candle format.

    - Partial support for price candle format to be used in backtesting

    - For the format see :py:func:`load_vault_price_data`

    - Only USD stablecoin denominated vaults supported for now

    - Adds the derived ``pair_id`` column to ``raw_prices_df`` in place

    Example:

    .. code-block: python

        # Load data only for IPOR USDC vault on Base
        exchanges, pairs_df = load_multiple_vaults([(ChainId.base, "0x45aa96f0b3188d47a1dafdbefce1db6b37f58216")])
        vault_prices_df = load_vault_price_data(pairs_df)
        assert len(vault_prices_df) == 176  # IPOR has 176 days worth of data

        # Create pair universe based on the vault data
        exchange_universe = ExchangeUniverse({e.exchange_id: e for e in exchanges})
        pair_universe = PandasPairUniverse(pairs_df, exchange_universe=exchange_universe)

        # Create price candles from vault share price scrape
        candle_df, liquidity_df = convert_vault_prices_to_candles(vault_prices_df, "1h")
        candle_universe = GroupedCandleUniverse(candle_df, time_bucket=TimeBucket.h1)
        assert candle_universe.get_candle_count() == 4201
        assert candle_universe.get_pair_count() == 1

        liquidity_universe = GroupedLiquidityUniverse(liquidity_df, time_bucket=TimeBucket.h1)
        assert liquidity_universe.get_sample_count() == 4201
        assert liquidity_universe.get_pair_count() == 1

        # Get share price as candles for a single vault
        ipor_usdc = pair_universe.get_pair_by_smart_contract("0x45aa96f0b3188d47a1dafdbefce1db6b37f58216")
        prices = candle_universe.get_candles_by_pair(ipor_usdc)
        assert len(prices) == 4201

        # Query single price sample
        timestamp = pd.Timestamp("2025-04-01 04:00")
        price, when = candle_universe.get_price_with_tolerance(
            pair=ipor_usdc,
            when=timestamp,
            tolerance=pd.Timedelta("2h"),
        )
        assert price == pytest.approx(1.0348826417292332)

        # Query TVL
        liquidity, when = liquidity_universe.get_liquidity_with_tolerance(
            pair_id=ipor_usdc.pair_id,
            when=timestamp,
            tolerance=pd.Timedelta("2h"),
        )
        assert liquidity == pytest.approx(1429198.98104)

    :return:
        Prices dataframe, TVL dataframe
    """

    assert "chain" in raw_prices_df.columns, f"Got {raw_prices_df.columns}"
    assert "address" in raw_prices_df.columns, f"Got {raw_prices_df.columns}"

    assert frequency in ["1d", "1h"], f"Got {frequency}"

    # Callers read the derived pair ids from the source frame afterwards, e.g.
    # the live stale vault data check, so this column is added in place
    raw_prices_df["pair_id"] = _derive_pair_ids(raw_prices_df["address"])

    # Even for daily data, we need to resample, because built-in vault price example
    # data is not midnight aligned
    return _resample_vault_candles(raw_prices_df, frequency)


def _resample_vault_candles(df: pd.DataFrame, frequency: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Resample share price and TVL samples of all vaults in one grouped pass.

    Produces the same candles as running
    :py:func:`tradingstrategy.utils.forward_fill.resample_candles_multiple_pairs`
    once over share price and once over TVL OHLC columns:

    - every bucket between a vault's first and last sample exists
    - an empty bucket is flat at the last close, with zero volume
    - ``forward_filled`` marks buckets without a real sample

    That resampler loops over vaults and calls ``DataFrame.resample()`` for each.
    Run twice over 500+ HyperCore vaults and 1.5M+ samples it takes about
    10 seconds of every live universe construction, while a single
    ``groupby(pair, bucket)`` over both value columns takes a fraction of a second.

    :param df:
        Vault samples with ``pair_id``, ``share_price`` and ``total_assets``,
        timestamped by a ``timestamp`` column or a DatetimeIndex.

    :param frequency:
        ``1d`` or ``1h``. For these, a ``resample()`` bucket equals the floored timestamp.

    :return:
        Price candles, TVL candles
    """
    value_columns = ["share_price", "total_assets"]
    has_forward_filled = "forward_filled" in df.columns
    columns = ["pair_id", *value_columns, *(["forward_filled"] if has_forward_filled else [])]

    # first and last depend on the row order within a bucket, so order rows as the
    # per-vault resampler saw them. It sorted a timestamp column with sort_index(),
    # and resample() stable sorts an unordered DatetimeIndex itself.
    if not isinstance(df.index, pd.DatetimeIndex) and "timestamp" in df.columns:
        source = df[["timestamp", *columns]].set_index("timestamp").sort_index()
    else:
        source = df[columns].sort_index(kind="mergesort")

    # One aggregation pass for both candle sets
    bucket_width = pd.Timedelta(frequency)
    buckets = source.index.floor(bucket_width)
    aggregations = {}
    for column in value_columns:
        aggregations.update({
            f"{column}_open": (column, "first"),
            f"{column}_high": (column, "max"),
            f"{column}_low": (column, "min"),
            f"{column}_close": (column, "last"),
        })
    if has_forward_filled:
        aggregations["forward_filled"] = ("forward_filled", "max")
    aggregated = source.groupby([source["pair_id"].to_numpy(), buckets], sort=True).agg(**aggregations)

    # groupby() only yields buckets that have samples, while resample() also
    # creates the empty buckets in between. Build each vault's full bucket range,
    # first to last bucket, with vectorised arithmetic instead of a per-vault
    # date_range(), then reindex so the missing buckets appear as NaN rows.
    vault_buckets = pd.Series(aggregated.index.get_level_values(1))
    bounds = vault_buckets.groupby(aggregated.index.get_level_values(0).to_numpy(), sort=True).agg(["min", "max"])
    # Keep the source timestamp resolution, e.g. milliseconds from parquet
    unit = np.datetime_data(bounds["min"].to_numpy().dtype)[0]
    step = bucket_width.to_timedelta64().astype(f"timedelta64[{unit}]")
    counts = ((bounds["max"] - bounds["min"]) // bucket_width).to_numpy(dtype=np.int64) + 1
    # Position of each output row within its own vault's bucket range
    offsets = np.arange(counts.sum()) - np.repeat(np.cumsum(counts) - counts, counts)
    full_pair_ids = np.repeat(bounds.index.to_numpy(), counts)
    full_buckets = np.repeat(bounds["min"].to_numpy(), counts) + offsets * step
    aggregated = aggregated.reindex(pd.MultiIndex.from_arrays([full_pair_ids, full_buckets]))
    index = pd.DatetimeIndex(full_buckets, name=source.index.name)

    def build(column: str) -> pd.DataFrame:
        """Assemble one candle set, with columns in the order the per-vault resampler produced."""
        candles = pd.DataFrame({
            "open": aggregated[f"{column}_open"].to_numpy(),
            "high": aggregated[f"{column}_high"].to_numpy(),
            "low": aggregated[f"{column}_low"].to_numpy(),
            "close": aggregated[f"{column}_close"].to_numpy(),
            # Vault samples carry no volume. resample() summed zeros, also in empty buckets.
            "volume": np.zeros(len(index), dtype=np.int64),
        }, index=index)
        if has_forward_filled:
            candles["forward_filled"] = aggregated["forward_filled"].to_numpy()
        candles["timestamp"] = candles.index
        candles["pair_id"] = full_pair_ids
        # Mark buckets without a real observation before filling them
        missing = candles["close"].isna()
        if has_forward_filled:
            # Reindexed buckets hold NaN markers. Nullable boolean fills them without the
            # deprecated object downcast that a plain fillna(False) triggers.
            candles["forward_filled"] = candles["forward_filled"].astype("boolean").fillna(False).astype(bool) | missing
        else:
            candles["forward_filled"] = missing
        # Fill within each vault only, so one vault's close never leaks into the next
        candles["close"] = candles["close"].groupby(full_pair_ids, sort=False).ffill()
        # An empty interval is a flat candle at the last observed close
        for ohl_column in ("open", "high", "low"):
            candles[ohl_column] = candles[ohl_column].fillna(candles["close"])
        candles.attrs["forward_filled_until"] = None
        return candles

    return build("share_price"), build("total_assets")


#: Map our supported candle frequencies to pandas resample offsets.
_VAULT_STATE_FREQUENCIES = {"1d": "1D", "1h": "1h"}


def _normalise_bool_like(series: pd.Series) -> pd.Series:
    """Normalise a ``true``/``false``/NA-ish column to pandas nullable boolean.

    The cleaned vault parquet stores ``deposits_open`` / ``redemption_open`` as strings
    (``"true"`` / ``"false"``) with NA for unknown. Anything that is not an explicit
    ``true`` / ``false`` becomes :py:data:`pandas.NA` (unknown). The downstream pricing
    model decides whether unknown state permits deposits for the protocol and date.
    """
    lowered = series.astype("string").str.lower()
    out = pd.Series(pd.NA, index=series.index, dtype="boolean")
    out[lowered == "true"] = True
    out[lowered == "false"] = False
    return out


def convert_vault_prices_to_vault_state(
    raw_prices_df: pd.DataFrame,
    frequency: str = "1d",
    permission_history_df: pd.DataFrame | None = None,
) -> pd.DataFrame | None:
    """Select coherent vault state without refreshing original observation clocks.

    Other chains retain their legacy last-whole-row, floored-bucket behaviour.
    HyperCore receipts become available at the next decision boundary. Genuine
    responses, including unknowns, beat inferred legacy price flags. Independent
    sidecar observations can change state after the newest price without creating
    synthetic price or TVL rows. Publication and ``written_at`` never refresh
    permission or recorded policy inputs. Clockless, untagged legacy flags use
    the original price timestamp with explicit inferred provenance; tagged
    corrupt flags remain unknown. Recorded shares and caps survive absent
    capacity clocks, and NULL caps remain distinct from explicit zero.

    :param raw_prices_df: Price projections, optionally carrying original permission metadata.
    :param frequency: Decision buckets, ``1d`` or ``1h``.
    :param permission_history_df: Explicit sidecar from the same recovery generation.
    :return: Sparse state with original clocks and provenance, or ``None`` when no availability data exists.
    """
    assert frequency in _VAULT_STATE_FREQUENCIES, f"Got {frequency}"
    if "timestamp" not in raw_prices_df and raw_prices_df.index.name == "timestamp":
        raw_prices_df = raw_prices_df.reset_index()
    present = [c for c in VAULT_STATE_COLUMNS if c in raw_prices_df]
    if not present and permission_history_df is None:
        return None
    df = raw_prices_df.copy().reset_index(drop=True)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert(None).astype("datetime64[ns]")
    df["address"] = df["address"].str.lower()
    df["pair_id"] = _derive_pair_ids(df["address"])
    hypercore = df.get("chain", pd.Series(0, index=df.index)).eq(ChainId.hypercore.value)
    freq = _VAULT_STATE_FREQUENCIES[frequency]
    other = df.loc[~hypercore, ["timestamp", "pair_id", "address", *present]].copy()
    other = other.sort_values("timestamp", kind="stable")
    other["timestamp"] = other["timestamp"].dt.floor(freq)
    other = other.drop_duplicates(["pair_id", "timestamp"], keep="last")
    if not hypercore.any() and (permission_history_df is None or permission_history_df.empty):
        for col in ("deposits_open", "redemption_open"):
            if col in other:
                other[col] = _normalise_bool_like(other[col])
        return other.reset_index(drop=True)
    prices = df.loc[hypercore].copy()
    observations = prices.rename(columns={
        "address": "vault_address", "permission_provenance": "provenance",
        "permission_observation_id": "observation_id",
    })
    if "source_order" not in observations:
        observations["source_order"] = range(len(observations))
    observations["record_kind"] = "observation"
    observations["permission_observed_at"] = pd.to_datetime(observations.get("permission_observed_at", pd.Series(pd.NaT, index=observations.index)), utc=True).dt.tz_convert(None).astype("datetime64[ns]")
    original_provenance = observations.get("provenance", pd.Series(None, index=observations.index, dtype=object))
    inferred = observations["permission_observed_at"].isna() & (original_provenance.isna() | original_provenance.eq("legacy_price_timestamp"))
    observations.loc[inferred, "permission_observed_at"] = observations.loc[inferred, "timestamp"]
    if "provenance" not in observations:
        observations["provenance"] = None
    observations.loc[inferred, "provenance"] = "legacy_price_timestamp"
    observations["provenance"] = observations["provenance"].fillna("observed")
    observations["observation_id"] = observations.get("observation_id", pd.Series(None, index=observations.index, dtype=object))
    # Format names only for rows lacking an id. Formatting a name for every one
    # of the 1.5M+ price rows and discarding most of them cost about half a second.
    missing_ids = observations["observation_id"].isna()
    observations.loc[missing_ids, "observation_id"] = observations.loc[missing_ids, "source_order"].map(lambda n: f"price-row-{n}")
    # A convenience projection can repeat one receipt on many price rows.
    # Retain its first coherent projection, including a capacity policy cap.
    observations = observations.sort_values("timestamp", kind="stable").drop_duplicates(["vault_address", "observation_id"], keep="first")
    corrupted = observations["provenance"].isin(("corrupted_unknown", "legacy_unverified"))
    observations["reason"] = None
    observations.loc[corrupted, "record_kind"] = "uncertainty_boundary"
    observations.loc[corrupted, "effective_from"] = observations.loc[corrupted, "timestamp"]
    observations.loc[corrupted, "reason"] = "Legacy permission flags are unverified or corrupted"
    if permission_history_df is not None and not permission_history_df.empty:
        sidecar = permission_history_df.copy().reset_index(drop=True)
        if "source_order" not in sidecar:
            sidecar["source_order"] = range(len(prices), len(prices) + len(sidecar))
        sidecar["vault_address"] = sidecar["vault_address"].str.lower()
        # Derive permission only from flags in the same response. An explicit
        # closure is sufficient; otherwise both flags are needed to prove Open.
        closed = _normalise_bool_like(sidecar["is_closed"])
        allowed = _normalise_bool_like(sidecar["allow_deposits"])
        parent = sidecar["relationship_type"].eq("parent")
        sidecar["deposits_open"] = (~closed & (allowed | parent)).astype("boolean")
        sidecar["deposit_closed_reason"] = None
        sidecar.loc[closed.fillna(False), "deposit_closed_reason"] = "Vault is permanently closed"
        sidecar.loc[~closed.fillna(False) & ~parent & allowed.eq(False).fillna(False), "deposit_closed_reason"] = "Vault deposits disabled by leader"
        # New sidecars record nullable policy caps. Preserve explicit values and
        # NULLs; only older schemas without this column need policy derivation.
        if "max_deposit" not in sidecar:
            fraction = pd.to_numeric(sidecar["leader_fraction"], errors="coerce")
            low_share = fraction.lt(0.055) & sidecar["relationship_type"].fillna("normal").eq("normal")
            sidecar["max_deposit"] = float("nan")
            sidecar.loc[low_share | sidecar["deposits_open"].eq(False).fillna(False), "max_deposit"] = 0.0
        # Exact sidecar evidence takes precedence over a convenience projection
        # of the same receipt, whose capacity fields may have been cleaned.
        observations = pd.concat([observations, sidecar], ignore_index=True)
        observations = observations.drop_duplicates(["vault_address", "observation_id"], keep="last")
    defaults = {**{c: None for c in (*STATE_FIELDS, *POLICY_FIELDS)}, "capacity_observed_at": pd.NaT, "evidence_available_at": pd.NaT, "effective_from": pd.NaT, "effective_to": pd.NaT, "source_endpoint": "price projection", "reason": None}
    for name, default in defaults.items():
        if name not in observations:
            observations[name] = default
    for name in ("permission_observed_at", "capacity_observed_at", "evidence_available_at", "effective_from", "effective_to"):
        observations[name] = pd.to_datetime(observations[name], utc=True).dt.tz_convert(None).astype("datetime64[ns]")
    for name in ("deposits_open", "redemption_open", "is_closed", "allow_deposits"):
        observations[name] = _normalise_bool_like(observations[name])
    clocks = observations.melt(
        id_vars="vault_address",
        value_vars=["permission_observed_at", "evidence_available_at", "effective_from", "effective_to"],
        value_name="decision_at",
    ).rename(columns={"decision_at": "timestamp"})[["vault_address", "timestamp"]]
    decisions = pd.concat([clocks, prices[["address", "timestamp"]].rename(columns={"address": "vault_address"})], ignore_index=True)
    decisions["timestamp"] = pd.to_datetime(decisions["timestamp"]).astype("datetime64[ns]").dt.ceil(freq)
    decisions = decisions.dropna(subset=["timestamp"]).drop_duplicates()
    selected = select_permission_state(observations, decisions, freq)
    selected = selected.rename(columns={"vault_address": "address", "provenance": "permission_provenance", "observation_id": "permission_observation_id"})
    selected["pair_id"] = _derive_pair_ids(selected["address"])
    columns = ["timestamp", "pair_id", "address", *VAULT_STATE_COLUMNS, *POLICY_FIELDS, "permission_observed_at", "permission_provenance", "permission_observation_id", "capacity_observed_at", "evidence_available_at", "source_order"]
    result = pd.concat([other, selected[columns]], ignore_index=True)
    for col in ("deposits_open", "redemption_open"):
        if col in result:
            result[col] = _normalise_bool_like(result[col])
    return result.sort_values(["pair_id", "timestamp"], kind="stable").reset_index(drop=True)


def _parse_period_metrics(pm_dict: dict):
    """Parse a PeriodMetrics object from JSON dict.

    :param pm_dict:
        Dictionary from JSON with period metrics fields.

    :return:
        PeriodMetrics instance.
    """
    from eth_defi.research.vault_metrics import PeriodMetrics

    def _parse_timestamp(val):
        if val is None:
            return None
        if isinstance(val, str):
            # ISO format timestamp
            try:
                return pd.Timestamp(val)
            except Exception:
                return None
        return val

    return PeriodMetrics(
        period=pm_dict.get("period"),
        error_reason=pm_dict.get("error_reason"),
        period_start_at=_parse_timestamp(pm_dict.get("period_start_at")),
        period_end_at=_parse_timestamp(pm_dict.get("period_end_at")),
        share_price_start=pm_dict.get("share_price_start"),
        share_price_end=pm_dict.get("share_price_end"),
        raw_samples=pm_dict.get("raw_samples", 0),
        samples_start_at=_parse_timestamp(pm_dict.get("samples_start_at")),
        samples_end_at=_parse_timestamp(pm_dict.get("samples_end_at")),
        daily_samples=pm_dict.get("daily_samples", 0),
        returns_gross=pm_dict.get("returns_gross"),
        returns_net=pm_dict.get("returns_net"),
        cagr_gross=pm_dict.get("cagr_gross"),
        cagr_net=pm_dict.get("cagr_net"),
        volatility=pm_dict.get("volatility"),
        sharpe=pm_dict.get("sharpe"),
        max_drawdown=pm_dict.get("max_drawdown"),
        tvl_start=pm_dict.get("tvl_start"),
        tvl_end=pm_dict.get("tvl_end"),
        tvl_low=pm_dict.get("tvl_low"),
        tvl_high=pm_dict.get("tvl_high"),
        ranking_overall=pm_dict.get("ranking_overall"),
        ranking_chain=pm_dict.get("ranking_chain"),
        ranking_protocol=pm_dict.get("ranking_protocol"),
    )


def _parse_vault_metadata(entry: dict) -> VaultMetadata:
    """Parse VaultMetadata from JSON entry.

    :param entry:
        JSON dict from the vault universe JSON blob.

    :return:
        VaultMetadata instance with all available fields populated.
    """
    def _parse_datetime(val, *, naive_utc: bool = False):
        if val is None:
            return None
        if isinstance(val, str):
            try:
                val = datetime.datetime.fromisoformat(val.replace("Z", "+00:00"))
            except ValueError:
                return None
        if naive_utc and isinstance(val, datetime.datetime) and val.tzinfo is not None:
            return val.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        return val

    # Parse period_results if present
    period_results = None
    if entry.get("period_results"):
        period_results = [_parse_period_metrics(pm) for pm in entry["period_results"]]

    def _parse_enum(enum_class: type[Enum], field_name: str) -> Enum | None:
        value = entry.get(field_name)
        if value is None:
            return None
        try:
            return enum_class(value)
        except ValueError:
            logger.warning(
                "Unknown %s value %r for vault %s-%s; treating it as unknown",
                field_name,
                value,
                entry.get("chain_id"),
                entry.get("address"),
            )
            return enum_class("unknown")

    # Parse features and typed vault status values.
    features = entry.get("features", [])
    deposit_status = _parse_enum(VaultDepositStatus, "deposit_status")
    redemption_status = _parse_enum(VaultRedemptionStatus, "redemption_status")
    deposit_permission = _parse_enum(VaultDepositPermission, "deposit_permission")
    other_data = entry.get("other_data") or {}
    if "vault_display_flags" in entry:
        vault_display_flags = entry["vault_display_flags"]
    else:
        vault_display_flags = other_data.get("vault_display_flags")

    return VaultMetadata(
        vault_name=entry.get("name"),
        protocol_name=entry.get("protocol"),
        protocol_slug=entry.get("protocol_slug"),
        features=features,
        curator_slug=entry.get("curator_slug"),
        curator_name=entry.get("curator_name"),
        protocol_curator=entry.get("protocol_curator"),
        performance_fee=entry.get("performance_fee"),
        management_fee=entry.get("management_fee"),
        lifetime_return=entry.get("lifetime_return"),
        lifetime_return_net=entry.get("lifetime_return_net"),
        cagr=entry.get("cagr"),
        cagr_net=entry.get("cagr_net"),
        three_months_return=entry.get("three_months_return"),
        three_months_return_net=entry.get("three_months_return_net"),
        three_months_cagr=entry.get("three_months_cagr"),
        three_months_cagr_net=entry.get("three_months_cagr_net"),
        volatility=entry.get("volatility") or entry.get("three_months_volatility"),
        sharpe=entry.get("sharpe") or entry.get("three_months_sharpe"),
        max_drawdown=entry.get("max_drawdown"),
        tvl=entry.get("current_nav"),
        tvl_peak=entry.get("peak_nav"),
        age_years=entry.get("age"),
        first_updated_at=_parse_datetime(entry.get("lifetime_start")),
        last_updated_at=_parse_datetime(entry.get("lifetime_end")),
        deposit_fee=entry.get("deposit_fee"),
        withdrawal_fee=entry.get("withdraw_fee"),
        lockup_days=entry.get("lockup"),
        risk_level=entry.get("risk"),
        notes=entry.get("notes"),
        deposit_status=deposit_status,
        redemption_status=redemption_status,
        deposit_permission=deposit_permission,
        deposit_closed_reason=entry.get("deposit_closed_reason"),
        deposit_status_source=entry.get("deposit_status_source"),
        deposit_status_observed_at=_parse_datetime(entry.get("deposit_status_observed_at"), naive_utc=True),
        deposit_status_observed_block=entry.get("deposit_status_observed_block"),
        generated_at=_parse_datetime(entry.get("generated_at"), naive_utc=True),
        redemption_closed_reason=entry.get("redemption_closed_reason"),
        deposit_next_open=_parse_datetime(entry.get("deposit_next_open")),
        redemption_next_open=_parse_datetime(entry.get("redemption_next_open")),
        one_month_return=entry.get("one_month_return"),
        one_month_return_net=entry.get("one_month_return_net"),
        one_month_cagr=entry.get("one_month_cagr"),
        one_month_cagr_net=entry.get("one_month_cagr_net"),
        vault_slug=entry.get("vault_slug"),
        address=entry.get("address"),
        chain=entry.get("chain"),
        chain_id=entry.get("chain_id"),
        share_token_address=entry.get("share_token_address"),
        denomination_token_address=entry.get("denomination_token_address"),
        denomination=entry.get("denomination"),
        share_token=entry.get("share_token"),
        event_count=entry.get("event_count"),
        last_share_price=entry.get("last_share_price"),
        stablecoinish=entry.get("stablecoinish"),
        fee_mode=entry.get("fee_mode"),
        fee_internalised=entry.get("fee_internalised"),
        fee_label=entry.get("fee_label"),
        link=entry.get("link"),
        trading_strategy_link=entry.get("trading_strategy_link"),
        flags=entry.get("flags"),
        vault_display_flags=vault_display_flags,
        period_results=period_results,
    )


def load_vault_database_with_metadata(
    json_data: dict,
) -> VaultUniverse:
    """Load vault universe with rich metadata from JSON blob.

    Creates Vault instances with embedded VaultMetadata populated from
    the pre-computed JSON produced by the current eth-defi vault metrics and
    post-processing pipeline.

    Example:

    .. code-block:: python

        import json
        from tradingstrategy.alternative_data.vault import load_vault_database_with_metadata

        with open("top_vaults_by_chain.json") as f:
            json_data = json.load(f)

        vault_universe = load_vault_database_with_metadata(json_data)
        for vault in vault_universe.iterate_vaults():
            print(vault.name, vault.metadata.cagr)

    :param json_data:
        JSON data from top_vaults_by_chain.json containing:

        - ``generated_at``: timestamp when the data was generated
        - ``vaults``: list of vault metadata dicts
        - per-vault ``curator_slug``, ``curator_name`` and
          ``protocol_curator`` values supplied by the vault metrics pipeline

    :return:
        VaultUniverse with Vault instances containing full metadata.
    """
    vaults = []

    for vault_entry in json_data.get("vaults", []):
        try:
            # Parse VaultMetadata from JSON entry
            metadata = _parse_vault_metadata(vault_entry)

            # Get chain_id
            chain_id_val = vault_entry.get("chain_id")
            if chain_id_val is None:
                continue

            chain_id = ChainId(chain_id_val)

            # Get required fields
            vault_address = vault_entry.get("address")
            if not vault_address:
                continue

            name = vault_entry.get("name")
            if not name:
                continue

            # Create Vault with metadata reference
            vault = Vault(
                chain_id=chain_id,
                vault_address=vault_address.lower(),
                name=name,
                token_symbol=vault_entry.get("share_token") or name,
                denomination_token_address=(vault_entry.get("denomination_token_address") or "").lower(),
                denomination_token_symbol=vault_entry.get("denomination") or "",
                # Do NOT default missing decimals to a constant. A missing
                # value used to default to 18, which silently scaled raw
                # amounts by 10**12 for 6-decimal tokens like USDC and reverted
                # on-chain transfers. Leave it None so consumers resolve the
                # real decimals on-chain (the data source — top_vaults_by_chain
                # .json — now carries denomination_decimals / share_token_decimals).
                denomination_token_decimals=vault_entry.get("denomination_decimals"),
                share_token_address=(vault_entry.get("share_token_address") or vault_address).lower(),
                share_token_symbol=vault_entry.get("share_token") or name,
                share_token_decimals=vault_entry.get("share_token_decimals"),
                protocol_name=vault_entry.get("protocol") or "",
                protocol_slug=vault_entry.get("protocol_slug") or "",
                performance_fee=vault_entry.get("performance_fee"),
                management_fee=vault_entry.get("management_fee"),
                tvl=vault_entry.get("current_nav"),
                features=set(),  # Features parsed from metadata
                metadata=metadata,
            )
            vaults.append(vault)

        except Exception as e:
            # Skip entries that fail to parse
            logger.warning("Failed to parse vault entry: %s", e)
            continue

    return VaultUniverse(vaults)
