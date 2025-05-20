"""Vault data sideloading.

To repackage the vault bundle:

.. code-block:: shell

    # Copy scanned vault bundles to Python package data
    ./scripts/repackage-vault-data.sh


"""

import pickle
from pathlib import Path
from typing import Iterable

import pandas as pd
import zstandard

from eth_defi.erc_4626.core import ERC4262VaultDetection
from tradingstrategy.chain import ChainId
from tradingstrategy.exchange import Exchange
from tradingstrategy.types import NonChecksummedAddress
from tradingstrategy.utils.groupeduniverse import resample_candles_multiple_pairs
from tradingstrategy.vault import VaultUniverse, Vault, _derive_pair_id_from_address

#: Path to the bundled vault database
DEFAULT_VAULT_BUNDLE = Path(__file__).parent / ".." / "data_bundles" / "vault-db.pickle.zstd"

#: Path to the example vault price data
DEFAULT_VAULT_PRICE_BUNDLE = Path(__file__).parent / ".." / "data_bundles" / "vault-prices.parquet"


def load_vault_database(path: Path | None = None) -> VaultUniverse:
    """Load pickled vault metadata database generated with an offline script.

    - For sideloading vault data

    - Normalises vault data in a good documented format

    - For the generation `see this tutorial <https://web3-ethereum-defi.readthedocs.io/tutorials/erc-4626-scan-prices.html>`__

    :param path:
        Path to the pickle file.

        If not given use the default location.

        Can be zstd compressed with .zstd suffix.
    """

    if path is None:
        path = DEFAULT_VAULT_BUNDLE

    assert path.exists(), f"No vault file: {path}"

    vault_db: dict

    if path.suffix == ".zstd":
        with zstandard.open(path, "rb") as inp:
            vault_db = pickle.load(inp)
    else:
        # Normal pickle
        vault_db = pickle.load(path.open("rb"))

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

    for address, entry in vault_db.items():
        try:
            detection: ERC4262VaultDetection = entry["_detection_data"]

            if (not entry["Name"]) or (not entry["Denomination"]):
                # Skip invalid entries as all other requird data is missing
                continue

            if "unknown" in entry["Name"]:
                # Skip nameless / broken entries
                continue

            protocol_slug = entry["Protocol"].lower().replace(" ", "-")

            vault = Vault(
                chain_id=ChainId(detection.chain),
                name=entry["Name"],
                token_symbol=entry["Symbol"],
                vault_address=entry["Address"],
                denomination_token_address=entry["_denomination_token"]["address"],
                denomination_token_symbol=entry["_denomination_token"]["symbol"],
                denomination_token_decimals=entry["_denomination_token"]["decimals"],
                share_token_address=entry["_share_token"]["address"],
                share_token_symbol=entry["_share_token"]["symbol"],
                share_token_decimals=entry["_share_token"]["decimals"],
                protocol_name=entry["Protocol"],
                protocol_slug=protocol_slug,
                performance_fee=entry["Perf fee"],
                management_fee=entry["Mgmt fee"],
                deployed_at=detection.first_seen_at,
                features=detection.features,
                denormalised_data_updated_at=detection.updated_at,
                tvl=entry["NAV"],
                issued_shares=entry["Shares"],
            )
        except Exception as e:
            raise RuntimeError(f"Could not decode entry: {entry}") from e

        vaults.append(vault)

    return VaultUniverse(vaults)


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
    vault_universe.limit_to_single(chain_id, vault_address)
    return convert_vaults_to_trading_pairs(vault_universe.export_all_vaults())


def load_multiple_vaults(
    vaults: list[tuple[ChainId, NonChecksummedAddress]],
    path=DEFAULT_VAULT_BUNDLE,
) -> tuple[list[Exchange], pd.DataFrame]:
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
    vaults_to_match = [(row.chain_id, row.address) for idx, row in pairs_df.iterrows()]

    assert len(vaults_to_match) < 1000, f"The vaults to load number looks too high: {len(vaults_to_match)}"
    df = pd.read_parquet(prices_path)
    mask = df.apply(lambda r: (r["chain"], r["address"]) in vaults_to_match, axis=1)
    df = df[mask]
    return df



def convert_vault_prices_to_candles(
    raw_prices_df: pd.DataFrame,
    frequency: str = "1d",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Convert vault price data to candle format.

    - Partial support for price candle format to be used in backtesting

    - For the format see :py:func:`load_vault_price_data`

    - Only USD stablecoin denominated vaults supported for now

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

    #
    # Price candles
    #
    df = raw_prices_df
    df["open"] = df["share_price"]
    df["low"] = df["share_price"]
    df["high"] = df["share_price"]
    df["close"] = df["share_price"]
    df["volume"] = 0
    df["buy_volume"] = 0
    df["sell_volume"] = 0
    df["pair_id"] = df["address"].apply(_derive_pair_id_from_address)

    # Even for daily data, we need to resample, because built-in vault price example
    # data is not midnight aligned
    df = _resample(df, frequency)

    prices_df = df

    #
    # Liquidity candles
    #
    df = raw_prices_df
    df["open"] = df["total_assets"]
    df["low"] = df["total_assets"]
    df["high"] = df["total_assets"]
    df["close"] = df["total_assets"]
    df["pair_id"] = df["address"].apply(_derive_pair_id_from_address)

    # Even for daily data, we need to resample, because built-in vault price example
    # data is not midnight aligned

    tvl_df = _resample(df, frequency)

    return prices_df, tvl_df


def _resample(df: pd.DataFrame, frequency: str) -> pd.DataFrame:
    """Multipair resample helper."""
    df = resample_candles_multiple_pairs(df, frequency)
    return df






