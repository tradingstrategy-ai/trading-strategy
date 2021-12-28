"""Generic units used in data models used in Capitalgram.

Types aliases are used to give human-readable meaning for various arguments and return values.
These are also used to hint :term:`Pyarrow` schemas to make :term:`Parquet` files more compact.

TODO: Clean up after Explicit Type Aliases have been merged and is widely supported https://www.python.org/dev/peps/pep-0613/
"""


class CapitalgramType:
    """Market class for our internal type definitions."""


class PrimaryKey(CapitalgramType, int):
    """64-bit integer based primary key.

    Alias to int.

    Primary keys are not stable across different dataset. Blockchain data healing process
    may require to regenerate the data which means the old data is purged,
    reimported, with new primary keys.
    """


class NonChecksummedAddress(CapitalgramType, str):
    """Ethereum address that does *not* use EIP-55 checksumming.

     EIP-55 https://github.com/ethereum/EIPs/blob/master/EIPS/eip-55.md

    Alias to str.
    """


class USDollarAmount(CapitalgramType, float):
    """Express USD monetary amount.

    Used in exchange rates, volumes and prices in candle data.
    Normally you should not use float for pricing,
    but because we are already losing a lot of accuraty in float conversion
    and our price data is not for exact transactions, but for modelling,
    this is ok.
    """


class UNIXTimestamp(CapitalgramType, int):
    """Seconds since 1.1.1970 as UTC time as integer."""


class BlockNumber(CapitalgramType, int):
    """EVM block number from 1 to infinity"""


class BasisPoint(CapitalgramType, int):
    """Multiplier as 1/10000 or 0.01%"""
