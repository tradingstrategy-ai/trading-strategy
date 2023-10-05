"""Generic units used in data models used in Capitalgram.

Types aliases are used to give human-readable meaning for various arguments and return values.
These are also used to hint :term:`Pyarrow` schemas to make :term:`Parquet` files more compact.
"""

from typing import TypeAlias

#: 64-bit integer based primary key.
#:
#:     Also referred as "internal id" on website and data streams.
#:
#:     This is a type alias to Python integer type.
#:
#:     Primary keys are not stable across different dataset. Blockchain data healing process
#:     may require to regenerate the data which means the old data is purged,
#:     reimported, with new primary keys.
PrimaryKey: TypeAlias = int



#: Ethereum address that does *not* use EIP-55 checksumming.
#:
#  - String
#:
#  - Always starts 0x
#:
#: - Lowercased
#:
#: `See EIP-55 <https://github.com/ethereum/EIPs/blob/master/EIPS/eip-55.md>`__.
#
NonChecksummedAddress: TypeAlias = str


#: Express USD monetary amount.
#:
#: Used in exchange rates, volumes and prices in candle data.
#; Normally you should not use float for pricing,
#: but because we are already losing a lot of accuraty in float conversion
#: and our price data is not for exact transactions, but for modelling,
#: this is ok.
#:
USDollarAmount: TypeAlias = float

#: Seconds since 1.1.1970 as UTC time as integer
UNIXTimestamp: TypeAlias = int


#: EVM block number from 1 to infinity
BlockNumber: TypeAlias = int

#: Multiplier as 1/10000 or 0.01%
BasisPoint: TypeAlias = int

#: Chain id that is not a wrapped enum.
#:
#: See :py:class:`tradingstategy.chain.ChainId` for details
RawChainId: TypeAlias = int


#: Percent expressed as float 0...1
#:
#: 1 = 100%
#:
#: 0.00030 = 0.3% = 30 BPS
#:
#: See also
#:
#: - :py:data:`BasisPoint`
#:
Percent: TypeAlias = float

#: Slug is a machine friendly and URL friendly id generated from a name.
#:
#: E.g. `Aave v3` -> `aave-v3`
#:
Slug: TypeAlias = str


#: Token symbol is a the ERC-20 symbol() output.
#:
#:
#: E.g. `USDC`
#:
TokenSymbol: TypeAlias = str

#: URL as a string type
#:
URL: TypeAlias = str