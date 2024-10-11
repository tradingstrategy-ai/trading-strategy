"""Top trading pair queries.

- Data structures for /top end point

- Used for adding new pairs to open ended trading universe in external trading signal processor

- See :py:func:`tradingstrategy.client.Client.fetch_top_pairs` for usage.
"""
import datetime

from dataclasses import dataclass

from dataclasses_json import dataclass_json


@dataclass_json
@dataclass(slots=True)
@dataclass(slots=True)
class TopPairData:
    """See open-defi-api.yaml"""

    #: When this entry was queried
    #:
    #: Wall clock UTC time.
    #:
    queried_at: datetime.datetime

    #: Blockchain this pair is on
    chain_id: int

    #: Internal pair primary key (may change)
    pair_id: int

    #: Internal pair exchange id (may change)
    exchange_id: int

    #: Human readable exchange URL slug (may change)
    exchange_slug: str

    #: Smart contract address of pool smart contract.
    #:
    #: Uniswap v2 pair contract address, Uniswap v3 pool contract address.
    #:
    pool_address: str

    #: Human readable base token
    base_token: str

    #: Human readable quote token
    quote_token: str

    #: Pair fee in 0...1, 0.0030 is 30 BPS
    fee: float

    #: Volume over the last 24h
    #:
    #: May not be available due to latency/denormalisation/etc. issues
    #:
    volume_24h_usd: float | None

    #: Last USD TVL (Uniswap v3) or XY Liquidity (Uniswap v2)
    #:
    #: May not be available due to latency/denormalisation/etc. issues
    #:
    tvl_latest_usd: float | None

    #: When TVL measurement was updated.
    #:
    #: How old data are we using.
    #:
    tvl_updated_at: datetime.datetime | None

    #: When volume measurement was updated
    #:
    #: How old data are we using.
    #:
    volume_updated_at: datetime.datetime | None

    #: If this pair was excluded from the top pairs, what was the human-readable heuristics reason we did this.
    #:
    #: This allows you to diagnose better why some trading pairs might not end up in the trading universe.
    #:
    exclude_reason: str | None

    def get_persistent_id(self) -> str:
        """Stable id over long period of time and across different systems."""
        return f"{self.chain_id}-{self.pool_address}"


@dataclass_json
@dataclass(slots=True)
class TopPairsReply:
    """/top endpoint reply.

    - Get a list of trading pairs, both included and excluded

    """
    included: list[TopPairData]
    excluded: list[TopPairData]