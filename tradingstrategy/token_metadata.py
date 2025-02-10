"""Token metadata structures.

- Token metadata includes its core data combined with additional data sources like Coingecko and TokenSniffer

"""
import datetime
from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class TokenMetadata:
    """Metadata definition for one token.

    Extra metadata that needs to be loaded separate for tokens/trading pairs.

    - Risk profiling: :py:attr:`token_sniffer_data`
    - Categorisation: :py:attr:`coingecko_data`

    See :py:func:`tradingstrategy.utils.token_extra_data.load_token_metadata` for usage.
    """

    #: When this entry was queried.
    #:
    #: Server-side timestamp generated on load.
    #:
    #: Wall clock UTC time. Can used to purge disk files later.
    #:
    queried_at: datetime.datetime

    #: Blockchain this pair is on
    chain_id: int

    #: Internal token id
    token_id: int

    #: Smart contract address for base token
    token_address: str

    #: Human readable name
    name: str

    #: Human readable base token
    symbol: str

    #: ERC-20
    decimals: int | None

    #: Website slug
    slug: str

    #: List of internal trading pair ids where this token appears.
    #:
    #: Filling this list is disabled by default, because entries like WETH or USDC may contain tens of thousands entries.
    #:
    pair_ids: list[int] | None

    #: TokenSniffer data for the base token.
    #:
    #: Used in the filtering of scam tokens.
    #:
    #: Not available for all tokens that are filtered out for other reasons.
    #: This is the last check.
    #:
    #: `See more information here <https://web3-ethereum-defi.readthedocs.io/api/token_analysis/_autosummary_token_analysis/eth_defi.token_analysis.tokensniffer.html>`__.
    #:
    token_sniffer_data: dict | None

    #: Coingecko metadata
    #:
    #: Passed as is https://docs.coingecko.com/reference/coins-contract-address.
    #: market_data removed to keep download size smaller.
    #:
    coingecko_data: dict | None

    #: Was this item cached on the server side
    cached: bool = None

    #: Was this item cached on locally on a disk
    disk_cached: bool = None

    def get_persistent_id(self) -> str:
        """Stable id over long period of time and across different systems."""
        return f"{self.chain_id}-{self.token_address}"

    @property
    def token_sniffer_score(self) -> int | None:
        """What was the TokenSniffer score for the base token."""

        if self.token_sniffer_data is None:
            return None

        return self.token_sniffer_data["score"]

    def get_coingecko_categories(self) -> set[str] | None:
        """Get CoinGecko categories of this token.

        :return:
            None if Coingecko data not available
        """
        if self.coingecko_data is None:
            return None

        return set(self.coingecko_data.get("categories", []))

