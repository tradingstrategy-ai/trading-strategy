"""Token metadata structures.

- Token metadata includes its core data combined with additional data sources like Coingecko and TokenSniffer

"""
import datetime
import json
from dataclasses import dataclass
from pathlib import Path
from pprint import pformat

import orjson


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

    #: Was this item loaded from the disk or server
    cached: bool = None

    #: TokenSniffer API error if the sniff data could not be loaded.
    #:
    #: Usually rate limit or such temporary error.
    #:
    token_sniffer_error: str | None = None

    def get_persistent_id(self) -> str:
        """Stable id over long period of time and across different systems."""
        return f"{self.chain_id}-{self.token_address}"

    @property
    def token_sniffer_score(self) -> int | None:
        """What was the TokenSniffer score for the base token."""

        if self.token_sniffer_data is None:
            return None

        return self.token_sniffer_data.get("score")

    def get_coingecko_categories(self) -> set[str] | None:
        """Get CoinGecko categories of this token.

        :return:
            None if Coingecko data not available
        """
        if self.coingecko_data is None:
            return None

        return set(self.coingecko_data.get("categories", []))

    def has_tax_data(self) -> bool | None:
        """Do we have tax data for this pair.

        The token tax data availability comes from TokenSniffer.
        No insight what tells whether it should be available or not.

        :return:
            True/False is TokenSniffer data is available, otherwise None.
        """
        token_sniffer_data = self.token_sniffer_data
        if token_sniffer_data is not None:
            try:
                if ("swap_simulation" in token_sniffer_data) and (token_sniffer_data["swap_simulation"].get("buy_fee") is not None):
                    return True
            except Exception as e:
                # {'is_sellable': True}
                # import ipdb ; ipdb.set_trace()
                raise
        return False

    def get_buy_tax(self, epsilon=0.0001, rounding=4) -> float | None:
        """What was the TokenSniffer buy tax for the base token.

        See also :py:meth:`has_tax_data`.

        :param epsilon:
            Deal with rounding errors.

        :param rounding:
            Deal with tax estimation accuracy

        :return:
            Buy tax 0....1 or None if not available
        """

        if self.token_sniffer_data is None:
            return None

        if not self.has_tax_data():
            return None

        raw_buy_fee = self.token_sniffer_data["swap_simulation"].get("buy_fee")
        if raw_buy_fee is None:
            # {'is_sellable': None, 'buy_fee': None, 'sell_fee': None, 'message': 'not available'}
            return None

        fee = float(raw_buy_fee) / 100
        if fee < epsilon:
            return 0
        return round(fee, rounding)

    def get_sell_tax(self, epsilon=0.0001, rounding=4) -> float | None:
        """What was the TokenSniffer sell tax for the base token.

        See also :py:meth:`has_tax_data`.

        :param epsilon:
            Deal with rounding errors.

         :param rounding:
            Deal with tax estimation accuracy

        :return:
            Sell tax 0....1 or None if not available
        """

        if self.token_sniffer_data is None:
            return None

        if not self.has_tax_data():
            return None

        raw_sell_fee = self.token_sniffer_data["swap_simulation"].get("sell_fee")
        if raw_sell_fee is None:
            return None

        fee = float(raw_sell_fee) / 100
        if fee < epsilon:
            return 0
        return round(fee, rounding)

    @staticmethod
    def read_json(path: Path) -> "TokenMetadata":
        """Read token metadata from JSON file."""

        assert isinstance(path, Path)
        assert path.is_file()

        data = orjson.loads(path.read_bytes())
        try:
            metadata = TokenMetadata(**data)
        except TypeError as e:
            raise TypeError(f"Not valid metadata {path}:\n{pformat(data)}") from e

        return metadata
