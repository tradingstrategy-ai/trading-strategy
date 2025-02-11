"""Top trading pair queries.

- Data structures for /top end point

- Used for adding new pairs to open ended trading universe in external trading signal processor

- See :py:func:`tradingstrategy.client.Client.fetch_top_pairs` for usage.
"""
import datetime
import enum
import itertools

from dataclasses import dataclass, field

from dataclasses_json import dataclass_json, config
from marshmallow import fields



class TopPairMethod(enum.Enum):
    """Method to query top pair data.

    - For given exchanges or token addresses, find the best trading pairs
    """

    #: Give the endpoint a list of exchange slugs and get the best trading pairs on these exchanges.
    #:
    #:
    #:
    sorted_by_liquidity_with_filtering = "sorted-by-liquidity-with-filtering"

    #: Give the endpoint a list of **token** smart contract addresses and get the best trading pairs for these.
    #:
    #:
    by_token_addresses = "by-addresses"


@dataclass_json
@dataclass(slots=True)
class TopPairData:
    """Entry for one trading pair data in top picks.

    Contains
    - Latest cached volue :py:attr:`volume_24h_usd`
    - Latest cached liquidity/TVL :py:attr:`tvl_latest_usd` (may not be yet available for all tokens)
    - TokenSniffer risk score :py:attr:`risk_score`
    - TokenSniffer token tax data :py:meth:`get_buy_tax` /:py:meth:`get_sekk_tax`

    See also

    - :py:func:`tradingstrategy.utils.token_extra_data.load_extra_metadata`

    Example:

    .. code-block:: python

        comp_weth = top_reply.included[0]
        assert comp_weth.base_token == "COMP"
        assert comp_weth.quote_token == "WETH"
        assert comp_weth.get_buy_tax() == 0
        assert comp_weth.get_sell_tax() == 0
        assert comp_weth.volume_24h_usd > 100.0
        assert comp_weth.tvl_latest_usd > 100.0

    """

    #: When this entry was queried
    #:
    #: Wall clock UTC time.
    #:
    #: Because the server serialises as ISO, we need special decoder
    #:
    #: https://github.com/lidatong/dataclasses-json?tab=readme-ov-file#Overriding
    #:
    queried_at: datetime.datetime = field(
        metadata=config(
            decoder=datetime.datetime.fromisoformat,
            mm_field=fields.DateTime(format='iso')
        )
    )

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

    #: 0x lowercased address
    base_token_address: str

    #: 0x lowercased address
    quote_token_address: str

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
    tvl_updated_at: datetime.datetime | None = field(
        metadata=config(
            decoder=datetime.datetime.fromisoformat,
            mm_field=fields.DateTime(format='iso')
        )
    )


    #: When volume measurement was updated
    #:
    #: How old data are we using.
    #:
    volume_updated_at: datetime.datetime | None = field(
        metadata=config(
            decoder=datetime.datetime.fromisoformat,
            mm_field=fields.DateTime(format='iso')
        )
    )


    #: If this pair was excluded from the top pairs, what was the human-readable heuristics reason we did this.
    #:
    #: This allows you to diagnose better why some trading pairs might not end up in the trading universe.
    #:
    exclude_reason: str | None

    #: TokenSniffer data for this token.
    #:
    #: Used in the filtering of scam tokens.
    #:
    #: Not available for all tokens that are filtered out for other reasons.
    #: This is the last check.
    #:
    #: `See more information here <https://web3-ethereum-defi.readthedocs.io/api/token_analysis/_autosummary_token_analysis/eth_defi.token_analysis.tokensniffer.html>`__.
    #:
    token_sniffer_data: dict | None

    def __repr__(self):
        return f"<Pair {self.base_token} - {self.quote_token} on {self.exchange_slug}, address {self.pool_address} - reason {self.exclude_reason}>"

    def get_ticker(self) -> str:
        """Simple marker ticker identifier for this pair."""
        return f"{self.base_token} - {self.quote_token}"

    def get_exchange_slug(self) -> str:
        """Human readable id for the DEX this pair trades on."""
        return f"{self.exchange_slug}"

    def get_persistent_string_id(self) -> str:
        """Stable id over long period of time and across different systems."""
        return f"{self.chain_id}-{self.pool_address}"

    @property
    def token_sniffer_score(self) -> int | None:
        """What was the TokenSniffer score for the base token."""

        if self.token_sniffer_data is None:
            return None

        return self.token_sniffer_data.get("score")

    def has_tax_data(self) -> bool | None:
        """Do we have tax data for this pair.

        The token tax data availability comes from TokenSniffer.
        No insight what tells whether it should be available or not.

        :return:
            True/False is TokenSniffer data is available, otherwise None.
        """
        if self.token_sniffer_data is not None:
            return "swap_simulation" in self.token_sniffer_data

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


@dataclass_json
@dataclass(slots=True)
class TopPairsReply:
    """/top endpoint reply.

    - Get a list of trading pairs, both included and excluded

    """

    #: The top list at the point of time the request was made
    included: list[TopPairData]

    #: Tokens that were considered for top list, but excluded for some reason
    #:
    #: They had enough liquidity, but they failed e.g. TokenSniffer scam check,
    #: or had a trading pair for the same base token with better fees, etc.
    #:
    excluded: list[TopPairData]

    def __repr__(self):
        return f"<TopPairsReply included {len(self.included)}, excluded {len(self.excluded)}>"

    def as_token_address_map(self) -> dict[str, TopPairData]:
        """Make base token address lookupable data.

        Includes both excluded and included pairs.
        Included takes priority if multiple pairs.

        :return:
            Map with all token addresses lowercase.
         """
        exc_data = {entry.base_token_address: entry for entry in self.excluded}
        inc_data = {entry.base_token_address: entry for entry in self.included}
        return exc_data | inc_data

    def find_pair_data_for_token(self, token_address: str) -> TopPairData | None:
        """Get token data.

        - Can be excluded or included pair

        - If there are multiple trading pairs for the same token, get the first one

        Example:

        .. code-block:: python

            import logging
            import sys

            from tradingstrategy.chain import ChainId
            from tradingstrategy.client import Client
            from tradingstrategy.top import TopPairMethod

            logging.basicConfig(
                level=logging.INFO,
                stream=sys.stdout,
            )

            client = Client.create_live_client()
            chain_id = ChainId.binance
            token_address = "0xb1aed8969439efb4ac70d7397ba90b276587b27d"
            token_addresses = {token_address}

            top_pair_reply = client.fetch_top_pairs(
                {chain_id},
                addresses=token_addresses,
                method=TopPairMethod.by_token_addresses,
                min_volume_24h_usd=0,
                risk_score_threshold=0,
            )

            pair_data = top_pair_reply.find_pair_data_for_token(token_address)
            print(pair_data)

        """
        token_address = token_address.lower()
        for data in itertools.chain(self.included, self.excluded):
            if data.base_token_address == token_address:
                return data

        return None
