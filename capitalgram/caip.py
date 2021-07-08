"""CAIP supports chain-agnosticaddress formats.

For more information please see https://github.com/ChainAgnostic/CAIPs
"""
from dataclasses import dataclass

from eth_utils import is_checksum_address


class BadChainAddressTuple(Exception):
    pass


class InvalidChainId(BadChainAddressTuple):
    pass


class InvalidChecksum(BadChainAddressTuple):
    pass


@dataclass
class ChainAddressTuple:
    """Present one chain-agnostic address."""

    #: See ChainId for more information - here we have just the raw int value
    chain_id: int

    #: Could be checksummed or non-checksummed address
    address: str

    @staticmethod
    def parse_naive(v: str):
        """Parses chain_id and EVM address tuple.

        Example tuple: `1:0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc` - ETH-USDC on Uniswap v2
        """
        assert type(v) == str

        if not v:
            raise BadChainAddressTuple("Empty string passed")

        parts = v.split(":")

        if len(parts) != 2:
            raise BadChainAddressTuple(f"Cannot split chain id in address {v}")

        address = parts[1]
        if not is_checksum_address(address):
            raise InvalidChecksum("Address checksum or format invalid")

        try:
            chain_id = int(parts[0])
        except ValueError:
            raise InvalidChainId(f"Invalid chain_id on {v}")

        if chain_id <= 0:
            raise InvalidChainId("Invalid chain_id")

        return ChainAddressTuple(chain_id, address)


