"""Token presentation."""
from typing import Optional
from dataclasses import dataclass
from dataclasses_json import dataclass_json

from .chain import ChainId


@dataclass_json
@dataclass
class Token:
    """Token presentation.

    Capture token essentials.
    """

    chain_id: ChainId

    symbol: Optional[str]

    #: Ethereum address of this token.
    #: Always lowercase - no checksum.
    address: str

    decimals: int

    def __eq__(self, other):
        """Implemented for set()"""
        assert isinstance(other, Token)
        return self.address == other.address

    def __hash__(self) -> int:
        """Implemented for set()"""
        return int(self.address, 16)

    def __repr__(self):
        return f"<Token {self.symbol} at {self.address} on {self.chain_id.name} with {self.decimals} decimals>"

    def __post_init__(self):
        assert type(self.address) == str, f"Got address {self.address} as {type(self.address)}"
        assert self.address.startswith("0x")
        assert self.address.lower() == self.address
        assert type(self.chain_id) == ChainId
        assert self.decimals is not None, f"Cannot create tradeable token without decimals set"
