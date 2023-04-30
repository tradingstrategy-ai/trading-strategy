import pytest

from tradingstrategy.caip import BadAddress, ChainAddressTuple
from tradingstrategy.chain import ChainId


def test_caip_parse_naive():
    tuple = ChainAddressTuple.parse_naive("1:0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc")
    assert tuple.chain_id == 1
    assert tuple.address == "0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc"


@pytest.mark.skip(reason="Skipped because eth-utils dependency issues")
def test_caip_bad_checksum():
    # Notive lower b, as Ethereum encodes the checksum in the hex capitalisation
    with pytest.raises(BadAddress):
        ChainAddressTuple.parse_naive("1:0xb4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc")


def test_chain_name():
    c = ChainId(1)
    assert c.get_name() == "Ethereum"


def test_chain_homepage():
    c = ChainId(1)
    assert c.get_homepage() == "https://ethereum.org"


def test_bsc():
    c = ChainId(56)
    assert c.get_name() == "BNB Smart Chain"
    assert c.get_slug() == "binance"

    d = ChainId.binance
    assert d.get_name() == "BNB Smart Chain"
    assert d.get_slug() == "binance"


def test_avalanche():
    c = ChainId(43114)
    assert c.get_name() == "Avalanche C-chain"
    assert c.get_slug() == "avalanche"


def test_arbitrum():
    c = ChainId(42161)
    assert c.get_name() == "Arbitrum One"
    assert c.get_slug() == "arbitrum"


def test_resolve_by_slug():
    c = ChainId.get_by_slug("binance")
    assert c.value == 56

    c = ChainId.get_by_slug("arbitrum")
    assert c.value == 42161

