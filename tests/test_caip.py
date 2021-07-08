import pytest

from capitalgram.caip import ChainAddressTuple, InvalidChecksum


def test_caip_parse_naive():
    tuple = ChainAddressTuple.parse_naive("1:0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc")
    assert tuple.chain_id == 1
    assert tuple.address == "0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc"


def test_caip_bad_checksum():
    # Notive lower b, as Ethereum encodes the checksum in the hex capitalisation
    with pytest.raises(InvalidChecksum):
        ChainAddressTuple.parse_naive("1:0xb4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc")
