"""Synthetic vault pair and exchange ids must be identical in every Python process."""

import os
import subprocess
import sys

from tradingstrategy.types import SPECIAL_PAIR_ID_RANGE
from tradingstrategy.vault import _derive_pair_id_from_address, _stable_text_hash

#: Prints ids for non-hex vault addresses and a protocol slug, the inputs that used the built-in hash()
_PRINT_IDS = """
from tradingstrategy.vault import _derive_pair_id_from_address, _stable_text_hash
for address in ("vlt:2zqo", "lighter-pool-281474976710654", "hibachi-vault-1"):
    print(_derive_pair_id_from_address(address))
print(_stable_text_hash("hypercore"))
"""


def test_non_hex_vault_ids_are_stable_across_processes() -> None:
    """Derive the same non-hex vault pair ids and exchange id hashes regardless of PYTHONHASHSEED.

    The built-in ``hash()`` of a string is randomised per process, so ids for
    GRVT, Lighter and Hibachi vault addresses and vault exchange ids differed
    between restarts, grid search workers and on-disk caches.

    1. Derive the ids in two processes with different hash seeds.
    2. Verify both processes print identical ids.
    3. Verify hex addresses keep their address-based ids and non-hex ids stay in the vault id range.
    """
    # 1. Derive the ids in two processes with different hash seeds.
    outputs = [
        subprocess.run(
            [sys.executable, "-c", _PRINT_IDS],
            env={**os.environ, "PYTHONHASHSEED": seed},
            capture_output=True,
            text=True,
            check=True,
        ).stdout
        for seed in ("1", "2")
    ]

    # 2. Verify both processes print identical ids.
    assert outputs[0] == outputs[1]
    assert outputs[0].split()[-1] == str(_stable_text_hash("hypercore"))

    # 3. Verify hex addresses keep their address-based ids and non-hex ids stay in the vault id range.
    address = "0x0000000000000000000000000000000000000abc"
    assert _derive_pair_id_from_address(address) == SPECIAL_PAIR_ID_RANGE + 0xABC
    assert SPECIAL_PAIR_ID_RANGE <= _derive_pair_id_from_address("vlt:2zqo") < SPECIAL_PAIR_ID_RANGE + 2**24
