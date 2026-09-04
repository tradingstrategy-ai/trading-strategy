"""Client configuration."""

from dataclasses import dataclass

from dataclasses_json import dataclass_json


@dataclass_json
@dataclass
class Configuration:
    """Configuration for Capitalgram client."""

    #: Trading Strategy oracle API key, starts with ``secret-token:tradingstrategy-...``.
    api_key: str | None = None

    #: Vaults Pro (Creem) licence key for the gated vault datasets.
    #:
    #: Stored and reused alongside :py:attr:`api_key` so the vault datasets do not
    #: prompt again on every notebook run. See :py:mod:`tradingstrategy.vault_data_client`.
    vault_pro_api_key: str | None = None
