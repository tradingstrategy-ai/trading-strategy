"""Client configuration."""

from dataclasses import dataclass
from typing import Optional

from dataclasses_json import dataclass_json


@dataclass_json
@dataclass
class Configuration:
    """Configuration for Capitalgram client."""
    api_key: Optional[str] = None