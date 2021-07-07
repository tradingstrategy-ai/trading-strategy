from dataclasses import dataclass
from typing import Optional


@dataclass
class Configuration:
    api_key: Optional[str] = None
