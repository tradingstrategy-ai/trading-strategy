"""Token cache maintenance

- Token cache created by :py:meth:`tradingstrategy.transport.cache.CachedHTTPTransport.fetch_token_metadata`

- Because we cache entries where we could not get TokenSniffer metadata, some of the data might be missing.
  This is because due to TokenSniffer API throttling.

"""
import datetime
import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from pprint import pformat
from typing import Iterable

from tradingstrategy.token_metadata import TokenMetadata
from tradingstrategy.transport.cache import CachedHTTPTransport


@dataclass(slots=True, frozen=True)
class TokenCacheEntry:
    """Convenience wrapper around token metadata cache file."""

    #: Where this item was stored
    path: Path

    #: Stored JSON data
    metadata: TokenMetadata

    @property
    def updated_at(self):
        return datetime.datetime.utcfromtimestamp(self.path.stat().st_mtime)

    @property
    def file_size(self) -> int:
        """File size in bytes"""
        return self.path.stat().st_size

    def has_tokensniffer_data(self) -> bool:
        return self.metadata.token_sniffer_data is not None

    def has_coingecko_data(self) -> bool:
        return self.metadata.coingecko_data is not None

    def has_tax_data(self) -> bool:
        return self.metadata.has_tax_data()

    def has_tax(self) -> bool:
        return self.metadata.has_tax_data() and (self.metadata.get_buy_tax() > 0 or self.metadata.get_sell_tax() > 0)

    def purge(self):
        self.path.unlink()


def read_token_cache(transport: CachedHTTPTransport) -> Iterable[TokenCacheEntry]:
    """Read all written token cache entries."""

    assert isinstance(transport, CachedHTTPTransport)
    token_cache_path = Path(transport.cache_path) / "token-metadata"

    assert token_cache_path.exists(), f"Does not exist: {token_cache_path}"
    assert token_cache_path.is_dir(), f"Not a directory: {token_cache_path}"

    for cache_file in token_cache_path.glob("*.json"):
        metadata = TokenMetadata.read_json(cache_file)
        entry = TokenCacheEntry(
            path=cache_file,
            metadata=metadata
        )
        yield entry


def calculate_token_cache_summary(cached_entries: Iterable[TokenCacheEntry]) -> dict:
    """Display the summary of cached files."""

    stats = Counter()

    sizes = []
    timestamps = []
    buy_tax = []
    sell_tax = []
    chains = set()

    for entry in cached_entries:
        stats["count"] += 1
        stats["tokensniffer_data"] += 1 if entry.has_tokensniffer_data() else 0
        stats["coingecko_data"] += 1 if entry.has_coingecko_data() else 0
        stats["tax_data"] += 1 if entry.has_tax_data() else 0
        stats["tax"] += 1 if entry.has_tax() else 0
        if entry.has_tax():
            # Scam tokens have tax at 100%
            if entry.metadata.token_sniffer_score != 0:
                buy_tax.append(entry.metadata.get_buy_tax())
            if entry.metadata.token_sniffer_score != 0:
                sell_tax.append(entry.metadata.get_sell_tax())
        sizes.append(entry.file_size)
        timestamps.append(entry.updated_at)
        chains.add(entry.metadata.chain_id)


    stats["chains"] = ", ".join([str(i) for i in chains])
    stats["avg_file_size"] = f"{int(sum(sizes) / len(sizes)):,}"
    if timestamps:
        stats["oldest"] = min(timestamps)
        stats["newest"] = max(timestamps)

    if buy_tax:
        stats["max_buy_tax"] = f"{max(buy_tax):,.2%}"

    if sell_tax:
        stats["max_sell_tax"] = f"{max(sell_tax):,.2%}"

    return stats


def display_token_metadata(cached_entries: Iterable[TokenCacheEntry]) -> list[dict]:
    """Print stored metadata in human readable format."""

    data = []
    for entry in cached_entries:
        metadata = entry.metadata
        data.append({
            "Symbol": metadata.symbol,
            "Address": metadata.token_address,
            "Updated": entry.updated_at,
            "Risk score": entry.metadata.token_sniffer_score,
            "Buy tax": f"{metadata.get_buy_tax() or 0:,.2%}",
            "Sell tax": f"{metadata.get_sell_tax() or 0:,.2%}",
        })

    return data

