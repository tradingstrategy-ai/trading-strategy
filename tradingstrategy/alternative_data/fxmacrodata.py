"""FXMacroData alternative macro data helpers."""

from __future__ import annotations

import os
from typing import Any, Optional

import pandas as pd
import requests

FXMACRODATA_BASE_URL = "https://api.fxmacrodata.com/v1"


def fetch_fxmacrodata_calendar(
    currency: str = "usd",
    *,
    limit: int = 100,
    min_tier: Optional[int] = 2,
    api_key: Optional[str] = None,
    base_url: str = FXMACRODATA_BASE_URL,
) -> pd.DataFrame:
    """Fetch FXMacroData release-calendar events as a DataFrame.

    This helper is intended for strategy notebooks that join macro event dates
    to candle, liquidity, or lending-rate data.
    """

    limit_count = max(1, min(int(limit), 100))
    params: dict[str, str] = {"limit": str(limit_count)}
    token = api_key or os.getenv("FXMACRODATA_API_KEY")
    if token:
        params["api_key"] = token

    response = requests.get(
        f"{base_url.rstrip('/')}/calendar/{currency.lower()}",
        params=params,
        timeout=20,
    )
    response.raise_for_status()
    events: list[dict[str, Any]] = response.json().get("data", [])
    if min_tier is not None:
        events = [
            event
            for event in events
            if int(event.get("market_tier") or 99) <= min_tier
        ]

    frame = pd.DataFrame(events[:limit_count])
    if not frame.empty and "date" in frame.columns:
        frame["date"] = pd.to_datetime(frame["date"])
    if not frame.empty and "announcement_datetime" in frame.columns:
        frame["announcement_datetime"] = pd.to_datetime(
            frame["announcement_datetime"],
            unit="s",
            utc=True,
        )
    return frame
