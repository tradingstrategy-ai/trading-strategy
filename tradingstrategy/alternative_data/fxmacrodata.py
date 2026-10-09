"""FXMacroData alternative macro data helpers."""

from __future__ import annotations

import os
from typing import Any, Optional

import pandas as pd
import requests

FXMACRODATA_BASE_URL = "https://api.fxmacrodata.com/v1"


class FXMacroDataError(RuntimeError):
    """FXMacroData returned an error or an unexpected payload."""


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
    token = (api_key or os.getenv("FXMACRODATA_API_KEY") or "").strip()
    if any(ord(char) < 33 or ord(char) == 127 for char in token):
        raise FXMacroDataError("FXMacroData API key contains whitespace or control characters")
    headers = {"X-API-Key": token} if token else {}

    try:
        # Redirects are not followed so the key is never sent to another host
        response = requests.get(
            f"{base_url.rstrip('/')}/calendar/{currency.lower()}",
            params=params,
            headers=headers,
            timeout=20,
            allow_redirects=False,
        )
    except requests.RequestException as e:
        raise FXMacroDataError(f"FXMacroData request failed: {type(e).__name__}") from None
    if response.status_code != 200:
        raise FXMacroDataError(f"FXMacroData request failed with HTTP {response.status_code}")
    try:
        payload = response.json()
    except ValueError:
        raise FXMacroDataError("FXMacroData returned a non-JSON response") from None
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, list):
        detail = payload.get("detail") if isinstance(payload, dict) else None
        raise FXMacroDataError(f"FXMacroData returned an unexpected response: {detail or 'missing data list'}")
    events: list[dict[str, Any]] = [event for event in data if isinstance(event, dict)]
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
