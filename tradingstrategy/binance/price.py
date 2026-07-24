"""Incrementally cached Binance reference prices.

- Fetch candles for any Binance spot symbol (BTC/USDT by default) from the
  public Binance klines REST API

- Cache the candles locally in a DuckDB file, so that repeated calls only
  fetch the candles that appeared since the previous call

- Used as a market-factor reference price (e.g. rolling beta of vault returns
  to BTC and the residuals of that regression) where we want a clean
  centralised-exchange price series instead of a DEX pair's price

Example:

.. code-block:: python

    from tradingstrategy.binance.price import fetch_binance_price

    btc_df = fetch_binance_price()  # BTCUSDT daily by default
    eth_df = fetch_binance_price(symbol="ETHUSDT")
    btc_daily_returns = btc_df["close"].pct_change()
"""

import datetime
import logging
from pathlib import Path

import pandas as pd
import requests

logger = logging.getLogger(__name__)


#: Default DuckDB file used to cache the reference price candles.
#:
#: Lives under ``~/.tradingstrategy`` with the other trading-strategy local caches.
DEFAULT_BINANCE_PRICE_CACHE_PATH = Path("~/.tradingstrategy/binance-price.duckdb")

#: Public Binance klines REST endpoint. No API key is needed for market data.
DEFAULT_BINANCE_API_BASE_URL = "https://api.binance.com"

#: Official Binance public market-data mirror.
#:
#: Serves the same ``/api/v3/klines`` endpoint but is not geo-restricted,
#: used as an automatic fallback when the main endpoint refuses the request.
FALLBACK_BINANCE_API_BASE_URL = "https://data-api.binance.vision"

#: BTCUSDT spot trading started on Binance on this date - the default fetch start.
DEFAULT_FETCH_START = datetime.datetime(2017, 8, 17)

#: Maximum number of candles the Binance klines endpoint returns per request.
_PAGE_LIMIT = 1000

#: Candle interval string to its duration, for the supported intervals.
_INTERVAL_DURATIONS: dict[str, datetime.timedelta] = {
    "1m": datetime.timedelta(minutes=1),
    "5m": datetime.timedelta(minutes=5),
    "15m": datetime.timedelta(minutes=15),
    "1h": datetime.timedelta(hours=1),
    "4h": datetime.timedelta(hours=4),
    "1d": datetime.timedelta(days=1),
}


def _from_unix_ms(unix_ms: int) -> datetime.datetime:
    """Convert Binance millisecond epoch to a naive UTC datetime."""
    return datetime.datetime.fromtimestamp(unix_ms / 1000.0, datetime.timezone.utc).replace(tzinfo=None)


def _fetch_klines(
    api_base_url: str,
    symbol: str,
    interval: str,
    start: datetime.datetime,
    request_timeout: float,
) -> list[tuple]:
    """Page through the Binance klines endpoint from ``start`` until now.

    :return:
        List of ``(timestamp, open, high, low, close, volume)`` tuples with
        naive UTC timestamps. The still-forming candle is excluded so the
        cache only ever contains closed candles.
    """
    session = requests.Session()
    rows: list[tuple] = []
    start_ms = int(start.replace(tzinfo=datetime.timezone.utc).timestamp() * 1000)
    now_ms = int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000)
    base_url = api_base_url

    while start_ms < now_ms:
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": start_ms,
            "limit": _PAGE_LIMIT,
        }
        response = session.get(f"{base_url}/api/v3/klines", params=params, timeout=request_timeout)
        if response.status_code in (403, 451) and base_url != FALLBACK_BINANCE_API_BASE_URL:
            logger.info(
                "Binance endpoint %s refused the request with HTTP %d, falling back to %s",
                base_url,
                response.status_code,
                FALLBACK_BINANCE_API_BASE_URL,
            )
            base_url = FALLBACK_BINANCE_API_BASE_URL
            continue
        response.raise_for_status()
        batch = response.json()
        if not batch:
            break
        for kline in batch:
            open_time_ms = int(kline[0])
            close_time_ms = int(kline[6])
            if close_time_ms > now_ms:
                # Still-forming candle
                continue
            rows.append(
                (
                    _from_unix_ms(open_time_ms),
                    float(kline[1]),
                    float(kline[2]),
                    float(kline[3]),
                    float(kline[4]),
                    float(kline[5]),
                )
            )
        last_open_ms = int(batch[-1][0])
        next_start_ms = last_open_ms + 1
        if next_start_ms <= start_ms:
            break
        start_ms = next_start_ms
        if len(batch) < _PAGE_LIMIT:
            break

    return rows


def fetch_binance_price(
    cache_path: Path | str | None = None,
    symbol: str = "BTCUSDT",
    interval: str = "1d",
    start: datetime.datetime = DEFAULT_FETCH_START,
    api_base_url: str = DEFAULT_BINANCE_API_BASE_URL,
    request_timeout: float = 30.0,
) -> pd.DataFrame:
    """Get a Binance reference price series, incrementally updated and locally cached.

    On the first call, fetches the whole candle history from Binance and stores
    it in a local DuckDB file. On subsequent calls, only the candles newer than
    the cached maximum timestamp are fetched, so repeated notebook runs cost a
    single small HTTP request.

    All timestamps are naive UTC datetimes, following the repository convention.

    :param cache_path:
        DuckDB file used for the local cache.
        Defaults to :py:data:`DEFAULT_BINANCE_PRICE_CACHE_PATH`.

    :param symbol:
        Binance spot symbol, e.g. ``BTCUSDT``, ``ETHUSDT``.

    :param interval:
        Candle interval, one of ``1m``, ``5m``, ``15m``, ``1h``, ``4h``, ``1d``.

    :param start:
        Fetch history from this naive UTC timestamp onwards on the first call.

    :param api_base_url:
        Binance API endpoint. Automatically falls back to the public
        market-data mirror if the main endpoint is geo-blocked.

    :param request_timeout:
        HTTP request timeout in seconds.

    :return:
        DataFrame indexed by naive UTC candle open timestamps, with columns
        ``open``, ``high``, ``low``, ``close``, ``volume``, sorted ascending.
    """
    import duckdb

    assert interval in _INTERVAL_DURATIONS, f"Unsupported interval: {interval}, pick one of {list(_INTERVAL_DURATIONS)}"

    cache_path = Path(cache_path or DEFAULT_BINANCE_PRICE_CACHE_PATH).expanduser()
    cache_path.parent.mkdir(parents=True, exist_ok=True)

    connection = duckdb.connect(str(cache_path))
    try:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS binance_price (
                symbol VARCHAR NOT NULL,
                interval VARCHAR NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume DOUBLE,
                PRIMARY KEY (symbol, interval, timestamp)
            )
            """
        )
        cached_max = connection.execute(
            "SELECT max(timestamp) FROM binance_price WHERE symbol = ? AND interval = ?",
            [symbol, interval],
        ).fetchone()[0]

        if cached_max is None:
            fetch_from = start
        else:
            # Refetch the last cached candle as well, in case the cached copy
            # was written from a not-yet-final data point.
            fetch_from = pd.Timestamp(cached_max).to_pydatetime()

        rows = _fetch_klines(api_base_url, symbol, interval, fetch_from, request_timeout)
        if rows:
            connection.executemany(
                f"INSERT OR REPLACE INTO binance_price VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                [(symbol, interval, *row) for row in rows],
            )
            logger.info("Cached %d new %s %s candles in %s", len(rows), symbol, interval, cache_path)

        df = connection.execute(
            """
            SELECT timestamp, open, high, low, close, volume
            FROM binance_price
            WHERE symbol = ? AND interval = ?
            ORDER BY timestamp
            """,
            [symbol, interval],
        ).df()
    finally:
        connection.close()

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df.set_index("timestamp")
