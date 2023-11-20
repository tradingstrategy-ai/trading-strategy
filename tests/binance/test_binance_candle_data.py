import datetime

from tradingstrategy.binance.candlestick import fetch_binance_candlestick_data
from tradingstrategy.timebucket import TimeBucket


def test_read_fresh_candle_data():
    """Test reading fresh candle data."""
    df = fetch_binance_candlestick_data(
        "ETHUSDC",
        TimeBucket.h1,
        datetime.datetime(2021, 1, 1),
        datetime.datetime(2021, 1, 2),
        force_redownload=True,
    )

    pass


def test_read_cached_candle_data():
    """Test reading cached candle data."""
    pass


def test_purge_cached_candle_data():
    """Test purging cached candle data."""
    pass
