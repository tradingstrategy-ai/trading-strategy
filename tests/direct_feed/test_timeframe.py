import pandas as pd

from tradingstrategy.direct_feed.timeframe import Timeframe


def test_snap_down():
    tf = Timeframe("1D")
    assert tf.round_timestamp_down(pd.Timestamp("2020-01-01 01:00")) == pd.Timestamp("2020-01-01 00:00")

def test_snap_down_with_offset():
    tf = Timeframe("1H", offset=pd.Timedelta(minutes=-5))
    assert tf.round_timestamp_down(pd.Timestamp("2020-01-01 02:13")) == pd.Timestamp("2020-01-01 01:55")