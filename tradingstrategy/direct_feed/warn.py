import warnings


def disable_pandas_warnings():
    #   /Users/moo/code/ts/trade-executor/deps/trading-strategy/tradingstrategy/direct_feed/trade_feed.py:338: FutureWarning: The behavior of Timestamp.utcfromtimestamp is deprecated, in a future version will return a timezone-aware Timestamp with UTC timezone. To keep the old behavior, use Timestamp.utcfromtimestamp(ts).tz_localize(None). To get the future behavior, use Timestamp.fromtimestamp(ts, 'UTC')
    #warnings.filterwarnings(action='ignore', category=FutureWarning, message=".*utcfromtimestamp.*")
    warnings.filterwarnings(action='ignore', category=FutureWarning)