import pandas as pd


def prepare_candles_for_backtrader(candles: pd.DataFrame) -> pd.DataFrame:
    """Prepare DataFrame format so that Backtrader strategy can read it.

    What assumptions :py:method:`Celebro.addfeed` makes about Pandas data.
    """

    # Our index must be the timestamp
    candles = candles.set_index(pd.DatetimeIndex(candles["timestamp"]))

    # Create volume column
    candles["volume"] = candles["buy_volume"] + candles["sell_volume"]

    return candles
