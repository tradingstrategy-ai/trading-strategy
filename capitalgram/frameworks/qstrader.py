import pandas as pd

from qstrader.asset.asset import Asset

from capitalgram.pair import DEXPair


class DEXAsset(Asset):

    def __init__(
        self,
        pair_info: DEXPair,
    ):
        self.cash_like = False
        self.pair_info = pair_info

    def __repr__(self):
        """
        String representation of the Equity Asset.
        """
        return f"<DEXAsset {self.pair_info}>"


def prepare_candles_for_qstrader(candles: pd.DataFrame) -> pd.DataFrame:
    """Prepare DataFrame format so that QSTrader strategy can read it.

    QSTrader assumes Yahoo Finance CSV file format with the following columns:

    `Date,Open,High,Low,Close,Adj Close,Volume`
    """
    candles = candles.rename(columns={
        "open": "Open",
        "close": "Close",
        "high": "High",
        "low": "Low",
        "timestamp": "Date",
    })

    # Our index must be the timestamp
    candles = candles.set_index(pd.DatetimeIndex(candles["Date"]))

    # Create volume column
    candles["Volume"] = candles["buy_volume"] + candles["sell_volume"]
    candles["Adj Close"] = candles["Close"]

    return candles

