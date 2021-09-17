import datetime

from tradingstrategy.candle import Candle
from tradingstrategy.chain import ChainId


def test_serialise_pandas():
    """Create Pandas dataframes using Candle class."""

    # This dataframe has pd.Series set up
    frame = Candle.to_dataframe()

    data = dict(
        timestamp=datetime.datetime.now(),
        chain_id=ChainId.ethereum.value,
        open=4.506716236944988,
        buys=0,
        sells=1,
        buy_volume=0,
        sell_volume=3.3484309999999997,
        avg_trade=4.506716236944988,
        exchange_rate=1.0,
        start_block_number=10_194_847,
        end_block_number=10_194_847,
    )

    frame.append(data, ignore_index=True)


