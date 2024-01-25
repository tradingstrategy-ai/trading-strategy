"""Wick filter tests"""

import pandas as pd

from eth_defi.price_oracle.oracle import TrustedStablecoinOracle, FixedPriceOracle

from tradingstrategy.direct_feed.candle_feed import CandleFeed, prepare_raw_candle_data
from eth_defi.event_reader.reorganisation_monitor import MockChainAndReorganisationMonitor
from tradingstrategy.direct_feed.synthetic_feed import SyntheticTradeFeed
from tradingstrategy.direct_feed.timeframe import Timeframe
from tradingstrategy.utils.groupeduniverse import filter_bad_wicks, fix_bad_wicks, remove_zero_candles


def test_filter_wick():
    """See we filter out bad wicks.

    """

    mock_chain = MockChainAndReorganisationMonitor(block_duration_seconds=12)
    mock_chain.produce_blocks(1000)
    timeframe = Timeframe("1min")

    feed = SyntheticTradeFeed(
        ["ETH-USD"],
        {"ETH-USD": TrustedStablecoinOracle()},
        mock_chain,
        timeframe=timeframe,
        start_price_range=1000,
        end_price_range=1000,
        min_amount=-50,
        max_amount=50,
        broken_wick_block_frequency=70,  # Each 70th block has a broken wick
    )

    candle_feed =  CandleFeed(
        ["ETH-USD"],
        timeframe=timeframe,
    )

    delta = feed.backfill_buffer(1000, None)
    candle_feed.apply_delta(delta)

    candles = candle_feed.get_candles_by_pair("ETH-USD")
    candles = prepare_raw_candle_data(candles)
    assert len(candles) == 201

    # timestamp
    # 1970-01-01 00:00:00  185.737167     187.041977    0.184675  184.397705 1970-01-01 00:00:00            1.0            1          4   412.136108  24.243300     8      9
    # 1970-01-01 00:03:00  173.230637     181.385696    0.176682  181.385696 1970-01-01 00:03:00            1.0           15         19   593.300659  28.252414     8     13
    # 1970-01-01 00:05:00  184.232758     192.895233    0.183959  190.483109 1970-01-01 00:05:00            1.0           25         29   304.288879  16.904938     8     10
    # 1970-01-01 00:07:00  187.152039  183846.593750  178.582916  178.897247 1970-01-01 00:07:00            1.0           35         39   428.652252  22.560645     9     10
    # 1970-01-01 00:11:00  173.231171     175.470596    0.168990  165.914276 1970-01-01 00:11:00            1.0           55         59   737.052490  24.568416    21      9
    # ...                         ...            ...         ...         ...                 ...            ...          ...        ...          ...        ...   ...    ...
    # 1970-01-01 03:09:00   65.888611      77.790543    0.066213   71.476082 1970-01-01 03:09:00            1.0          945        949   618.344788  24.733792    13     12
    # 1970-01-01 03:15:00   52.565765   55555.296875   52.565765   58.915375 1970-01-01 03:15:00            1.0          975        979  1004.283081  29.537738    16     18
    # 1970-01-01 03:17:00   55.522102      67.499176    0.061094   59.592865 1970-01-01 03:17:00            1.0          985        989   783.948669  23.057314    17     17
    # 1970-01-01 03:18:00   62.080311      79.483459    0.066638   79.483459 1970-01-01 03:18:00            1.0          990        994   797.907776  28.496706    13     15
    # 1970-01-01 03:20:00   78.620239   80748.343750   77.683197   77.683197 1970-01-01 03:20:00            1.0         1000       1000   206.801514  20.680151     4      6
    #
    # [93 rows x 12 columns]

    wicked = filter_bad_wicks(candles)
    assert len(wicked) == 59

    candles = fix_bad_wicks(candles)
    wicked = filter_bad_wicks(candles)
    assert len(wicked) == 0
    assert len(candles) == 201


def test_remove_zero_candles():
    data = {
        'Date': ['2021-01-01', '2021-01-02', '2021-01-03', '2021-01-04'],
        'open': [100, 0, 105, 99],
        'high': [110, 115, 0, 100],
        'low': [95, 0, 100, 95],
        'close': [105, 110, 0, 96],
    }
    
    df = pd.DataFrame(data).set_index('Date')
    assert len(df) == 4

    new_df = remove_zero_candles(df)
    assert len(new_df) == 2
    
    _new_df = pd.DataFrame(
        data = {
            'Date': ['2021-01-01', '2021-01-04'],
            'open': [100, 99],
            'high': [110, 100],
            'low': [95, 95],
            'close': [105, 96],
        },
    ).set_index('Date')
    
    assert new_df.equals(_new_df)