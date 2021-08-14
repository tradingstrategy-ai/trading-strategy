"""QSTrader ape-in strategy test."""
import datetime
import logging
import os
from typing import Dict

import pandas as pd
import pytz

from capitalgram.candle import GroupedCandleUniverse
from capitalgram.exchange import ExchangeUniverse
from qstrader.alpha_model.alpha_model import AlphaModel
from qstrader.alpha_model.fixed_signals import FixedSignalsAlphaModel
from qstrader.asset.equity import Equity
from qstrader.asset.universe.static import StaticUniverse
from qstrader.data.backtest_data_handler import BacktestDataHandler
from qstrader.data.daily_bar_csv import CSVDailyBarDataSource
from qstrader.data.daily_bar_dataframe import DataframeDailyBarDataSource
from qstrader.statistics.tearsheet import TearsheetStatistics
from qstrader.trading.backtest import BacktestTradingSession

from capitalgram.chain import ChainId
from capitalgram.frameworks.qstrader import DEXAsset, prepare_candles_for_qstrader
from capitalgram.liquidity import GroupedLiquidityUniverse
from capitalgram.pair import PandasPairUniverse, DEXPair
from capitalgram.timebucket import TimeBucket

logger = logging.getLogger(__name__)


def prefilter_pairs(all_pairs_dataframe: pd.DataFrame) -> pd.DataFrame:
    """Get rid of pairs that we definitely are not interested in.

    This will greatly speed up the later backtesting computations, as we do not need to
    calculate the opening volumes for thousands of pairs.

    Note that may induce survivorship bias - we use thiws mainly
    to ensure the example strategy completes fast enough.
    """
    pairs: pd.DataFrame = all_pairs_dataframe.loc[
        (all_pairs_dataframe['buy_volume_all_time'] > 5_000_000)  # 500k min buys
    ]
    return pairs


def update_pair_liquidity_threshold(
        now_: datetime.datetime,
        threshold: float,
        reached_state: dict,
        pair_universe: PandasPairUniverse,
        liquidity_universe: GroupedLiquidityUniverse) -> dict:
    """Check which pairs reach the liquidity threshold on a given day.

    :param threshold: Available liquidity, in US dollar

    :return: Dict of pair ids who reached the liquidity threshold and how much liquidity they had
    """

    new_entries = {}

    # QSTrader carries hours in its timestamp
    # Timestamp('2020-10-01 14:30:00+0000', tz='UTC')
    ts = pd.Timestamp(now_.date())

    for pair_id in pair_universe.get_all_pair_ids():
        if pair_id not in reached_state:
            # Get the todays liquidity
            liquidity_samples = liquidity_universe.get_samples_by_pair(pair_id)
            # We determine the available liquidity by the daily open
            try:
                liquidity_today = liquidity_samples["open"][ts]
            except KeyError:
                liquidity_today = 0

            if liquidity_today >= threshold:
                reached_state[pair_id] = now_
                new_entries[pair_id] = liquidity_today

    return new_entries


class LiquidityThresholdReachedAlphaModel(AlphaModel):
    """
    A simple AlphaModel that provides a single scalar forecast
    value for each Asset in the Universe.

    Parameters
    ----------
    signal_weights : `dict{str: float}`
        The signal weights per asset symbol.
    universe : `Universe`, optional
        The Assets to make signal forecasts for.
    data_handler : `DataHandler`, optional
        An optional DataHandler used to preserve interface across AlphaModels.
    """

    def __init__(
            self,
            exchange_universe: ExchangeUniverse,
            pair_universe: PandasPairUniverse,
            candle_universe: GroupedCandleUniverse,
            liquidity_universe: GroupedLiquidityUniverse,
            min_liquidity=500_000,
            data_handler=None
    ):
        self.exchange_universe = exchange_universe
        self.pair_universe = pair_universe
        self.candle_universe = candle_universe
        self.liquidity_universe = liquidity_universe
        self.data_handler = data_handler
        self.min_liquidity = min_liquidity
        self.liquidity_reached_state = {}

    def __call__(self, dt) -> Dict[int, float]:
        """
        Produce the dictionary of scalar signals for
        each of the Asset instances within the Universe.

        Parameters
        ----------
        dt : `pd.Timestamp`
            The time 'now' used to obtain appropriate data and universe
            for the the signals.

        Returns
        -------
        `dict{str: float}`

            The Asset symbol keyed scalar-valued signals.
        """

        # Refresh which cross the liquidity threshold today
        new_entries = update_pair_liquidity_threshold(
            dt,
            self.min_liquidity,
            self.liquidity_reached_state,
            self.pair_universe,
            self.liquidity_universe
        )

        logger.info("Got data %s %s", dt, new_entries)

        return {}


def test_qstrader_ape_in(persistent_test_client):
    """Run QSTrader buy and hold against SUSHI-WETH pair on SushiSwap."""

    client = persistent_test_client

    exchange_universe = client.fetch_exchange_universe()

    # Decompress the pair dataset to Python map
    columnar_pair_table = client.fetch_pair_universe()

    # Make our universe 40x smaller and faster to compute
    filtered_pairs = prefilter_pairs(columnar_pair_table.to_pandas())

    # We limit candles to a specific date range to make this notebook deterministic
    start = pd.Timestamp('2020-10-01 14:30:00', tz=pytz.UTC)
    end = pd.Timestamp('2021-07-01 23:59:00', tz=pytz.UTC)

    # Make the trading pair data easily accessible
    pair_universe = PandasPairUniverse(filtered_pairs)
    wanted_pair_ids = pair_universe.get_all_pair_ids()

    # Get daily candles as Pandas DataFrame
    all_candles = client.fetch_all_candles(TimeBucket.d1).to_pandas()
    all_candles = all_candles.loc[all_candles["pair_id"].isin(wanted_pair_ids)]
    candle_universe = GroupedCandleUniverse(prepare_candles_for_qstrader(all_candles))

    all_liquidity = client.fetch_all_liquidity_samples(TimeBucket.d1).to_pandas()
    all_liquidity = all_liquidity.loc[all_liquidity["pair_id"].isin(wanted_pair_ids)]
    liquidity_universe = GroupedLiquidityUniverse(all_liquidity)

    logger.info("Starting the strategy. We have %d pairs, %d candles, %d liquidity samples",
                pair_universe.get_count(),
                candle_universe.get_candle_count(),
                liquidity_universe.get_sample_count())

    asset_bar_frames = {pair_id: df for pair_id, df in candle_universe.get_all_pairs()}

    strategy_assets = list(asset_bar_frames.keys())
    strategy_universe = StaticUniverse(strategy_assets)

    # To avoid loading all CSV files in the directory, set the
    # data source to load only those provided symbols
    data_source = DataframeDailyBarDataSource(asset_bar_frames, DEXAsset)
    data_handler = BacktestDataHandler(strategy_universe, data_sources=[data_source])

    # Construct an Alpha Model that simply provides a fixed
    # signal for the single GLD ETF at 100% allocation
    # with a backtest that does not rebalance
    strategy_alpha_model = LiquidityThresholdReachedAlphaModel(
        exchange_universe,
        pair_universe,
        candle_universe,
        liquidity_universe)

    strategy_backtest = BacktestTradingSession(
        start,
        end,
        strategy_universe,
        strategy_alpha_model,
        rebalance='daily',
        long_only=True,
        cash_buffer_percentage=0.01,
        data_handler=data_handler
    )
    strategy_backtest.run()

    # Performance Output
    tearsheet = TearsheetStatistics(
        strategy_equity=strategy_backtest.get_equity_curve(),
        title=f'Ape in the latest'
    )
    # tearsheet.plot_results()
