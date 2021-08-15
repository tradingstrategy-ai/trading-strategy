"""QSTrader ape-in strategy test."""
import cProfile
import datetime
import logging
import os
from pstats import Stats
from typing import Dict

import pandas as pd
import pytz
from IPython.core.display import display

from capitalgram.analysis.tradeanalyzer import expand_timeline
from capitalgram.candle import GroupedCandleUniverse
from capitalgram.exchange import ExchangeUniverse

from capitalgram.frameworks.qstrader import analyse_portfolio
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
from capitalgram.frameworks.qstrader import DEXAsset, prepare_candles_for_qstrader, CapitalgramDataSource
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

        # Skip pairs we know reached liquidity threshold earlier
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
            max_assets_per_portfolio=5,
            data_handler=None
    ):
        self.exchange_universe = exchange_universe
        self.pair_universe = pair_universe
        self.candle_universe = candle_universe
        self.liquidity_universe = liquidity_universe
        self.data_handler = data_handler
        self.min_liquidity = min_liquidity
        self.max_assets_per_portfolio = max_assets_per_portfolio
        self.liquidity_reached_state = {}

    def construct_shopping_basked(self, dt: pd.Timestamp, new_entries: dict) -> Dict[int, float]:
        """Construct a pair id """

        # Sort entire by volume
        sorted_by_volume = sorted(new_entries.items(), key=lambda x: x[1], reverse=True)

        # Weight all entries equally based on our maximum N entries size
        pick_count = min(len(sorted_by_volume), self.max_assets_per_portfolio)

        ts = pd.Timestamp(dt.date())

        if pick_count:
            weight = 1.0 / pick_count
            picked = {}
            for i in range(pick_count):
                pair_id, vol = sorted_by_volume[i]

                # An asset may have liquidity added, but not a single trade yet (EURS-USDC on 2020-10-1)
                # Ignore them, because we cannot backtest something with no OHLCV data
                candles = self.candle_universe.get_candles_by_pair(pair_id)

                # Note daily bars here, not open-close bars as internally used by QSTrader
                if ts not in candles["Close"]:
                    name = self.translate_pair(pair_id)
                    logger.warning("Tried to trade too early %s at %s", name, ts)
                    continue

                picked[pair_id] = weight

            return picked

        # No new feasible assets today
        return {}

    def translate_pair(self, pair_id: int) -> str:
        """Make pari ids human readable for logging."""
        pair_info = self.pair_universe.get_pair_by_id(pair_id)
        return pair_info.get_friendly_name(self.exchange_universe)

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
        logger.debug("New entries coming to the market %zs %s", dt, new_entries)
        picked = self.construct_shopping_basked(dt, new_entries)

        if picked:
            logger.info("On day %s our picks are", dt)
            for pair_id, weight in picked.items():
                logger.info("    %s: %f", self.translate_pair(pair_id), weight)
        else:
            logger.info("On day %s there is nothing new interesting at the markets", dt)
        return picked


def test_qstrader_ape_in(persistent_test_client):
    """Run QSTrader buy and hold against SUSHI-WETH pair on SushiSwap."""

    client = persistent_test_client

    exchange_universe = client.fetch_exchange_universe()

    # Decompress the pair dataset to Python map
    columnar_pair_table = client.fetch_pair_universe()

    # Make our universe 40x smaller and faster to compute
    filtered_pairs = prefilter_pairs(columnar_pair_table.to_pandas())

    # We limit candles to a specific date range to make this notebook deterministic
    # To make the test run wall clock time shorter, we only do one month sample
    start = pd.Timestamp('2021-01-01 14:30:00')
    end = pd.Timestamp('2021-01-07 23:59:00')

    # Make the trading pair data easily accessible
    pair_universe = PandasPairUniverse(filtered_pairs)
    wanted_pair_ids = pair_universe.get_all_pair_ids()

    # Get daily candles as Pandas DataFrame
    all_candles = client.fetch_all_candles(TimeBucket.d1).to_pandas()
    all_candles = all_candles.loc[all_candles["pair_id"].isin(wanted_pair_ids)]
    candle_universe = GroupedCandleUniverse(prepare_candles_for_qstrader(all_candles), timestamp_column="Date")

    all_liquidity = client.fetch_all_liquidity_samples(TimeBucket.d1).to_pandas()
    all_liquidity = all_liquidity.loc[all_liquidity["pair_id"].isin(wanted_pair_ids)]
    all_liquidity = all_liquidity.set_index(all_liquidity["timestamp"])
    liquidity_universe = GroupedLiquidityUniverse(all_liquidity)

    logger.info("Starting the strategy. We have %d pairs, %d candles, %d liquidity samples",
                pair_universe.get_count(),
                candle_universe.get_candle_count(),
                liquidity_universe.get_sample_count())

    data_source = CapitalgramDataSource(exchange_universe, pair_universe, candle_universe)

    strategy_assets = list(data_source.asset_bar_frames.keys())
    strategy_universe = StaticUniverse(strategy_assets)

    data_handler = BacktestDataHandler(strategy_universe, data_sources=[data_source])

    # Construct an Alpha Model that simply provides a fixed
    # signal for the single GLD ETF at 100% allocation
    # with a backtest that does not rebalance
    strategy_alpha_model = LiquidityThresholdReachedAlphaModel(
        exchange_universe,
        pair_universe,
        candle_universe,
        liquidity_universe)

    # Start strategy with $10k
    strategy_backtest = BacktestTradingSession(
        start,
        end,
        strategy_universe,
        strategy_alpha_model,
        initial_cash=10_000,
        rebalance='daily',
        long_only=True,
        cash_buffer_percentage=0.25,
        data_handler=data_handler,
        # Chop off the first day from the strategy, as the first day
        # will report all existing Uniswap pairs as "new"
        burn_in_dt=start - datetime.timedelta(days=2)
    )
    logger.info("Running the strategy")

    strategy_backtest.run()

    portfolio = strategy_backtest.broker.portfolios["000001"]
    trade_analysis = analyse_portfolio(portfolio.history)

    timeline = trade_analysis.create_timeline()
    expanded_timeline = expand_timeline(exchange_universe, pair_universe, timeline)
    display(expanded_timeline)
    import ipdb ; ipdb.set_trace()

    summary = trade_analysis.calculate_summary_statistics()

    print(summary)
    import ipdb ; ipdb.set_trace()

    # Though the result can be somewhat random,
    # assume we have done at least one winning and one losing trade
    assert summary.won > 0
    assert summary.lost > 0
    assert -10000 < summary.realised_profit < 100_000

    # Check the time range makes sense
    assert trade_analysis.get_first_opened_at().date() == start.date()
    assert trade_analysis.get_last_closed_at().date() == end.date() - datetime.timedelta(days=1)

    # Performance Output
    tearsheet = TearsheetStatistics(
        strategy_equity=strategy_backtest.get_equity_curve(),
        title=f'Ape in the latest'
    )
    # tearsheet.plot_results()
