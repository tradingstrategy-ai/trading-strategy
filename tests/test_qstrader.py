"""QSTrader integration test."""
import datetime

from capitalgram.candle import Candle
from capitalgram.chain import ChainId

import os

import pandas as pd
import pytz

from capitalgram.frameworks.qstrader import DEXAsset, prepare_candles_for_qstrader
from capitalgram.pair import PandasPairUniverse, DEXPair
from capitalgram.timebucket import TimeBucket
from qstrader.alpha_model.fixed_signals import FixedSignalsAlphaModel
from qstrader.asset.equity import Equity
from qstrader.asset.universe.static import StaticUniverse
from qstrader.data.backtest_data_handler import BacktestDataHandler
from qstrader.data.daily_bar_csv import CSVDailyBarDataSource
from qstrader.data.daily_bar_dataframe import DataframeDailyBarDataSource
from qstrader.statistics.tearsheet import TearsheetStatistics
from qstrader.trading.backtest import BacktestTradingSession


def test_qstrader_vanilla():
    """Run the vanilla QSTrader buy and hold example.

    See that our changes do not break QSTrader.
    """
    start_dt = pd.Timestamp('2004-11-19 14:30:00', tz=pytz.UTC)
    end_dt = pd.Timestamp('2019-12-31 23:59:00', tz=pytz.UTC)

    # Construct the symbol and asset necessary for the backtest
    strategy_symbols = ['GLD']
    strategy_assets = ['EQ:GLD']
    strategy_universe = StaticUniverse(strategy_assets)

    # To avoid loading all CSV files in the directory, set the
    # data source to load only those provided symbols
    csv_dir = os.environ.get('QSTRADER_CSV_DATA_DIR', 'tests')
    data_source = CSVDailyBarDataSource(csv_dir, Equity, csv_symbols=strategy_symbols)
    data_handler = BacktestDataHandler(strategy_universe, data_sources=[data_source])

    # Construct an Alpha Model that simply provides a fixed
    # signal for the single GLD ETF at 100% allocation
    # with a backtest that does not rebalance
    strategy_alpha_model = FixedSignalsAlphaModel({'EQ:GLD': 1.0})
    strategy_backtest = BacktestTradingSession(
        start_dt,
        end_dt,
        strategy_universe,
        strategy_alpha_model,
        rebalance='buy_and_hold',
        long_only=True,
        cash_buffer_percentage=0.01,
        data_handler=data_handler
    )
    strategy_backtest.run()

    # Performance Output
    tearsheet = TearsheetStatistics(
        strategy_equity=strategy_backtest.get_equity_curve(),
        title='Buy & Hold GLD ETF'
    )
    tearsheet.plot_results()


def test_qstrader_crypto(persistent_test_client):
    """Run QSTrader buy and hold against SUSHI-WETH pair on SushiSwap."""
    start_dt = pd.Timestamp('2020-10-01 00:00:00', tz=pytz.UTC)
    end_dt = pd.Timestamp('2021-10-01 00:00:00', tz=pytz.UTC)

    capitalgram = persistent_test_client

    exchange_universe = capitalgram.fetch_exchange_universe()

    # Fetch all trading pairs across all exchanges
    columnar_pair_table = capitalgram.fetch_pair_universe()
    pair_universe = PandasPairUniverse(columnar_pair_table.to_pandas())

    # Pick SUSHI-USDT trading on SushiSwap
    sushi_swap = exchange_universe.get_by_name_and_chain(ChainId.ethereum, "sushiswap")
    sushi_eth: DEXPair = pair_universe.get_one_pair_from_pandas_universe(
        sushi_swap.exchange_id,
        "SUSHI",
        "WETH")

    # Get daily candles as Pandas DataFrame
    all_candles = capitalgram.fetch_all_candles(TimeBucket.d1).to_pandas()
    sushi_eth_candles: pd.DataFrame  = all_candles.loc[all_candles["pair_id"] == sushi_eth.pair_id]

    sushi_eth_candles = prepare_candles_for_qstrader(sushi_eth_candles)

    # Construct the symbol and asset necessary for the backtest
    # strategy_symbols = ['GLD']
    strategy_assets = [sushi_eth]
    strategy_universe = StaticUniverse(strategy_assets)

    asset_bar_frames = {
        sushi_eth.pair_id: sushi_eth_candles
    }

    # To avoid loading all CSV files in the directory, set the
    # data source to load only those provided symbols
    data_source = DataframeDailyBarDataSource(asset_bar_frames, DEXAsset)
    data_handler = BacktestDataHandler(strategy_universe, data_sources=[data_source])

    # Construct an Alpha Model that simply provides a fixed
    # signal for the single GLD ETF at 100% allocation
    # with a backtest that does not rebalance
    strategy_alpha_model = FixedSignalsAlphaModel({sushi_eth.pair_id: 1.0})
    strategy_backtest = BacktestTradingSession(
        start_dt,
        end_dt,
        strategy_universe,
        strategy_alpha_model,
        rebalance='buy_and_hold',
        long_only=True,
        cash_buffer_percentage=0.01,
        data_handler=data_handler
    )
    strategy_backtest.run()

    # Performance Output
    tearsheet = TearsheetStatistics(
        strategy_equity=strategy_backtest.get_equity_curve(),
        title=f'Buy & Hold {sushi_eth}'
    )
    tearsheet.plot_results()


