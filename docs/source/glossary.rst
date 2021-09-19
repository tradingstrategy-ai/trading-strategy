
.. _glossary:

Glossary
========

.. glossary::
    :sorted:

    AMM
        Automated market maker (AMM) is a `bonding curve based <https://docs.ethhub.io/guides/graphical-guide-for-understanding-uniswap/>`__ decentralised exchange. It does not
      have an order book.

    On-chain

        On-chain refers to any activity that happens purely on a public blockchain. It means the data
        and trading venues are publicly and fairly available for anyone.

    Backtest

        Simulating the efficiency of a trading strategy against historical data.

    Decentralised exchange

        An exchange where all trades happen purely :term:`on-chain`. These exchanges are public, fair, cheap and especially censorship proof. There is no middleman like a broker when you are trading on these venues, but you get equal access to the trade flow. Decentralised exchanges can be :term:`AMM` based or order book based. Some of the most popular decentralised exchanges are Uniswap, Sushiwap and PancakeSwap.

    Autonomous agent

        An agent software that acts without human intervention. Once started, there is no further need for system administration or other work.

    Smart contract

        An automated transactional service running on any of blockchains supporting smart contracts. Typically
        runs on Ethereum based blockchain and is written in Solidity programming language.

    Jupyter notebook

        A popular Python based data science tool. Jupyter allows users to run data research :term:`notebooks <notebook>` interactively. Jupyter notebooks can be easily shared, run on your local computer or on a hosted cloud environment, both free and paid. `More information <https://jupyter.org/>`__.

    Pandas

        A popular Python based data analysis library.  `More information <https://pandas.pydata.org/>`__.

    Uniswap

        The most popular :term:`AMM` based :term:`decentralised exchange`. Uniswap has two major versions.
        In version 2 (v2) the liquidity is evenly distributed across the bonding curve. In version 3, the liquidity providers can have liquidity on a partial curve, simulate order book and have better capital efficiency. Most decentralised exchanges are Uniswap v2 :term:`clones <clone>`.

    Clone

        Also known as fork. A product launched based on the open source code of another existing product.
        In the context of :term:`on-chain`, usually hostile to the original product and competes from the same :term:`liquidity`.

    Candle

        A candle or a candlestick is a type of price chart used in technical analysis that displays the high, low, open, and closing prices of an asset for a specific time period, or :term:`bucket`. `More information <https://en.wikipedia.org/wiki/Candlestick_chart>`__.

    Bucket

        The (time) bucket to a time period for :term:`candle` data. For example, you can have one minute, one hour or time buckets, describing for what period of time the candle includes the trades.

    OHLCV

        A typical :term:`candle` contains open, high, low and close price and trade volume for a :term:`time bucket <bucket>`. Because on-chain exposes more data than centralised exchanges, Trading Strategy data also contains individual buys and sells, US dollar exchange rate and so forth.

    Parquet

        A popular file format for large datasets from Apache Arrow project. `More information <https://parquet.apache.org/>`__.

    Pyarrow

        Python API for :term:`Arrow` library. `More information <https://arrow.apache.org/docs/python/>`__.

    Arrow

        Apache Arrow is a popular open-source in-memory analytics technology kit. `More information <https://arrow.apache.org/docs/index.html>`__.

    Dataclass

        Standard Python way to annotate data structures. `More information <https://realpython.com/python-data-classes/>`__.

    Dataset

        A data bundle consisting of :term:`candles <candle>` or other quantitative data sources.
        The most usual dataset is hourly or daily candles for multiple assets, distributed as a downloadable archive of several hundreds of megabytes.

    Dataset server

        The server than indexes blockchains and creates :term:`candle` and other :term:`datasets <dataset>` for research, analysis and trade execution. Currently centralised and you need an API key to access.

    Notebook

        Notebook refers to an interactively editable Python script or application, mixed with diagrams and notes. The format was popularised by :term:`Jupyter notebook`.

    Strategy

        Also known as trading strategy or algorithm. The trading strategy is the rulebook what trades to make an how. In the context of quantative finance, and especially automated trading, this rulebook can be expressed as an algorithm and trading bot that has programmed rules for every situation the strategy may encounter.

    Technical analysis

        Technical analysis is a trading discipline employed to evaluate investments and identify trading opportunities by analyzing statistical trends gathered from trading activity, such as price movement and volume. `More information <https://www.investopedia.com/terms/t/technicalanalysis.asp>`__.

    Backtrader

        An old Python based framework for strategy backtesting and live execution. `See documentation <https://www.backtrader.com/>`__.

    Fastquant

        Fastquant allows you to easily backtest investment strategies with as few as three lines of Python code. Its goal is to promote data-driven investments by making quantitative analysis in finance accessible to everyone. Fastquant builds on the top of :term:`Backtrader`. See `Github repository <https://github.com/enzoampil/fastquant>`__.

    Base token

        The token you want to buy or sell in a trading pair. See also :term:`quote token`.

    Quote token

        The token acts as a nominator for the price when you are buying or selling. Usually, this is a more well-known token of the pair: ETH, BTC or any of various USD stablecoins. See also :term:`base token`.

    Liquidity

        Liquidity refers to the depth of the order books: how much volume a single trade can achieve without moving the price. It can be expressed as :term:`slippage` or absolute depth of the order book. The latter is very easy for :term:`AMM` based exchanges where the liquidity is a continuous function. Trading Strategy provides :term:`datasets <dataset>` for AMM liquidity in :py:mod:`capitalgram.liquidity` module.

    Slippage

        Slippage tells you how much you will lose in a trade because there is not enough :term:`liquidity` to satisfy the deal. `More information <https://www.investopedia.com/terms/s/slippage.asp>`__.

    Yield farming

        Pooling assets of multiple people for passive trading strategies. Usually yield farming pools rely on liquidity mining token distribution which they immediately sell (auto compounding). Yield farms operate solely on smart contracts and their strategies are limited. Yield farms almost always take zero market risk agains their :term:`quote token`.

    Exposure

        The risk of a strategy for the volatility of a particular asset. For example, if you have 100% exposure to ETH and ETH prices drops to zero, you lose all of your money.

    Market neutral strategy

        Market neutral strategies are trading strategies that have little or no :term:`exposure` to crypto asset volatility. They are often :term:`high-frequency trading` strategies, like arbitrage. Good market neutral strategies can make 10% - 20% monthly yield in cryptocurrency markets.

    High-frequency trading

        High-frequency trading, or HFT for short, is a trading strategy where you do arbitration, cross-market market making or such and compete against the other actors with your technical speed. Trading Strategy is not a suitable framework for HFT trading, though its data can aid to come up with good HFT strategies.

    Directional strategy

        A trading strategy where you bet the market to go up or down.

    Active strategy

        Buying and selling assets based on the market movement. Differs from buy-and-hold by actively (hourly, daily, weekly) engaging in trading. `Read more <https://www.investopedia.com/articles/active-trading/11/four-types-of-active-traders.asp>`__.

    Non-custodial

        A smart contract-based service model where the owner of the assets never loses control of the assets. This is opposite to most traditional finance services where you cannot see what happens to your money after the deposit or whether you can withdraw. The integrity of the service provider in traditional finance thus needs to be guaranteed through regulation or government bailouts. The non-custodial model is specific to smart contracts and cannot be achieved without a blockchain. `Read more <https://stackoverflow.com/questions/65009246/what-does-non-custodial-mean>`__.

    Private strategy

        A trading strategy where the source code of the strategy is not disclosed to the public. Private strategies can still be :term:`non-custodial` and enjoy the benefits of Trading Strategy protocol trade execution and fee distribution. :ref`Read more <Private strategies>`.

    Risk-free rate

        The expected return for the money is considered (almost) risk-free. On the traditional markets, this is the treasury note or government bond yield (although you still have some risks like the sovereignty risk). In DeFi this is considered an US dollar lending pool rate, like one you would get from Aave :term:`USDC` pool.

    Drawdown

        How many % the asset can go down. `Read more <https://en.wikipedia.org/wiki/Drawdown_(economics)>`__.

    USDC

        A popular US cash and US treasury note backed stablecoin from Circle. `Read more <https://www.circle.com/en/usdc>`__.

    Pine Script

        A proprietary trading strategy programming language for :ref:`TradingView`. `Read more <https://www.tradingview.com/support/solutions/43000561836-what-is-pine-script/>`__.

    TradingView

        Trading view is the world most popular trading strategy platform. It lets you discover investment ideas and showcase your talents to a large and active community of traders. Easy and intuitive for beginners, and powerful enough for advanced chartists. Trading View has all the charting tools you need to share and view trading ideas. Real-time data and browser-based charts let you do your research from anywhere since there are no installations or complex setups. `Read more <https://www.quora.com/What-is-TradingView>`__.
