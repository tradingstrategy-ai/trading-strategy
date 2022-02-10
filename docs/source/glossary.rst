
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

        Decentralised exchange (DEX) is an asset trading exchange where all trades happen purely :term:`on-chain`. These exchanges are public, fair, cheap and especially censorship proof. There is no middleman like a broker when you are trading on these venues, but you get equal access to the trade flow. Decentralised exchanges can be :term:`AMM` based or order book based. Some of the most popular decentralised exchanges are Uniswap, Sushiwap and PancakeSwap.

    Autonomous agent

        An agent software that acts without human intervention. Once started, there is no further need for system administration or othe work.

    Smart contract

        An automated transactional service running on any of the blockchains supporting smart contracts. Typically
        runs on Ethereum based blockchain and is written in the Solidity programming language.

    Jupyter notebook

        A popular Python based data science tool. Jupyter allows users to run data research :term:`notebooks <notebook>` interactively. Jupyter notebooks can be easily shared, run on your local computer or on a hosted cloud environment, both free and paid. `More information <https://jupyter.org/>`__.

    Pandas

        A popular Python based data analysis library.  `More information <https://pandas.pydata.org/>`__.

    Uniswap

        The most popular :term:`AMM` based :term:`decentralised exchange`. Uniswap has two major versios.
        In version 2 (v2) the liquidity is evenly distributed across the bonding curve. In version 3, the
        liquidity providers can have liquidity on a partial curve, simulate order book and have better
        capital efficiency. Most decentralised exchanges are Uniswap v2 :term:`clones <clone>`.

    Clone

        Also known as fork. A product launched based on the open source code of another existing product.
        In the context of :term:`on-chain`, usually hostile to the original product and competes from the
        same :term:`liquidity`.

    Candle

        Candle, or a candlestick is a type of price chart used in technical analysis that displays the high, low, open, and closing prices of an asset for a specific time period, or :term:`bucket`. `More information <https://en.wikipedia.org/wiki/Candlestick_chart>`__.

    Bucket

        The (time) bucket to a time period for :term:`candle` data. For example, you can have one minute, one hour or time buckets, describing for the what period of a time the candle includes the trades.

        Also known as time frame, candle length or candle duration.

    OHLCV

        A typical :term:`candle` contains open, high, low and close price and trade volume for a :term:`time bucket <bucket>`. Because on-chain exposes more data than centralised exchanges, Trading Strategy data also contains individual buys and sells, US dollar exchange rate and so forth.

    Parquet

        A popular file format for large datasets from Apache Arrow project. `More information <https://parquet.apache.org/>`__.

    Pyarrow

        Python API for :term:`Arrow` library. `More information <https://arrow.apache.org/docs/python/>`__.

    Arrow

        Apache Arrow is a popular open source in-memory analytics technology kit. `More information <https://arrow.apache.org/docs/index.html>`__.

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

        Also known as trading strategy or algorithm. A trading strategy is a rulebook of what trades to make an how. In the context of quantative finance, and especially automated trading, this rulebook can be expressed as an algorithm and trading bot that has programmed rules for every situation the strategy may encounter.

    Technical analysis

        Technical analysis is a trading discipline employed to evaluate investments and identify trading opportunities by analyzing statistical trends gathered from trading activity, such as price movement and volume. `More information <https://www.investopedia.com/terms/t/technicalanalysis.asp>`__.

    Backtrader

        An old Python based framework for strategy backtesting and live execution. `See documentation <https://www.backtrader.com/>`__.

    Fastquant

        Fastquant allows you to easily backtest investment strategies with as few as three lines of Python code. Its goal is to promote data driven investments by making quantitative analysis in finance accessible to everyone. Fastquant builds on the top of :term:`Backtrader`. See `Github repository <https://github.com/enzoampil/fastquant>`__.

    Base token

        The token you want to buy or sell in a trading pair. See also :term:`quote token`.

    Quote token

        The token that acts as a nominator for the price when you are buying or selling. Usually this is more well-known token of the pair: ETH, BTC or any of various USD stablecoins. See also :term:`base token`.

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

        A smart contract based service model where the owner of the assets never lose the control of the assets. This is opposite to most traditional finance services where you cannot see what happens to your money after the deposit or whether you are able to withdraw. The integrity of the service provider in the traditional finance thus needs to be guaranteted through regulation or government bailouts. The non-custodial model is specific to smart contracts and cannot be achieved without a blockchain. `Read more <https://stackoverflow.com/questions/65009246/what-does-non-custodial-mean>`__.

    Private strategy

        A trading strategy where the source code of the strategy is not disclosed to public. Private strategies can still be :term:`non-custodial` and enjoy the benefits of Trading Strategy protocol trade execution and fee distribution.

    Risk-free rate

        The expected return for the money that is considered (almost) risk-free. On the traditional markets, this is the tresury note or government bond yield (although you still have some risks like the sovereignity risk). In DeFi this is considered an US dollar lending pool rate, like one you would get from Aave :term:`USDC` pool.

    Drawdown

        How many % the asset can go down. `Read more <https://en.wikipedia.org/wiki/Drawdown_(economics)>`__.

    USDC

        A popular US cash and US treasury note backed stablecoin from Circle. `Read more <https://www.circle.com/en/usdc>`__.

    Pine Script

        A proprietary trading strategy programming language for :term:`TradingView`. `Read more <https://www.tradingview.com/support/solutions/43000561836-what-is-pine-script/>`__.

    TradingView

        Trading view is the world most popular trading strategy platform. It lets you discover investment ideas and showcase your talents to a large and active community of traders. Easy and intuitive for beginners, and powerful enough for advanced chartists. Trading View has all the charting tools you need to share and view trading ideas. Real-time data and browser-based charts let you do your research from anywhere, since there are no installations or complex setups. `Read more <https://www.quora.com/What-is-TradingView>`__.

    EVM compatible

        EVM refers to Ethereum Virtual Machine. EVM compatible blockchain is a blockchain that runs Ethereum Virtual Machine that can run smart contracts written for Ethereum.
        Having EVM compatibility makes it efficient to bring existing Ethereum projects to alternative L1 and L2 blockchains.
        EVM compatible blockchains started to get popular in 2020.
        A well-known EVM compatible blockchains include Polygon, Avalanche, Binance Smart Chain, Harmony, Telos EVM and Fantom.
        Smart contract programmers do not need to modify their existing Solidity or Vyper code and they can re-deploy contracts
        on any EVM chain.

    Bonding curve

        In a bonding curve based exchange, like an :term:`AMM`, market makers do not set limit
        orders to provide liquidity. Instead, the liquidity follows a predefined mathematical function. Every time
        there is a buy or a sell, the price moves up or down defined by this function.

        `Read more about xy=k curve slippage, price impact on Paradigm's post <https://research.paradigm.xyz/amm-price-impact>`_.

        See also: :term:`XY liquidity model`.

    XY liquidity model

        XY liquidity model, as known as XYK, is a :term:`bonding curve` model where the price of an asset follows the equation:

        :math:`x*y=k_{market\_maker}`

        This model was popularised by :term:`Uniswap` version 2 :term:`decentralised exchange`.
        Anyone can buy or sell coins by essentially shifting the market maker's, also known as a liquidity provider, position on the ``x*y=k`` curve.

        On Trading Strategy, the available liquidity is usually expressed as the US dollar amount of one side of the pair. For example adding 100 BNB + 5000 USD to the liquidity
        is presented as 5000 USD available liquidity.

        See also :term:`price impact` and :term:`slippage`.

        `Read more about slippage and price impact on Paradigm's post <https://research.paradigm.xyz/amm-price-impact>`_.

        `Read more about XY liquidity model <https://medium.com/phoenix-finance/understanding-the-xyk-model-of-pooled-liquidity-7340fdc20d9c>`_.

    Price impact

        Price impact is the difference between the current market price and the price you will actually pay when performing a swap on a decentralized exchange.

        Price impact tells how much less your market taker order gets filled because there is not available liquidity.
        For example, if you are trying to buy 5000 USD worth of BNB token, but there isn't available liquidity
        you end up with 4980 USD worth of token at the end of the trade when you pay 5000 USD.
        The missing fill is the price impact.
        It can be expressed as USD value or as percent of the trade amount.

        Illiquid pairs have more slippage than liquid pairs.

        Liquidity provider fees are included in the price impact in AMM models.

        Another way to see this: AMMs usually have a trading fee, of 0.30%, for liquidity providers and sometimes for the protocol.
        This translates to a spread of 0.6% between the best buy order and the best sell order.
        In other words, even the most liquid AMM trade has an implicit 0.3% price impact. Note that due to competition, the LP fees
        are going down on newer AMMs.

        `Read a detailed analysis of how price impact is calculated on Uniswap v2 style AMMs <https://ethereum.stackexchange.com/a/111334/620>`_.

        `See ParaSwap documentation on price impact <https://doc.paraswap.network/price-impact-slippage>`_.

        See also :term:`XY liquidity model`.

        See also :term:`Slippage`.

    Slippage

        Slippage occurs because of changing market conditions between the moment the transaction is submitted and its verification.
        Slippage cannot be backtested easily, because it is based on the trade execution delays and those cannot be usually simulated
        (but can be measured).

        DEX swap orders have a slippage parameter with them. You set it when the order is created.
        If the price changes more then the slippage between the creation of the order and the execution of the order,
        the DEX will cancel the order (revert).

        `See ParaSwap documentation on slippage <https://doc.paraswap.network/price-impact-slippage>`_.

        See also :term:`Price impact`.


