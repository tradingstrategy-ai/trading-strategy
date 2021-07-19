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

        An agent software that acts without human intervention. Once started, there is no further need for system administration or othe work.

    Smart contract

        An automated transactional service running on any of blockchains supporting smart contracts. Typically
        runs on Ethereum based blockchain and is written in Solidity programming language.

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

    Liquidity

        Refers to the depth of tradeable asset on an exchange. More there is liquidity, larger trades you can do
        without moving the price.

    Candle

        Candle, or a candlestick is a type of price chart used in technical analysis that displays the high, low, open, and closing prices of an asset for a specific time period, or :term:`bucket`. `More information <https://en.wikipedia.org/wiki/Candlestick_chart>`__.

    Bucket

        Refers to a time period for :term:`candle` data. For example, you can have one minute, one hour or daily buckets.

    OHLCV

        A typical :term:`candle` contains open, high, low and close price and trade volume for a :term:`time bucket <bucket>`. Because on-chain exposes more data than centralised exchanges, Capitalgram data also contains individual buys and sells, US dollar exchange rate and so forth.

    Parquet

        A popular file format for large datasets from Apache Arrow project. `More information <https://parquet.apache.org/>`__.

    Pyarrow

        Python API for :term:`Arrow` library. `More information <https://arrow.apache.org/docs/python/>`__.

    Arrow

        Apache Arrow is a popular open source in-memory analytics technology kit. `More information <https://arrow.apache.org/docs/index.html>`__.

    Dataclass

        Standard Python way to annotate data structures. `More information <https://realpython.com/python-data-classes/>`__.

    Dataset server

        The server than indexes blockchains and creates :term:`candle` and other data for research, analysis and trade execution. Currently centralised and you need an API key to access.

    Notebook

        Notebook refers to an interactively editable Python script or application, mixed with diagrams and notes. The format was popularised by :term:`Jupyter notebook`.