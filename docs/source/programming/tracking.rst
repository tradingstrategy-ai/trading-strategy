.. _tracking:

Tracked trading pairs and tokens
================================

Currently, there exist more than a million tokens on Trading Strategy oracle supported blockchains.
Most of these tokens do not have any real value; they are just tests or projects that never took off.
As it would be impractical to generate complex market data analysis for such a large number of tokens,
a filtering criteria is applied to tokens and trading pairs being tracked.

Trading pair filtering
----------------------

The following criteria is applied for trading pairs.

We have the following kinds of data collection for a trading pair.

* **Untracked pairs**: The data exist on a blockchain, but the oracle indexer does not
  read it at all. For example, trading pairs on incompatible exchanges.

* **Tracked pairs**: The oracle indexer is reading and indexing the raw data.
  A summary of the trading pair info is generated and the trading pair
  appears e.g. in the trading pair counts. However, no market data is yet
  produced.

* **Active pairs**: The oracle indexer is generating the market data
  feed for this pairs. The market data feed includes data like OHLCV candles,
  momentum, market cap and so on.

Below is the criteria for a trading pair to be considered active:

.. raw:: html

    <table class="table table-doc">
        <thead>
            <tr>
                <th></th>
                <th>Criterion</th>
                <th>Filtering</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>#1</td>
                <td>
                    Supported blockchain
                </td>
                <td>
                    The trading pair must trade on a decentralised exchange on one of the <a href="https://tradingstrategy.ai/trading-view/blockchains">supported blockchains.</a>
                </td>
            </tr>

            <tr>
                <td>#2</td>
                <td>
                    Supported exchange
                </td>
                <td>
                    The decentralised exchange must be on one of the supported exchange types.
                    (Currently Uniswap v2 ABI compatible.)
                </td>
            </tr>

            <tr>
                <td>#3</td>
                <td>
                    Metadata validity
                </td>
                <td>
                    The trading pair tokens must have valid metadata. This includes
                    token names, symbols and decimals.
                </td>
            </tr>

            <tr>
                <td>#4</td>
                <td>
                    Supported quote token
                </td>
                <td>
                    Each trading pair consists of base token and quote token. In decentralised exchange, the token order does not matter.
                    However, for the sake of better data research, trading strategy converts all trading pairs to the format where
                    we have a well known quote token and the quote token price can be directly translated to a US dollar price.
                    Thus, the quote token will be always be one of the likes of USDC, USDT, ETH, BNB or similar high liquidity
                    pair with direct dollar reference price available.
                </td>
            </tr>


            <tr>
                <td>#5</td>
                <td>
                    Minimum activity threshold
                </td>
                <td>
                    The pair must meet the minimum trading activity threshold.
                    This is a different for each blockchain (Ethereum, Binance Smart Chain).
                    The threshold may include numbers like the minimum number of swaps,
                    and the minimum liquity.
                </td>
            </tr>

        </tbody>
    </table>


Token filtering
---------------

Like trading pairs, tokens themselves have some eligibility criteria to be included in the datasets.

* **Untracked tokens**: The data exist on a blockchain, but the oracle indexer does not
  read it at all. For example, tokens that are not being traded on any compatible exchange.

* **Tracked tokens**: Tokens appear in any of tracked trading pairs.

* **Active tokens**: Tokens appear in any of active trading pairs. These tokens have a token page created, showing the
  the token market cap, available trading pairs and volume.

Volume calculations
-------------------

Volume can be only calculated for trading pairs with a supported quote token.
If a trading pair has an unsupported quote token, as explained above, any trade on this pair is not included
in the exchange or blockchain trading volume.

This is because there must exist a stable, liquid, path to convert any trading volume to US dollar.