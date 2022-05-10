.. _token-tax:

Token tax and deflationary tokens
=================================

A "token" tax is often used term to describe tokens with **transfer fees**
that cause deflation or redistribute trade profits to the protocol development:

- Each time a token is transferred, some of the transferred amounts are burned,
  redirected to a development fund or otherwise "taxed".

- Token tax is usually paid by the holder who initiates the transfer. The tax is
  taken from the sent amount during the transfer: initiated transfer amount > received transfer amount.

- Token tax usually reduces the token supply, thus creating deflationary tokens.

- Token tax can redirect some of the transfer and trading fees to the
  development fund and grants of the protocol to guarantee sustainable protocol development.

- The token tax term is not used for the native gas token on a blockchain,
  like Ether (ETH) on Ethereum, where any transfer fee is considered to be a natural part of the core protocol.
  The token tax term applies more to ERC-20 like tokens which historically have lacked
  transfer fee features. There is no terminology standard, so different terms
  are applied in different contexts.

- Different "taxes" may apply to different types of transactions like
  buy, sell, and treasury management.

Transfer fee and deflationary rationale
---------------------------------------

Token tax and deflationary tokens attempt to create more sustainable tokenomics,
where some of the fees captured from token transfers are redirected to the protocol revenue
and development.

The most successful taxed token has been `ZCash <https://www.coindesk.com/tech/2020/11/18/zcash-undergoes-first-halving-as-major-upgrade-drops-founders-reward/>`_
with its "Founder reward":

- 80% of the ZCash transaction fee went to miners.

- 20% of the ZCash transaction fee went to the founders (Electric Coin co.), to offset the cost of developing the protocol.

Instead of raising a large amount of capital upfront to support the
software development related to ZCash, the development was funded from stable revenue streams
of the protocol.
`The founder reward mechanism was controversial in the cryptocurrency community
<https://crypto.news/zcash-zec-halves-founder-reward/>`_.

Many deflationary tokens have strong "ponzinomcs" even though they are not real ponzis
by the definition of a ponzi. The tokenomics are designed to discourage
short-term speculation and to encourage long-term investing.

.. note ::

    Here we use the terms *deflationary* and *inflationary* in a technical protocol context.
    Although Bitcoin is claimed to be deflationary, it is currently inflationary
    and is going to be "non-inflationary" after all 21 million coins have been mined.
    Bitcoin protocol does not burn, redirect or otherwise reduce supply on transfers.
    Bitcoin might be deflationary monetary policy-wise, but it is not deflationary
    accounting-wise.

Issues
------

All native gas tokens on blockchains are "taxed". E.g. when you transfer ETH on Ethereum mainnet, some of the ETH gets burnt in the transaction,
or given to the block producers.

However, the concept of a transfer fee has not taken off outside the native gas tokens.
Because accounting for the token tax is difficult, it is not realistic to see deflationary tokens
listed on centralized exchanges.

So far, there hasn't been any successfully mainstream token, outside native gas tokens,
that would implement a token tax feature. Most taxed tokens rely on tokenomics for their success and lack
fundamental value creation and innovation.

Token tax-based projects often have anonymous teams and weak governance. Usually, the token tax rate can be updated
by the governance. Sometimes rogue dev teams flip the token tax to 100% creating a so-called honeypot and
causing a project :term:`rug pull`.

Despite the issues with taxed tokens, some of them have had a good lifecycle and built enough
capital through a fair launch. Tax on tokens could be one of the keys to long-term sustainable development.
For example, `Elephant Money <https://tradingstrategy.ai/trading-view/binance/pancakeswap-v2/elephant-busd>`_ was doing ok before they had an
`incident with flash loans <https://twitter.com/BlockSecTeam/status/1513966074357698563?ref_src=twsrc%5Etfw%7Ctwcamp%5Etweetembed%7Ctwterm%5E1513966074357698563%7Ctwgr%5E%7Ctwcon%5Es1_&ref_url=https%3A%2F%2Fu.today%2Felephant-money-defi-hacked-are-funds-safu>`_
allowing attackers `to get away with $11M <https://therecord.media/hackers-steal-more-than-11-million-from-elephant-money-defi-platform/>`_.

Honeypots and other "rug pull" risks
------------------------------------

As different token taxes may created freely,
sometimes this feature is exploited to launch hostile tokens to markets.
These hostile tokens are called *honeypots*.

Honeypots are tokens that are baiting unsuspecting users and algorithms to buy them.
These tokens have promising :term:`OHLCV` data to make it look like an attractive
buy from a :term:`technical analysis` perspective, like artificially created
token price pumps. After buying the token, a user cannot sell it for profit.

Honeypots include, but not limited to

- Non-transferable tokens like
  `JustHoldIt <https://tradingstrategy.ai/trading-view/binance/tokens/0x6e97ae491035cf21d4d3975cf794e66cbc4ae211>`_
  - buy transaction is the only whitelisted transfer.

- Tokens with 100% or high (>40%) sell tax,
  making tokens effectively unsellable for profit.

- Other impossible sell condition to meet, like
  one on `JUMPN <https://docs.jumpn.today/tokens/jst/how-to-sell-usdjst-token>`_

.. warning ::

    If you lose money by buying a honeypot token, you will not get your money back.

Token tax data
--------------

Trading Strategy includes token tax and deflation data as a part of its datasets.

Data is collected by a trading pair, not by a token, because different taxes may apply
to transactions based on the underlying activity. 
Most taxed tokens have the same flat tax across all transfers, but this is not always the case.
For example, different liquidity pools have different addresses, 
and thus the coded token tax can differ between these pools. 

Token tax data is available at

- `Trading Strategy website <https://tradingstrategy.ai/>`_: See *Token tax* entry for each trading pair

- `Real-time APIs <https://tradingstrategy.ai/api/explorer/>`_: See `PairDetails` structure

- `Backtesting datasets <https://tradingstrategy.ai/trading-view/backtesting>`_:
   See :py:mod:`tradingstrategy.pair` module.

Transfer fees presentation
--------------------------

Trading Strategy measures token transfer fees in different life cycles of token trading.

Trading Strategy presents transfer fees in the format of:

.. code-block::

    buy tax % / transfer tax % / sell tax %

E.g.

.. code-block::

    5% / 5% / 5%

.. warning::

    Token tax measurements are not real-time. There are no guarantees that tokens with bad governance
    won't change their tax structure, creating a honey pot and effective rug pull.
    Never trade taxed tokens unless you are willing to lose all of your capital.

Token tax examples
~~~~~~~~~~~~~~~~~~

Here are some examples of different token taxes:

- `Example of a taxed trading pair: ELEPHANT-BUSD on PancakeSwap <https://tradingstrategy.ai/trading-view/binance/pancakeswap-v2/elephant-bnb-2>`_ - 10% tax

- `Example of a non-taxed trading pair: BNB-USDT on PancakeSwap <https://tradingstrategy.ai/trading-view/binance/pancakeswap-v2/bnb-usdt>`_ - no fees

- `Example of a token with buy and sell tax, but no transfer tax: DHOLD-ETH on Uniswap <https://tradingstrategy.ai/trading-view/ethereum/uniswap-v2/dhold-eth>`_ - taxed 10%/0%/10%

- `Example of a token with sell tax only: LBLOCK-BNB <https://tradingstrategy.ai/trading-view/binance/pancakeswap-v2/lblock-bnb>`_

- `Example of a honeypot trading pair: JST-BNB <https://tradingstrategy.ai/trading-view/binance/pancakeswap-v2/jst-bnb-2>`_ - In practice, one cannot sell Jump Satoshi token and it can be considered as a honeypot. Even if the fact that it is in practice unsellable is disclosed in the whitepaper, the token smart contract source code is obfuscated. The BSCScan comment section is filled wiht angry users.

- `Example of too low liquidity trading pair: Omega Protocol Money-ETH on Uniswap <https://tradingstrategy.ai/trading-view/ethereum/uniswap-v2/opm-eth-2>`_ - cannot measure tax because there is not enough liquidity to trade


Real-time API example
~~~~~~~~~~~~~~~~~~~~~

Here is an example to get a token tax for popular Sushiswap v2 trading pair on Ethereum mainnet:

.. code-block:: shell

    curl -X GET "https://tradingstrategy.ai/api/pair-details?exchange_slug=sushiswap&chain_slug=ethereum&pair_slug=ETH-USDC" -H  "accept: application/json"

.. code-block:: json

    {
      "additional_details": {
        "chain_name": "Ethereum",
        "chain_link": "https://ethereum.org",
        "chain_logo": "https://upload.wikimedia.org/wikipedia/commons/0/05/Ethereum_logo_2014.svg",
        "exchange_name": "Sushi",
        "pair_contract_address": "0x397ff1542f962076d0bfe58ea045ffa2d347aca0",
        "first_trade_at": "2020-09-09T21:31:51",
        "last_trade_at": "2022-05-01T17:35:02",
        "trade_link": "https://app.sushi.com/swap?inputCurrency=0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48&outputCurrency=ETH",
        "buy_link": "https://app.sushi.com/swap?inputCurrency=0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48&outputCurrency=ETH",
        "sell_link": "https://app.sushi.com/swap?inputCurrency=ETH&outputCurrency=0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
        "explorer_link": "https://etherscan.io/address/0x397ff1542f962076d0bfe58ea045ffa2d347aca0",
        "pair_explorer_link": "https://etherscan.io/address/0x397ff1542f962076d0bfe58ea045ffa2d347aca0",
        "base_token_explorer_link": "https://etherscan.io/address/0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
        "quote_token_explorer_link": "https://etherscan.io/address/0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
        "buy_tax": 0,
        "transfer_tax": 0,
        "sell_tax": 0
      }
    }

Token tax error codes
~~~~~~~~~~~~~~~~~~~~~

For machine readable data:

- The tax percent is presented as 0.0...1 (100%) floating point.

- Values > 1 are error codes meaning the token tax measurement has failed
  and token is most likely out of liquidity, broken or a honeypot.

- Missing data or null values indicate the has not been measured yet.

- The final list of error codes is To Be Done.

Development of deflationary tokens
----------------------------------

ERC-20 tokens do not have a clean interface to describe deflationary behavior.
Thus, a manual off-chain database about taxes on tokens needs to be maintained.

A token tax is usually implemented as a complicated ERC-20 `_transfer()` function
that checks for various whitelisted addresses and then constructs `fee`
for the transfer based on a logic.

Example of a Solidity code for a token with transfer tax:

.. code-block::


    function _transfer(
        address from,
        address to,
        uint256 amount
    ) private {
        require(from != address(0), "ERC20: transfer from the zero address");
        require(to != address(0), "ERC20: transfer to the zero address");
        require(amount > 0, "Transfer amount must be greater than zero");

        // is the token balance of this contract address over the min number of
        // tokens that we need to initiate a swap + liquidity lock?
        // also, don't get caught in a circular liquidity event.
        // also, don't swap & liquify if sender is uniswap pair.
        uint256 contractTokenBalance = balanceOf(address(this));


        bool overMinTokenBalance = contractTokenBalance >= numTokensSellToAddToLiquidity;
        if (
            overMinTokenBalance &&
            !inSwapAndLiquify &&
            from != uniswapV2Pair &&
            swapAndLiquifyEnabled
        ) {
            contractTokenBalance = numTokensSellToAddToLiquidity;
            //add liquidity
            swapAndLiquify(contractTokenBalance);
        }

        //indicates if fee should be deducted from transfer
        bool takeFee = true;

        //if any account belongs to _isExcludedFromFee account then remove the fee
        if(_isExcludedFromFee[from] || _isExcludedFromFee[to]){
            takeFee = false;
        }

        //transfer amount, it will take tax, burn, liquidity fee
        _tokenTransfer(from,to,amount,takeFee);
    }

    //this method is responsible for taking all fee, if takeFee is true
    function _tokenTransfer(address sender, address recipient, uint256 amount,bool takeFee) private {
        if(!takeFee)
            removeAllFee();

        if (_isExcluded[sender] && !_isExcluded[recipient]) {
            _transferFromExcluded(sender, recipient, amount);
        } else if (!_isExcluded[sender] && _isExcluded[recipient]) {
            _transferToExcluded(sender, recipient, amount);
        } else if (!_isExcluded[sender] && !_isExcluded[recipient]) {
            _transferStandard(sender, recipient, amount);
        } else if (_isExcluded[sender] && _isExcluded[recipient]) {
            _transferBothExcluded(sender, recipient, amount);
        } else {
            _transferStandard(sender, recipient, amount);
        }

        if(!takeFee)
            restoreAllFee();
    }

    function _transferStandard(address sender, address recipient, uint256 tAmount) private {
        (uint256 rAmount, uint256 rTransferAmount, uint256 rFee, uint256 tTransferAmount, uint256 tFee, uint256 tLiquidity) = _getValues(tAmount);
        _rOwned[sender] = _rOwned[sender].sub(rAmount);
        _rOwned[recipient] = _rOwned[recipient].add(rTransferAmount);
        _takeLiquidity(tLiquidity);
        _reflectFee(rFee, tFee);
        emit Transfer(sender, recipient, tTransferAmount);
    }
