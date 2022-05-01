.. _token-tax:

Token tax and deflationary tokens
=================================

A "token" tax is often used term to describe deflationary tokens.
Deflationary tokens have a transfer fee feature:

- Each time a token is transferred, some of the transferred amount is burned,
  redirected or otherwise taxed.

- Token tax is usually paid by the holder who initiates the transfer. The tax is
  taken from the sent amount during the transfer: initiated transfer amount > received transfer amount.

- Token tax usually reduces the token supply, thus creating deflationary tokens.

- The token tax term is not used for the native gas token on a blockchain,
  like Ether (ETH) on Ethereum where any transfer fee is considered to be a natural part of the core protocol,
  whereas this is not the case for ERC-20 tokens.

- Different "taxes" may apply to different type of of transactions like
  buy, sell and treasury management.

Deflationary rationale
----------------------

Token tax and deflationary tokens attempts to create more sustainable tokenomics,
where some of the fees captured from token transfers are redirected to the protocol revenue
and development.

The most successful deflationary token has been `ZCash <https://www.coindesk.com/tech/2020/11/18/zcash-undergoes-first-halving-as-major-upgrade-drops-founders-reward/>`_
with its "Founder reward":

- 80% of ZCash transaction fee went to miners.

- 20% of ZCash tranaction fee went to the founders, to offset the cost of developing the protocol.

Instead of raising large amount of capital upfront to support the
software development related to ZCash, the development was feed from stable revenue streams
of the protocol.
`The founder reward mechanism was controversial in the cryptocurrency community
<https://crypto.news/zcash-zec-halves-founder-reward/>`_.

Many of deflationary tokens have strong "ponzinomcs", although they are not real ponzis
by the definiton of a ponzi. The tokenomics are designed in a way that it discourages
short term holding or active trading and encourages long term investing.

Issues
------

All native gas tokens are "taxed". E.g. when you transfer ETH on Ethereum mainnet, some of ETH gets burnt in the transaction.
However, the concept of transfer fee has not taken off outside the native gas tokens.
Because accounting the token tax is difficult, it is not realistic to see any one deflationary tokens
to be listed on centralised exchanges any time in the future.

So far, there hasn't been any successfully mainstream token, outside native gas tokens,
that would implement a token tax feature. Most taxed tokens rely on tokenomics on their success and lack
fundamental value creation and innovation.

Token tax based projects often have anonymous teams and weak governance. Because token tax can be updated
by a governance, sometimes rogue dev team flip the token tax to 100% creating so-called honeypot and
causing a project :term:`rug pull`.

Despite issues on taxed tokens, some tokens have had a good lifecycle and build enough
capital through fair launch on taxes that could have made long term development sustainable.
For example, `Elephant Money <https://tradingstrategy.ai/trading-view/binance/pancakeswap-v2/elephant-busd>`_ was doing ok before they had an
`incident with flash loans <https://twitter.com/BlockSecTeam/status/1513966074357698563?ref_src=twsrc%5Etfw%7Ctwcamp%5Etweetembed%7Ctwterm%5E1513966074357698563%7Ctwgr%5E%7Ctwcon%5Es1_&ref_url=https%3A%2F%2Fu.today%2Felephant-money-defi-hacked-are-funds-safu>`_
allowing attackers `to get away with $11M <https://therecord.media/hackers-steal-more-than-11-million-from-elephant-money-defi-platform/>`_.

Honeypots and other risks
-------------------------

Because different token taxes may apply to different transactions based on source and destination addresses,
wacky baiting games can be played, especially on automated traders.

So called honeypots are tokens that are baiting algorithmic trading to buy them.
These tokens have promising looking :term:`OHLCV` data to make it look like an attractive
buy from a :term:`technical analysis` perspective.

However these tokens are often "buy only" with 100% sell tax, so one won't be able to sell these token.

Other kind of honepoys
include tokens with name spam that mimics popular tokens (AAVE, USDC) in hope to have someone accidentally
buying the fake token.

.. note ::

    If you lose money by buying a honeypot token you are not going to get your money back.

Token tax data
--------------

Trading Strategy includes token tax and deflation data as part of its dataset.
Data is collected by trading pair (not by token), because different tax rules can apply
to transactions based on the underlying activity. Different liquidity pools have different addresses
and thus tax can vary trading pair by trading pair, although most of taxed tokens have a flat
tax on all transactions.

- Example of a taxed trading pair

- Example of a non-taxed trading pair

Token tax data is available at

- `Trading Strategy web site <https://tradingstrategy.ai/>`_: See *Token tax* entry for each trading pair

- `Real-time APIs <https://tradingstrategy.ai/api/explorer/>`_: See `PairDetails` structure

- `Backtesting datasets <https://tradingstrategy.ai/trading-view/backtesting>`_:
   See :py:mod:`tradingstrategy.pair` module.

Token tax presentation
----------------------

Trading Strategy represents token tax in the format of:

.. code-block::

    buy tax / transfer tax / sell tax

E.g.

.. code-block::

    5% / 5% / 5%

Trading Strategy attempts to measure in the different life cycles of token trading.

.. warning::

    Measured token tax is not real-time and there is no guarantees that tokens with bad governance
    won't change their tax structure, creating a honey pot and effective rug pull.
    Never trade taxed tokens unless you are willing to lose all of your capital.

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
