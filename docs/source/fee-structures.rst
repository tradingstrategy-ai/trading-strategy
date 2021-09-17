.. _fee structures:

Fee structures
==============

TradingStrategy.ai offers flexible fee structures that strike an optimal balance between the investor demand and the algorithmic vendor earnings.

.. raw:: html

   <img class="intro" src="_static/trading-strategy-fee-structures.png" alt="Fee structure options for a quantitative trading fund">

Fee collection and distribution
-------------------------------

The trading strategy smart contract collects and distributes fees in real-time.

This happens when an investor enters or exists the strategy, when trading position is closed, or when the management fees have accumulated.

Fee options
-----------

Fees can be combination of several options. Not all fee options need to be used.

* Management fee: % of assets under management, daily or yearly

* Performance fee: % of profits made

* Deposit fee: % of investment amount, on the moment of investing

* Withdrawal fee: % of withdrawn amount, on the moment of withdrawal

Setting the feeds for a trading strategy
----------------------------------------

* The fees are decided and accepted when the strategy is deployed

* The fees can be a mixture of performance, management, deposit and withdrawal fees

* The algorithm vendor proposes the fee structure when they propose a new algorithm for the protocol

* The TradingStrategy accepts the fee structure and deploys the corresponding smart contract

* The fees can be later adjusted with the mutual decision of the algorithm vendor and TradingStrategy governance

Splitting the fees
------------------

The revenue share makes it possible for the protocol to grow in scalable manner. However, we also need to balance out that it is more profitable for the top algorithm vendors to run their strategy on the protocol, instead of running it privately e.g. with a debt money.

* Algorithm vendors will enjoy new income from new investors

* TradingStrategy protocol governance can invest to marketing and attracting new investors

The protocol fees need to be split with the following parties. Each will receive portiion of the fees earned by a strategy.

Example fee structure
---------------------

Below is a fee structure breakdown with the proposed default allocations.

* Algorithm vendor: suggested split 10% - 40% of total depending on the algorithm performance

* Protocol partipants: The remaining 90% - 60%) that is split among

        - Protocol treasury: 75%

        - Referrals: 15%  (includes ones like wallet applications, influencer websites, DeFi exchanges and projects, benchmark and data websites)

        - Oracle server operators: 10%

Claiming earnings from the fees
-------------------------------

The fees are automatically paid to the designed address given by one of the ecosystem parties.

Fees might accumulate on the smart contract and a separate claim transaction might need to be made by the relevant party. This is to save in blockchain gas fees.
