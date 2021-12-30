.. _internal-id:

Internal ids
============

Trading Strategy has several datasets that refer to each other. For example pair dataset refers to exchange dataset via `exchange_id`.

These foreign keys, or primary keys, are not stable. They can across different datasets, when data is being healed or reimported.
Always access the data using symbolic notation, called *slugs* or token symbols.

Duplicate token symbols
-----------------------

Exchanges may have duplicate trading pairs with the same token symbol. The duplicates are usually scam tokens.
Trading Strategy access methods try automatically picked the real trading pair amount multiple entries by its trading volume.
There is no curated list availble, so be careful when accessing tokens and trading pairs by their symbol.

More information
----------------

- See :py:class:`tradingstrategy.types.PrimaryKey` type alias.

- See :py:meth:`tradingstrategy.exchange.ExchangeUniverse.get_by_chain_and_slug` method

- See :py:meth:`tradingstrategy.pair.PairUniverse.get_pair_by_ticker_by_exchange` method