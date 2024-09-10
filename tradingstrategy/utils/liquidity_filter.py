"""Filtering Uniswap trading pairs for liquidity.

- Used to build a trading pair universe with tradeable pairs of enough liquidity, no survivorship bias

- See :py:func:`build_liquidity_summary` for usage

"""
from collections import Counter
from typing import Collection, Iterable, Tuple

import pandas as pd
from pandas.core.groupby import DataFrameGroupBy

from tradingstrategy.pair import PandasPairUniverse
from tradingstrategy.types import USDollarAmount, PrimaryKey
from tradingstrategy.utils.time import floor_pandas_week


def get_somewhat_realistic_max_liquidity(
    liquidity_df,
    pair_id,
    samples=10,
    broken_liquidity=100_000_000,
) -> float:
    """Get the max liquidity of a trading pair over its history.

    - Get the token by its maximum ever liquidity, so we avoid survivorship bias

    - Instead of picking the absolute top, we pick n top samples
      and choose lowest of those

    - This allows us to avoid data sampling issues when the liquidity value,
      as calculated with the function of price, might have been weird when the token launched

    :param broken_liquidity:
        Cannot have more than 100M USD

    """
    try:
        liquidity_samples = liquidity_df.obj.loc[pair_id]["close"].nlargest(samples)
        sample = min(liquidity_samples)
        if sample > broken_liquidity:
            # Filter out bad data
            return -1
        return sample
    except KeyError:
        # Pair not available, because liquidity data is not there, or zero, or broken
        return -1


def get_liquidity_today(
    liquidity_df,
    pair_id,
    delay=pd.Timedelta(days=30)
) -> float:
    """Get the current liquidity of a trading pair

    :param delay:
        Look back X days.

        To avoid indexer delays.

    :return:
        US dollars
    """

    try:
        timestamp = floor_pandas_week(pd.Timestamp.now() - delay)
        sample = liquidity_df.obj.loc[pair_id]["close"][timestamp]
        return sample
    except KeyError:
        # Pair not available, because liquidity data is not there, or zero, or broken
        return -1


def build_liquidity_summary(
    liquidity_df: pd.DataFrame | DataFrameGroupBy,
    pair_ids: Collection[PrimaryKey] | pd.Series,
    delay=pd.Timedelta(days=21)
) -> tuple[Counter[PrimaryKey, USDollarAmount], Counter[PrimaryKey, USDollarAmount]]:
    """Build a liquidity status of the trading pairs

    - Get the historical max liquidity of a pair, so we can use this for filtering without survivorship bias

    - Get the most recent liquidity (w/delay of few days)

    Example:

    .. code-block:: python

        chain_id = ChainId.ethereum
        time_bucket = TimeBucket.d1  # OHCLV data frequency
        liquidity_time_bucket = TimeBucket.d1  # TVL data for Uniswap v3 is only sampled daily, more fine granular is not needed
        exchange_slugs = {"uniswap-v3", "uniswap-v2", "sushi"}
        exported_top_pair_count = 100
        liquidity_comparison_date = floor_pandas_week(pd.Timestamp.now() - pd.Timedelta(days=7))  # What date we use to select top 100 liquid pairs
        tokensniffer_threshold = 24  # We want our TokenSniffer score to be higher than this for base tokens
        min_liquidity_threshold = 4_000_000  # Prefilter pairs with this liquidity before calling token sniffer
        allowed_pairs_for_token_sniffer = 150  # How many pairs we let to go through TokenSniffer filtering process (even if still above min_liquidity_threshold)

        #
        # Set up output files - use Trading Strategy client's cache folder
        #
        client = Client.create_jupyter_client()
        cache_path = client.transport.cache_path

        #
        # Download - process - save
        #

        print("Downloading/opening exchange dataset")
        exchange_universe = client.fetch_exchange_universe()

        # Resolve uniswap-v3 internal id
        exchanges = [exchange_universe.get_by_chain_and_slug(chain_id, exchange_slug) for exchange_slug in exchange_slugs]
        exchange_ids = [exchange.exchange_id for exchange in exchanges]
        print(f"Exchange {exchange_slugs} ids are {exchange_ids}")

        # We need pair metadata to know which pairs belong to Polygon
        print("Downloading/opening pairs dataset")
        pairs_df = client.fetch_pair_universe().to_pandas()

        our_chain_pairs = filter_pairs_default(
            pairs_df,
            chain_id=chain_id,
            exchange_ids=exchange_ids,
        )
        our_chain_pair_ids = our_chain_pairs["pair_id"]

        print(f"We have data for {len(our_chain_pair_ids)} trading pairs on {fname} set")
        print("Building pair metadata map")
        pairs_df = pairs_df.set_index("pair_id")
        pair_metadata = {pair_id: row for pair_id, row in pairs_df.iterrows()}
        uni_v3_pair_metadata = {pair_id: row for pair_id, row in pairs_df.iterrows() if row["exchange_slug"] == "uniswap-v3"}
        print(f"From this, Uniswap v3 data has {len(uni_v3_pair_metadata)} pairs")

        # Download all liquidity data, extract
        # trading pairs that exceed our prefiltering threshold
        print(f"Downloading/opening TVL/liquidity dataset {liquidity_time_bucket}")
        liquidity_df = client.fetch_all_liquidity_samples(liquidity_time_bucket).to_pandas()
        print(f"Setting up per-pair liquidity filtering, raw liquidity data os {len(liquidity_df)} entries")
        liquidity_df = liquidity_df.loc[liquidity_df.pair_id.isin(our_chain_pair_ids)]
        liquidity_df = liquidity_df.set_index("timestamp").groupby("pair_id")
        print(f"Forward-filling liquidity, before forward-fill the size is {len(liquidity_df)} samples, target frequency is {liquidity_time_bucket.to_frequency()}")
        liquidity_df = forward_fill(liquidity_df, liquidity_time_bucket.to_frequency(), columns=("close",))  # Only daily close liq needed for analysis, don't bother resample other cols

        # Get top liquidity for all of our pairs
        print(f"Filtering out historical liquidity of pairs")
        pair_liquidity_max_historical, pair_liquidity_today = build_liquidity_summary(liquidity_df, our_chain_pair_ids)
        print(f"Chain {chain_id.name} has liquidity data for {len(pair_liquidity_max_historical)} pairs at {liquidity_comparison_date}")

        # Check how many pairs did not have good values for liquidity
        broken_pairs = {pair_id for pair_id, liquidity in pair_liquidity_max_historical.items() if liquidity < 0}
        print(f"Liquidity data is broken for {len(broken_pairs)} trading pairs")

        uniswap_v3_liquidity_pairs = {pair_id for pair_id in pair_liquidity_max_historical.keys() if pair_id in uni_v3_pair_metadata}
        print(f"From this, Uniswap v3 is {len(uniswap_v3_liquidity_pairs)} pairs")
        assert len(uniswap_v3_liquidity_pairs) > 0, "No Uniswap v3 liquidity detected"

        # Remove duplicate pairs
        print("Prefiltering and removing duplicate pairs")
        top_liquid_pairs_filtered = Counter()
        for pair_id, liquidity in pair_liquidity_max_historical.most_common():
            ticker = make_simple_ticker(pair_metadata[pair_id])
            if liquidity < min_liquidity_threshold:
                # Prefilter pairs
                continue
            if ticker in top_liquid_pairs_filtered:
                # This pair is already in the dataset under a different pool
                # with more liquidity
                continue
            top_liquid_pairs_filtered[pair_id] = liquidity

        print(f"After prefilter, we have {len(top_liquid_pairs_filtered):,} pairs left")

    :param liquidity_df:
        Liquidity data. **MUST BE forward filled for no gaps and timestamp indexed.**

        Must be daily/weekly timeframe to include TVL data and match our lookup functions.

    :param pair_ids:
        Pairs we are interested in

    :param delay:
        The time lag to check the "current" today's liquidity.

        Ensure the data is indexed by the time we run this code.

    :return:
        Two counters of historical max liquidity, liquidity today.

        All in USD.

        Pair liquidity value is set to `-1` if the lookup failed (data not available, data contains inrealistic values, etc.)
    """

    assert isinstance(liquidity_df, (pd.DataFrame, DataFrameGroupBy))
    assert isinstance(pair_ids, (pd.Series, list, set, tuple))
    assert len(pair_ids) > 0

    if not isinstance(liquidity_df, DataFrameGroupBy):
        # TODO: This is unlikely to work but let's try be helpful anyway
        liquidity_df = liquidity_df.set_index("timestamp").groupby("pair_id")

    # Get top liquidity for all of our pairs
    pair_liquidity_max_historical = Counter()
    pair_liquidity_today = Counter()
    for pair_id in pair_ids:
        pair_liquidity_max_historical[pair_id] = get_somewhat_realistic_max_liquidity(liquidity_df, pair_id)
        pair_liquidity_today[pair_id] = get_liquidity_today(liquidity_df, pair_id, delay=delay)
    return pair_liquidity_max_historical, pair_liquidity_today


def get_top_liquidity_pairs_by_base_token(
    pair_universe: PandasPairUniverse,
    pair_liquidity_map: Counter,
    good_base_tokens: list[str],
    count: int,
) -> Iterable[Tuple[PrimaryKey, USDollarAmount]]:
    """Get the top liquidity for pairs.

    :param good_base_token:
        Process pairs in the order of this list, one base token add a time.
    """

    # Then get all pairs with this base token
    result_set = []

    assert len(good_base_tokens) > 0
    assert good_base_tokens[0].startswith("0x")

    #
    for base_token_address in good_base_tokens:
        for pair_id,  liquidity in pair_liquidity_map.items():
            pair_metadata = pair_universe.get_pair_by_id(pair_id)
            if pair_metadata.base_token_address == base_token_address:
                result_set.append((pair_id, liquidity))

        if len(result_set) >= count:
            break

    return result_set
