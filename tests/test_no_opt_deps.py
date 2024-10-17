def test_no_opt_deps():
    """Test for the installation without optional dependenceis."""
    # See we can import important modules
    # Before we had:
    # ModuleNotFound: eth_defi when importing lending
    import tradingstrategy.utils.token_filter
    import tradingstrategy.lending
    import tradingstrategy.stablecoin
    import tradingstrategy.utils.wrangle
    import tradingstrategy.pair
    import tradingstrategy.candle
    import tradingstrategy.liquidity
    import tradingstrategy.clmm
    import tradingstrategy.exchange
