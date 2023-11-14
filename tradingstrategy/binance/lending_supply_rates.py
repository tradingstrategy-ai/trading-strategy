import datetime
import requests
import pandas as pd

from tradingstrategy.timebucket import TimeBucket
from tradingstrategy.utils.time import generate_monthly_timestamps
from tradingstrategy.utils.groupeduniverse import resample_candles

def convert_binance_lending_rates_to_supply(interestRates: pd.Series) -> pd.Series:
    """Convert Binance lending rates to supply rates."""
    assert isinstance(interestRates, pd.Series), f"Expected pandas Series, got {interestRates.__class__}: {interestRates}"
    assert isinstance(interestRates.index, pd.DatetimeIndex), f"Expected DateTimeIndex, got {interestRates.index.__class__}: {interestRates.index}"
    return interestRates * 0.95


def get_binance_lending_interest_rates(
    asset_symbol:str,
    start_date:datetime.datetime,
    end_date:datetime.datetime,
    time_bucket: TimeBucket,
) -> pd.DataFrame:
    """Get daily lending interest rates for a given asset from Binance, resampled to the given time bucket.

    :param asset_symbol:

        List of all valid asset symbols as at 2023-11-06 18:00 UTC:

        asset_names = [
            "BTC", "ETH", "XRP", "BNB", "TRX", "USDT", "LINK", "EOS", "ADA", "ONT",
            "USDC", "ETC", "LTC", "XLM", "XMR", "NEO", "ATOM", "DASH", "ZEC", "MATIC",
            "BUSD", "BAT", "IOST", "VET", "QTUM", "IOTA", "XTZ", "BCH", "RVN", "ZIL",
            "ONE", "ANKR", "CELR", "TFUEL", "IOTX", "HBAR", "FTM", "SXP", "BNT", "DOT",
            "REN", "ALGO", "ZRX", "THETA", "COMP", "KNC", "OMG", "KAVA", "BAND", "DOGE",
            "RLC", "WAVES", "MKR", "SNX", "YFI", "CRV", "SUSHI", "UNI", "MANA", "STORJ",
            "UMA", "JST", "AVAX", "NEAR", "FIL", "TRB", "RSR", "TOMO", "OCEAN", "AAVE",
            "SAND", "CHZ", "ARPA", "COTI", "FET", "TROY", "CHR", "ORN", "NMR", "GRT",
            "STPT", "LRC", "KSM", "ROSE", "REEF", "STMX", "ALPHA", "STX", "ENJ", "RUNE",
            "SKL", "INJ", "OXT", "CTSI", "OGN", "EGLD", "1INCH", "DODO", "LIT", "NKN",
            "MDT", "CKB", "CAKE", "SOL", "XEM", "LINA", "GLM", "XVS", "MDX", "SUPER",
            "GTC", "PUNDIX", "AUDIO", "BOND", "SLP", "TRU", "POND", "ERN", "ATA", "NULS",
            "DENT", "TVK", "DF", "FLOW", "AR", "DYDX", "MASK", "UNFI", "AXS", "LUNA",
            "SHIB", "ENS", "BAKE", "ALICE", "TLM", "ICP", "C98", "GALA", "ONG", "HIVE",
            "DAR", "IDEX", "ANT", "CLV", "WAXP", "BNX", "KLAY", "MINA", "XEC", "RNDR",
            "JASMY", "QUICK", "LPT", "AGLD", "BICO", "CTXC", "DUSK", "HOT", "SFP", "YGG",
            "FLUX", "ICX", "CELO", "BETA", "BLZ", "MTL", "PEOPLE", "QNT", "PYR", "KEY",
            "PAXG", "FRONT", "TWT", "RAD", "QI", "GMT", "APE", "BSW", "KDA", "MBL", "ASTR",
            "API3", "CTK", "WOO", "GAL", "OP", "REI", "LEVER", "LDO", "FIDA", "FLM", "BURGER",
            "AUCTION", "IMX", "SPELL", "STG", "BEL", "WING", "AVA", "LOKA", "LUNC", "PHB",
            "LOOM", "AMB", "SANTOS", "VIB", "EPX", "HARD", "USTC", "DEGO", "HIGH", "GMX",
            "LAZIO", "PORTO", "ACH", "STRAX", "KP3R", "REQ", "POLYX", "APT", "PHA", "OSMO",
            "GLMR", "MAGIC", "HOOK", "AGIX", "HFT", "CFX", "ZEN", "SSV", "LQTY", "ALCX",
            "FXS", "PERP", "TUSD", "USDP", "GNS", "JOE", "RIF", "SYN", "ID", "ARB", "OAX",
            "RDNT", "EDU", "SUI", "FLOKI", "PEPE", "COMBO", "MAV", "XVG", "PENDLE", "ARKM",
            "WLD", "T", "FDUSD", "RPL", "SEI", "CYBER", "VTHO", "WBETH", "NTRN", "HIFI",
            "CVX", "ARK", "ARDR", "ACA", "VIDT", "GHST", "GAS", "OOKI", "TIA", "POWR",
            "AERGO", "SNT", "STEEM", "MEME", "PLA", "MULTI", "UFT", "ILV"
        ]

        To see current list of all valid asset symbols, submit API request https://api1.binance.com/sapi/v1/margin/allAssets with your Binance API key. 

    :param start_date:
        Start date for the data. Note this value cannot be eariler than datetime.datetime(2019,4,1) due to Binance data limitations

    """
    assert type(asset_symbol) == str, "asset_symbol must be a string"
    assert type(start_date) == datetime.datetime, "start_date must be a datetime.datetime object"
    assert type(end_date) == datetime.datetime, "end_date must be a datetime.datetime object"
    assert type(time_bucket) == TimeBucket, "time_delta must be a pandas Timedelta object"
    # assert start_date >= datetime.datetime(2019,4,1), "start_date cannot be earlier than 2019-04-01 due to Binance data limitations"

    monthly_timestamps = generate_monthly_timestamps(start_date, end_date)
    response_data = []

    # API calls to get the data
    for i in range(len(monthly_timestamps) - 1):
        start_timestamp = monthly_timestamps[i] * 1000
        end_timestamp = monthly_timestamps[i + 1] * 1000
        url = f"https://www.binance.com/bapi/margin/v1/public/margin/vip/spec/history-interest-rate?asset={asset_symbol}&vipLevel=0&size=90&startTime={start_timestamp}&endTime={end_timestamp}"
        response = requests.get(url)
        if response.status_code == 200:
            json_data = response.json()
            data = json_data["data"]
            if len(data) > 0:
                response_data.extend(data)
        else:
            print(f"Error fetching data for {asset_symbol} between {start_timestamp} and {end_timestamp}")
            print(f"Response: {response.status_code} {response.text}")

    dates = []
    interest_rates = []
    for data in response_data:
        dates.append(pd.to_datetime(data["timestamp"], unit="ms"))
        interest_rates.append(float(data["dailyInterestRate"]))
    
    # TODO: ensure index has no missing dates i.e. evenly spaced intervals throughout the period

    unsampled_rates = pd.Series(data=interest_rates, index=dates).sort_index()

    return resample_candles(unsampled_rates, time_bucket.to_pandas_timedelta(), forward_fill=True)