"""Value formatting utilities."""

import datetime


def format_price(v: float, decimals=6) -> str:
    """Crypto prices in dollars may have significant decimals up to 6 decimal points.

    :return:
        Price as $ prefixed string.
    """

    if decimals == 0:
        return f"${v:,.0f}"

    format_str = f"${{v:,.{decimals}f}}"
    return format_str.format(v=v)


def format_value(v: float) -> str:
    """Format US dollar trade value, assume value significantly > $1 dollar.

    Two decimals.
    """
    return f"${v:,.2f}"


def format_percent(v: float) -> str:
    return f"{v:.0%}"


def format_percent_2_decimals(v: float) -> str:
    return f"{v:.2%}"


def format_duration_days_hours_mins(d: datetime.timedelta) -> str:
    seconds = d.total_seconds()
    days, remainder = divmod(seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, seconds = divmod(remainder, 60)
    return '{} {} {}    '.format(
            "" if int(days) == 0 else str(int(days)) + ' days',
            "" if int(hours) == 0 else str(int(hours)) + ' hours',
            "" if int(minutes) == 0 else str(int(minutes))  + ' mins'
        )


def string_to_eth_address(input_string) -> str:
    """Convert a string to an Ethereum address deterministically.

    :param input_string: Input string to convert to an Ethereum address.
    :return: Ethereum address.
    """
    from web3 import Web3  # Soft dependency
    hashed_bytes = Web3.keccak(text=input_string)
    eth_address = hashed_bytes[-20:].hex()
    return eth_address
