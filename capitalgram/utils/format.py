

def format_price(v: float) -> str:
    """Crypto prices in dollars may have significant decimals up to 6 decimal points"""
    return f"${v:,.6f}"


def format_value(v: float) -> str:
    """Format US dollar trade value, assume value significantly > $1 dollar.

    Two decimals.
    """
    return f"${v:,.2f}"


def format_percent(v: float) -> str:
    return f"{v:.0%}"