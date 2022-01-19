"""Summary table dataframe helpers.

You can annotate the format of different values.
"""

import enum
from dataclasses import dataclass

import pandas as pd


class Format(enum.Enum):
    integer = "int"
    percent = "percent"
    dollar = "dollar"

    #: Value cannot be calculated, e.g division by zero
    missing = "missing"


FORMATTERS = {
    Format.integer: "{v:.0f}",
    Format.percent: "{v:.0%}",
    Format.dollar: "${v:,.2f}",
    Format.missing: "-",
}


@dataclass
class Value:
    v: object
    format: Format


def as_dollar(v) -> Value:
    """Format value as US dollars"""
    return Value(v, Format.dollar)


def as_integer(v)-> Value:
    """Format value as an integer"""
    return Value(v, Format.integer)


def as_percent(v) -> Value:
    """Format value as a percent"""
    return Value(v, Format.percent)


def as_missing() -> Value:
    """Format a missing value e.g. because of division by zero"""
    return Value(None, Format.missing)


def format_value(v_instance: Value) -> str:
    assert isinstance(v_instance, Value), f"Expected Value instance, got {v_instance}"
    formatter = FORMATTERS[v_instance.format]
    if v_instance.v is not None:
        return formatter.format(v=float(v_instance.v))
    else:
        # missing values
        return formatter.format(v=v_instance.v)


def create_summary_table(data: dict) -> pd.DataFrame:
    """Create a summary table from a human readable data.

    * Keys are human readable labels
    * Values are instances of :py:class:`Value`
    """
    formatted_data = {k: format_value(v) for k, v in data.items()}
    df = pd.DataFrame.from_dict(formatted_data, orient="index")
    df.style.hide_index()
    df.style.hide_columns()
    df.style.set_table_styles([
        {'selector': 'thead', 'props': [('display', 'none')]}
    ])
    return df
