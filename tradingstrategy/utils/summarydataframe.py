"""Summary table dataframe helpers.

You can annotate the format of different values.
"""
import datetime
import enum
from dataclasses import dataclass

import pandas as pd


class Format(enum.Enum):
    """Format different summary value cells."""
    integer = "int"
    percent = "percent"
    dollar = "dollar"
    duration_days_hours = "duration_days_hours"
    duration_hours_minutes = "duration_hours_minutes"
    num_bars = "num_bars"

    #: Value cannot be calculated, e.g division by zero
    missing = "missing"



FORMATTERS = {
    Format.integer: "{v:.0f}",
    Format.percent: "{v:.2%}",
    Format.dollar: "${v:,.2f}",
    Format.duration_days_hours: "{days} days {hours} hours",
    Format.duration_hours_minutes: "{hours} hours {minutes} minutes",
    Format.num_bars: "{v:.0f} bars",
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


def as_duration(v: datetime.timedelta) -> Value:
    """Format value as a duration"""
    if v.days > 0:
        return Value(v, Format.duration_days_hours)
    else:
        return Value(v, Format.duration_hours_minutes)

def as_bars(v: float) -> Value:
    """Format value as number of bars"""
    return Value(v, Format.num_bars)

def as_missing() -> Value:
    """Format a missing value e.g. because of division by zero"""
    return Value(None, Format.missing)

def format_value(v_instance: Value) -> str:
    """Format a single value
    
    :param v_instance: A :py:class:`Value` instance
    
    :return: A formatted string
    """
    assert isinstance(v_instance, Value), f"Expected Value instance, got {v_instance}"
    formatter = FORMATTERS[v_instance.format]
    if v_instance.v is not None:
        # TODO: Remove the hack
        if isinstance(v_instance.v, datetime.timedelta):
            return formatter.format(days=v_instance.v.days, hours=v_instance.v.seconds // 3600, minutes=(v_instance.v.seconds // 60) % 60)
        else:
            return formatter.format(v=float(v_instance.v))
    else:
        # missing values
        return FORMATTERS[Format.missing].format(v=v_instance.v)
    
def format_values(values: list[Value]) -> list[str]:
    """Format a list of values
    
    :param values: A list of :py:class:`Value` instances

    :return: A list of formatted strings
    """
    return [format_value(v) for v in values]


def create_summary_table(data: dict, column_names: list[str] | str | None = None, index_name: str | None = None) -> pd.DataFrame:
    """Create a summary table from a human readable data.

    * Keys are human readable labels

    * Values are instances of :py:class:`Value`

    TODO: If column_names is not provided, we get column header "zero" that needs to be hidden.

    :param data: Human readable data in the form of a dict

    :param column_names: Column names for the dataframe. If None, no column names are used.

    :param index_name: Name of the index column. If None, no index name is used.

    :return: A styled pandas dataframe
    """

    formatted_data = {}
    counter = 0
    list_length = 0
    for k, v in data.items():
        if isinstance(v, Value):
            formatted_data[k] = format_value(v)
        elif isinstance(v, list):
            if counter == 0:
                list_length = len(v)
            else:
                assert len(v) == list_length, f"If one value in the dict is a list, all values must be lists of the same length. Expected list of length {list_length}, got {v}"
            
            formatted_data[k] = format_values(v)

        counter += 1

    df = pd.DataFrame.from_dict(formatted_data, orient="index")
    if column_names is not None:
        if isinstance(column_names, str):
            column_names = [column_names]
        df.columns = column_names
    if index_name is not None:
        df.index.name = index_name

    # https://pandas.pydata.org/docs/dev/reference/api/pandas.io.formats.style.Styler.hide.html
    df.style.hide(axis="index", names=True)
    df.style.hide(axis="columns", names=False)
    # df.style.hide_columns()
    df.style.set_table_styles([
        {'selector': 'thead', 'props': [('display', 'none')]}
    ])
    return df
