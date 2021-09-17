"""Schema manipulation utitilies.

For more information about Pyarrow Schemas, see https://arrow.apache.org/docs/python/api/datatypes.html
"""

import typing
from enum import Enum
from typing import Optional, Dict, Callable, List
from dataclasses import fields, Field

import pyarrow as pa

from tradingstrategy.chain import ChainId
from tradingstrategy.types import PrimaryKey, NonChecksummedAddress, BlockNumber, UNIXTimestamp, BasisPoint


class CannotMap(Exception):
    pass


def unmappable(t):
    raise CannotMap(f"Cannot automatically map {t}")


#: Default mappings for automatic schema generation,
#: Including our own type definitions,
DEFAULT_MAPPINGS = {
    PrimaryKey: lambda t: pa.uint32(),
    ChainId: lambda t: pa.uint16(),
    NonChecksummedAddress: lambda t: pa.string(),
    BlockNumber: lambda t: pa.uint32(),
    UNIXTimestamp: lambda t: pa.timestamp("s"),
    BasisPoint: lambda t: pa.uint32(),
    bool: lambda t: pa.bool_(),
    float: lambda t: pa.float32(),
    int: lambda t: pa.uint32(),
    str: lambda t: pa.string(),
    dict: lambda t: unmappable(t),
    list: lambda t: unmappable(t),
}


def map_field_to_arrpw(field: Field, hints: Dict[str, pa.DataType], core_mappings: Dict[str, Callable]) -> pa.DataType:
    """Map a dataclass field to a pyarrow equivalent, respect hints"""
    hinted = hints.get(field.name)
    if hinted:
        return hinted

    # Resolve optional
    origin = typing.get_origin(field.type)
    if origin == typing.Union:
        # Optional type
        args = typing.get_args(field.type)
        assert len(args) == 2
        assert args[1] == type(None)
        field_type = args[0]
        true_origin = typing.get_origin(args[0])
    else:
        field_type = field.type
        true_origin = origin

    if true_origin == list:
        args = typing.get_args(field_type)
        value_type = args[0]
        mapped_value_type = core_mappings[value_type](field)
        return pa.list_(mapped_value_type)
    elif true_origin == dict:
        # Only string string dicts supported
        return pa.map_(pa.string(), pa.string())
    else:

        if issubclass(field_type, Enum):
            # No support for category compaction yet
            field_type = str

        return core_mappings[field_type](field)


def create_pyarrow_schema_for_dataclass(
        cls,
        hints: Optional[typing.Dict[str, pa.DataType]] = None,
        core_mappings=DEFAULT_MAPPINGS) -> pa.Schema:
    """Map a Python dataclass to Pyarrow schema.

    Most fields map automatically, but you can also provide per field name hints what types they should use.
    """

    if not hints:
        hints = {}

    pa_fields = [(field.name, map_field_to_arrpw(field, hints, core_mappings)) for field in fields(cls)]
    return pa.schema(pa_fields)


def create_columnar_work_buffer(cls) -> Dict[str, list]:
    """Create a columnar work buffer to export data into Pyarrow Tables."""
    return {field.name: [] for field in fields(cls)}


def append_to_columnar_work_buffer(buffer: Dict[str, list], item):
    """Convert tabular data items to columnar.

    Automatically handle the special case of enum.
    """

    def process_value(key: str):
        try:
            v = getattr(item, key)
            if isinstance(v, Enum):
                v = v.value
            else:
                v = v
            buffer[key].append(v)
        except (AttributeError, ValueError) as e:
            raise RuntimeError(f"Could not serialised {key} for {item}") from e

    for key in buffer:
        process_value(key)