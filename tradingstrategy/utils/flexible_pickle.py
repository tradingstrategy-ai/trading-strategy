"""Flexible pickle implementation.

- Don't crash on missing enum values
- Log info about broken/missing values for developer awareness
"""

import enum
import io
import logging
import pickle
from typing import BinaryIO

logger = logging.getLogger(__name__)


class BrokenEnumValue:
    """Sentinel marker for enum values that could not be deserialised.

    - Used when an enum value in pickled data does not exist in current code
    - Allows graceful degradation instead of crashing
    - Hashable so it can be stored in sets alongside real enum values
    """

    def __init__(self, enum_class_name: str, value: str):
        self.enum_class_name = enum_class_name
        self.value = value

    def __repr__(self):
        return f"<BrokenEnumValue {self.enum_class_name}.{self.value}>"

    def __hash__(self):
        return hash((self.enum_class_name, self.value))

    def __eq__(self, other):
        if isinstance(other, BrokenEnumValue):
            return self.enum_class_name == other.enum_class_name and self.value == other.value
        return False


def _create_flexible_enum_loader(enum_class: type[enum.Enum]):
    """Create a flexible enum loader that handles missing values.

    :param enum_class:
        The enum class to wrap

    :return:
        A callable that can be used in place of the enum class during unpickling
    """

    class FlexibleEnumLoader:
        """Proxy class that wraps enum construction with error handling."""

        def __call__(self, value):
            try:
                return enum_class(value)
            except ValueError:
                logger.info(
                    "Missing enum value during unpickling: %s.%s - "
                    "this value may have been removed from the codebase",
                    enum_class.__name__,
                    value,
                )
                return BrokenEnumValue(enum_class.__name__, value)

    return FlexibleEnumLoader()


class FlexibleUnpickler(pickle.Unpickler):
    """Custom unpickler that handles missing enum values gracefully.

    - Intercepts enum class lookups during unpickling
    - Wraps enum reconstruction with error handling
    - Logs info about missing/broken values

    Example usage::

        from tradingstrategy.utils.flexible_pickle import flexible_load

        with open("vault-db.pickle", "rb") as f:
            data = flexible_load(f)
    """

    def find_class(self, module: str, name: str):
        """Override to intercept enum class lookups.

        When loading an enum class, wrap it with flexible error handling.
        """
        cls = super().find_class(module, name)

        # Check if this is an enum class
        if isinstance(cls, type) and issubclass(cls, enum.Enum):
            return _create_flexible_enum_loader(cls)

        return cls


def flexible_load(file: BinaryIO) -> object:
    """Load a pickle file with flexible enum handling.

    - Does not crash on missing enum values
    - Logs info about broken/missing values

    :param file:
        Binary file to read from

    :return:
        Unpickled object

    Example::

        with open("vault-db.pickle", "rb") as f:
            data = flexible_load(f)
    """
    return FlexibleUnpickler(file).load()


def flexible_loads(data: bytes) -> object:
    """Load pickle data from bytes with flexible enum handling.

    :param data:
        Pickle data as bytes

    :return:
        Unpickled object
    """
    return flexible_load(io.BytesIO(data))


def filter_broken_enum_values(collection: set | list | frozenset) -> set | list | frozenset:
    """Filter out BrokenEnumValue markers from a collection.

    - Use after unpickling to clean up sets/lists containing broken values
    - Logs the filtered values

    :param collection:
        Set, frozenset, or list that may contain BrokenEnumValue markers

    :return:
        New collection with broken values removed, same type as input
    """
    if isinstance(collection, set):
        broken = {v for v in collection if isinstance(v, BrokenEnumValue)}
        if broken:
            logger.info(
                "Filtering %d broken enum values from set: %s",
                len(broken),
                broken,
            )
        return collection - broken
    elif isinstance(collection, frozenset):
        broken = frozenset(v for v in collection if isinstance(v, BrokenEnumValue))
        if broken:
            logger.info(
                "Filtering %d broken enum values from frozenset: %s",
                len(broken),
                broken,
            )
        return collection - broken
    elif isinstance(collection, list):
        broken = [v for v in collection if isinstance(v, BrokenEnumValue)]
        if broken:
            logger.info(
                "Filtering %d broken enum values from list: %s",
                len(broken),
                broken,
            )
        return [v for v in collection if not isinstance(v, BrokenEnumValue)]
    else:
        raise TypeError(f"Expected set, frozenset, or list, got {type(collection)}")
