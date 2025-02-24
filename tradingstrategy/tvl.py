"""TVL data.

- Data format when streaming /tvl endpoint

"""
from dataclasses import dataclass

import pyarrow as pa


@dataclass
class TVL:
    """Define a single TVL candle.

    - Currently only has Arrow schema
    """

    def get_pyarrow_schema(cls) -> pa.Schema:
        # This schema is based on the original example files in Demeter repo
        schema = pa.schema([
            ("pair_id", pa.int32()),
            ("bucket", pa.timestamp("s")),
            ("open", pa.float64()),
            ("high", pa.float64()),
            ("low", pa.float64()),
            ("close", pa.float64()),
        ])
        return schema

