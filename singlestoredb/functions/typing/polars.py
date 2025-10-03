import json
from typing import Annotated
from typing import Any

import polars as pl
from polars import DataFrame  # noqa: F401
from polars import Series  # noqa: F401

try:
    from typing import TypeAlias  # type: ignore
except ImportError:
    from typing_extensions import TypeAlias  # type: ignore

from . import UDFAttrs
from . import json_or_null_dumps
from . import json_or_null_loads
from .. import dtypes


StringSeries: TypeAlias = Annotated[
    pl.Series, UDFAttrs(sql_type=dtypes.TEXT(nullable=False)),
]
StrSeries: TypeAlias = StringSeries

BytesSeries: TypeAlias = Annotated[
    pl.Series, UDFAttrs(sql_type=dtypes.BLOB(nullable=False)),
]

Float32Series: TypeAlias = Annotated[
    pl.Series, UDFAttrs(sql_type=dtypes.FLOAT(nullable=False)),
]
FloatSeries: TypeAlias = Float32Series

Float64Series: TypeAlias = Annotated[
    pl.Series, UDFAttrs(sql_type=dtypes.DOUBLE(nullable=False)),
]
DoubleSeries: TypeAlias = Float64Series

IntSeries: TypeAlias = Annotated[
    pl.Series, UDFAttrs(sql_type=dtypes.INT(nullable=False)),
]

Int8Series: TypeAlias = Annotated[
    pl.Series, UDFAttrs(sql_type=dtypes.TINYINT(nullable=False)),
]

Int16Series: TypeAlias = Annotated[
    pl.Series, UDFAttrs(sql_type=dtypes.SMALLINT(nullable=False)),
]

Int32Series: TypeAlias = Annotated[
    pl.Series, UDFAttrs(sql_type=dtypes.INT(nullable=False)),
]

Int64Series: TypeAlias = Annotated[
    pl.Series, UDFAttrs(sql_type=dtypes.BIGINT(nullable=False)),
]

UInt8Series: TypeAlias = Annotated[
    pl.Series, UDFAttrs(sql_type=dtypes.TINYINT_UNSIGNED(nullable=False)),
]

UInt16Series: TypeAlias = Annotated[
    pl.Series, UDFAttrs(sql_type=dtypes.SMALLINT_UNSIGNED(nullable=False)),
]

UInt32Series: TypeAlias = Annotated[
    pl.Series, UDFAttrs(sql_type=dtypes.INT_UNSIGNED(nullable=False)),
]

UInt64Series: TypeAlias = Annotated[
    pl.Series, UDFAttrs(sql_type=dtypes.BIGINT_UNSIGNED(nullable=False)),
]

DateTimeSeries: TypeAlias = Annotated[
    pl.Series, UDFAttrs(sql_type=dtypes.DATETIME(nullable=False)),
]

TimeDeltaSeries: TypeAlias = Annotated[
    pl.Series, UDFAttrs(sql_type=dtypes.TIME(nullable=False)),
]


class PolarsJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder that converts Polars Series / scalar types to Python types."""

    def default(self, obj: Any) -> Any:
        if isinstance(obj, pl.Series):
            # Convert Polars Series to Python list
            return obj.to_list()
        elif hasattr(obj, 'dtype') and \
                str(obj.dtype).startswith(('Int', 'UInt', 'Float')):
            # Handle Polars scalar integer and float types
            return obj.item() if hasattr(obj, 'item') else obj
        elif isinstance(
            obj, (
                pl.datatypes.Int8, pl.datatypes.Int16, pl.datatypes.Int32,
                pl.datatypes.Int64, pl.datatypes.UInt8, pl.datatypes.UInt16,
                pl.datatypes.UInt32, pl.datatypes.UInt64,
            ),
        ):
            return int(obj)
        elif isinstance(obj, (pl.datatypes.Float32, pl.datatypes.Float64)):
            return float(obj)
        return super().default(obj)


JSONSeries: TypeAlias = Annotated[
    pl.Series,
    UDFAttrs(
        sql_type=dtypes.JSON(nullable=False),
        args_transformer=json_or_null_loads,
        returns_transformer=lambda x: json_or_null_dumps(x, cls=PolarsJSONEncoder),
    ),
]


__all__ = ['DataFrame'] + [x for x in globals().keys() if x.endswith('Series')]
