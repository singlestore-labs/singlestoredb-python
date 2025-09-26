import json
from typing import Annotated

import pandas as pd
from pandas import DataFrame  # noqa: F401
from pandas import Series  # noqa: F401

try:
    from typing import TypeAlias
except ImportError:
    from typing_extensions import TypeAlias

from . import UDFAttrs
from .. import dtypes


StringSeries: TypeAlias = Annotated[
    pd.Series, UDFAttrs(sql_type=dtypes.TEXT(nullable=False)),
]
StrSeries: TypeAlias = StringSeries

BytesSeries: TypeAlias = Annotated[
    pd.Series, UDFAttrs(sql_type=dtypes.BLOB(nullable=False)),
]

Float32Series: TypeAlias = Annotated[
    pd.Series, UDFAttrs(sql_type=dtypes.FLOAT(nullable=False)),
]
FloatSeries: TypeAlias = Float32Series

Float64Series: TypeAlias = Annotated[
    pd.Series, UDFAttrs(sql_type=dtypes.DOUBLE(nullable=False)),
]
DoubleSeries: TypeAlias = Float64Series

IntSeries: TypeAlias = Annotated[
    pd.Series, UDFAttrs(sql_type=dtypes.INT(nullable=False)),
]

Int8Series: TypeAlias = Annotated[
    pd.Series, UDFAttrs(sql_type=dtypes.TINYINT(nullable=False)),
]

Int16Series: TypeAlias = Annotated[
    pd.Series, UDFAttrs(sql_type=dtypes.SMALLINT(nullable=False)),
]

Int32Series: TypeAlias = Annotated[
    pd.Series, UDFAttrs(sql_type=dtypes.INT(nullable=False)),
]

Int64Series: TypeAlias = Annotated[
    pd.Series, UDFAttrs(sql_type=dtypes.BIGINT(nullable=False)),
]

UInt8Series: TypeAlias = Annotated[
    pd.Series, UDFAttrs(sql_type=dtypes.TINYINT_UNSIGNED(nullable=False)),
]

UInt16Series: TypeAlias = Annotated[
    pd.Series, UDFAttrs(sql_type=dtypes.SMALLINT_UNSIGNED(nullable=False)),
]

UInt32Series: TypeAlias = Annotated[
    pd.Series, UDFAttrs(sql_type=dtypes.INT_UNSIGNED(nullable=False)),
]

UInt64Series: TypeAlias = Annotated[
    pd.Series, UDFAttrs(sql_type=dtypes.BIGINT_UNSIGNED(nullable=False)),
]

DateTimeSeries: TypeAlias = Annotated[
    pd.Series, UDFAttrs(sql_type=dtypes.DATETIME(nullable=False)),
]

TimeSeries: TypeAlias = Annotated[
    pd.Series, UDFAttrs(sql_type=dtypes.TIME(nullable=False)),
]

JSONSeries: TypeAlias = Annotated[
    pd.Series,
    UDFAttrs(
        sql_type=dtypes.JSON(nullable=False),
        input_transformer=json.loads,
        output_transformer=json.dumps,
    ),
]


__all__ = ['DataFrame'] + [x for x in globals().keys() if x.endswith('Series')]
