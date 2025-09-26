import json
from typing import Annotated

import pyarrow as pa
from pyarrow import Array  # noqa: F401
from pyarrow import Table  # noqa: F401

try:
    from typing import TypeAlias
except ImportError:
    from typing_extensions import TypeAlias

from . import UDFAttrs
from .. import dtypes


StringArray: TypeAlias = Annotated[
    pa.Array, UDFAttrs(sql_type=dtypes.TEXT(nullable=False)),
]
StrArray: TypeAlias = StringArray

BytesArray: TypeAlias = Annotated[
    pa.Array, UDFAttrs(sql_type=dtypes.BLOB(nullable=False)),
]

Float32Array: TypeAlias = Annotated[
    pa.Array, UDFAttrs(sql_type=dtypes.FLOAT(nullable=False)),
]
FloatArray: TypeAlias = Float32Array

Float64Array: TypeAlias = Annotated[
    pa.Array, UDFAttrs(sql_type=dtypes.DOUBLE(nullable=False)),
]
DoubleArray: TypeAlias = Float64Array

IntArray: TypeAlias = Annotated[
    pa.Array, UDFAttrs(sql_type=dtypes.INT(nullable=False)),
]

Int8Array: TypeAlias = Annotated[
    pa.Array, UDFAttrs(sql_type=dtypes.TINYINT(nullable=False)),
]

Int16Array: TypeAlias = Annotated[
    pa.Array, UDFAttrs(sql_type=dtypes.SMALLINT(nullable=False)),
]

Int32Array: TypeAlias = Annotated[
    pa.Array, UDFAttrs(sql_type=dtypes.INT(nullable=False)),
]

Int64Array: TypeAlias = Annotated[
    pa.Array, UDFAttrs(sql_type=dtypes.BIGINT(nullable=False)),
]

UInt8Array: TypeAlias = Annotated[
    pa.Array, UDFAttrs(sql_type=dtypes.TINYINT_UNSIGNED(nullable=False)),
]

UInt16Array: TypeAlias = Annotated[
    pa.Array, UDFAttrs(sql_type=dtypes.SMALLINT_UNSIGNED(nullable=False)),
]

UInt32Array: TypeAlias = Annotated[
    pa.Array, UDFAttrs(sql_type=dtypes.INT_UNSIGNED(nullable=False)),
]

UInt64Array: TypeAlias = Annotated[
    pa.Array, UDFAttrs(sql_type=dtypes.BIGINT_UNSIGNED(nullable=False)),
]

DateTimeArray: TypeAlias = Annotated[
    pa.Array, UDFAttrs(sql_type=dtypes.DATETIME(nullable=False)),
]

TimeDeltaArray: TypeAlias = Annotated[
    pa.Array, UDFAttrs(sql_type=dtypes.TIME(nullable=False)),
]

JSONArray: TypeAlias = Annotated[
    pa.Array,
    UDFAttrs(
        sql_type=dtypes.JSON(nullable=False),
        input_transformer=json.loads,
        output_transformer=json.dumps,
    ),
]


__all__ = ['Table'] + [x for x in globals().keys() if x.endswith('Array')]
