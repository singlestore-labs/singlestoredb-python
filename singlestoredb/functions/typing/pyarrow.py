import json
from typing import Annotated
from typing import Any

import pyarrow as pa
from pyarrow import Array  # noqa: F401
from pyarrow import array  # noqa: F401
from pyarrow import Table  # noqa: F401

try:
    from typing import TypeAlias  # type: ignore
except ImportError:
    from typing_extensions import TypeAlias  # type: ignore

from . import UDFAttrs
from . import json_or_null_dumps
from . import json_or_null_loads  # noqa: F401
from .. import dtypes


StringArray: TypeAlias = Annotated[
    pa.Array, UDFAttrs(sql_type=dtypes.TEXT(nullable=True)),
]
StrArray: TypeAlias = StringArray

BytesArray: TypeAlias = Annotated[
    pa.Array, UDFAttrs(sql_type=dtypes.BLOB(nullable=True)),
]

Float32Array: TypeAlias = Annotated[
    pa.Array, UDFAttrs(sql_type=dtypes.FLOAT(nullable=True)),
]
FloatArray: TypeAlias = Float32Array

Float64Array: TypeAlias = Annotated[
    pa.Array, UDFAttrs(sql_type=dtypes.DOUBLE(nullable=True)),
]
DoubleArray: TypeAlias = Float64Array

IntArray: TypeAlias = Annotated[
    pa.Array, UDFAttrs(sql_type=dtypes.INT(nullable=True)),
]

Int8Array: TypeAlias = Annotated[
    pa.Array, UDFAttrs(sql_type=dtypes.TINYINT(nullable=True)),
]

Int16Array: TypeAlias = Annotated[
    pa.Array, UDFAttrs(sql_type=dtypes.SMALLINT(nullable=True)),
]

Int32Array: TypeAlias = Annotated[
    pa.Array, UDFAttrs(sql_type=dtypes.INT(nullable=True)),
]

Int64Array: TypeAlias = Annotated[
    pa.Array, UDFAttrs(sql_type=dtypes.BIGINT(nullable=True)),
]

UInt8Array: TypeAlias = Annotated[
    pa.Array, UDFAttrs(sql_type=dtypes.TINYINT_UNSIGNED(nullable=True)),
]

UInt16Array: TypeAlias = Annotated[
    pa.Array, UDFAttrs(sql_type=dtypes.SMALLINT_UNSIGNED(nullable=True)),
]

UInt32Array: TypeAlias = Annotated[
    pa.Array, UDFAttrs(sql_type=dtypes.INT_UNSIGNED(nullable=True)),
]

UInt64Array: TypeAlias = Annotated[
    pa.Array, UDFAttrs(sql_type=dtypes.BIGINT_UNSIGNED(nullable=True)),
]

DateTimeArray: TypeAlias = Annotated[
    pa.Array, UDFAttrs(sql_type=dtypes.DATETIME(nullable=True)),
]

TimeDeltaArray: TypeAlias = Annotated[
    pa.Array, UDFAttrs(sql_type=dtypes.TIME(nullable=True)),
]


class PyArrowJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder that converts PyArrow scalar types to Python types."""

    def default(self, obj: Any) -> Any:
        if hasattr(obj, 'as_py'):
            # Handle PyArrow scalar types (including individual ints and floats)
            return obj.as_py()
        elif isinstance(obj, pa.Array):
            # Convert PyArrow Array to Python list
            return obj.to_pylist()
        elif isinstance(obj, pa.Table):
            # Convert PyArrow Table to list of dictionaries
            return obj.to_pydict()
        return super().default(obj)

#
# NOTE: We don't use input_transformer=json.loads because it doesn't handle
#       all cases (e.g., when the input is already a dict/list).
#


JSONArray: TypeAlias = Annotated[
    pa.Array,
    UDFAttrs(
        sql_type=dtypes.JSON(nullable=True),
        # input_transformer=json_or_null_loads,
        returns_transformer=lambda x: json_or_null_dumps(x, cls=PyArrowJSONEncoder),
    ),
]


__all__ = ['Table', 'array'] + [x for x in globals().keys() if x.endswith('Array')]
