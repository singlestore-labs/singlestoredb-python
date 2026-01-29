import json
from typing import Annotated
from typing import Any

import numpy as np
import numpy.typing as npt
from numpy import array  # noqa: F401

try:
    from typing import TypeAlias  # type: ignore
except ImportError:
    from typing_extensions import TypeAlias  # type: ignore

from . import msgpack_or_null_dumps
from . import msgpack_or_null_loads
from . import UDFAttrs
from . import json_or_null_dumps
from . import json_or_null_loads
from .. import sql_types

NDArray = npt.NDArray


StringArray: TypeAlias = Annotated[
    npt.NDArray[np.str_], UDFAttrs(sql_type=sql_types.TEXT(nullable=False)),
]
StrArray: TypeAlias = StringArray

BytesArray: TypeAlias = Annotated[
    npt.NDArray[np.bytes_], UDFAttrs(sql_type=sql_types.BLOB(nullable=False)),
]

Float32Array: TypeAlias = Annotated[
    npt.NDArray[np.float32], UDFAttrs(sql_type=sql_types.FLOAT(nullable=False)),
]
FloatArray: TypeAlias = Float32Array

Float64Array: TypeAlias = Annotated[
    npt.NDArray[np.float64], UDFAttrs(sql_type=sql_types.DOUBLE(nullable=False)),
]
DoubleArray: TypeAlias = Float64Array

IntArray: TypeAlias = Annotated[
    npt.NDArray[np.int_], UDFAttrs(sql_type=sql_types.INT(nullable=False)),
]

Int8Array: TypeAlias = Annotated[
    npt.NDArray[np.int8], UDFAttrs(sql_type=sql_types.TINYINT(nullable=False)),
]

Int16Array: TypeAlias = Annotated[
    npt.NDArray[np.int16], UDFAttrs(sql_type=sql_types.SMALLINT(nullable=False)),
]

Int32Array: TypeAlias = Annotated[
    npt.NDArray[np.int32], UDFAttrs(sql_type=sql_types.INT(nullable=False)),
]

Int64Array: TypeAlias = Annotated[
    npt.NDArray[np.int64], UDFAttrs(sql_type=sql_types.BIGINT(nullable=False)),
]

UInt8Array: TypeAlias = Annotated[
    npt.NDArray[np.uint8], UDFAttrs(sql_type=sql_types.TINYINT_UNSIGNED(nullable=False)),
]

UInt16Array: TypeAlias = Annotated[
    npt.NDArray[np.uint16],
    UDFAttrs(sql_type=sql_types.SMALLINT_UNSIGNED(nullable=False)),
]

UInt32Array: TypeAlias = Annotated[
    npt.NDArray[np.uint32], UDFAttrs(sql_type=sql_types.INT_UNSIGNED(nullable=False)),
]

UInt64Array: TypeAlias = Annotated[
    npt.NDArray[np.uint64], UDFAttrs(sql_type=sql_types.BIGINT_UNSIGNED(nullable=False)),
]

DateTimeArray: TypeAlias = Annotated[
    npt.NDArray[np.datetime64], UDFAttrs(sql_type=sql_types.DATETIME(nullable=False)),
]

TimeDeltaArray: TypeAlias = Annotated[
    npt.NDArray[np.timedelta64], UDFAttrs(sql_type=sql_types.TIME(nullable=False)),
]


class NumpyJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder that converts numpy scalar types to Python types."""

    def default(self, obj: Any) -> Any:
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return super().default(obj)


JSONArray: TypeAlias = Annotated[
    npt.NDArray[np.object_],
    UDFAttrs(
        sql_type=sql_types.JSON(nullable=False),
        args_transformer=json_or_null_loads,
        returns_transformer=lambda x: json_or_null_dumps(x, cls=NumpyJSONEncoder),
    ),
]


def msgpack_numpy_default(obj: Any) -> Any:
    """Default function for msgpack that handles numpy types."""
    if isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    raise TypeError(f'Object of type {type(obj)} is not msgpack serializable')


MessagePackArray: TypeAlias = Annotated[
    npt.NDArray[np.object_],
    UDFAttrs(
        sql_type=sql_types.BLOB(nullable=False),
        args_transformer=msgpack_or_null_loads,
        returns_transformer=lambda x: msgpack_or_null_dumps(
            x, default=msgpack_numpy_default,
        ),
    ),
]


__all__ = ['array'] + [
    x for x in globals().keys()
    if x.endswith('Array')
]
