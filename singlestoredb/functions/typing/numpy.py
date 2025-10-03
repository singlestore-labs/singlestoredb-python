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

from . import UDFAttrs
from . import json_or_null_dumps
from . import json_or_null_loads
from .. import dtypes

NDArray = npt.NDArray


StringArray: TypeAlias = Annotated[
    npt.NDArray[np.str_], UDFAttrs(sql_type=dtypes.TEXT(nullable=False)),
]
StrArray: TypeAlias = StringArray

BytesArray: TypeAlias = Annotated[
    npt.NDArray[np.bytes_], UDFAttrs(sql_type=dtypes.BLOB(nullable=False)),
]

Float32Array: TypeAlias = Annotated[
    npt.NDArray[np.float32], UDFAttrs(sql_type=dtypes.FLOAT(nullable=False)),
]
FloatArray: TypeAlias = Float32Array

Float64Array: TypeAlias = Annotated[
    npt.NDArray[np.float64], UDFAttrs(sql_type=dtypes.DOUBLE(nullable=False)),
]
DoubleArray: TypeAlias = Float64Array

IntArray: TypeAlias = Annotated[
    npt.NDArray[np.int_], UDFAttrs(sql_type=dtypes.INT(nullable=False)),
]

Int8Array: TypeAlias = Annotated[
    npt.NDArray[np.int8], UDFAttrs(sql_type=dtypes.TINYINT(nullable=False)),
]

Int16Array: TypeAlias = Annotated[
    npt.NDArray[np.int16], UDFAttrs(sql_type=dtypes.SMALLINT(nullable=False)),
]

Int32Array: TypeAlias = Annotated[
    npt.NDArray[np.int32], UDFAttrs(sql_type=dtypes.INT(nullable=False)),
]

Int64Array: TypeAlias = Annotated[
    npt.NDArray[np.int64], UDFAttrs(sql_type=dtypes.BIGINT(nullable=False)),
]

UInt8Array: TypeAlias = Annotated[
    npt.NDArray[np.uint8], UDFAttrs(sql_type=dtypes.TINYINT_UNSIGNED(nullable=False)),
]

UInt16Array: TypeAlias = Annotated[
    npt.NDArray[np.uint16], UDFAttrs(sql_type=dtypes.SMALLINT_UNSIGNED(nullable=False)),
]

UInt32Array: TypeAlias = Annotated[
    npt.NDArray[np.uint32], UDFAttrs(sql_type=dtypes.INT_UNSIGNED(nullable=False)),
]

UInt64Array: TypeAlias = Annotated[
    npt.NDArray[np.uint64], UDFAttrs(sql_type=dtypes.BIGINT_UNSIGNED(nullable=False)),
]

DateTimeArray: TypeAlias = Annotated[
    npt.NDArray[np.datetime64], UDFAttrs(sql_type=dtypes.DATETIME(nullable=False)),
]

TimeDeltaArray: TypeAlias = Annotated[
    npt.NDArray[np.timedelta64], UDFAttrs(sql_type=dtypes.TIME(nullable=False)),
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
        sql_type=dtypes.JSON(nullable=False),
        args_transformer=json_or_null_loads,
        returns_transformer=lambda x: json_or_null_dumps(x, cls=NumpyJSONEncoder),
    ),
]


__all__ = ['array'] + [x for x in globals().keys() if x.endswith('Array')]
