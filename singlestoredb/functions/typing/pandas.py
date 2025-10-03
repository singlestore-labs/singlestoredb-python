import json
from typing import Annotated
from typing import Any

import numpy as np
import pandas as pd
from pandas import DataFrame  # noqa: F401
from pandas import Series  # noqa: F401

try:
    from typing import TypeAlias  # type: ignore
except ImportError:
    from typing_extensions import TypeAlias  # type: ignore

from . import UDFAttrs
from . import json_or_null_dumps
from . import json_or_null_loads
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


class PandasJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles pandas Series and numpy scalar types."""

    def default(self, obj: Any) -> Any:
        if hasattr(obj, 'dtype') and hasattr(obj, 'tolist'):
            # Handle pandas Series and numpy arrays
            return obj.tolist()
        elif isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif hasattr(obj, 'item'):
            # Handle pandas scalar types
            return obj.item()
        return super().default(obj)


JSONSeries: TypeAlias = Annotated[
    pd.Series,
    UDFAttrs(
        sql_type=dtypes.JSON(nullable=False),
        args_transformer=json_or_null_loads,
        returns_transformer=lambda x: json_or_null_dumps(x, cls=PandasJSONEncoder),
    ),
]


__all__ = ['DataFrame'] + [x for x in globals().keys() if x.endswith('Series')]
