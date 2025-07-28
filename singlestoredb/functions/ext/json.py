#!/usr/bin/env python3
import base64
import json
from collections.abc import Callable
from typing import Any
from typing import List
from typing import Optional
from typing import Tuple
from typing import TYPE_CHECKING

from ..dtypes import DEFAULT_VALUES
from ..dtypes import NUMPY_TYPE_MAP
from ..dtypes import PANDAS_TYPE_MAP
from ..dtypes import POLARS_TYPE_MAP
from ..dtypes import PYARROW_TYPE_MAP
from ..dtypes import PYTHON_CONVERTERS

if TYPE_CHECKING:
    try:
        import numpy as np
    except ImportError:
        pass
    try:
        import pandas as pd
    except ImportError:
        pass
    try:
        import polars as pl
    except ImportError:
        pass
    try:
        import pyarrow as pa
    except ImportError:
        pass


class JSONEncoder(json.JSONEncoder):

    def default(self, obj: Any) -> Any:
        if isinstance(obj, bytes):
            return base64.b64encode(obj).decode('utf-8')
        return json.JSONEncoder.default(self, obj)


def decode_row(
    colspec: List[Tuple[str, int, Optional[Callable[..., Any]]]],
    row: List[Any],
) -> List[Any]:
    out = []
    for (name, dtype, transformer), item in zip(colspec, row):
        out.append(
            apply_transformer(
                transformer, PYTHON_CONVERTERS[dtype](item),  # type: ignore
            ),
        )
    return out


def decode_value(coltype: int, data: Any) -> Any:
    return PYTHON_CONVERTERS[coltype](data)  # type: ignore


def apply_transformer(
    transformer: Optional[Callable[..., Any]],
    value: Any,
) -> Any:
    return transformer(value) if transformer is not None else value


def load(
    colspec: List[Tuple[str, int, Optional[Callable[..., Any]]]],
    data: bytes,
) -> Tuple[List[int], List[Any]]:
    '''
    Convert bytes in JSON format into rows of data.

    Parameters
    ----------
    colspec : Iterable[Tuple[str, int, Optional[Callable[..., Any]]]]
        An Iterable of column data types
    data : bytes
        The data in JSON format

    Returns
    -------
    Tuple[List[int], List[Any]]

    '''
    row_ids = []
    rows = []
    for row_id, *row in json.loads(data.decode('utf-8'))['data']:
        row_ids.append(row_id)
        rows.append(decode_row(colspec, row))
    return row_ids, rows


def _load_vectors(
    colspec: List[Tuple[str, int, Optional[Callable[..., Any]]]],
    data: bytes,
) -> Tuple[List[int], List[Any]]:
    '''
    Convert bytes in JSON format into rows of data.

    Parameters
    ----------
    colspec : Iterable[Tuple[str, int, Optional[Callable[..., Any]]]]
        An Iterable of column data types
    data : bytes
        The data in JSON format

    Returns
    -------
    Tuple[List[int] List[List[Any]]]

    '''
    row_ids = []
    cols: List[Tuple[Any, Any]] = []
    defaults: List[Any] = []
    for row_id, *row in json.loads(data.decode('utf-8'))['data']:
        row_ids.append(row_id)
        if not defaults:
            defaults = [DEFAULT_VALUES[colspec[i][1]] for i, _ in enumerate(row)]
        if not cols:
            cols = [([], []) for _ in row]
        for i, (spec, x) in enumerate(zip(colspec, row)):
            cols[i][0].append(
                apply_transformer(
                    spec[2],
                    decode_value(spec[1], x)
                    if x is not None else defaults[i],
                ),
            )
            cols[i][1].append(False if x is not None else True)
    return row_ids, cols


def load_pandas(
    colspec: List[Tuple[str, int, Optional[Callable[..., Any]]]],
    data: bytes,
) -> Tuple[List[int], List[Any]]:
    '''
    Convert bytes in JSON format into pd.Series

    Parameters
    ----------
    colspec : Iterable[Tuple[str, int, Optional[Callable[..., Any]]]]
        An Iterable of column data types
    data : bytes
        The data in JSON format

    Returns
    -------
    Tuple[pd.Series[int], List[pd.Series[Any]]

    '''
    import numpy as np
    import pandas as pd
    row_ids, cols = _load_vectors(colspec, data)
    index = pd.Series(row_ids, dtype=np.longlong)
    return index, \
        [
            (
                pd.Series(
                    data, index=index, name=spec[0],
                    dtype=PANDAS_TYPE_MAP[spec[1]],
                ),
                pd.Series(mask, index=index, dtype=np.longlong),
            )
            for (data, mask), spec in zip(cols, colspec)
        ]


def load_polars(
    colspec: List[Tuple[str, int, Optional[Callable[..., Any]]]],
    data: bytes,
) -> Tuple[List[int], List[Any]]:
    '''
    Convert bytes in JSON format into polars.Series

    Parameters
    ----------
    colspec : Iterable[Tuple[str, int, Optional[Callable[..., Any]]]]
        An Iterable of column data types
    data : bytes
        The data in JSON format

    Returns
    -------
    Tuple[polars.Series[int], List[polars.Series[Any]]

    '''
    import polars as pl
    row_ids, cols = _load_vectors(colspec, data)
    return pl.Series(None, row_ids, dtype=pl.Int64), \
        [
            (
                pl.Series(
                    spec[0],
                    data,
                    # We have to use the TEXT type for JSON objects at the moment,
                    # so don't explicitly cast the dtypes for a db type of 254.
                    dtype=POLARS_TYPE_MAP[spec[1]] if spec[1] != 254 else None,
                ),
                pl.Series(None, mask, dtype=pl.Boolean),
            )
            for (data, mask), spec in zip(cols, colspec)
        ]


def load_numpy(
    colspec: List[Tuple[str, int, Optional[Callable[..., Any]]]],
    data: bytes,
) -> Tuple[Any, List[Any]]:
    '''
    Convert bytes in JSON format into np.ndarrays

    Parameters
    ----------
    colspec : Iterable[Tuple[str, int, Optional[Callable[..., Any]]]]
        An Iterable of column data types
    data : bytes
        The data in JSON format

    Returns
    -------
    Tuple[np.ndarray[int], List[np.ndarray[Any]]

    '''
    import numpy as np
    row_ids, cols = _load_vectors(colspec, data)
    return np.asarray(row_ids, dtype=np.longlong), \
        [
            (
                np.asarray(data, dtype=NUMPY_TYPE_MAP[spec[1]]),  # type: ignore
                np.asarray(mask, dtype=np.bool_),  # type: ignore
            )
            for (data, mask), spec in zip(cols, colspec)
        ]


def load_arrow(
    colspec: List[Tuple[str, int, Optional[Callable[..., Any]]]],
    data: bytes,
) -> Tuple[Any, List[Any]]:
    '''
    Convert bytes in JSON format into pyarrow.Arrays

    Parameters
    ----------
    colspec : Iterable[Tuple[str, int, Optional[Callable[..., Any]]]]
        An Iterable of column data types
    data : bytes
        The data in JSON format

    Returns
    -------
    Tuple[pyarrow.Array[int], List[pyarrow.Array[Any]]

    '''
    import pyarrow as pa
    row_ids, cols = _load_vectors(colspec, data)
    return pa.array(row_ids, type=pa.int64()), \
        [
            (
                pa.array(
                    data, type=PYARROW_TYPE_MAP[dtype],
                    mask=pa.array(mask, type=pa.bool_()),
                ),
                pa.array(mask, type=pa.bool_()),
            )
            for (data, mask), (name, dtype, transformer) in zip(cols, colspec)
        ]


def dump(
    returns: List[Tuple[str, int, Callable[..., Any]]],
    row_ids: List[int],
    rows: List[List[Any]],
) -> bytes:
    '''
    Convert a list of lists of data into JSON format.

    Parameters
    ----------
    returns : List[Tuple[str, int, Callable[..., Any]]]
        The returned data type
    row_ids : List[int]
        Row IDs
    rows : List[List[Any]]
        The rows of data to serialize

    Returns
    -------
    bytes

    '''
    rows = [
        [apply_transformer(returns[i][2], item) for i, item in enumerate(row)]
        for row in rows
    ]
    data = list(zip(row_ids, *list(zip(*rows))))
    return json.dumps(dict(data=data), cls=JSONEncoder).encode('utf-8')


def _dump_vectors(
    returns: List[Tuple[str, int, Optional[Callable[..., Any]]]],
    row_ids: List[int],
    cols: List[Tuple[Any, Any]],
) -> bytes:
    '''
    Convert a list of lists of data into JSON format.

    Parameters
    ----------
    returns : List[Tuple[str, int, Optional[Callable[..., Any]]]]
        The returned data type
    row_ids : List[int]
        Row IDs
    cols : List[Tuple[Any, Any]]
        The rows of data to serialize

    Returns
    -------
    bytes

    '''
    masked_cols = []
    for i, (data, mask) in enumerate(cols):
        if mask is not None:
            masked_cols.append([
                apply_transformer(returns[i][2], d)
                if m is not None else None for d, m in zip(data, mask)
            ])
        else:
            masked_cols.append([
                apply_transformer(returns[i][2], d) for d in data
            ])
    data = list(zip(row_ids, *masked_cols))
    return json.dumps(dict(data=data), cls=JSONEncoder).encode('utf-8')


load_list = _load_vectors
dump_list = _dump_vectors


def dump_pandas(
    returns: List[Tuple[str, int, Optional[Callable[..., Any]]]],
    row_ids: 'pd.Series[int]',
    cols: List[Tuple['pd.Series[int]', 'pd.Series[bool]']],
) -> bytes:
    '''
    Convert a list of pd.Series of data into JSON format.

    Parameters
    ----------
    returns : List[Tuple[str, int, Optional[Callable[..., Any]]]]
        The returned data type
    row_ids : pd.Series[int]
        Row IDs
    cols : List[Tuple[pd.Series[Any], pd.Series[bool]]]
        The rows of data to serialize

    Returns
    -------
    bytes

    '''
    return _dump_vectors(
        returns,
        row_ids.tolist(),
        [(x[0].tolist(), x[1].tolist() if x[1] is not None else None) for x in cols],
    )


def dump_polars(
    returns: List[Tuple[str, int, Optional[Callable[..., Any]]]],
    row_ids: 'pl.Series[int]',
    cols: List[Tuple['pl.Series[Any]', 'pl.Series[int]']],
) -> bytes:
    '''
    Convert a list of polars.Series of data into JSON format.

    Parameters
    ----------
    returns : List[Tuple[str, int, Optional[Callable[..., Any]]]]
        The returned data type
    row_ids : List[int]
    cols : List[Tuple[polars.Series[Any], polars.Series[bool]]
        The rows of data to serialize

    Returns
    -------
    bytes

    '''
    return _dump_vectors(
        returns,
        row_ids.to_list(),
        [(x[0].to_list(), x[1].to_list() if x[1] is not None else None) for x in cols],
    )


def dump_numpy(
    returns: List[Tuple[str, int, Optional[Callable[..., Any]]]],
    row_ids: 'np.typing.NDArray[np.int64]',
    cols: List[Tuple['np.typing.NDArray[Any]', 'np.typing.NDArray[np.bool_]']],
) -> bytes:
    '''
    Convert a list of np.ndarrays of data into JSON format.

    Parameters
    ----------
    returns : List[Tuple[str, int, Optional[Callable[..., Any]]]]
        The returned data type
    row_ids : List[int]
        Row IDs
    cols : List[Tuple[np.ndarray[Any], np.ndarray[bool]]]
        The rows of data to serialize

    Returns
    -------
    bytes

    '''
    return _dump_vectors(
        returns,
        row_ids.tolist(),
        [(x[0].tolist(), x[1].tolist() if x[1] is not None else None) for x in cols],
    )


def dump_arrow(
    returns: List[Tuple[str, int, Optional[Callable[..., Any]]]],
    row_ids: 'pa.Array[int]',
    cols: List[Tuple['pa.Array[int]', 'pa.Array[bool]']],
) -> bytes:
    '''
    Convert a list of pyarrow.Arrays of data into JSON format.

    Parameters
    ----------
    returns : List[Tuple[str, int, Optional[Callable[..., Any]]]]
        The returned data type
    row_ids : pyarrow.Array[int]
        Row IDs
    cols : List[Tuple[pyarrow.Array[Any], pyarrow.Array[Any]]]
        The rows of data to serialize

    Returns
    -------
    bytes

    '''
    return _dump_vectors(
        returns,
        row_ids.tolist(),
        [(x[0].tolist(), x[1].tolist() if x[1] is not None else None) for x in cols],
    )
