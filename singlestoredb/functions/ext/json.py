#!/usr/bin/env python3
import json
from typing import Any
from typing import Iterable
from typing import List
from typing import Tuple


def load(colspec: Iterable[Tuple[str, int]], data: bytes) -> Tuple[List[int], List[Any]]:
    '''
    Convert bytes in JSON format into rows of data.

    Parameters
    ----------
    colspec : Iterable[Tuple[str, int]]
        An Iterable of column data types
    data : bytes
        The data in JSON format

    Returns
    -------
    Tuple[List[int] List[List[Any]]]

    '''
    row_ids = []
    rows = []
    for row_id, *row in json.loads(data.decode('utf-8'))['data']:
        row_ids.append(row_id)
        rows.append(row)
    return row_ids, rows


def _load_vectors(
    colspec: Iterable[Tuple[str, int]],
    data: bytes,
) -> Tuple[List[int], List[Any]]:
    '''
    Convert bytes in JSON format into rows of data.

    Parameters
    ----------
    colspec : Iterable[Tuple[str, int]]
        An Iterable of column data types
    data : bytes
        The data in JSON format

    Returns
    -------
    Tuple[List[int] List[List[Any]]]

    '''
    row_ids, rows = load(colspec, data)
    return row_ids, list(map(list, zip(*rows)))


def load_pandas(
    colspec: Iterable[Tuple[str, int]],
    data: bytes,
) -> Tuple[List[int], List[Any]]:
    '''
    Convert bytes in JSON format into pd.Series

    Parameters
    ----------
    colspec : Iterable[Tuple[str, int]]
        An Iterable of column data types
    data : bytes
        The data in JSON format

    Returns
    -------
    Tuple[pd.Series[int], List[pd.Series[Any]]

    '''
    import pandas as pd
    row_ids, cols = _load_vectors(colspec, data)
    index = pd.Series(row_ids)
    return pd.Series(row_ids), \
        [pd.Series(data, index=index, name=x[0]) for x in zip(cols, colspec)]


def load_polars(
    colspec: Iterable[Tuple[str, int]],
    data: bytes,
) -> Tuple[List[int], List[Any]]:
    '''
    Convert bytes in JSON format into polars.Series

    Parameters
    ----------
    colspec : Iterable[Tuple[str, int]]
        An Iterable of column data types
    data : bytes
        The data in JSON format

    Returns
    -------
    Tuple[polars.Series[int], List[polars.Series[Any]]

    '''
    import polars as pl
    row_ids, cols = _load_vectors(colspec, data)
    return pl.Series(None, row_ids), [pl.Series(x[0], data) for x in zip(cols, colspec)]


def load_numpy(
    colspec: Iterable[Tuple[str, int]],
    data: bytes,
) -> Tuple[Any, List[Any]]:
    '''
    Convert bytes in JSON format into np.ndarrays

    Parameters
    ----------
    colspec : Iterable[Tuple[str, int]]
        An Iterable of column data types
    data : bytes
        The data in JSON format

    Returns
    -------
    Tuple[np.ndarray[int], List[np.ndarray[Any]]

    '''
    import numpy as np
    row_ids, cols = _load_vectors(colspec, data)
    return np.asarray(row_ids), [np.asarray(x) for x in cols]


def load_arrow(
    colspec: Iterable[Tuple[str, int]],
    data: bytes,
) -> Tuple[Any, List[Any]]:
    '''
    Convert bytes in JSON format into pyarrow.Arrays

    Parameters
    ----------
    colspec : Iterable[Tuple[str, int]]
        An Iterable of column data types
    data : bytes
        The data in JSON format

    Returns
    -------
    Tuple[pyarrow.Array[int], List[pyarrow.Array[Any]]

    '''
    import pyarrow as pa
    row_ids, cols = _load_vectors(colspec, data)
    return pa.array(row_ids), [pa.array(x) for x in cols]


def dump(
    returns: Iterable[int],
    data: Iterable[Tuple[int, Any]],
) -> bytes:
    '''
    Convert a list of lists of data into JSON format.

    Parameters
    ----------
    returns : List[int]
        The returned data type
    data : Tuple[int, Any]
        The rows of data to serialize

    Returns
    -------
    bytes

    '''
    return json.dumps(dict(data=list(data))).encode('utf-8')


def dump_pandas(
    returns: Iterable[int],
    data: Iterable[Tuple[int, Any]],
) -> bytes:
    '''
    Convert a list of pd.Series of data into JSON format.

    Parameters
    ----------
    returns : List[int]
        The returned data type
    data : List[pd.Series[Any]]
        The rows of data to serialize

    Returns
    -------
    bytes

    '''
    import pandas as pd
    return dump(returns, pd.concat(data, axis=1).to_json(orient='values'))


def dump_polars(
    returns: Iterable[int],
    data: Iterable[Tuple[int, Any]],
) -> bytes:
    '''
    Convert a list of polars.Series of data into JSON format.

    Parameters
    ----------
    returns : List[int]
        The returned data type
    data : List[polars.Series[Any]]
        The rows of data to serialize

    Returns
    -------
    bytes

    '''
    return dump(returns, list(zip(*data)))


def dump_numpy(
    returns: Iterable[int],
    data: Iterable[Tuple[int, Any]],
) -> bytes:
    '''
    Convert a list of np.ndarrays of data into JSON format.

    Parameters
    ----------
    returns : List[int]
        The returned data type
    data : List[np.ndarray[Any]]
        The rows of data to serialize

    Returns
    -------
    bytes

    '''
    return dump(returns, list(zip(*data)))


def dump_arrow(
    returns: Iterable[int],
    data: Iterable[Tuple[int, Any]],
) -> bytes:
    '''
    Convert a list of pyarrow.Arrays of data into JSON format.

    Parameters
    ----------
    returns : List[int]
        The returned data type
    data : List[pyarrow.Array[Any]]
        The rows of data to serialize

    Returns
    -------
    bytes

    '''
    return dump(
        returns,
        list(zip(*[x.to_numpy(zero_copy_only=False) for x in data])),  # type: ignore
    )
