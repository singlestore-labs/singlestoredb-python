#!/usr/bin/env python3
import struct
import warnings
from io import BytesIO
from typing import Any
from typing import Iterable
from typing import List
from typing import Tuple

try:
    import _singlestoredb_accel
except ImportError:
    warnings.warn(
        'could not load accelerated data reader for external functions; '
        'using pure Python implementation.',
        RuntimeWarning,
    )
    _singlestoredb_accel = None

from ...mysql.constants import FIELD_TYPE as ft


def _load_rows(
    colspec: Iterable[Tuple[str, int]],
    data: bytes,
) -> Tuple[List[int], List[Any]]:
    '''
    Convert bytes in rowdat_1 format into rows of data.

    Parameters
    ----------
    colspec : Iterable[str]
        An Iterable of column data types
    data : bytes
        The data in rowdat_1 format

    Returns
    -------
    Tuple[List[int], List[Any]]

    '''
    data_len = len(data)
    data_io = BytesIO(data)
    row_ids = []
    rows = []
    val = None
    while data_io.tell() < data_len:
        row_ids.append(struct.unpack('<q', data_io.read(8))[0])
        row = []
        for _, ctype in colspec:
            is_null = data_io.read(1) == b'\x01'
            if ctype is ft.LONGLONG:
                val = struct.unpack('<q', data_io.read(8))[0]
            elif ctype is ft.DOUBLE:
                val = struct.unpack('<d', data_io.read(8))[0]
            elif ctype is ft.STRING:
                slen = struct.unpack('<q', data_io.read(8))[0]
                val = data_io.read(slen).decode('utf-8')
            else:
                raise TypeError(f'unrecognized column type: {ctype}')
            row.append(None if is_null else val)
        rows.append(row)
    return row_ids, rows


def _load_vectors(
    colspec: Iterable[Tuple[str, int]],
    data: bytes,
) -> Tuple[List[int], List[Any]]:
    '''
    Convert bytes in rowdat_1 format into columns of data.

    Parameters
    ----------
    colspec : Iterable[str]
        An Iterable of column data types
    data : bytes
        The data in rowdat_1 format

    Returns
    -------
    Tuple[List[int], List[Any]]

    '''
    data_len = len(data)
    data_io = BytesIO(data)
    row_ids = []
    cols: List[List[Any]] = [[] for _ in colspec]
    val = None
    while data_io.tell() < data_len:
        row_ids.append(struct.unpack('<q', data_io.read(8))[0])
        for i, (_, ctype) in enumerate(colspec):
            is_null = data_io.read(1) == b'\x01'
            if ctype is ft.LONGLONG:
                val = struct.unpack('<q', data_io.read(8))[0]
            elif ctype is ft.DOUBLE:
                val = struct.unpack('<d', data_io.read(8))[0]
            elif ctype is ft.STRING:
                slen = struct.unpack('<q', data_io.read(8))[0]
                val = data_io.read(slen).decode('utf-8')
            else:
                raise TypeError(f'unrecognized column type: {ctype}')
            cols[i].append(None if is_null else val)
    return row_ids, cols


def _load_pandas(
    colspec: Iterable[Tuple[str, int]],
    data: bytes,
) -> Tuple[List[int], List[Any]]:
    '''
    Convert bytes in rowdat_1 format into rows of data.

    Parameters
    ----------
    colspec : Iterable[str]
        An Iterable of column data types
    data : bytes
        The data in rowdat_1 format

    Returns
    -------
    Tuple[pd.Series[int], List[pd.Series[Any]]]

    '''
    import pandas as pd
    row_ids, cols = _load_vectors(colspec, data)
    index = pd.Series(row_ids)
    return pd.Series(row_ids), [
        pd.Series(data, index=index, name=spec[0])
        for data, spec in zip(cols, colspec)
    ]


def _load_polars(
    colspec: Iterable[Tuple[str, int]],
    data: bytes,
) -> Tuple[List[int], List[Any]]:
    '''
    Convert bytes in rowdat_1 format into rows of data.

    Parameters
    ----------
    colspec : Iterable[str]
        An Iterable of column data types
    data : bytes
        The data in rowdat_1 format

    Returns
    -------
    Tuple[polars.Series[int], List[polars.Series[Any]]]

    '''
    import polars as pl
    row_ids, cols = _load_vectors(colspec, data)
    return pl.Series(None, row_ids), \
        [pl.Series(spec[0], data) for data, spec in zip(cols, colspec)]


def _load_numpy(
    colspec: Iterable[Tuple[str, int]],
    data: bytes,
) -> Tuple[Any, List[Any]]:
    '''
    Convert bytes in rowdat_1 format into rows of data.

    Parameters
    ----------
    colspec : Iterable[str]
        An Iterable of column data types
    data : bytes
        The data in rowdat_1 format

    Returns
    -------
    Tuple[np.ndarray[int], List[np.ndarray[Any]]]

    '''
    import numpy as np
    row_ids, cols = _load_vectors(colspec, data)
    return np.asarray(row_ids), [np.asarray(x) for x in cols]


def _load_arrow(
    colspec: Iterable[Tuple[str, int]],
    data: bytes,
) -> Tuple[Any, List[Any]]:
    '''
    Convert bytes in rowdat_1 format into rows of data.

    Parameters
    ----------
    colspec : Iterable[str]
        An Iterable of column data types
    data : bytes
        The data in rowdat_1 format

    Returns
    -------
    Tuple[pyarrow.Array[int], List[pyarrow.Array[Any]]]

    '''
    import pyarrow as pa
    row_ids, cols = _load_vectors(colspec, data)
    return pa.array(row_ids), [pa.array(x) for x in cols]


def _dump(
    returns: Iterable[int],
    data: Iterable[Tuple[int, Any]],
) -> bytes:
    '''
    Convert a list of lists of data into rowdat_1 format.

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
    out = BytesIO()
    for row_id, value in data:
        out.write(struct.pack('<q', row_id))
        for rtype in returns:
            out.write(b'\x01' if value is None else b'\x00')
            if rtype is ft.LONGLONG:
                if value is None:
                    out.write(struct.pack('<q', 0))
                else:
                    out.write(struct.pack('<q', value))
            elif rtype is ft.DOUBLE:
                if value is None:
                    out.write(struct.pack('<d', 0.0))
                else:
                    out.write(struct.pack('<d', value))
            elif rtype is ft.STRING:
                if value is None:
                    out.write(struct.pack('<q', 0))
                else:
                    sval = value.encode('utf-8')
                    out.write(struct.pack('<q', len(sval)))
                    out.write(sval)
            else:
                raise TypeError(f'unrecognized column type: {rtype}')
    return out.getvalue()


def _dump_arrow(
    returns: Iterable[int],
    data: Iterable[Tuple[int, Any]],
) -> bytes:
    items = list(data)
    return _dump(
        returns,
        (items[0].to_numpy(), items[1].to_numpy(zero_copy_only=False)),  # type: ignore
    )


def _dump_arrow_accel(
    returns: Iterable[int],
    data: Iterable[Tuple[int, Any]],
) -> bytes:
    items = list(data)
    return _singlestoredb_accel.dump_rowdat_1(
        returns,
        (items[0].to_numpy(), items[1].to_numpy(zero_copy_only=False)),  # type: ignore
    )


if _singlestoredb_accel is None:
    load = _load_rows
    dump = _dump
    load_pandas = _load_pandas
    dump_pandas = _dump
    load_numpy = _load_numpy
    dump_numpy = _dump
    load_arrow = _load_arrow
    dump_arrow = _dump_arrow
    load_polars = _load_polars
    dump_polars = _dump

else:
    load = _singlestoredb_accel.load_rowdat_1
    dump = _singlestoredb_accel.dump_rowdat_1
    load_pandas = _singlestoredb_accel.load_rowdat_1_pandas
    dump_pandas = _singlestoredb_accel.dump_rowdat_1
    load_numpy = _singlestoredb_accel.load_rowdat_1_numpy
    dump_numpy = _singlestoredb_accel.dump_rowdat_1
    load_arrow = _singlestoredb_accel.load_rowdat_1_arrow
    dump_arrow = _dump_arrow_accel
    load_polars = _singlestoredb_accel.load_rowdat_1_polars
    dump_polars = _singlestoredb_accel.dump_rowdat_1
