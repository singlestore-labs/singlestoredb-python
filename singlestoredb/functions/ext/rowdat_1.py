#!/usr/bin/env python3
import struct
import warnings
from io import BytesIO
from typing import Any
from typing import List
from typing import Optional
from typing import Sequence
from typing import Tuple

from ...config import get_option
from ..dtypes import DEFAULT_VALUES
from ..dtypes import NUMPY_TYPE_MAP
from ..dtypes import PANDAS_TYPE_MAP
from ..dtypes import POLARS_TYPE_MAP
from ..dtypes import PYARROW_TYPE_MAP

try:
    import numpy as np
    has_numpy = True
except ImportError:
    has_numpy = False

try:
    import polars as pl
    has_polars = True
except ImportError:
    has_polars = False

try:
    import pandas as pd
    has_pandas = True
except ImportError:
    has_pandas = False

try:
    import pyarrow as pa
    import pyarrow.compute as pc
    has_pyarrow = True
except ImportError:
    has_pyarrow = False

from ...mysql.constants import FIELD_TYPE as ft

has_accel = False
try:
    if not get_option('pure_python'):
        import _singlestoredb_accel
        has_accel = True
except ImportError:
    warnings.warn(
        'could not load accelerated data reader for external functions; '
        'using pure Python implementation.',
        RuntimeWarning,
    )

numeric_formats = {
    ft.TINY: '<b',
    -ft.TINY: '<B',
    ft.SHORT: '<h',
    -ft.SHORT: '<H',
    ft.INT24: '<i',
    -ft.INT24: '<I',
    ft.LONG: '<i',
    -ft.LONG: '<I',
    ft.LONGLONG: '<q',
    -ft.LONGLONG: '<Q',
    ft.FLOAT: '<f',
    ft.DOUBLE: '<d',
}
numeric_sizes = {
    ft.TINY: 1,
    -ft.TINY: 1,
    ft.SHORT: 2,
    -ft.SHORT: 2,
    ft.INT24: 4,
    -ft.INT24: 4,
    ft.LONG: 4,
    -ft.LONG: 4,
    ft.LONGLONG: 8,
    -ft.LONGLONG: 8,
    ft.FLOAT: 4,
    ft.DOUBLE: 8,
}
medium_int_types = set([ft.INT24, -ft.INT24])
int_types = set([
    ft.TINY, -ft.TINY, ft.SHORT, -ft.SHORT, ft.INT24, -ft.INT24,
    ft.LONG, -ft.LONG, ft.LONGLONG, -ft.LONGLONG,
])
string_types = set([15, 245, 247, 248, 249, 250, 251, 252, 253, 254])
binary_types = set([-x for x in string_types])


def _load(
    colspec: List[Tuple[str, int]],
    data: bytes,
) -> Tuple[List[int], List[Any]]:
    '''
    Convert bytes in rowdat_1 format into rows of data.

    Parameters
    ----------
    colspec : List[str]
        An List of column data types
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
            if ctype in numeric_formats:
                val = struct.unpack(
                    numeric_formats[ctype],
                    data_io.read(numeric_sizes[ctype]),
                )[0]
            elif ctype in string_types:
                slen = struct.unpack('<q', data_io.read(8))[0]
                val = data_io.read(slen).decode('utf-8')
            elif ctype in binary_types:
                slen = struct.unpack('<q', data_io.read(8))[0]
                val = data_io.read(slen)
            else:
                raise TypeError(f'unrecognized column type: {ctype}')
            row.append(None if is_null else val)
        rows.append(row)
    return row_ids, rows


def _load_vectors(
    colspec: List[Tuple[str, int]],
    data: bytes,
) -> Tuple[List[int], List[Tuple[Sequence[Any], Optional[Sequence[Any]]]]]:
    '''
    Convert bytes in rowdat_1 format into columns of data.

    Parameters
    ----------
    colspec : List[str]
        An List of column data types
    data : bytes
        The data in rowdat_1 format

    Returns
    -------
    Tuple[List[int], List[Tuple[Any, Any]]]

    '''
    data_len = len(data)
    data_io = BytesIO(data)
    row_ids = []
    cols: List[Any] = [[] for _ in colspec]
    masks: List[Any] = [[] for _ in colspec]
    val = None
    while data_io.tell() < data_len:
        row_ids.append(struct.unpack('<q', data_io.read(8))[0])
        for i, (_, ctype) in enumerate(colspec):
            default = DEFAULT_VALUES[ctype]
            is_null = data_io.read(1) == b'\x01'
            if ctype in numeric_formats:
                val = struct.unpack(
                    numeric_formats[ctype],
                    data_io.read(numeric_sizes[ctype]),
                )[0]
            elif ctype in string_types:
                slen = struct.unpack('<q', data_io.read(8))[0]
                val = data_io.read(slen).decode('utf-8')
            elif ctype in binary_types:
                slen = struct.unpack('<q', data_io.read(8))[0]
                val = data_io.read(slen)
            else:
                raise TypeError(f'unrecognized column type: {ctype}')
            cols[i].append(default if is_null else val)
            masks[i].append(True if is_null else False)
    return row_ids, [(x, y) for x, y in zip(cols, masks)]


def _load_pandas(
    colspec: List[Tuple[str, int]],
    data: bytes,
) -> Tuple[
    'pd.Series[np.int64]',
    List[Tuple['pd.Series[Any]', 'pd.Series[np.bool_]']],
]:
    '''
    Convert bytes in rowdat_1 format into rows of data.

    Parameters
    ----------
    colspec : List[str]
        An List of column data types
    data : bytes
        The data in rowdat_1 format

    Returns
    -------
    Tuple[pd.Series[int], List[Tuple[pd.Series[Any], pd.Series[bool]]]]

    '''
    if not has_pandas or not has_numpy:
        raise RuntimeError('pandas must be installed for this operation')

    row_ids, cols = _load_vectors(colspec, data)
    index = pd.Series(row_ids)
    return pd.Series(row_ids, dtype=np.int64), [
        (
            pd.Series(data, index=index, name=name, dtype=PANDAS_TYPE_MAP[dtype]),
            pd.Series(mask, index=index, dtype=np.bool_),
        )
        for (data, mask), (name, dtype) in zip(cols, colspec)
    ]


def _load_polars(
    colspec: List[Tuple[str, int]],
    data: bytes,
) -> Tuple[
    'pl.Series[pl.Int64]',
    List[Tuple['pl.Series[Any]', 'pl.Series[pl.Boolean]']],
]:
    '''
    Convert bytes in rowdat_1 format into rows of data.

    Parameters
    ----------
    colspec : List[str]
        An List of column data types
    data : bytes
        The data in rowdat_1 format

    Returns
    -------
    Tuple[polars.Series[int], List[polars.Series[Any]]]

    '''
    if not has_polars:
        raise RuntimeError('polars must be installed for this operation')

    row_ids, cols = _load_vectors(colspec, data)
    return pl.Series(None, row_ids, dtype=pl.Int64), \
        [
            (
                pl.Series(name=name, values=data, dtype=POLARS_TYPE_MAP[dtype]),
                pl.Series(values=mask, dtype=pl.Boolean),
            )
            for (data, mask), (name, dtype) in zip(cols, colspec)
        ]


def _load_numpy(
    colspec: List[Tuple[str, int]],
    data: bytes,
) -> Tuple[
    'np.typing.NDArray[np.int64]',
    List[Tuple['np.typing.NDArray[Any]', 'np.typing.NDArray[np.bool_]']],
]:
    '''
    Convert bytes in rowdat_1 format into rows of data.

    Parameters
    ----------
    colspec : List[str]
        An List of column data types
    data : bytes
        The data in rowdat_1 format

    Returns
    -------
    Tuple[np.ndarray[int], List[np.ndarray[Any]]]

    '''
    if not has_numpy:
        raise RuntimeError('numpy must be installed for this operation')

    row_ids, cols = _load_vectors(colspec, data)
    return np.asarray(row_ids, dtype=np.int64), \
        [
            (
                np.asarray(data, dtype=NUMPY_TYPE_MAP[dtype]),
                np.asarray(mask, dtype=np.bool_),
            )
            for (data, mask), (name, dtype) in zip(cols, colspec)
        ]


def _load_arrow(
    colspec: List[Tuple[str, int]],
    data: bytes,
) -> Tuple[
    'pa.Array[pa.int64()]',
    List[Tuple['pa.Array[Any]', 'pa.Array[pa.bool_()]']],
]:
    '''
    Convert bytes in rowdat_1 format into rows of data.

    Parameters
    ----------
    colspec : List[str]
        An List of column data types
    data : bytes
        The data in rowdat_1 format

    Returns
    -------
    Tuple[pyarrow.Array[int], List[pyarrow.Array[Any]]]

    '''
    if not has_pyarrow:
        raise RuntimeError('pyarrow must be installed for this operation')

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
            for (data, mask), (name, dtype) in zip(cols, colspec)
        ]


def _dump(
    returns: List[int],
    row_ids: List[int],
    rows: List[List[Any]],
) -> bytes:
    '''
    Convert a list of lists of data into rowdat_1 format.

    Parameters
    ----------
    returns : List[int]
        The returned data type
    row_ids : List[int]
        The row IDs
    rows : List[List[Any]]
        The rows of data and masks to serialize

    Returns
    -------
    bytes

    '''
    out = BytesIO()

    if len(rows) == 0 or len(row_ids) == 0:
        return out.getbuffer()

    for row_id, *values in zip(row_ids, *list(zip(*rows))):
        out.write(struct.pack('<q', row_id))
        for rtype, value in zip(returns, values):
            out.write(b'\x01' if value is None else b'\x00')
            default = DEFAULT_VALUES[rtype]
            if rtype in numeric_formats:
                if value is None:
                    out.write(struct.pack(numeric_formats[rtype], default))
                else:
                    if rtype in int_types:
                        if rtype == ft.INT24:
                            if int(value) > 8388607 or int(value) < -8388608:
                                raise ValueError(
                                    'value is outside range of MEDIUMINT',
                                )
                        elif rtype == -ft.INT24:
                            if int(value) > 16777215 or int(value) < 0:
                                raise ValueError(
                                    'value is outside range of UNSIGNED MEDIUMINT',
                                )
                        out.write(struct.pack(numeric_formats[rtype], int(value)))
                    else:
                        out.write(struct.pack(numeric_formats[rtype], float(value)))
            elif rtype in string_types:
                if value is None:
                    out.write(struct.pack('<q', 0))
                else:
                    sval = value.encode('utf-8')
                    out.write(struct.pack('<q', len(sval)))
                    out.write(sval)
            elif rtype in binary_types:
                if value is None:
                    out.write(struct.pack('<q', 0))
                else:
                    out.write(struct.pack('<q', len(value)))
                    out.write(value)
            else:
                raise TypeError(f'unrecognized column type: {rtype}')

    return out.getbuffer()


def _dump_vectors(
    returns: List[int],
    row_ids: List[int],
    cols: List[Tuple[Sequence[Any], Optional[Sequence[Any]]]],
) -> bytes:
    '''
    Convert a list of columns of data into rowdat_1 format.

    Parameters
    ----------
    returns : List[int]
        The returned data type
    row_ids : List[int]
        The row IDs
    cols : List[Tuple[Any, Any]]
        The rows of data and masks to serialize

    Returns
    -------
    bytes

    '''
    out = BytesIO()

    if len(cols) == 0 or len(row_ids) == 0:
        return out.getbuffer()

    for j, row_id in enumerate(row_ids):

        out.write(struct.pack('<q', row_id))

        for i, rtype in enumerate(returns):
            value = cols[i][0][j]
            if cols[i][1] is not None:
                is_null = cols[i][1][j]  # type: ignore
            else:
                is_null = False

            out.write(b'\x01' if is_null or value is None else b'\x00')
            default = DEFAULT_VALUES[rtype]
            try:
                if rtype in numeric_formats:
                    if value is None:
                        out.write(struct.pack(numeric_formats[rtype], default))
                    else:
                        if rtype in int_types:
                            if rtype == ft.INT24:
                                if int(value) > 8388607 or int(value) < -8388608:
                                    raise ValueError(
                                        'value is outside range of MEDIUMINT',
                                    )
                            elif rtype == -ft.INT24:
                                if int(value) > 16777215 or int(value) < 0:
                                    raise ValueError(
                                        'value is outside range of UNSIGNED MEDIUMINT',
                                    )
                            out.write(struct.pack(numeric_formats[rtype], int(value)))
                        else:
                            out.write(struct.pack(numeric_formats[rtype], float(value)))
                elif rtype in string_types:
                    if value is None:
                        out.write(struct.pack('<q', 0))
                    else:
                        sval = value.encode('utf-8')
                        out.write(struct.pack('<q', len(sval)))
                        out.write(sval)
                elif rtype in binary_types:
                    if value is None:
                        out.write(struct.pack('<q', 0))
                    else:
                        out.write(struct.pack('<q', len(value)))
                        out.write(value)
                else:
                    raise TypeError(f'unrecognized column type: {rtype}')

            except struct.error as exc:
                raise ValueError(str(exc))

    return out.getbuffer()


def _dump_arrow(
    returns: List[int],
    row_ids: 'pa.Array[int]',
    cols: List[Tuple['pa.Array[Any]', 'pa.Array[bool]']],
) -> bytes:
    if not has_pyarrow:
        raise RuntimeError('pyarrow must be installed for this operation')

    return _dump_vectors(
        returns,
        row_ids.tolist(),
        [(x.tolist(), y.tolist() if y is not None else None) for x, y in cols],
    )


def _dump_numpy(
    returns: List[int],
    row_ids: 'np.typing.NDArray[np.int64]',
    cols: List[Tuple['np.typing.NDArray[Any]', 'np.typing.NDArray[np.bool_]']],
) -> bytes:
    if not has_numpy:
        raise RuntimeError('numpy must be installed for this operation')

    return _dump_vectors(
        returns,
        row_ids.tolist(),
        [(x.tolist(), y.tolist() if y is not None else None) for x, y in cols],
    )


def _dump_pandas(
    returns: List[int],
    row_ids: 'pd.Series[np.int64]',
    cols: List[Tuple['pd.Series[Any]', 'pd.Series[np.bool_]']],
) -> bytes:
    if not has_pandas or not has_numpy:
        raise RuntimeError('pandas must be installed for this operation')

    return _dump_vectors(
        returns,
        row_ids.to_list(),
        [(x.to_list(), y.to_list() if y is not None else None) for x, y in cols],
    )


def _dump_polars(
    returns: List[int],
    row_ids: 'pl.Series[pl.Int64]',
    cols: List[Tuple['pl.Series[Any]', 'pl.Series[pl.Boolean]']],
) -> bytes:
    if not has_polars:
        raise RuntimeError('polars must be installed for this operation')

    return _dump_vectors(
        returns,
        row_ids.to_list(),
        [(x.to_list(), y.to_list() if y is not None else None) for x, y in cols],
    )


def _load_numpy_accel(
    colspec: List[Tuple[str, int]],
    data: bytes,
) -> Tuple[
    'np.typing.NDArray[np.int64]',
    List[Tuple['np.typing.NDArray[Any]', 'np.typing.NDArray[np.bool_]']],
]:
    if not has_numpy:
        raise RuntimeError('numpy must be installed for this operation')
    if not has_accel:
        raise RuntimeError('could not load SingleStoreDB extension')

    return _singlestoredb_accel.load_rowdat_1_numpy(colspec, data)


def _dump_numpy_accel(
    returns: List[int],
    row_ids: 'np.typing.NDArray[np.int64]',
    cols: List[Tuple['np.typing.NDArray[Any]', 'np.typing.NDArray[np.bool_]']],
) -> bytes:
    if not has_numpy:
        raise RuntimeError('numpy must be installed for this operation')
    if not has_accel:
        raise RuntimeError('could not load SingleStoreDB extension')

    return _singlestoredb_accel.dump_rowdat_1_numpy(returns, row_ids, cols)


def _load_pandas_accel(
    colspec: List[Tuple[str, int]],
    data: bytes,
) -> Tuple[
    'pd.Series[np.int64]',
    List[Tuple['pd.Series[Any]', 'pd.Series[np.bool_]']],
]:
    if not has_pandas or not has_numpy:
        raise RuntimeError('pandas must be installed for this operation')
    if not has_accel:
        raise RuntimeError('could not load SingleStoreDB extension')

    numpy_ids, numpy_cols = _singlestoredb_accel.load_rowdat_1_numpy(colspec, data)
    cols = [
        (
            pd.Series(data, name=name, dtype=PANDAS_TYPE_MAP[dtype]),
            pd.Series(mask, dtype=np.bool_),
        )
        for (name, dtype), (data, mask) in zip(colspec, numpy_cols)
    ]
    return pd.Series(numpy_ids, dtype=np.int64), cols


def _dump_pandas_accel(
    returns: List[int],
    row_ids: 'pd.Series[np.int64]',
    cols: List[Tuple['pd.Series[Any]', 'pd.Series[np.bool_]']],
) -> bytes:
    if not has_pandas or not has_numpy:
        raise RuntimeError('pandas must be installed for this operation')
    if not has_accel:
        raise RuntimeError('could not load SingleStoreDB extension')

    numpy_ids = row_ids.to_numpy()
    numpy_cols = [
        (
            data.to_numpy(),
            mask.to_numpy() if mask is not None else None,
        )
        for data, mask in cols
    ]
    return _singlestoredb_accel.dump_rowdat_1_numpy(returns, numpy_ids, numpy_cols)


def _load_polars_accel(
    colspec: List[Tuple[str, int]],
    data: bytes,
) -> Tuple[
    'pl.Series[pl.Int64]',
    List[Tuple['pl.Series[Any]', 'pl.Series[pl.Boolean]']],
]:
    if not has_polars:
        raise RuntimeError('polars must be installed for this operation')
    if not has_accel:
        raise RuntimeError('could not load SingleStoreDB extension')

    numpy_ids, numpy_cols = _singlestoredb_accel.load_rowdat_1_numpy(colspec, data)
    cols = [
        (
            pl.Series(
                name=name, values=data.tolist()
                if dtype in string_types or dtype in binary_types else data,
                dtype=POLARS_TYPE_MAP[dtype],
            ),
            pl.Series(values=mask, dtype=pl.Boolean),
        )
        for (name, dtype), (data, mask) in zip(colspec, numpy_cols)
    ]
    return pl.Series(values=numpy_ids, dtype=pl.Int64), cols


def _dump_polars_accel(
    returns: List[int],
    row_ids: 'pl.Series[pl.Int64]',
    cols: List[Tuple['pl.Series[Any]', 'pl.Series[pl.Boolean]']],
) -> bytes:
    if not has_polars:
        raise RuntimeError('polars must be installed for this operation')
    if not has_accel:
        raise RuntimeError('could not load SingleStoreDB extension')

    numpy_ids = row_ids.to_numpy()
    numpy_cols = [
        (
            data.to_numpy(),
            mask.to_numpy() if mask is not None else None,
        )
        for data, mask in cols
    ]
    return _singlestoredb_accel.dump_rowdat_1_numpy(returns, numpy_ids, numpy_cols)


def _load_arrow_accel(
    colspec: List[Tuple[str, int]],
    data: bytes,
) -> Tuple[
    'pa.Array[pa.int64()]',
    List[Tuple['pa.Array[Any]', 'pa.Array[pa.bool_()]']],
]:
    if not has_pyarrow:
        raise RuntimeError('pyarrow must be installed for this operation')
    if not has_accel:
        raise RuntimeError('could not load SingleStoreDB extension')

    numpy_ids, numpy_cols = _singlestoredb_accel.load_rowdat_1_numpy(colspec, data)
    cols = [
        (
            pa.array(data, type=PYARROW_TYPE_MAP[dtype], mask=mask),
            pa.array(mask, type=pa.bool_()),
        )
        for (data, mask), (name, dtype) in zip(numpy_cols, colspec)
    ]
    return pa.array(numpy_ids, type=pa.int64()), cols


def _create_arrow_mask(
    data: 'pa.Array[Any]',
    mask: 'pa.Array[pa.bool_()]',
) -> 'pa.Array[pa.bool_()]':
    if mask is None:
        return data.is_null().to_numpy(zero_copy_only=False)
    return pc.or_(data.is_null(), mask.is_null()).to_numpy(zero_copy_only=False)


def _dump_arrow_accel(
    returns: List[int],
    row_ids: 'pa.Array[pa.int64()]',
    cols: List[Tuple['pa.Array[Any]', 'pa.Array[pa.bool_()]']],
) -> bytes:
    if not has_pyarrow:
        raise RuntimeError('pyarrow must be installed for this operation')
    if not has_accel:
        raise RuntimeError('could not load SingleStoreDB extension')

    numpy_cols = [
        (
            data.fill_null(DEFAULT_VALUES[dtype]).to_numpy(zero_copy_only=False),
            _create_arrow_mask(data, mask),
        )
        for (data, mask), dtype in zip(cols, returns)
    ]
    return _singlestoredb_accel.dump_rowdat_1_numpy(
        returns, row_ids.to_numpy(), numpy_cols,
    )


if not has_accel:
    load = _load_accel = _load
    dump = _dump_accel = _dump
    load_pandas = _load_pandas_accel = _load_pandas  # noqa: F811
    dump_pandas = _dump_pandas_accel = _dump_pandas  # noqa: F811
    load_numpy = _load_numpy_accel = _load_numpy  # noqa: F811
    dump_numpy = _dump_numpy_accel = _dump_numpy  # noqa: F811
    load_arrow = _load_arrow_accel = _load_arrow  # noqa: F811
    dump_arrow = _dump_arrow_accel = _dump_arrow  # noqa: F811
    load_polars = _load_polars_accel = _load_polars  # noqa: F811
    dump_polars = _dump_polars_accel = _dump_polars  # noqa: F811

else:
    _load_accel = _singlestoredb_accel.load_rowdat_1
    _dump_accel = _singlestoredb_accel.dump_rowdat_1
    load = _load_accel
    dump = _dump_accel
    load_pandas = _load_pandas_accel
    dump_pandas = _dump_pandas_accel
    load_numpy = _load_numpy_accel
    dump_numpy = _dump_numpy_accel
    load_arrow = _load_arrow_accel
    dump_arrow = _dump_arrow_accel
    load_polars = _load_polars_accel
    dump_polars = _dump_polars_accel
