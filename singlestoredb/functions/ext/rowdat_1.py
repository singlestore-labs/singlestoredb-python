#!/usr/bin/env python3
import struct
import warnings
from collections.abc import Sequence
from io import BytesIO
from typing import Any
from typing import List
from typing import Optional
from typing import Tuple
from typing import TYPE_CHECKING

from ...config import get_option
from ...mysql.constants import FIELD_TYPE as ft
from ..dtypes import DEFAULT_VALUES
from ..dtypes import NUMPY_TYPE_MAP
from ..dtypes import PANDAS_TYPE_MAP
from ..dtypes import POLARS_TYPE_MAP
from ..dtypes import PYARROW_TYPE_MAP
from .utils import apply_transformer
from .utils import Transformer

if TYPE_CHECKING:
    try:
        import numpy as np
    except ImportError:
        pass
    try:
        import polars as pl
    except ImportError:
        pass
    try:
        import pandas as pd
    except ImportError:
        pass
    try:
        import pyarrow as pa
    except ImportError:
        pass
    try:
        import pyarrow.compute as pc  # noqa: F401
    except ImportError:
        pass

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
medium_int_types = {ft.INT24, -ft.INT24}
int_types = {
    ft.TINY, -ft.TINY, ft.SHORT, -ft.SHORT, ft.INT24, -ft.INT24, ft.LONG,
    -ft.LONG, ft.LONGLONG, -ft.LONGLONG,
}
string_types = {15, 245, 247, 248, 249, 250, 251, 252, 253, 254}
binary_types = set([-x for x in string_types])


def _load(
    colspec: List[Tuple[str, int, Optional[Transformer]]],
    data: bytes,
) -> Tuple[List[int], List[Any]]:
    '''
    Convert bytes in rowdat_1 format into rows of data.

    Parameters
    ----------
    colspec : List[Tuple[str, int, Optional[Transformer]]]
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
        for _, ctype, transformer in colspec:
            is_null = data_io.read(1) == b'\x01'
            if ctype in numeric_formats:
                val = apply_transformer(
                    transformer,
                    struct.unpack(
                        numeric_formats[ctype],
                        data_io.read(numeric_sizes[ctype]),
                    )[0],
                )
            elif ctype in string_types:
                slen = struct.unpack('<q', data_io.read(8))[0]
                val = apply_transformer(
                    transformer,
                    data_io.read(slen).decode('utf-8'),
                )
            elif ctype in binary_types:
                slen = struct.unpack('<q', data_io.read(8))[0]
                val = apply_transformer(
                    transformer,
                    data_io.read(slen),
                )
            else:
                raise TypeError(f'unrecognized column type: {ctype}')
            row.append(None if is_null else val)
        rows.append(row)
    return row_ids, rows


def _load_vectors(
    colspec: List[Tuple[str, int, Optional[Transformer]]],
    data: bytes,
) -> Tuple[List[int], List[Tuple[Sequence[Any], Optional[Sequence[Any]]]]]:
    '''
    Convert bytes in rowdat_1 format into columns of data.

    Parameters
    ----------
    colspec : List[str, int, Optional[Transformer]]
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
        for i, (_, ctype, transformer) in enumerate(colspec):
            default = DEFAULT_VALUES[ctype]
            is_null = data_io.read(1) == b'\x01'
            if ctype in numeric_formats:
                val = apply_transformer(
                    transformer,
                    struct.unpack(
                        numeric_formats[ctype],
                        data_io.read(numeric_sizes[ctype]),
                    )[0],
                )
            elif ctype in string_types:
                slen = struct.unpack('<q', data_io.read(8))[0]
                val = apply_transformer(
                    transformer,
                    data_io.read(slen).decode('utf-8'),
                )
            elif ctype in binary_types:
                slen = struct.unpack('<q', data_io.read(8))[0]
                val = apply_transformer(
                    transformer,
                    data_io.read(slen),
                )
            else:
                raise TypeError(f'unrecognized column type: {ctype}')
            cols[i].append(default if is_null else val)
            masks[i].append(True if is_null else False)
    return row_ids, [(x, y) for x, y in zip(cols, masks)]


def _load_pandas(
    colspec: List[Tuple[str, int, Optional[Transformer]]],
    data: bytes,
) -> Tuple[
    'pd.Series[np.int64]',
    List[Tuple['pd.Series[Any]', 'pd.Series[np.bool_]']],
]:
    '''
    Convert bytes in rowdat_1 format into rows of data.

    Parameters
    ----------
    colspec : List[str, int, Optional[Transformer]]
        An List of column data types
    data : bytes
        The data in rowdat_1 format

    Returns
    -------
    Tuple[pd.Series[int], List[Tuple[pd.Series[Any], pd.Series[bool]]]]

    '''
    import numpy as np
    import pandas as pd

    row_ids, cols = _load_vectors(colspec, data)
    index = pd.Series(row_ids)
    return pd.Series(row_ids, dtype=np.int64), [
        (
            pd.Series(data, index=index, name=name, dtype=PANDAS_TYPE_MAP[dtype]),
            pd.Series(mask, index=index, dtype=np.bool_),
        )
        for (data, mask), (name, dtype, _) in zip(cols, colspec)
    ]


def _load_polars(
    colspec: List[Tuple[str, int, Optional[Transformer]]],
    data: bytes,
) -> Tuple[
    'pl.Series[pl.Int64]',
    List[Tuple['pl.Series[Any]', 'pl.Series[pl.Boolean]']],
]:
    '''
    Convert bytes in rowdat_1 format into rows of data.

    Parameters
    ----------
    colspec : List[str, int, Optional[Transformer]]
        An List of column data types
    data : bytes
        The data in rowdat_1 format

    Returns
    -------
    Tuple[polars.Series[int], List[polars.Series[Any]]]

    '''
    import polars as pl

    row_ids, cols = _load_vectors(colspec, data)

    return pl.Series(None, row_ids, dtype=pl.Int64), \
        [
            (
                pl.Series(name=name, values=data, dtype=POLARS_TYPE_MAP[dtype]),
                pl.Series(values=mask, dtype=pl.Boolean),
            )
            for (data, mask), (name, dtype, _) in zip(cols, colspec)
        ]


def _load_numpy(
    colspec: List[Tuple[str, int, Optional[Transformer]]],
    data: bytes,
) -> Tuple[
    'np.typing.NDArray[np.int64]',
    List[Tuple['np.typing.NDArray[Any]', 'np.typing.NDArray[np.bool_]']],
]:
    '''
    Convert bytes in rowdat_1 format into rows of data.

    Parameters
    ----------
    colspec : List[str, int, Optional[Transformer]]
        An List of column data types
    data : bytes
        The data in rowdat_1 format

    Returns
    -------
    Tuple[np.ndarray[int], List[np.ndarray[Any]]]

    '''
    import numpy as np

    row_ids, cols = _load_vectors(colspec, data)

    return np.asarray(row_ids, dtype=np.int64), \
        [
            (
                np.asarray(data, dtype=NUMPY_TYPE_MAP[dtype]),  # type: ignore
                np.asarray(mask, dtype=np.bool_),  # type: ignore
            )
            for (data, mask), (name, dtype, _) in zip(cols, colspec)
        ]


def _load_arrow(
    colspec: List[Tuple[str, int, Optional[Transformer]]],
    data: bytes,
) -> Tuple[
    'pa.Array[pa.int64]',
    List[Tuple['pa.Array[Any]', 'pa.Array[pa.bool_]']],
]:
    '''
    Convert bytes in rowdat_1 format into rows of data.

    Parameters
    ----------
    colspec : List[str, int, Optional[Transformer]]
        An List of column data types
    data : bytes
        The data in rowdat_1 format

    Returns
    -------
    Tuple[pyarrow.Array[int], List[pyarrow.Array[Any]]]

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
            for (data, mask), (name, dtype, _) in zip(cols, colspec)
        ]


def _dump(
    returns: List[Tuple[str, int, Optional[Transformer]]],
    row_ids: List[int],
    rows: List[List[Any]],
) -> bytes:
    '''
    Convert a list of lists of data into rowdat_1 format.

    Parameters
    ----------
    returns : List[Tuple[str, int, Optional[Transformer]]]
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
        return out.getvalue()

    for row_id, *values in zip(row_ids, *list(zip(*rows))):
        out.write(struct.pack('<q', row_id))
        for (_, rtype, transformer), value in zip(returns, values):
            out.write(b'\x01' if value is None else b'\x00')
            default = DEFAULT_VALUES[rtype]
            value = apply_transformer(transformer, value)
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

    return out.getvalue()


def _dump_vectors(
    returns: List[Tuple[str, int, Optional[Transformer]]],
    row_ids: List[int],
    cols: List[Tuple[Sequence[Any], Optional[Sequence[Any]]]],
) -> bytes:
    '''
    Convert a list of columns of data into rowdat_1 format.

    Parameters
    ----------
    returns : List[Tuple[str, int, Optional[Transformer]]]
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
        return out.getvalue()

    for j, row_id in enumerate(row_ids):

        out.write(struct.pack('<q', row_id))

        for i, (_, rtype, transformer) in enumerate(returns):
            value = apply_transformer(transformer, cols[i][0][j])
            if cols[i][1] is not None:
                is_null = cols[i][1][j]  # type: ignore
            else:
                is_null = False

            out.write(b'\x01' if is_null or value is None else b'\x00')
            default = DEFAULT_VALUES[rtype]
            try:
                if rtype in numeric_formats:
                    if is_null or value is None:
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
                            out.write(
                                struct.pack(numeric_formats[rtype], int(value)),
                            )
                        else:
                            out.write(
                                struct.pack(
                                    numeric_formats[rtype], float(value),
                                ),
                            )
                elif rtype in string_types:
                    if is_null or value is None:
                        out.write(struct.pack('<q', 0))
                    else:
                        sval = value.encode('utf-8')
                        out.write(struct.pack('<q', len(sval)))
                        out.write(sval)
                elif rtype in binary_types:
                    if is_null or value is None:
                        out.write(struct.pack('<q', 0))
                    else:
                        out.write(struct.pack('<q', len(value)))
                        out.write(value)
                else:
                    raise TypeError(f'unrecognized column type: {rtype}')

            except struct.error as exc:
                raise ValueError(str(exc))

    return out.getvalue()


def _dump_arrow(
    returns: List[Tuple[str, int, Optional[Transformer]]],
    row_ids: 'pa.Array[int]',
    cols: List[Tuple['pa.Array[Any]', 'pa.Array[bool]']],
) -> bytes:
    return _dump_vectors(
        returns,
        row_ids.tolist(),
        [(x.tolist(), y.tolist() if y is not None else None) for x, y in cols],
    )


def _dump_numpy(
    returns: List[Tuple[str, int, Optional[Transformer]]],
    row_ids: 'np.typing.NDArray[np.int64]',
    cols: List[Tuple['np.typing.NDArray[Any]', 'np.typing.NDArray[np.bool_]']],
) -> bytes:
    return _dump_vectors(
        returns,
        row_ids.tolist(),
        [(x.tolist(), y.tolist() if y is not None else None) for x, y in cols],
    )


def _dump_pandas(
    returns: List[Tuple[str, int, Optional[Transformer]]],
    row_ids: 'pd.Series[np.int64]',
    cols: List[Tuple['pd.Series[Any]', 'pd.Series[np.bool_]']],
) -> bytes:
    return _dump_vectors(
        returns,
        row_ids.to_list(),
        [(x.to_list(), y.to_list() if y is not None else None) for x, y in cols],
    )


def _dump_polars(
    returns: List[Tuple[str, int, Optional[Transformer]]],
    row_ids: 'pl.Series[pl.Int64]',
    cols: List[Tuple['pl.Series[Any]', 'pl.Series[pl.Boolean]']],
) -> bytes:
    return _dump_vectors(
        returns,
        row_ids.to_list(),
        [(x.to_list(), y.to_list() if y is not None else None) for x, y in cols],
    )


def _load_numpy_accel(
    colspec: List[Tuple[str, int, Optional[Transformer]]],
    data: bytes,
) -> Tuple[
    'np.typing.NDArray[np.int64]',
    List[Tuple['np.typing.NDArray[Any]', 'np.typing.NDArray[np.bool_]']],
]:
    if not has_accel:
        raise RuntimeError('could not load SingleStoreDB extension')

    import numpy as np

    numpy_ids, numpy_cols = _singlestoredb_accel.load_rowdat_1_numpy(colspec, data)

    for i, (_, dtype, transformer) in enumerate(colspec):
        if transformer is not None:
            # Numpy will try to be "helpful" and create multidimensional arrays
            # from nested iterables. We don't usually want that. What we want is
            # numpy arrays of Python objects (e.g., lists, dicts, etc). To do that,
            # we have to create an empty array of the correct length and dtype=object,
            # then fill it in with the transformed values. The transformer may have
            # an output_type attribute that we can use to create a more specific type.
            if getattr(transformer, 'output_type', None):
                new_col = np.empty(
                    len(numpy_cols[i][0]),
                    dtype=transformer.output_type,  # type: ignore
                )
                new_col[:] = list(map(transformer, numpy_cols[i][0]))
            else:
                new_col = np.array(list(map(transformer, numpy_cols[i][0])))
            numpy_cols[i] = (new_col, numpy_cols[i][1])

    return numpy_ids, numpy_cols


def _dump_numpy_accel(
    returns: List[Tuple[str, int, Optional[Transformer]]],
    row_ids: 'np.typing.NDArray[np.int64]',
    cols: List[Tuple['np.typing.NDArray[Any]', 'np.typing.NDArray[np.bool_]']],
) -> bytes:
    if not has_accel:
        raise RuntimeError('could not load SingleStoreDB extension')

    import numpy as np

    for i, (_, dtype, transformer) in enumerate(returns):
        if transformer is not None:
            cols[i] = (np.array(list(map(transformer, cols[i][0]))), cols[i][1])

    return _singlestoredb_accel.dump_rowdat_1_numpy(returns, row_ids, cols)


def _load_pandas_accel(
    colspec: List[Tuple[str, int, Optional[Transformer]]],
    data: bytes,
) -> Tuple[
    'pd.Series[np.int64]',
    List[Tuple['pd.Series[Any]', 'pd.Series[np.bool_]']],
]:
    if not has_accel:
        raise RuntimeError('could not load SingleStoreDB extension')

    import numpy as np
    import pandas as pd

    numpy_ids, numpy_cols = _load_numpy_accel(colspec, data)

    cols = [
        (
            pd.Series(data, name=name, dtype=PANDAS_TYPE_MAP[dtype]),
            pd.Series(mask, dtype=np.bool_),
        )
        for (name, dtype, _), (data, mask) in zip(colspec, numpy_cols)
    ]

    return pd.Series(numpy_ids, dtype=np.int64), cols


def _dump_pandas_accel(
    returns: List[Tuple[str, int, Optional[Transformer]]],
    row_ids: 'pd.Series[np.int64]',
    cols: List[Tuple['pd.Series[Any]', 'pd.Series[np.bool_]']],
) -> bytes:
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

    return _dump_numpy_accel(returns, numpy_ids, numpy_cols)


def _load_polars_accel(
    colspec: List[Tuple[str, int, Optional[Transformer]]],
    data: bytes,
) -> Tuple[
    'pl.Series[pl.Int64]',
    List[Tuple['pl.Series[Any]', 'pl.Series[pl.Boolean]']],
]:
    if not has_accel:
        raise RuntimeError('could not load SingleStoreDB extension')

    import polars as pl

    numpy_ids, numpy_cols = _load_numpy_accel(colspec, data)

    cols = [
        (
            pl.Series(
                name=name, values=data.tolist()
                if dtype in string_types or dtype in binary_types else data,
                dtype=POLARS_TYPE_MAP[dtype],
            ),
            pl.Series(values=mask, dtype=pl.Boolean),
        )
        for (name, dtype, _), (data, mask) in zip(colspec, numpy_cols)
    ]

    return pl.Series(values=numpy_ids, dtype=pl.Int64), cols


def _dump_polars_accel(
    returns: List[Tuple[str, int, Optional[Transformer]]],
    row_ids: 'pl.Series[pl.Int64]',
    cols: List[Tuple['pl.Series[Any]', 'pl.Series[pl.Boolean]']],
) -> bytes:
    if not has_accel:
        raise RuntimeError('could not load SingleStoreDB extension')

    import numpy as np
    import polars as pl

    numpy_ids = row_ids.to_numpy()
    numpy_cols = [
        (
            # Polars will try to be "helpful" and convert nested iterables into
            # multidimensional arrays. We don't usually want that. What we want is
            # numpy arrays of Python objects (e.g., lists, dicts, etc). To
            # do that, we have to convert the Series to a list first.
            np.array(data.to_list())
            if isinstance(data.dtype, (pl.Struct, pl.Object)) else data.to_numpy(),
            mask.to_numpy() if mask is not None else None,
        )
        for data, mask in cols
    ]

    return _dump_numpy_accel(returns, numpy_ids, numpy_cols)


def _load_arrow_accel(
    colspec: List[Tuple[str, int, Optional[Transformer]]],
    data: bytes,
) -> Tuple[
    'pa.Array[pa.int64]',
    List[Tuple['pa.Array[Any]', 'pa.Array[pa.bool_]']],
]:
    if not has_accel:
        raise RuntimeError('could not load SingleStoreDB extension')

    import pyarrow as pa

    numpy_ids, numpy_cols = _load_numpy_accel(colspec, data)
    cols = [
        (
            pa.array(data, type=PYARROW_TYPE_MAP[dtype], mask=mask),
            pa.array(mask, type=pa.bool_()),
        )
        for (data, mask), (name, dtype, _) in zip(numpy_cols, colspec)
    ]
    return pa.array(numpy_ids, type=pa.int64()), cols


def _create_arrow_mask(
    data: 'pa.Array[Any]',
    mask: 'pa.Array[pa.bool_]',
) -> 'pa.Array[pa.bool_]':
    import pyarrow.compute as pc  # noqa: F811

    if mask is None:
        return data.is_null().to_numpy(zero_copy_only=False)

    return pc.or_(data.is_null(), mask).to_numpy(zero_copy_only=False)


def _dump_arrow_accel(
    returns: List[Tuple[str, int, Optional[Transformer]]],
    row_ids: 'pa.Array[pa.int64]',
    cols: List[Tuple['pa.Array[Any]', 'pa.Array[pa.bool_]']],
) -> bytes:
    if not has_accel:
        raise RuntimeError('could not load SingleStoreDB extension')

    numpy_cols = [
        (
            data.fill_null(DEFAULT_VALUES[dtype]).to_numpy(zero_copy_only=False),
            _create_arrow_mask(data, mask),
        )
        for (data, mask), (_, dtype, _) in zip(cols, returns)
    ]

    return _dump_numpy_accel(returns, row_ids.to_numpy(), numpy_cols)


def _dump_rowdat_1_accel(
    returns: List[Tuple[str, int, Optional[Transformer]]],
    row_ids: List[int],
    rows: List[List[Any]],
) -> bytes:
    # C function now handles transformers internally
    return _singlestoredb_accel.dump_rowdat_1(returns, row_ids, rows)


def _load_rowdat_1_accel(
    colspec: List[Tuple[str, int, Optional[Transformer]]],
    data: bytes,
) -> Tuple[List[int], List[Any]]:
    # C function now handles transformers internally
    return _singlestoredb_accel.load_rowdat_1(colspec, data)


if not has_accel:
    load = _load_accel = _load
    dump = _dump_accel = _dump
    load_list = _load_vectors  # noqa: F811
    dump_list = _dump_vectors  # noqa: F811
    load_pandas = _load_pandas_accel = _load_pandas  # noqa: F811
    dump_pandas = _dump_pandas_accel = _dump_pandas  # noqa: F811
    load_numpy = _load_numpy_accel = _load_numpy  # noqa: F811
    dump_numpy = _dump_numpy_accel = _dump_numpy  # noqa: F811
    load_arrow = _load_arrow_accel = _load_arrow  # noqa: F811
    dump_arrow = _dump_arrow_accel = _dump_arrow  # noqa: F811
    load_polars = _load_polars_accel = _load_polars  # noqa: F811
    dump_polars = _dump_polars_accel = _dump_polars  # noqa: F811

else:
    _load_accel = _load_rowdat_1_accel
    _dump_accel = _dump_rowdat_1_accel
    load = _load_accel
    dump = _dump_accel
    load_list = _load_vectors
    dump_list = _dump_vectors
    load_pandas = _load_pandas_accel
    dump_pandas = _dump_pandas_accel
    load_numpy = _load_numpy_accel
    dump_numpy = _dump_numpy_accel
    load_arrow = _load_arrow_accel
    dump_arrow = _dump_arrow_accel
    load_polars = _load_polars_accel
    dump_polars = _dump_polars_accel
