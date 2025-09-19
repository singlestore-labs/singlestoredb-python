#!/usr/bin/env python3
from io import BytesIO
from typing import Any
from typing import List
from typing import Optional
from typing import Tuple

from singlestoredb.functions.ext.utils import apply_transformer
from singlestoredb.functions.ext.utils import Transformer

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
    import pyarrow.feather
    has_pyarrow = True
except ImportError:
    has_pyarrow = False


def load(
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
        The data in Apache Feather format

    Returns
    -------
    Tuple[List[int], List[Any]]

    '''
    if not has_pyarrow:
        raise RuntimeError('pyarrow must be installed for this operation')

    table = pa.feather.read_table(BytesIO(data))
    row_ids = table.column(0).to_pylist()
    rows = []
    for row in table.to_pylist():
        converted_row = []
        for i, col_name in enumerate(table.column_names[1:]):
            value = apply_transformer(
                colspec[i][2],
                row[col_name],
            )
            converted_row.append(value)
        rows.append(converted_row)
    return row_ids, rows


def _load_vectors(
    colspec: List[Tuple[str, int, Optional[Transformer]]],
    data: bytes,
) -> Tuple[
    'pa.Array[pa.int64]',
    List[Tuple['pa.Array[Any]', 'pa.Array[pa.bool_]']],
]:
    '''
    Convert bytes in rowdat_1 format into columns of data.

    Parameters
    ----------
    colspec : List[Tuple[str, int, Optional[Transformer]]]
        An List of column data types
    data : bytes
        The data in Apache Feather format

    Returns
    -------
    Tuple[List[int], List[Tuple[Any, Any]]]

    '''
    if not has_pyarrow:
        raise RuntimeError('pyarrow must be installed for this operation')

    import pyarrow as pa

    table = pa.feather.read_table(BytesIO(data))
    row_ids = table.column(0)
    out = []
    for i, col in enumerate(table.columns[1:]):
        out.append((
            apply_transformer(
                colspec[i][2], col,
            ), col.is_null(),
        ))

    return row_ids, out


def load_pandas(
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
    colspec : List[Tuple[str, int, Optional[Transformer]]]
        An List of column data types
    data : bytes
        The data in Apache Feather format

    Returns
    -------
    Tuple[pd.Series[int], List[Tuple[pd.Series[Any], pd.Series[bool]]]]

    '''
    if not has_pandas or not has_numpy:
        raise RuntimeError('pandas must be installed for this operation')

    row_ids, cols = _load_vectors(colspec, data)
    index = row_ids.to_pandas()

    return index, \
        [
            (
                data.to_pandas().reindex(index),
                mask.to_pandas().reindex(index),
            )
            for (data, mask), (name, dtype, _) in zip(cols, colspec)
        ]


def load_polars(
    colspec: List[Tuple[str, int, Optional[Transformer]]],
    data: bytes,
) -> Tuple[
    'pl.Series[pl.Int64]',
    List[Tuple['pl.Series[Any]', 'pl.Series[pl.Boolean]']],
]:
    '''
    Convert bytes in Apache Feather format into rows of data.

    Parameters
    ----------
    colspec : List[Tuple[str, int, Optional[Transformer]]]
        An List of column data types
    data : bytes
        The data in Apache Feather format

    Returns
    -------
    Tuple[polars.Series[int], List[polars.Series[Any]]]

    '''
    if not has_polars:
        raise RuntimeError('polars must be installed for this operation')

    row_ids, cols = _load_vectors(colspec, data)

    return (
        pl.from_arrow(row_ids),  # type: ignore
        [
            (
                pl.from_arrow(data),  # type: ignore
                pl.from_arrow(mask),  # type: ignore
            )
            for (data, mask), (name, dtype, _) in zip(cols, colspec)
        ],
    )


def load_numpy(
    colspec: List[Tuple[str, int, Optional[Transformer]]],
    data: bytes,
) -> Tuple[
    'np.typing.NDArray[np.int64]',
    List[Tuple['np.typing.NDArray[Any]', 'np.typing.NDArray[np.bool_]']],
]:
    '''
    Convert bytes in Apache Feather format into rows of data.

    Parameters
    ----------
    colspec : List[Tuple[str, int, Optional[Transformer]]]
        An List of column data types
    data : bytes
        The data in Apache Feather format

    Returns
    -------
    Tuple[np.ndarray[int], List[np.ndarray[Any]]]

    '''
    if not has_numpy:
        raise RuntimeError('numpy must be installed for this operation')

    row_ids, cols = _load_vectors(colspec, data)

    return row_ids.to_numpy(), \
        [
            (
                data.to_numpy(),
                mask.to_numpy(),
            )
            for (data, mask), (name, dtype, _) in zip(cols, colspec)
        ]


def load_arrow(
    colspec: List[Tuple[str, int, Optional[Transformer]]],
    data: bytes,
) -> Tuple[
    'pa.Array[pa.int64()]',
    List[Tuple['pa.Array[Any]', 'pa.Array[pa.bool_()]']],
]:
    '''
    Convert bytes in Apache Feather format into rows of data.

    Parameters
    ----------
    colspec : List[Tuple[str, int, Optional[Transformer]]]
        An List of column data types
    data : bytes
        The data in Apache Feather format

    Returns
    -------
    Tuple[pyarrow.Array[int], List[pyarrow.Array[Any]]]

    '''
    if not has_pyarrow:
        raise RuntimeError('pyarrow must be installed for this operation')

    return _load_vectors(colspec, data)


def dump(
    returns: List[Tuple[int, Optional[Transformer]]],
    row_ids: List[int],
    rows: List[List[Any]],
) -> bytes:
    '''
    Convert a list of lists of data into Apache Feather format.

    Parameters
    ----------
    returns : List[Tuple[int, Optional[Transformer]]]
        The returned data type
    row_ids : List[int]
        The row IDs
    rows : List[List[Any]]
        The rows of data and masks to serialize

    Returns
    -------
    bytes

    '''
    if not has_pyarrow:
        raise RuntimeError('pyarrow must be installed for this operation')

    if len(rows) == 0 or len(row_ids) == 0:
        return BytesIO().getvalue()

    colnames = ['col{}'.format(x) for x in range(len(rows[0]))]

    # Process rows to handle JSON serialization
    processed_rows = []
    for row in rows:
        processed_row = []
        for i, value in enumerate(row):
            processed_row.append(
                apply_transformer(
                    returns[i][1], value,
                ),
            )
        processed_rows.append(processed_row)

    tbl = pa.Table.from_pylist([dict(list(zip(colnames, row))) for row in processed_rows])
    tbl = tbl.add_column(0, '__index__', pa.array(row_ids))

    sink = pa.BufferOutputStream()
    batches = tbl.to_batches()
    with pa.ipc.new_file(sink, batches[0].schema) as writer:
        for batch in batches:
            writer.write_batch(batch)
    return sink.getvalue()


def _dump_vectors(
    returns: List[Tuple[int, Optional[Transformer]]],
    row_ids: 'pa.Array[pa.int64]',
    cols: List[Tuple['pa.Array[Any]', Optional['pa.Array[pa.bool_]']]],
) -> bytes:
    '''
    Convert a list of columns of data into Apache Feather format.

    Parameters
    ----------
    returns : List[Tuple[int, Optional[Transformer]]]
        The returned data type
    row_ids : List[int]
        The row IDs
    cols : List[Tuple[Any, Any]]
        The rows of data and masks to serialize

    Returns
    -------
    bytes

    '''
    if not has_pyarrow:
        raise RuntimeError('pyarrow must be installed for this operation')

    if len(cols) == 0 or len(row_ids) == 0:
        return BytesIO().getvalue()

    # Process columns to handle JSON serialization
    processed_cols = []
    for i, (data, mask) in enumerate(cols):
        processed_cols.append((
            apply_transformer(
                returns[i][1], data,
            ), mask,
        ))

    tbl = pa.Table.from_arrays(
        [pa.array(data, mask=mask) for data, mask in processed_cols],
        names=['col{}'.format(x) for x in range(len(cols))],
    )
    tbl = tbl.add_column(0, '__index__', row_ids)

    sink = pa.BufferOutputStream()
    batches = tbl.to_batches()
    with pa.ipc.new_file(sink, batches[0].schema) as writer:
        for batch in batches:
            writer.write_batch(batch)
    return sink.getvalue()


def dump_arrow(
    returns: List[Tuple[int, Optional[Transformer]]],
    row_ids: 'pa.Array[int]',
    cols: List[Tuple['pa.Array[Any]', 'pa.Array[bool]']],
) -> bytes:
    if not has_pyarrow:
        raise RuntimeError('pyarrow must be installed for this operation')

    return _dump_vectors(returns, row_ids, cols)


def dump_numpy(
    returns: List[Tuple[int, Optional[Transformer]]],
    row_ids: 'np.typing.NDArray[np.int64]',
    cols: List[Tuple['np.typing.NDArray[Any]', 'np.typing.NDArray[np.bool_]']],
) -> bytes:
    if not has_numpy:
        raise RuntimeError('numpy must be installed for this operation')

    return _dump_vectors(
        returns,
        pa.array(row_ids),
        [(pa.array(x), pa.array(y) if y is not None else None) for x, y in cols],
    )


def dump_pandas(
    returns: List[Tuple[int, Optional[Transformer]]],
    row_ids: 'pd.Series[np.int64]',
    cols: List[Tuple['pd.Series[Any]', 'pd.Series[np.bool_]']],
) -> bytes:
    if not has_pandas or not has_numpy:
        raise RuntimeError('pandas must be installed for this operation')

    return _dump_vectors(
        returns,
        pa.array(row_ids),
        [(pa.array(x), pa.array(y) if y is not None else None) for x, y in cols],
    )


def dump_polars(
    returns: List[Tuple[int, Optional[Transformer]]],
    row_ids: 'pl.Series[pl.Int64]',
    cols: List[Tuple['pl.Series[Any]', 'pl.Series[pl.Boolean]']],
) -> bytes:
    if not has_polars:
        raise RuntimeError('polars must be installed for this operation')

    return _dump_vectors(
        returns,
        row_ids.to_arrow(),
        [(x.to_arrow(), y.to_arrow() if y is not None else None) for x, y in cols],
    )
