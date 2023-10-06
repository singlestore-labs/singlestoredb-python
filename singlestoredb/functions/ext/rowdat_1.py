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


def _load(colspec: Iterable[Tuple[str, int]], data: bytes) -> Tuple[List[int], List[Any]]:
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
    list[list[Any]]

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


def _dump(returns: Iterable[int], data: Iterable[Tuple[int, Any]]) -> bytes:
    '''
    Convert a list of lists of data into rowdat_1 format.

    Parameters
    ----------
    returns : str
        sThe returned data type
    data : list[list[Any]]
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


if _singlestoredb_accel is None:
    load = _load
    dump = _dump
else:
    load = _singlestoredb_accel.load_rowdat_1
    dump = _singlestoredb_accel.dump_rowdat_1
