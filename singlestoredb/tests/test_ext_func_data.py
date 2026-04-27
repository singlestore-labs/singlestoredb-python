#!/usr/bin/env python
# type: ignore
"""Test external function data parsing and formatting"""
import datetime
import decimal
import json
import unittest

import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa
from numpy.testing import assert_array_equal
from parameterized import parameterized

from singlestoredb.functions.ext import json as jsonx
from singlestoredb.functions.ext import rowdat_1
from singlestoredb.functions.ext.json import JSONEncoder
from singlestoredb.functions.ext.rowdat_1 import _pack_date
from singlestoredb.functions.ext.rowdat_1 import _pack_datetime
from singlestoredb.functions.ext.rowdat_1 import _pack_time
from singlestoredb.functions.ext.rowdat_1 import _unpack_date
from singlestoredb.functions.ext.rowdat_1 import _unpack_datetime
from singlestoredb.functions.ext.rowdat_1 import _unpack_time


TINYINT = 1
UNSIGNED_TINYINT = -1
SMALLINT = 2
UNSIGNED_SMALLINT = -2
MEDIUMINT = 9
UNSIGNED_MEDIUMINT = -9
INT = 3
UNSIGNED_INT = -3
BIGINT = 8
UNSIGNED_BIGINT = -8
FLOAT = 4
DOUBLE = 5
STRING = 254
BINARY = -254

col_spec = [
    ('tiny', TINYINT),
    ('unsigned_tiny', UNSIGNED_TINYINT),
    ('short', SMALLINT),
    ('unsigned_short', UNSIGNED_SMALLINT),
    ('long', INT),
    ('unsigned_long', UNSIGNED_INT),
    ('float', FLOAT),
    ('double', DOUBLE),
    ('longlong', BIGINT),
    ('unsigned_longlong', UNSIGNED_BIGINT),
    ('int24', MEDIUMINT),
    ('unsigned_int24', UNSIGNED_MEDIUMINT),
    ('string', STRING),
    ('binary', BINARY),
]

col_types = [x[1] for x in col_spec]
col_names = [x[0] for x in col_spec]

numpy_row_ids = np.array([1, 2, 3, 4])
numpy_nulls = np.array([False, False, False, True])

numpy_tiny_arr = np.array([-1, 100, 120, 0], dtype=np.int8)
numpy_unsigned_tiny_arr = np.array([1, 100, 130, 0], dtype=np.uint8)
numpy_short_arr = np.array([-1, 32700, 254, 0], dtype=np.int16)
numpy_unsigned_short_arr = np.array([1, 32800, 254, 0], dtype=np.uint16)
numpy_long_arr = np.array([-1, 2147483600, 254, 0], dtype=np.int32)
numpy_unsigned_long_arr = np.array([1, 2147483800, 254, 0], dtype=np.uint32)
numpy_float_arr = np.array([-1, 100, 3.14159, np.nan], dtype=np.float32)
numpy_double_arr = np.array([-1, 100, 3.14159, np.nan], dtype=np.float64)
numpy_longlong_arr = np.array([-1, 100, 9223372036854775000, 0], dtype=np.int64)
numpy_unsigned_longlong_arr = np.array([1, 100, 9223372036854776000, 0], dtype=np.uint64)
numpy_int24_arr = np.array([-1, 8388600, 254, 0], dtype=np.int32)
numpy_unsigned_int24_arr = np.array([1, 16777210, 254, 0], dtype=np.uint32)
numpy_string_arr = np.array(['hi', 'bye', 'foo', None], dtype=object)
numpy_binary_arr = np.array([b'hi', b'bye', b'foo', None], dtype=object)

numpy_data = [
    (numpy_tiny_arr, numpy_nulls),
    (numpy_unsigned_tiny_arr, numpy_nulls),
    (numpy_short_arr, numpy_nulls),
    (numpy_unsigned_short_arr, numpy_nulls),
    (numpy_long_arr, numpy_nulls),
    (numpy_unsigned_long_arr, numpy_nulls),
    (numpy_float_arr, numpy_nulls),
    (numpy_double_arr, numpy_nulls),
    (numpy_longlong_arr, numpy_nulls),
    (numpy_unsigned_longlong_arr, numpy_nulls),
    (numpy_int24_arr, numpy_nulls),
    (numpy_unsigned_int24_arr, numpy_nulls),
    (numpy_string_arr, numpy_nulls),
    (numpy_binary_arr, numpy_nulls),
]

polars_row_ids = pl.Series(None, [1, 2, 3, 4], dtype=pl.Int64)
polars_nulls = pl.Series(None, numpy_nulls, dtype=pl.Boolean)

polars_tiny_arr = \
    pl.Series(None, numpy_tiny_arr, dtype=pl.Int8)
polars_unsigned_tiny_arr = \
    pl.Series(None, numpy_unsigned_tiny_arr, dtype=pl.UInt8)
polars_short_arr = \
    pl.Series(None, numpy_short_arr, dtype=pl.Int16)
polars_unsigned_short_arr = \
    pl.Series(None, numpy_unsigned_short_arr, dtype=pl.UInt16)
polars_long_arr = \
    pl.Series(None, numpy_long_arr, dtype=pl.Int32)
polars_unsigned_long_arr = \
    pl.Series(None, numpy_unsigned_long_arr, dtype=pl.UInt32)
polars_float_arr = \
    pl.Series(None, numpy_float_arr, dtype=pl.Float32)
polars_double_arr = \
    pl.Series(None, numpy_double_arr, dtype=pl.Float64)
polars_longlong_arr = \
    pl.Series(None, numpy_longlong_arr, dtype=pl.Int64)
polars_unsigned_longlong_arr = \
    pl.Series(None, numpy_unsigned_longlong_arr, dtype=pl.UInt64)
polars_int24_arr = \
    pl.Series(None, numpy_int24_arr, dtype=pl.Int32)
polars_unsigned_int24_arr = \
    pl.Series(None, numpy_unsigned_int24_arr, dtype=pl.UInt32)
polars_string_arr = \
    pl.Series(None, numpy_string_arr.tolist(), dtype=pl.Utf8)
polars_binary_arr = \
    pl.Series(None, numpy_binary_arr.tolist(), dtype=pl.Binary)

polars_data = [
    (polars_tiny_arr, polars_nulls),
    (polars_unsigned_tiny_arr, polars_nulls),
    (polars_short_arr, polars_nulls),
    (polars_unsigned_short_arr, polars_nulls),
    (polars_long_arr, polars_nulls),
    (polars_unsigned_long_arr, polars_nulls),
    (polars_float_arr, polars_nulls),
    (polars_double_arr, polars_nulls),
    (polars_longlong_arr, polars_nulls),
    (polars_unsigned_longlong_arr, polars_nulls),
    (polars_int24_arr, polars_nulls),
    (polars_unsigned_int24_arr, polars_nulls),
    (polars_string_arr, polars_nulls),
    (polars_binary_arr, polars_nulls),
]

pandas_row_ids = pl.Series(None, [1, 2, 3, 4], dtype=pl.Int64)
pandas_nulls = pl.Series(None, numpy_nulls, dtype=pl.Boolean)

pandas_tiny_arr = \
    pd.Series(numpy_tiny_arr, dtype=np.int8)
pandas_unsigned_tiny_arr = \
    pd.Series(numpy_unsigned_tiny_arr, dtype=np.uint8)
pandas_short_arr = \
    pd.Series(numpy_short_arr, dtype=np.int16)
pandas_unsigned_short_arr = \
    pd.Series(numpy_unsigned_short_arr, dtype=np.uint16)
pandas_long_arr = \
    pd.Series(numpy_long_arr, dtype=np.int32)
pandas_unsigned_long_arr = \
    pd.Series(numpy_unsigned_long_arr, dtype=np.uint32)
pandas_float_arr = \
    pd.Series(numpy_float_arr, dtype=np.float32)
pandas_double_arr = \
    pd.Series(numpy_double_arr, dtype=np.float64)
pandas_longlong_arr = \
    pd.Series(numpy_longlong_arr, dtype=np.int64)
pandas_unsigned_longlong_arr = \
    pd.Series(numpy_unsigned_longlong_arr, dtype=np.uint64)
pandas_int24_arr = \
    pd.Series(numpy_int24_arr, dtype=np.int32)
pandas_unsigned_int24_arr = \
    pd.Series(numpy_unsigned_int24_arr, dtype=np.uint32)
pandas_string_arr = \
    pd.Series(numpy_string_arr, dtype=object)
pandas_binary_arr = \
    pd.Series(numpy_binary_arr, dtype=object)

pandas_data = [
    (pandas_tiny_arr, pandas_nulls),
    (pandas_unsigned_tiny_arr, pandas_nulls),
    (pandas_short_arr, pandas_nulls),
    (pandas_unsigned_short_arr, pandas_nulls),
    (pandas_long_arr, pandas_nulls),
    (pandas_unsigned_long_arr, pandas_nulls),
    (pandas_float_arr, pandas_nulls),
    (pandas_double_arr, pandas_nulls),
    (pandas_longlong_arr, pandas_nulls),
    (pandas_unsigned_longlong_arr, pandas_nulls),
    (pandas_int24_arr, pandas_nulls),
    (pandas_unsigned_int24_arr, pandas_nulls),
    (pandas_string_arr, pandas_nulls),
    (pandas_binary_arr, pandas_nulls),
]

pyarrow_row_ids = pa.array([1, 2, 3, 4], type=pa.int64())
pyarrow_nulls = pa.array(numpy_nulls, type=pa.bool_())

pyarrow_tiny_arr = \
    pa.array(numpy_tiny_arr, type=pa.int8(), mask=numpy_nulls)
pyarrow_unsigned_tiny_arr = \
    pa.array(numpy_unsigned_tiny_arr, type=pa.uint8(), mask=numpy_nulls)
pyarrow_short_arr = \
    pa.array(numpy_short_arr, type=pa.int16(), mask=numpy_nulls)
pyarrow_unsigned_short_arr = \
    pa.array(numpy_unsigned_short_arr, type=pa.uint16(), mask=numpy_nulls)
pyarrow_long_arr = \
    pa.array(numpy_long_arr, type=pa.int32(), mask=numpy_nulls)
pyarrow_unsigned_long_arr = \
    pa.array(numpy_unsigned_long_arr, type=pa.uint32(), mask=numpy_nulls)
pyarrow_float_arr = \
    pa.array(numpy_float_arr, type=pa.float32(), mask=numpy_nulls)
pyarrow_double_arr = \
    pa.array(numpy_double_arr, type=pa.float64(), mask=numpy_nulls)
pyarrow_longlong_arr = \
    pa.array(numpy_longlong_arr, type=pa.int64(), mask=numpy_nulls)
pyarrow_unsigned_longlong_arr = \
    pa.array(numpy_unsigned_longlong_arr, type=pa.uint64(), mask=numpy_nulls)
pyarrow_int24_arr = \
    pa.array(numpy_int24_arr, type=pa.int32(), mask=numpy_nulls)
pyarrow_unsigned_int24_arr = \
    pa.array(numpy_unsigned_int24_arr, type=pa.uint32(), mask=numpy_nulls)
pyarrow_string_arr = \
    pa.array(numpy_string_arr, type=pa.string(), mask=numpy_nulls)
pyarrow_binary_arr = \
    pa.array(numpy_binary_arr, type=pa.binary(), mask=numpy_nulls)

pyarrow_data = [
    (pyarrow_tiny_arr, pyarrow_nulls),
    (pyarrow_unsigned_tiny_arr, pyarrow_nulls),
    (pyarrow_short_arr, pyarrow_nulls),
    (pyarrow_unsigned_short_arr, pyarrow_nulls),
    (pyarrow_long_arr, pyarrow_nulls),
    (pyarrow_unsigned_long_arr, pyarrow_nulls),
    (pyarrow_float_arr, pyarrow_nulls),
    (pyarrow_double_arr, pyarrow_nulls),
    (pyarrow_longlong_arr, pyarrow_nulls),
    (pyarrow_unsigned_longlong_arr, pyarrow_nulls),
    (pyarrow_int24_arr, pyarrow_nulls),
    (pyarrow_unsigned_int24_arr, pyarrow_nulls),
    (pyarrow_string_arr, pyarrow_nulls),
    (pyarrow_binary_arr, pyarrow_nulls),
]

py_row_ids = [1, 2, 3, 4]
py_col_data = [
    [
        -1, 1, -1, 1, -1, 1, -1.0,
        -1.0, -1, 1, -1, 1, 'hi', b'hi',
    ],
    [
        100, 100, 32700, 32800, 2147483600, 2147483800, 100.0,
        100.0, 100, 100, 8388600, 16777210, 'bye', b'bye',
    ],
    [
        120, 130, 254, 254, 254, 254, 3.14159, 3.14159,
        9223372036854775000, 9223372036854776000, 254, 254, 'foo', b'foo',
    ],
    [
        None, None, None, None, None, None, None,
        None, None, None, None, None, None, None,
    ],
]


def assert_py_equal(x, y):
    """Compare rows of Python elements."""
    for i, (row_x, row_y) in enumerate(zip(x, y)):
        for j, (col_x, col_y) in enumerate(zip(row_x, row_y)):
            assert type(col_x) is type(col_y), f'{type(col_x)} != {type(col_y)}'
            if isinstance(col_x, float):
                assert col_x - col_y < 0.0005, f'{i},{j}: {col_x} != {col_y}'
            else:
                assert col_x == col_y, f'{i},{j}: {col_x} != {col_y}'


class TestRowdat1(unittest.TestCase):

    def test_numpy_accel(self):
        dump_res = rowdat_1._dump_numpy_accel(
            col_types, numpy_row_ids, numpy_data,
        )
        load_res = rowdat_1._load_numpy_accel(col_spec, dump_res)

        ids = load_res[0]
        columns = load_res[1]

        assert_array_equal(ids, numpy_row_ids)
        assert_array_equal(columns[0][0], numpy_tiny_arr, strict=True)
        assert_array_equal(columns[1][0], numpy_unsigned_tiny_arr, strict=True)
        assert_array_equal(columns[2][0], numpy_short_arr, strict=True)
        assert_array_equal(columns[3][0], numpy_unsigned_short_arr, strict=True)
        assert_array_equal(columns[4][0], numpy_long_arr, strict=True)
        assert_array_equal(columns[5][0], numpy_unsigned_long_arr, strict=True)
        assert_array_equal(columns[6][0], numpy_float_arr, strict=True)
        assert_array_equal(columns[7][0], numpy_double_arr, strict=True)
        assert_array_equal(columns[8][0], numpy_longlong_arr, strict=True)
        assert_array_equal(columns[9][0], numpy_unsigned_longlong_arr, strict=True)
        assert_array_equal(columns[10][0], numpy_int24_arr, strict=True)
        assert_array_equal(columns[11][0], numpy_unsigned_int24_arr, strict=True)
        assert_array_equal(columns[12][0], numpy_string_arr, strict=True)
        assert_array_equal(columns[13][0], numpy_binary_arr, strict=True)

    def test_numpy(self):
        dump_res = rowdat_1._dump_numpy(
            col_types, numpy_row_ids, numpy_data,
        )
        load_res = rowdat_1._load_numpy(col_spec, dump_res)

        ids = load_res[0]
        columns = load_res[1]

        assert_array_equal(ids, numpy_row_ids)
        assert_array_equal(columns[0][0], numpy_tiny_arr, strict=True)
        assert_array_equal(columns[1][0], numpy_unsigned_tiny_arr, strict=True)
        assert_array_equal(columns[2][0], numpy_short_arr, strict=True)
        assert_array_equal(columns[3][0], numpy_unsigned_short_arr, strict=True)
        assert_array_equal(columns[4][0], numpy_long_arr, strict=True)
        assert_array_equal(columns[5][0], numpy_unsigned_long_arr, strict=True)
        assert_array_equal(columns[6][0], numpy_float_arr, strict=True)
        assert_array_equal(columns[7][0], numpy_double_arr, strict=True)
        assert_array_equal(columns[8][0], numpy_longlong_arr, strict=True)
        assert_array_equal(columns[9][0], numpy_unsigned_longlong_arr, strict=True)
        assert_array_equal(columns[10][0], numpy_int24_arr, strict=True)
        assert_array_equal(columns[11][0], numpy_unsigned_int24_arr, strict=True)
        assert_array_equal(columns[12][0], numpy_string_arr, strict=True)
        assert_array_equal(columns[13][0], numpy_binary_arr, strict=True)

    @parameterized.expand([
        ('tinyint exceeds low', TINYINT, -129, ValueError),
        ('tinyint low', TINYINT, -128, -128),
        ('tinyint high', TINYINT, 127, 127),
        ('tinyint exceeds high', TINYINT, 128, ValueError),
        ('tinyint zero', TINYINT, 0, 0),

        ('unsigned tinyint exceeds low', UNSIGNED_TINYINT, -1, ValueError),
        ('unsigned tinyint low', UNSIGNED_TINYINT, 0, 0),
        ('unsigned tinyint high', UNSIGNED_TINYINT, 255, 255),
        ('unsigned tinyint exceeds high', UNSIGNED_TINYINT, 256, ValueError),
        ('unsigned tinyint zero', UNSIGNED_TINYINT, 0, 0),

        ('smallint exceeds low', SMALLINT, -32769, ValueError),
        ('smallint low', SMALLINT, -32768, -32768),
        ('smallint high', SMALLINT, 32767, 32767),
        ('smallint exceeds high', SMALLINT, 32768, ValueError),
        ('smallint zero', SMALLINT, 0, 0),

        ('unsigned smallint exceeds low', UNSIGNED_SMALLINT, -1, ValueError),
        ('unsigned smallint low', UNSIGNED_SMALLINT, 0, 0),
        ('unsigned smallint high', UNSIGNED_SMALLINT, 65535, 65535),
        ('unsigned smallint exceeds high', UNSIGNED_SMALLINT, 65536, ValueError),
        ('unsigned smallint zero', UNSIGNED_SMALLINT, 0, 0),

        ('mediumint exceeds low', MEDIUMINT, -8388609, ValueError),
        ('mediumint low', MEDIUMINT, -8388608, -8388608),
        ('mediumint high', MEDIUMINT, 8388607, 8388607),
        ('mediumint exceeds high', MEDIUMINT, 8388608, ValueError),
        ('mediumint zero', MEDIUMINT, 0, 0),

        ('unsigned mediumint exceeds low', UNSIGNED_MEDIUMINT, -1, ValueError),
        ('unsigned mediumint low', UNSIGNED_MEDIUMINT, 0, 0),
        ('unsigned mediumint high', UNSIGNED_MEDIUMINT, 16777215, 16777215),
        ('unsigned mediumint exceeds high', UNSIGNED_MEDIUMINT, 16777216, ValueError),
        ('unsigned mediumint zero', UNSIGNED_MEDIUMINT, 0, 0),

        ('int exceeds low', INT, -2147483649, ValueError),
        ('int low', INT, -2147483648, -2147483648),
        ('int high', INT, 2147483647, 2147483647),
        ('int exceeds high', INT, 2147483648, ValueError),
        ('int zero', INT, 0, 0),

        ('unsigned int exceeds low', UNSIGNED_INT, -1, ValueError),
        ('unsigned int low', UNSIGNED_INT, 0, 0),
        ('unsigned int high', UNSIGNED_INT, 4294967295, 4294967295),
        ('unsigned int exceeds high', UNSIGNED_INT, 4294967296, ValueError),
        ('unsigned int zero', UNSIGNED_INT, 0, 0),

        ('bigint exceeds low', BIGINT, -2**63 - 1, ValueError),
        ('bigint low', BIGINT, -2**63, -2**63),
        ('bigint high', BIGINT, 2**63 - 1, 2**63 - 1),
        ('bigint exceeds high', BIGINT, 2**63, ValueError),
        ('bigint zero', BIGINT, 0, 0),

        ('unsigned bigint exceeds low', UNSIGNED_BIGINT, -1, ValueError),
        ('unsigned bigint low', UNSIGNED_BIGINT, 0, 0),
        ('unsigned bigint high', UNSIGNED_BIGINT, 2**64 - 1, 2**64 - 1),
        ('unsigned bigint exceeds high', UNSIGNED_BIGINT, 2**64, ValueError),
        ('unsigned bigint zero', UNSIGNED_BIGINT, 0, 0),
    ])
    def test_numpy_accel_limits(self, name, dtype, data, res):
        numpy_row_ids = np.array([1])

        arr = np.array([data])

        if type(res) is type and issubclass(res, Exception):
            # Accelerated
            with self.assertRaises(res, msg=f'Expected {res} for {data} in {dtype}'):
                rowdat_1._dump_numpy_accel(
                    [dtype], numpy_row_ids, [(arr, None)],
                )

            # Pure Python
            if 'mediumint exceeds' in name:
                pass
            else:
                with self.assertRaises(res, msg=f'Expected {res} for {data} in {dtype}'):
                    rowdat_1._dump_numpy(
                        [dtype], numpy_row_ids, [(arr, None)],
                    )

        else:
            # Accelerated
            dump_res = rowdat_1._dump_numpy_accel(
                [dtype], numpy_row_ids, [(arr, None)],
            )
            load_res = rowdat_1._load_numpy_accel([('x', dtype)], dump_res)
            assert load_res[1][0][0] == res, \
                f'Expected {res} for {data}, but got {load_res[1][0][0]} in {dtype}'

            # Pure Python
            dump_res = rowdat_1._dump_numpy(
                [dtype], numpy_row_ids, [(arr, None)],
            )
            load_res = rowdat_1._load_numpy([('x', dtype)], dump_res)
            assert load_res[1][0][0] == res, \
                f'Expected {res} for {data}, but got {load_res[1][0][0]} in {dtype}'

    @parameterized.expand([
        (
            'tinyint from int64', TINYINT,
            np.array([-128, 0, 127, 67], dtype=np.int64),
            np.array([-128, 0, 127, 67], dtype=np.int8),
        ),
        (
            'unsigned tinyint from int64', UNSIGNED_TINYINT,
            np.array([0, 255, 241], dtype=np.int64),
            np.array([0, 255, 241], dtype=np.uint8),
        ),
        (
            'smallint from int64', SMALLINT,
            np.array([-32768, 0, 32767, 25557], dtype=np.int64),
            np.array([-32768, 0, 32767, 25557], dtype=np.int16),
        ),
        (
            'unsigned smallint from int64', UNSIGNED_SMALLINT,
            np.array([0, 65535, 40513], dtype=np.int64),
            np.array([0, 65535, 40513], dtype=np.uint16),
        ),
        (
            'mediumint from int64', MEDIUMINT,
            np.array([-8388608, 0, 8388607, 999678], dtype=np.int64),
            np.array([-8388608, 0, 8388607, 999678], dtype=np.int32),
        ),
        (
            'unsigned mediumint from int64', UNSIGNED_MEDIUMINT,
            np.array([0, 16777215, 9996781], dtype=np.int64),
            np.array([0, 16777215, 9996781], dtype=np.uint32),
        ),
        (
            'int from int64', INT,
            np.array([-2147483648, 0, 2147483647, 1123867689], dtype=np.int64),
            np.array([-2147483648, 0, 2147483647, 1123867689], dtype=np.int32),
        ),
        (
            'unsigned int from int64', UNSIGNED_INT,
            np.array([0, 4294967295, 339826098], dtype=np.int64),
            np.array([0, 4294967295, 339826098], dtype=np.uint32),
        ),
        (
            'bigint from int64', BIGINT,
            np.array([-2**63, 0, 2**63 - 1, 9381123867689], dtype=np.int64),
            np.array([-2**63, 0, 2**63 - 1, 9381123867689], dtype=np.int64),
        ),
        (
            'unsigned bigint from int64', UNSIGNED_BIGINT,
            np.array([0, 2**63 - 1, 2**63 - 87629], dtype=np.int64),
            np.array([0, 2**63 - 1, 2**63 - 87629], dtype=np.uint64),
        ),
        (
            'float from int64', FLOAT,
            np.array([-2**63, 0, 2**63 - 1, 9381123867689], dtype=np.int64),
            np.array([-2**63, 0, 2**63 - 1, 9381123867689], dtype=np.float32),
        ),
        (
            'double from int64', DOUBLE,
            np.array([-2**63, 0, 2**63 - 1, 9381123867689], dtype=np.int64),
            np.array([-2**63, 0, 2**63 - 1, 9381123867689], dtype=np.float64),
        ),

        (
            'tinyint from int16', TINYINT,
            np.array([-128, 0, 127, 67], dtype=np.int16),
            np.array([-128, 0, 127, 67], dtype=np.int8),
        ),
        (
            'unsigned tinyint from int16', UNSIGNED_TINYINT,
            np.array([0, 255, 241], dtype=np.int16),
            np.array([0, 255, 241], dtype=np.uint8),
        ),
        (
            'smallint from int16', SMALLINT,
            np.array([-32768, 0, 32767, 25557], dtype=np.int16),
            np.array([-32768, 0, 32767, 25557], dtype=np.int16),
        ),
        (
            'unsigned smallint from int16', UNSIGNED_SMALLINT,
            np.array([0, 32767, 25557], dtype=np.int16),
            np.array([0, 32767, 25557], dtype=np.uint16),
        ),
        (
            'mediumint from int16', MEDIUMINT,
            np.array([-32768, 0, 32767, 25557], dtype=np.int16),
            np.array([-32768, 0, 32767, 25557], dtype=np.int32),
        ),
        (
            'unsigned mediumint from int16', UNSIGNED_MEDIUMINT,
            np.array([0, 32767, 25557], dtype=np.int16),
            np.array([0, 32767, 25557], dtype=np.uint32),
        ),
        (
            'int from int16', INT,
            np.array([-32768, 0, 32767, 25557], dtype=np.int16),
            np.array([-32768, 0, 32767, 25557], dtype=np.int32),
        ),
        (
            'unsigned int from int16', UNSIGNED_INT,
            np.array([0, 32767, 25557], dtype=np.int16),
            np.array([0, 32767, 25557], dtype=np.uint32),
        ),
        (
            'bigint from int16', BIGINT,
            np.array([-32768, 0, 32767, 25557], dtype=np.int16),
            np.array([-32768, 0, 32767, 25557], dtype=np.int64),
        ),
        (
            'unsigned bigint from int16', UNSIGNED_BIGINT,
            np.array([0, 32767, 25557], dtype=np.int16),
            np.array([0, 32767, 25557], dtype=np.uint64),
        ),
        (
            'float from int16', FLOAT,
            np.array([-32768, 0, 32767, 25557], dtype=np.int16),
            np.array([-32768, 0, 32767, 25557], dtype=np.float32),
        ),
        (
            'double from int16', DOUBLE,
            np.array([-32768, 0, 32767, 25557], dtype=np.int16),
            np.array([-32768, 0, 32767, 25557], dtype=np.float64),
        ),

        (
            'tinyint from int32', TINYINT,
            np.array([-128, 0, 127, 67], dtype=np.int32),
            np.array([-128, 0, 127, 67], dtype=np.int8),
        ),
        (
            'unsigned tinyint from int32', UNSIGNED_TINYINT,
            np.array([0, 255, 241], dtype=np.int32),
            np.array([0, 255, 241], dtype=np.uint8),
        ),
        (
            'smallint from int32', SMALLINT,
            np.array([-32768, 0, 32767, 25557], dtype=np.int32),
            np.array([-32768, 0, 32767, 25557], dtype=np.int16),
        ),
        (
            'unsigned smallint from int32', UNSIGNED_SMALLINT,
            np.array([0, 65535, 40513], dtype=np.int32),
            np.array([0, 65535, 40513], dtype=np.uint16),
        ),
        (
            'mediumint from int32', MEDIUMINT,
            np.array([-8388608, 0, 8388607, 999678], dtype=np.int32),
            np.array([-8388608, 0, 8388607, 999678], dtype=np.int32),
        ),
        (
            'unsigned mediumint from int32', UNSIGNED_MEDIUMINT,
            np.array([0, 16777215, 9996781], dtype=np.int32),
            np.array([0, 16777215, 9996781], dtype=np.uint32),
        ),
        (
            'int from int32', INT,
            np.array([-2147483648, 0, 2147483647, 1123867689], dtype=np.int32),
            np.array([-2147483648, 0, 2147483647, 1123867689], dtype=np.int32),
        ),
        (
            'unsigned int from int32', UNSIGNED_INT,
            np.array([0, 2147483647, 3398268], dtype=np.int32),
            np.array([0, 2147483647, 3398268], dtype=np.uint32),
        ),
        (
            'bigint from int32', BIGINT,
            np.array([-2147483648, 0, 2147483647, 789768920], dtype=np.int32),
            np.array([-2147483648, 0, 2147483647, 789768920], dtype=np.int64),
        ),
        (
            'unsigned bigint from int32', UNSIGNED_BIGINT,
            np.array([0, 2147483647, 987362899], dtype=np.int32),
            np.array([0, 2147483647, 987362899], dtype=np.uint64),
        ),
        (
            'float from int32', FLOAT,
            np.array([-2147483648, 0, 2147483647, 789768920], dtype=np.int32),
            np.array([-2147483648, 0, 2147483647, 789768920], dtype=np.float32),
        ),
        (
            'double from int32', DOUBLE,
            np.array([-2147483648, 0, 2147483647, 789768920], dtype=np.int32),
            np.array([-2147483648, 0, 2147483647, 789768920], dtype=np.float64),
        ),

        (
            'tinyint from int64', TINYINT,
            np.array([-128, 0, 127, 67], dtype=np.int64),
            np.array([-128, 0, 127, 67], dtype=np.int8),
        ),
        (
            'unsigned tinyint from int64', UNSIGNED_TINYINT,
            np.array([0, 255, 241], dtype=np.int64),
            np.array([0, 255, 241], dtype=np.uint8),
        ),
        (
            'smallint from int64', SMALLINT,
            np.array([-32768, 0, 32767, 25557], dtype=np.int64),
            np.array([-32768, 0, 32767, 25557], dtype=np.int16),
        ),
        (
            'unsigned smallint from int64', UNSIGNED_SMALLINT,
            np.array([0, 65535, 40513], dtype=np.int64),
            np.array([0, 65535, 40513], dtype=np.uint16),
        ),
        (
            'mediumint from int64', MEDIUMINT,
            np.array([-8388608, 0, 8388607, 999678], dtype=np.int64),
            np.array([-8388608, 0, 8388607, 999678], dtype=np.int32),
        ),
        (
            'unsigned mediumint from int64', UNSIGNED_MEDIUMINT,
            np.array([0, 16777215, 9996781], dtype=np.int64),
            np.array([0, 16777215, 9996781], dtype=np.uint32),
        ),
        (
            'int from int64', INT,
            np.array([-2147483648, 0, 2147483647, 1123867689], dtype=np.int64),
            np.array([-2147483648, 0, 2147483647, 1123867689], dtype=np.int32),
        ),
        (
            'unsigned int from int64', UNSIGNED_INT,
            np.array([0, 2147483647, 3398268], dtype=np.int64),
            np.array([0, 2147483647, 3398268], dtype=np.uint32),
        ),
        (
            'bigint from int64', BIGINT,
            np.array([-2**63, 0, 2**63 - 1, 78976892012], dtype=np.int64),
            np.array([-2**63, 0, 2**63 - 1, 78976892012], dtype=np.int64),
        ),
        (
            'unsigned bigint from int64', UNSIGNED_BIGINT,
            np.array([0, 2**63 - 1, 987362899], dtype=np.int64),
            np.array([0, 2**63 - 1, 987362899], dtype=np.uint64),
        ),
        (
            'float from int64', FLOAT,
            np.array([-2**63, 0, 2**63 - 1, 78976892012], dtype=np.int64),
            np.array([-2**63, 0, 2**63 - 1, 78976892012], dtype=np.float32),
        ),
        (
            'double from int64', DOUBLE,
            np.array([-2**63, 0, 2**63 - 1, 78976892012], dtype=np.int64),
            np.array([-2**63, 0, 2**63 - 1, 78976892012], dtype=np.float64),
        ),

        (
            'tinyint from float32', TINYINT,
            np.array([-128, 0, 127, 67], dtype=np.float32),
            np.array([-128, 0, 127, 67], dtype=np.int8),
        ),
        (
            'unsigned tinyint from float32', UNSIGNED_TINYINT,
            np.array([0, 255, 241], dtype=np.float32),
            np.array([0, 255, 241], dtype=np.uint8),
        ),
        (
            'smallint from float32', SMALLINT,
            np.array([-32768, 0, 32767, 25557], dtype=np.float32),
            np.array([-32768, 0, 32767, 25557], dtype=np.int16),
        ),
        (
            'unsigned smallint from float32', UNSIGNED_SMALLINT,
            np.array([0, 65535, 40513], dtype=np.float32),
            np.array([0, 65535, 40513], dtype=np.uint16),
        ),
        (
            'mediumint from float32', MEDIUMINT,
            np.array([-8388608, 0, 8388607, 999678], dtype=np.float32),
            np.array([-8388608, 0, 8388607, 999678], dtype=np.int32),
        ),
        (
            'unsigned mediumint from float32', UNSIGNED_MEDIUMINT,
            np.array([0, 16777215, 9996781], dtype=np.float32),
            np.array([0, 16777215, 9996781], dtype=np.uint32),
        ),
        (
            'int from float32', INT,
            np.array([-2147483648, 0, 214748352, 1123867648], dtype=np.float32),
            np.array([-2147483648, 0, 214748352, 1123867648], dtype=np.int32),
        ),
        (
            'unsigned int from float32', UNSIGNED_INT,
            np.array([0, 214748368, 3398268], dtype=np.float32),
            np.array([0, 214748368, 3398268], dtype=np.uint32),
        ),
        (
            'bigint from float32', BIGINT,
            np.array([-2**63, 0, 2**23 - 1, 78976892928], dtype=np.float32),
            np.array([-2**63, 0, 2**23 - 1, 78976892928], dtype=np.int64),
        ),
        (
            'unsigned bigint from float32', UNSIGNED_BIGINT,
            np.array([0, 2**23 - 1, 987362880], dtype=np.float32),
            np.array([0, 2**23 - 1, 987362880], dtype=np.uint64),
        ),
        (
            'float from float32', FLOAT,
            np.array([-2**63, 0, 2**63 - 1, 78976892012], dtype=np.float32),
            np.array([-2**63, 0, 2**63 - 1, 78976892012], dtype=np.float32),
        ),
        (
            'double from float32', DOUBLE,
            np.array([-8388.099609, 0.0, 8388.099609, 1234.567017], dtype=np.float32),
            np.array([-8388.099609, 0.0, 8388.099609, 1234.567017], dtype=np.float64),
        ),

        (
            'tinyint from float64', TINYINT,
            np.array([-128, 0, 127, 67], dtype=np.float64),
            np.array([-128, 0, 127, 67], dtype=np.int8),
        ),
        (
            'unsigned tinyint from float64', UNSIGNED_TINYINT,
            np.array([0, 255, 241], dtype=np.float64),
            np.array([0, 255, 241], dtype=np.uint8),
        ),
        (
            'smallint from float64', SMALLINT,
            np.array([-32768, 0, 32767, 25557], dtype=np.float64),
            np.array([-32768, 0, 32767, 25557], dtype=np.int16),
        ),
        (
            'unsigned smallint from float64', UNSIGNED_SMALLINT,
            np.array([0, 65535, 40513], dtype=np.float64),
            np.array([0, 65535, 40513], dtype=np.uint16),
        ),
        (
            'mediumint from float64', MEDIUMINT,
            np.array([-8388608, 0, 8388607, 999678], dtype=np.float64),
            np.array([-8388608, 0, 8388607, 999678], dtype=np.int32),
        ),
        (
            'unsigned mediumint from float64', UNSIGNED_MEDIUMINT,
            np.array([0, 16777215, 9996781], dtype=np.float64),
            np.array([0, 16777215, 9996781], dtype=np.uint32),
        ),
        (
            'int from float64', INT,
            np.array([-2147483648, 0, 2147483647, 1123867689], dtype=np.float64),
            np.array([-2147483648, 0, 2147483647, 1123867689], dtype=np.int32),
        ),
        (
            'unsigned int from float64', UNSIGNED_INT,
            np.array([0, 2147483647, 3398268], dtype=np.float64),
            np.array([0, 2147483647, 3398268], dtype=np.uint32),
        ),
        (
            'bigint from float64', BIGINT,
            np.array([-2**63, 0, 2**53 - 1, 78976892012], dtype=np.float64),
            np.array([-2**63, 0, 2**53 - 1, 78976892012], dtype=np.int64),
        ),
        (
            'unsigned bigint from float64', UNSIGNED_BIGINT,
            np.array([0, 2**53 - 1, 987362899], dtype=np.float64),
            np.array([0, 2**53 - 1, 987362899], dtype=np.uint64),
        ),
        (
            'float from float64', FLOAT,
            np.array([-2**63, 0, 2**63 - 1, 78976892012], dtype=np.float64),
            np.array([-2**63, 0, 2**63 - 1, 78976892012], dtype=np.float32),
        ),
        (
            'double from float64', DOUBLE,
            np.array([-2**63, 0, 2**63 - 1, 78976892012], dtype=np.float64),
            np.array([-2**63, 0, 2**63 - 1, 78976892012], dtype=np.float64),
        ),
    ])
    def test_numpy_accel_casts(self, name, dtype, data, res):
        numpy_row_ids = np.array(list(range(len(data))))

        # Accelerated
        dump_res = rowdat_1._dump_numpy_accel(
            [dtype], numpy_row_ids, [(data, None)],
        )
        load_res = rowdat_1._load_numpy_accel([('x', dtype)], dump_res)

        if name == 'double from float32':
            assert load_res[1][0][0].dtype is res.dtype
            assert (load_res[1][0][0] - res < 0.00005).all()
        else:
            np.testing.assert_array_equal(load_res[1][0][0], res, strict=True)

        # Pure Python
        dump_res = rowdat_1._dump_numpy(
            [dtype], numpy_row_ids, [(data, None)],
        )
        load_res = rowdat_1._load_numpy([('x', dtype)], dump_res)

        if name == 'double from float32':
            assert load_res[1][0][0].dtype is res.dtype
            assert (load_res[1][0][0] - res < 0.00005).all()
        else:
            np.testing.assert_array_equal(load_res[1][0][0], res, strict=True)

    def test_python(self):
        dump_res = rowdat_1._dump(
            col_types, py_row_ids, py_col_data,
        )
        load_res = rowdat_1._load(col_spec, dump_res)

        ids = load_res[0]
        columns = load_res[1]

        assert ids == py_row_ids
        assert_py_equal(columns, py_col_data)

    def test_python_accel(self):
        dump_res = rowdat_1._dump_accel(
            col_types, py_row_ids, py_col_data,
        )
        load_res = rowdat_1._load_accel(col_spec, dump_res)

        ids = load_res[0]
        columns = load_res[1]

        assert ids == py_row_ids
        assert_py_equal(columns, py_col_data)

    def test_polars(self):
        dump_res = rowdat_1._dump_polars(
            col_types, polars_row_ids, polars_data,
        )
        load_res = rowdat_1._load_polars(col_spec, dump_res)

        ids = load_res[0]
        columns = load_res[1]

        assert_array_equal(ids, polars_row_ids)
        assert_array_equal(columns[0][0], polars_tiny_arr, strict=True)
        assert_array_equal(columns[1][0], polars_unsigned_tiny_arr, strict=True)
        assert_array_equal(columns[2][0], polars_short_arr, strict=True)
        assert_array_equal(columns[3][0], polars_unsigned_short_arr, strict=True)
        assert_array_equal(columns[4][0], polars_long_arr, strict=True)
        assert_array_equal(columns[5][0], polars_unsigned_long_arr, strict=True)
        assert_array_equal(columns[6][0], polars_float_arr, strict=True)
        assert_array_equal(columns[7][0], polars_double_arr, strict=True)
        assert_array_equal(columns[8][0], polars_longlong_arr, strict=True)
        assert_array_equal(columns[9][0], polars_unsigned_longlong_arr, strict=True)
        assert_array_equal(columns[10][0], polars_int24_arr, strict=True)
        assert_array_equal(columns[11][0], polars_unsigned_int24_arr, strict=True)
        assert_array_equal(columns[12][0], polars_string_arr, strict=True)
        assert_array_equal(columns[13][0], polars_binary_arr, strict=True)

    def test_polars_accel(self):
        dump_res = rowdat_1._dump_polars_accel(
            col_types, polars_row_ids, polars_data,
        )
        load_res = rowdat_1._load_polars_accel(col_spec, dump_res)

        ids = load_res[0]
        columns = load_res[1]

        assert_array_equal(ids, polars_row_ids)
        assert_array_equal(columns[0][0], polars_tiny_arr, strict=True)
        assert_array_equal(columns[1][0], polars_unsigned_tiny_arr, strict=True)
        assert_array_equal(columns[2][0], polars_short_arr, strict=True)
        assert_array_equal(columns[3][0], polars_unsigned_short_arr, strict=True)
        assert_array_equal(columns[4][0], polars_long_arr, strict=True)
        assert_array_equal(columns[5][0], polars_unsigned_long_arr, strict=True)
        assert_array_equal(columns[6][0], polars_float_arr, strict=True)
        assert_array_equal(columns[7][0], polars_double_arr, strict=True)
        assert_array_equal(columns[8][0], polars_longlong_arr, strict=True)
        assert_array_equal(columns[9][0], polars_unsigned_longlong_arr, strict=True)
        assert_array_equal(columns[10][0], polars_int24_arr, strict=True)
        assert_array_equal(columns[11][0], polars_unsigned_int24_arr, strict=True)
        assert_array_equal(columns[12][0], polars_string_arr, strict=True)
        assert_array_equal(columns[13][0], polars_binary_arr, strict=True)

    def test_pandas(self):
        dump_res = rowdat_1._dump_pandas(
            col_types, pandas_row_ids, pandas_data,
        )
        load_res = rowdat_1._load_pandas(col_spec, dump_res)

        ids = load_res[0]
        columns = load_res[1]

        assert_array_equal(ids, pandas_row_ids)
        assert_array_equal(columns[0][0], pandas_tiny_arr, strict=True)
        assert_array_equal(columns[1][0], pandas_unsigned_tiny_arr, strict=True)
        assert_array_equal(columns[2][0], pandas_short_arr, strict=True)
        assert_array_equal(columns[3][0], pandas_unsigned_short_arr, strict=True)
        assert_array_equal(columns[4][0], pandas_long_arr, strict=True)
        assert_array_equal(columns[5][0], pandas_unsigned_long_arr, strict=True)
        assert_array_equal(columns[6][0], pandas_float_arr, strict=True)
        assert_array_equal(columns[7][0], pandas_double_arr, strict=True)
        assert_array_equal(columns[8][0], pandas_longlong_arr, strict=True)
        assert_array_equal(columns[9][0], pandas_unsigned_longlong_arr, strict=True)
        assert_array_equal(columns[10][0], pandas_int24_arr, strict=True)
        assert_array_equal(columns[11][0], pandas_unsigned_int24_arr, strict=True)
        assert_array_equal(columns[12][0], pandas_string_arr, strict=True)
        assert_array_equal(columns[13][0], pandas_binary_arr, strict=True)

    def test_pandas_accel(self):
        dump_res = rowdat_1._dump_pandas_accel(
            col_types, pandas_row_ids, pandas_data,
        )
        load_res = rowdat_1._load_pandas_accel(col_spec, dump_res)

        ids = load_res[0]
        columns = load_res[1]

        assert_array_equal(ids, pandas_row_ids)
        assert_array_equal(columns[0][0], pandas_tiny_arr, strict=True)
        assert_array_equal(columns[1][0], pandas_unsigned_tiny_arr, strict=True)
        assert_array_equal(columns[2][0], pandas_short_arr, strict=True)
        assert_array_equal(columns[3][0], pandas_unsigned_short_arr, strict=True)
        assert_array_equal(columns[4][0], pandas_long_arr, strict=True)
        assert_array_equal(columns[5][0], pandas_unsigned_long_arr, strict=True)
        assert_array_equal(columns[6][0], pandas_float_arr, strict=True)
        assert_array_equal(columns[7][0], pandas_double_arr, strict=True)
        assert_array_equal(columns[8][0], pandas_longlong_arr, strict=True)
        assert_array_equal(columns[9][0], pandas_unsigned_longlong_arr, strict=True)
        assert_array_equal(columns[10][0], pandas_int24_arr, strict=True)
        assert_array_equal(columns[11][0], pandas_unsigned_int24_arr, strict=True)
        assert_array_equal(columns[12][0], pandas_string_arr, strict=True)
        assert_array_equal(columns[13][0], pandas_binary_arr, strict=True)

    def test_pyarrow(self):
        dump_res = rowdat_1._dump_arrow(
            col_types, pyarrow_row_ids, pyarrow_data,
        )
        load_res = rowdat_1._load_arrow(col_spec, dump_res)

        ids = load_res[0]
        columns = load_res[1]

        assert_array_equal(ids, pyarrow_row_ids)
        assert_array_equal(columns[0][0], pyarrow_tiny_arr, strict=True)
        assert_array_equal(columns[1][0], pyarrow_unsigned_tiny_arr, strict=True)
        assert_array_equal(columns[2][0], pyarrow_short_arr, strict=True)
        assert_array_equal(columns[3][0], pyarrow_unsigned_short_arr, strict=True)
        assert_array_equal(columns[4][0], pyarrow_long_arr, strict=True)
        assert_array_equal(columns[5][0], pyarrow_unsigned_long_arr, strict=True)
        assert_array_equal(columns[6][0], pyarrow_float_arr, strict=True)
        assert_array_equal(columns[7][0], pyarrow_double_arr, strict=True)
        assert_array_equal(columns[8][0], pyarrow_longlong_arr, strict=True)
        assert_array_equal(columns[9][0], pyarrow_unsigned_longlong_arr, strict=True)
        assert_array_equal(columns[10][0], pyarrow_int24_arr, strict=True)
        assert_array_equal(columns[11][0], pyarrow_unsigned_int24_arr, strict=True)
        assert_array_equal(columns[12][0], pyarrow_string_arr, strict=True)
        assert_array_equal(columns[13][0], pyarrow_binary_arr, strict=True)

    def test_pyarrow_accel(self):
        dump_res = rowdat_1._dump_arrow_accel(
            col_types, pyarrow_row_ids, pyarrow_data,
        )
        load_res = rowdat_1._load_arrow_accel(col_spec, dump_res)

        ids = load_res[0]
        columns = load_res[1]

        assert_array_equal(ids, pyarrow_row_ids)
        assert_array_equal(columns[0][0], pyarrow_tiny_arr, strict=True)
        assert_array_equal(columns[1][0], pyarrow_unsigned_tiny_arr, strict=True)
        assert_array_equal(columns[2][0], pyarrow_short_arr, strict=True)
        assert_array_equal(columns[3][0], pyarrow_unsigned_short_arr, strict=True)
        assert_array_equal(columns[4][0], pyarrow_long_arr, strict=True)
        assert_array_equal(columns[5][0], pyarrow_unsigned_long_arr, strict=True)
        assert_array_equal(columns[6][0], pyarrow_float_arr, strict=True)
        assert_array_equal(columns[7][0], pyarrow_double_arr, strict=True)
        assert_array_equal(columns[8][0], pyarrow_longlong_arr, strict=True)
        assert_array_equal(columns[9][0], pyarrow_unsigned_longlong_arr, strict=True)
        assert_array_equal(columns[10][0], pyarrow_int24_arr, strict=True)
        assert_array_equal(columns[11][0], pyarrow_unsigned_int24_arr, strict=True)
        assert_array_equal(columns[12][0], pyarrow_string_arr, strict=True)
        assert_array_equal(columns[13][0], pyarrow_binary_arr, strict=True)


class TestJSON(unittest.TestCase):

    def test_numpy(self):
        dump_res = jsonx.dump_numpy(
            col_types, numpy_row_ids, numpy_data,
        )
        load_res = jsonx.load_numpy(col_spec, dump_res)

        ids = load_res[0]
        columns = load_res[1]

        assert_array_equal(ids, numpy_row_ids)
        assert_array_equal(columns[0][0], numpy_tiny_arr, strict=True)
        assert_array_equal(columns[1][0], numpy_unsigned_tiny_arr, strict=True)
        assert_array_equal(columns[2][0], numpy_short_arr, strict=True)
        assert_array_equal(columns[3][0], numpy_unsigned_short_arr, strict=True)
        assert_array_equal(columns[4][0], numpy_long_arr, strict=True)
        assert_array_equal(columns[5][0], numpy_unsigned_long_arr, strict=True)
        assert_array_equal(columns[6][0], numpy_float_arr, strict=True)
        assert_array_equal(columns[7][0], numpy_double_arr, strict=True)
        assert_array_equal(columns[8][0], numpy_longlong_arr, strict=True)
        assert_array_equal(columns[9][0], numpy_unsigned_longlong_arr, strict=True)
        assert_array_equal(columns[10][0], numpy_int24_arr, strict=True)
        assert_array_equal(columns[11][0], numpy_unsigned_int24_arr, strict=True)
        assert_array_equal(columns[12][0], numpy_string_arr, strict=True)
        assert_array_equal(columns[13][0], numpy_binary_arr, strict=True)

    def test_python(self):
        dump_res = jsonx.dump(
            col_types, py_row_ids, py_col_data,
        )
        load_res = jsonx.load(col_spec, dump_res)

        ids = load_res[0]
        columns = load_res[1]

        assert ids == py_row_ids
        assert_py_equal(columns, py_col_data)

    def test_polars(self):
        dump_res = jsonx.dump_polars(
            col_types, polars_row_ids, polars_data,
        )
        load_res = jsonx.load_polars(col_spec, dump_res)

        ids = load_res[0]
        columns = load_res[1]

        assert_array_equal(ids, polars_row_ids)
        assert_array_equal(columns[0][0], polars_tiny_arr, strict=True)
        assert_array_equal(columns[1][0], polars_unsigned_tiny_arr, strict=True)
        assert_array_equal(columns[2][0], polars_short_arr, strict=True)
        assert_array_equal(columns[3][0], polars_unsigned_short_arr, strict=True)
        assert_array_equal(columns[4][0], polars_long_arr, strict=True)
        assert_array_equal(columns[5][0], polars_unsigned_long_arr, strict=True)
        assert_array_equal(columns[6][0], polars_float_arr, strict=True)
        assert_array_equal(columns[7][0], polars_double_arr, strict=True)
        assert_array_equal(columns[8][0], polars_longlong_arr, strict=True)
        assert_array_equal(columns[9][0], polars_unsigned_longlong_arr, strict=True)
        assert_array_equal(columns[10][0], polars_int24_arr, strict=True)
        assert_array_equal(columns[11][0], polars_unsigned_int24_arr, strict=True)
        assert_array_equal(columns[12][0], polars_string_arr, strict=True)
        assert_array_equal(columns[13][0], polars_binary_arr, strict=True)

    def test_pandas(self):
        dump_res = rowdat_1._dump_pandas(
            col_types, pandas_row_ids, pandas_data,
        )
        load_res = rowdat_1._load_pandas(col_spec, dump_res)

        ids = load_res[0]
        columns = load_res[1]

        assert_array_equal(ids, pandas_row_ids)
        assert_array_equal(columns[0][0], pandas_tiny_arr, strict=True)
        assert_array_equal(columns[1][0], pandas_unsigned_tiny_arr, strict=True)
        assert_array_equal(columns[2][0], pandas_short_arr, strict=True)
        assert_array_equal(columns[3][0], pandas_unsigned_short_arr, strict=True)
        assert_array_equal(columns[4][0], pandas_long_arr, strict=True)
        assert_array_equal(columns[5][0], pandas_unsigned_long_arr, strict=True)
        assert_array_equal(columns[6][0], pandas_float_arr, strict=True)
        assert_array_equal(columns[7][0], pandas_double_arr, strict=True)
        assert_array_equal(columns[8][0], pandas_longlong_arr, strict=True)
        assert_array_equal(columns[9][0], pandas_unsigned_longlong_arr, strict=True)
        assert_array_equal(columns[10][0], pandas_int24_arr, strict=True)
        assert_array_equal(columns[11][0], pandas_unsigned_int24_arr, strict=True)
        assert_array_equal(columns[12][0], pandas_string_arr, strict=True)
        assert_array_equal(columns[13][0], pandas_binary_arr, strict=True)

    def test_pyarrow(self):
        dump_res = rowdat_1._dump_arrow(
            col_types, pyarrow_row_ids, pyarrow_data,
        )
        load_res = rowdat_1._load_arrow(col_spec, dump_res)

        ids = load_res[0]
        columns = load_res[1]

        assert_array_equal(ids, pyarrow_row_ids)
        assert_array_equal(columns[0][0], pyarrow_tiny_arr, strict=True)
        assert_array_equal(columns[1][0], pyarrow_unsigned_tiny_arr, strict=True)
        assert_array_equal(columns[2][0], pyarrow_short_arr, strict=True)
        assert_array_equal(columns[3][0], pyarrow_unsigned_short_arr, strict=True)
        assert_array_equal(columns[4][0], pyarrow_long_arr, strict=True)
        assert_array_equal(columns[5][0], pyarrow_unsigned_long_arr, strict=True)
        assert_array_equal(columns[6][0], pyarrow_float_arr, strict=True)
        assert_array_equal(columns[7][0], pyarrow_double_arr, strict=True)
        assert_array_equal(columns[8][0], pyarrow_longlong_arr, strict=True)
        assert_array_equal(columns[9][0], pyarrow_unsigned_longlong_arr, strict=True)
        assert_array_equal(columns[10][0], pyarrow_int24_arr, strict=True)
        assert_array_equal(columns[11][0], pyarrow_unsigned_int24_arr, strict=True)
        assert_array_equal(columns[12][0], pyarrow_string_arr, strict=True)
        assert_array_equal(columns[13][0], pyarrow_binary_arr, strict=True)


# --- Datetime / Date / Time pack/unpack tests ---

DATETIME = 12
TIMESTAMP = 7
DATE = 10
TIME = 11

dt_col_spec = [
    ('dt', DATETIME),
    ('d', DATE),
    ('t', TIME),
]

dt_col_types = [x[1] for x in dt_col_spec]


class TestDatetimePacking(unittest.TestCase):

    def test_datetime_round_trip(self):
        dt = datetime.datetime(2024, 3, 15, 10, 30, 45, 123456)
        assert _unpack_datetime(_pack_datetime(dt)) == dt

    def test_datetime_min_values(self):
        dt = datetime.datetime(1, 1, 1, 0, 0, 0, 0)
        assert _unpack_datetime(_pack_datetime(dt)) == dt

    def test_datetime_max_microsecond(self):
        dt = datetime.datetime(2024, 12, 31, 23, 59, 59, 999999)
        assert _unpack_datetime(_pack_datetime(dt)) == dt

    def test_datetime_epoch(self):
        dt = datetime.datetime(1970, 1, 1, 0, 0, 0, 0)
        assert _unpack_datetime(_pack_datetime(dt)) == dt

    def test_datetime_no_microseconds(self):
        dt = datetime.datetime(2024, 6, 15, 12, 0, 0, 0)
        assert _unpack_datetime(_pack_datetime(dt)) == dt

    def test_date_round_trip(self):
        d = datetime.date(2024, 3, 15)
        assert _unpack_date(_pack_date(d)) == d

    def test_date_min(self):
        d = datetime.date(1, 1, 1)
        assert _unpack_date(_pack_date(d)) == d

    def test_date_leap_day(self):
        d = datetime.date(2024, 2, 29)
        assert _unpack_date(_pack_date(d)) == d

    def test_time_positive(self):
        td = datetime.timedelta(hours=10, minutes=30, seconds=45)
        assert _unpack_time(_pack_time(td)) == td

    def test_time_zero(self):
        td = datetime.timedelta(0)
        assert _unpack_time(_pack_time(td)) == td

    def test_time_with_microseconds(self):
        td = datetime.timedelta(
            hours=1, minutes=2, seconds=3, microseconds=456789,
        )
        assert _unpack_time(_pack_time(td)) == td

    def test_time_negative(self):
        td = datetime.timedelta(hours=-5, minutes=-30)
        assert _unpack_time(_pack_time(td)) == td

    def test_time_negative_with_microseconds(self):
        td = -datetime.timedelta(
            hours=1, minutes=2, seconds=3, microseconds=100000,
        )
        assert _unpack_time(_pack_time(td)) == td

    def test_time_large_hours(self):
        td = datetime.timedelta(hours=838, minutes=59, seconds=59)
        assert _unpack_time(_pack_time(td)) == td

    def test_time_only_microseconds(self):
        td = datetime.timedelta(microseconds=500000)
        assert _unpack_time(_pack_time(td)) == td

    def test_time_days_to_hours(self):
        td = datetime.timedelta(days=2, hours=3)
        assert _unpack_time(_pack_time(td)) == td


class TestDatetimeRowdat1RoundTrip(unittest.TestCase):

    def test_python_datetime_round_trip(self):
        dt = datetime.datetime(2024, 3, 15, 10, 30, 45, 123456)
        d = datetime.date(2024, 3, 15)
        td = datetime.timedelta(
            hours=10, minutes=30, seconds=45, microseconds=123456,
        )
        rows = [[dt, d, td]]
        dump_res = rowdat_1._dump(dt_col_types, [1], rows)
        ids, loaded = rowdat_1._load(dt_col_spec, dump_res)

        assert ids == [1]
        assert loaded[0][0] == dt
        assert loaded[0][1] == d
        assert loaded[0][2] == td

    def test_python_datetime_null(self):
        rows = [[None, None, None]]
        dump_res = rowdat_1._dump(dt_col_types, [1], rows)
        ids, loaded = rowdat_1._load(dt_col_spec, dump_res)
        assert loaded[0] == [None, None, None]

    def test_python_datetime_multiple_rows(self):
        dt1 = datetime.datetime(2024, 1, 1, 0, 0, 0, 0)
        dt2 = datetime.datetime(2024, 12, 31, 23, 59, 59, 999999)
        d1 = datetime.date(2024, 1, 1)
        d2 = datetime.date(2024, 12, 31)
        td1 = datetime.timedelta(hours=0)
        td2 = datetime.timedelta(hours=23, minutes=59, seconds=59)

        rows = [[dt1, d1, td1], [dt2, d2, td2]]
        dump_res = rowdat_1._dump(dt_col_types, [1, 2], rows)
        ids, loaded = rowdat_1._load(dt_col_spec, dump_res)

        assert ids == [1, 2]
        assert loaded[0] == [dt1, d1, td1]
        assert loaded[1] == [dt2, d2, td2]

    def test_python_negative_time_round_trip(self):
        td = -datetime.timedelta(hours=1, minutes=30)
        rows = [[td]]
        dump_res = rowdat_1._dump([TIME], [1], rows)
        ids, loaded = rowdat_1._load([('t', TIME)], dump_res)
        assert loaded[0][0] == td


class TestJSONEncoder(unittest.TestCase):

    def _encode(self, obj):
        return json.loads(json.dumps(obj, cls=JSONEncoder))

    def test_bytes_base64(self):
        assert self._encode(b'\x00\x01\x02') == 'AAEC'

    def test_datetime(self):
        dt = datetime.datetime(2024, 3, 15, 10, 30, 45, 123456)
        assert self._encode(dt) == '2024-03-15 10:30:45.123456'

    def test_datetime_no_microseconds(self):
        dt = datetime.datetime(2024, 3, 15, 10, 30, 45, 0)
        assert self._encode(dt) == '2024-03-15 10:30:45.000000'

    def test_date(self):
        d = datetime.date(2024, 3, 15)
        assert self._encode(d) == '2024-03-15'

    def test_timedelta_positive(self):
        td = datetime.timedelta(
            hours=10, minutes=30, seconds=45, microseconds=123456,
        )
        assert self._encode(td) == '10:30:45.123456'

    def test_timedelta_zero(self):
        td = datetime.timedelta(0)
        assert self._encode(td) == '0:00:00.000000'

    def test_timedelta_negative(self):
        td = -datetime.timedelta(hours=1, minutes=30)
        result = self._encode(td)
        assert result.startswith('-')

    def test_decimal(self):
        d = decimal.Decimal('123.456')
        assert self._encode(d) == '123.456'

    def test_decimal_negative(self):
        d = decimal.Decimal('-99.99')
        assert self._encode(d) == '-99.99'

    def test_decimal_integer(self):
        d = decimal.Decimal('42')
        assert self._encode(d) == '42'

    def test_unsupported_type_raises(self):
        with self.assertRaises(TypeError):
            json.dumps(object(), cls=JSONEncoder)


NEWDECIMAL = 246


class TestCallFunctionAccel(unittest.TestCase):
    """Test call_function_accel with datetime/date/time/decimal types."""

    def setUp(self):
        try:
            from _singlestoredb_accel import call_function_accel
            self.call_function_accel = call_function_accel
        except ImportError:
            self.skipTest('_singlestoredb_accel not available')

    def _round_trip(self, colspec, returns, rows, func):
        input_data = bytes(
            rowdat_1._dump(
                [c[1] for c in colspec], list(range(len(rows))), rows,
            ),
        )
        output_data = self.call_function_accel(
            colspec=colspec, returns=returns,
            data=input_data, func=func,
        )
        return rowdat_1._load(
            [(f'r{i}', t) for i, t in enumerate(returns)], output_data,
        )

    def test_datetime_pass_through(self):
        dt = datetime.datetime(2024, 3, 15, 10, 30, 45, 123456)
        colspec = [('dt', DATETIME)]
        ids, rows = self._round_trip(
            colspec, [DATETIME], [[dt]], lambda x: x,
        )
        assert rows[0][0] == dt

    def test_datetime_null(self):
        colspec = [('dt', DATETIME)]
        ids, rows = self._round_trip(
            colspec, [DATETIME], [[None]], lambda x: x,
        )
        assert rows[0][0] is None

    def test_date_pass_through(self):
        d = datetime.date(2024, 2, 29)
        colspec = [('d', DATE)]
        ids, rows = self._round_trip(
            colspec, [DATE], [[d]], lambda x: x,
        )
        assert rows[0][0] == d

    def test_date_null(self):
        colspec = [('d', DATE)]
        ids, rows = self._round_trip(
            colspec, [DATE], [[None]], lambda x: x,
        )
        assert rows[0][0] is None

    def test_time_pass_through(self):
        td = datetime.timedelta(hours=10, minutes=30, seconds=45, microseconds=123456)
        colspec = [('t', TIME)]
        ids, rows = self._round_trip(
            colspec, [TIME], [[td]], lambda x: x,
        )
        assert rows[0][0] == td

    def test_time_negative(self):
        td = -datetime.timedelta(hours=1, minutes=30)
        colspec = [('t', TIME)]
        ids, rows = self._round_trip(
            colspec, [TIME], [[td]], lambda x: x,
        )
        assert rows[0][0] == td

    def test_time_null(self):
        colspec = [('t', TIME)]
        ids, rows = self._round_trip(
            colspec, [TIME], [[None]], lambda x: x,
        )
        assert rows[0][0] is None

    def test_decimal_pass_through(self):
        colspec = [('d', NEWDECIMAL)]
        input_data = bytes(
            rowdat_1._dump(
                [NEWDECIMAL], [0], [['123.456']],
            ),
        )
        output_data = self.call_function_accel(
            colspec=colspec, returns=[NEWDECIMAL],
            data=input_data, func=lambda x: x,
        )
        # accel reads as Decimal, writes back as length-prefixed string;
        # _load treats 246 as string_types, so we get a string back
        _, rows = rowdat_1._load([('r0', NEWDECIMAL)], output_data)
        assert rows[0][0] == '123.456'

    def test_decimal_negative(self):
        colspec = [('d', NEWDECIMAL)]
        input_data = bytes(
            rowdat_1._dump(
                [NEWDECIMAL], [0], [['-99.99']],
            ),
        )
        output_data = self.call_function_accel(
            colspec=colspec, returns=[NEWDECIMAL],
            data=input_data, func=lambda x: x,
        )
        _, rows = rowdat_1._load([('r0', NEWDECIMAL)], output_data)
        assert rows[0][0] == '-99.99'

    def test_decimal_null(self):
        colspec = [('d', NEWDECIMAL)]
        input_data = bytes(
            rowdat_1._dump(
                [NEWDECIMAL], [0], [[None]],
            ),
        )
        output_data = self.call_function_accel(
            colspec=colspec, returns=[NEWDECIMAL],
            data=input_data, func=lambda x: x,
        )
        _, rows = rowdat_1._load([('r0', NEWDECIMAL)], output_data)
        assert rows[0][0] is None

    def test_mixed_datetime_types(self):
        dt = datetime.datetime(2024, 6, 15, 12, 0, 0, 0)
        d = datetime.date(2024, 6, 15)
        td = datetime.timedelta(hours=5, minutes=30)
        colspec = [('dt', DATETIME), ('d', DATE), ('t', TIME)]
        returns = [DATETIME, DATE, TIME]
        ids, rows = self._round_trip(
            colspec, returns, [[dt, d, td]], lambda a, b, c: (a, b, c),
        )
        assert rows[0] == [dt, d, td]

    def test_multiple_rows(self):
        dt1 = datetime.datetime(2024, 1, 1, 0, 0, 0, 0)
        dt2 = datetime.datetime(2024, 12, 31, 23, 59, 59, 999999)
        colspec = [('dt', DATETIME)]
        ids, rows = self._round_trip(
            colspec, [DATETIME], [[dt1], [dt2]], lambda x: x,
        )
        assert rows[0][0] == dt1
        assert rows[1][0] == dt2

    def test_func_raises(self):
        def bad_func(x):
            raise ValueError('intentional error')

        colspec = [('x', BIGINT)]
        input_data = bytes(rowdat_1._dump([BIGINT], [0], [[42]]))
        with self.assertRaises(Exception):
            self.call_function_accel(
                colspec=colspec, returns=[BIGINT],
                data=input_data, func=bad_func,
            )

    def test_wrong_return_type(self):
        colspec = [('x', BIGINT)]
        input_data = bytes(rowdat_1._dump([BIGINT], [0], [[42]]))
        with self.assertRaises(Exception):
            self.call_function_accel(
                colspec=colspec, returns=[STRING],
                data=input_data, func=lambda x: x,
            )

    def test_empty_input(self):
        colspec = [('x', BIGINT)]
        result = self.call_function_accel(
            colspec=colspec, returns=[BIGINT],
            data=b'', func=lambda x: x,
        )
        assert result == b''
