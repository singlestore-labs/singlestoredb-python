#!/usr/bin/env python3

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
    import pyarrow as pa
    has_pyarrow = True
except ImportError:
    has_pyarrow = False


DEFAULT_VALUES = {
    0: 0,  # Decimal
    1: 0,  # Tiny
    -1: 0,  # Unsigned Tiny
    2: 0,  # Short
    -2: 0,  # Unsigned Short
    3: 0,  # Long
    -3: 0,  # Unsigned Long
    4: float('nan'),  # Float
    5: float('nan'),  # Double,
    6: None,  # Null,
    7: 0,  # Timestamp
    8: 0,  # LongLong
    -8: 0,  # Unsigned Longlong
    9: 0,  # Int24
    -9: 0,  # Unsigned Int24
    10: 0,  # Date
    -10: 0,  # Date
    11: 0,  # Time
    12: 0,  # Datetime
    13: 0,  # Year
    15: None,  # Varchar
    -15: None,  # Varbinary
    16: 0,  # Bit
    245: None,  # JSON
    246: 0,  # NewDecimal
    -246: 0,  # NewDecimal
    247: None,  # Enum
    248: None,  # Set
    249: None,  # TinyText
    -249: None,  # TinyBlob
    250: None,  # MediumText
    -250: None,  # MediumBlob
    251: None,  # LongText
    -251: None,  # LongBlob
    252: None,  # Text
    -252: None,  # Blob
    253: None,  # VarString
    -253: None,  # VarBinary
    254: None,  # String
    -254: None,  # Binary
    255: None,  # Geometry
}


if has_numpy:
    NUMPY_TYPE_MAP = {
        0: object,  # Decimal
        1: np.int8,  # Tiny
        -1: np.uint8,  # Unsigned Tiny
        2: np.int16,  # Short
        -2: np.uint16,  # Unsigned Short
        3: np.int32,  # Long
        -3: np.uint32,  # Unsigned Long
        4: np.single,  # Float
        5: np.double,  # Double,
        6: object,  # Null,
        7: np.dtype('datetime64[us]'),  # Timestamp
        8: np.int64,  # LongLong
        -8: np.uint64,  # Unsigned LongLong
        9: np.int32,  # Int24
        -9: np.uint32,  # Unsigned Int24
        10: np.dtype('datetime64[D]'),  # Date
        11: np.dtype('timedelta64[us]'),  # Time
        12: np.dtype('datetime64[us]'),  # Datetime
        13: np.int16,  # Year
        15: object,  # Varchar
        -15: object,  # Varbinary
        16: object,  # Bit
        245: object,  # JSON
        246: object,  # NewDecimal
        -246: object,  # NewDecimal
        247: object,  # Enum
        248: object,  # Set
        249: object,  # TinyText
        -249: object,  # TinyBlob
        250: object,  # MediumText
        -250: object,  # MediumBlob
        251: object,  # LongText
        -251: object,  # LongBlob
        252: object,  # Blob
        -252: object,  # Text
        253: object,  # VarString
        -253: object,  # VarBlob
        254: object,  # String
        -254: object,  # Binary
        255: object,  # Geometry
    }
else:
    NUMPY_TYPE_MAP = {}

PANDAS_TYPE_MAP = NUMPY_TYPE_MAP

if has_pyarrow:
    PYARROW_TYPE_MAP = {
        0: pa.decimal128(18, 6),  # Decimal
        1: pa.int8(),  # Tiny
        -1: pa.uint8(),  # Unsigned Tiny
        2: pa.int16(),  # Short
        -2: pa.uint16(),  # Unsigned Short
        3: pa.int32(),  # Long
        -3: pa.uint32(),  # Unsigned Long
        4: pa.float32(),  # Float
        5: pa.float64(),  # Double,
        6: pa.null(),  # Null,
        7: pa.timestamp('us'),  # Timestamp
        8: pa.int64(),  # LongLong
        -8: pa.uint64(),  # Unsigned LongLong
        9: pa.int32(),  # Int24
        -9: pa.uint32(),  # Unsigned Int24
        10: pa.date64(),  # Date
        11: pa.duration('us'),  # Time
        12: pa.timestamp('us'),  # Datetime
        13: pa.int16(),  # Year
        15: pa.string(),  # Varchar
        -15: pa.binary(),  # Varbinary
        16: pa.binary(),  # Bit
        245: pa.string(),  # JSON
        246: pa.decimal128(18, 6),  # NewDecimal
        -246: pa.decimal128(18, 6),  # NewDecimal
        247: pa.string(),  # Enum
        248: pa.string(),  # Set
        249: pa.string(),  # TinyText
        -249: pa.binary(),  # TinyBlob
        250: pa.string(),  # MediumText
        -250: pa.binary(),  # MediumBlob
        251: pa.string(),  # LongText
        -251: pa.binary(),  # LongBlob
        252: pa.string(),  # Text
        -252: pa.binary(),  # Blob
        253: pa.string(),  # VarString
        -253: pa.binary(),  # VarBinary
        254: pa.string(),  # String
        -254: pa.binary(),  # Binary
        255: pa.string(),  # Geometry
    }
else:
    PYARROW_TYPE_MAP = {}

if has_polars:
    POLARS_TYPE_MAP = {
        0: pl.Decimal(10, 6),  # Decimal
        1: pl.Int8,  # Tiny
        -1: pl.UInt8,  # Unsigned Tiny
        2: pl.Int16,  # Short
        -2: pl.UInt16,  # Unsigned Short
        3: pl.Int32,  # Long
        -3: pl.UInt32,  # Unsigned Long
        4: pl.Float32,  # Float
        5: pl.Float64,  # Double,
        6: pl.Null,  # Null,
        7: pl.Datetime,  # Timestamp
        8: pl.Int64,  # LongLong
        -8: pl.UInt64,  # Unsigned LongLong
        9: pl.Int32,  # Int24
        -9: pl.UInt32,  # Unsigned Int24
        10: pl.Date,  # Date
        11: pl.Duration,  # Time
        12: pl.Datetime,  # Datetime
        13: pl.Int16,  # Year
        15: pl.Utf8,  # Varchar
        -15: pl.Utf8,  # Varbinary
        16: pl.Binary,  # Bit
        245: pl.Object,  # JSON
        246: pl.Decimal(10, 6),  # NewDecimal
        -246: pl.Decimal(10, 6),  # NewDecimal
        247: pl.Utf8,  # Enum
        248: pl.Utf8,  # Set
        249: pl.Utf8,  # TinyText
        -249: pl.Binary,  # TinyBlob
        250: pl.Utf8,  # MediumBlob
        -250: pl.Binary,  # MediumText
        251: pl.Utf8,  # LongBlob
        -251: pl.Binary,  # LongText
        252: pl.Utf8,  # Blob
        -252: pl.Binary,  # Text
        253: pl.Utf8,  # VarString
        -253: pl.Binary,  # VarBinary
        254: pl.Utf8,  # String
        -254: pl.Binary,  # Binary
        255: pl.Utf8,  # Geometry
    }
else:
    POLARS_TYPE_MAP = {}
