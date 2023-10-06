import datetime
import time
from decimal import Decimal
from typing import Any
from typing import Callable
from typing import Dict
from typing import Optional
from typing import Tuple
from typing import Union

from ..converters import converters as decoders
from .err import ProgrammingError

try:
    import numpy as np
    has_numpy = True
except ImportError:
    has_numpy = False

try:
    import shapely.geometry
    import shapely.wkt
    has_shapely = True
except ImportError:
    has_shapely = False

try:
    import pygeos
    has_pygeos = True
except ImportError:
    has_pygeos = False


Encoders = Dict[type, Callable[..., Union[str, Dict[str, str]]]]


def escape_item(val: Any, charset: str, mapping: Optional[Encoders] = None) -> str:
    if mapping is None:
        mapping = encoders
    encoder = mapping.get(type(val), None)

    # Fallback to default when no encoder found
    if encoder is None:
        try:
            encoder = mapping[str]
        except KeyError:
            raise TypeError('no default type converter defined')

    if encoder in (escape_dict, escape_sequence):
        val = encoder(val, charset, mapping)
    else:
        val = encoder(val, mapping)
    return val


def escape_dict(
    val: Dict[str, Any],
    charset: str,
    mapping: Optional[Encoders] = None,
) -> Dict[str, str]:
    n = {}
    for k, v in val.items():
        quoted = escape_item(v, charset, mapping)
        n[k] = quoted
    return n


def escape_sequence(
    val: Any,
    charset: str,
    mapping: Optional[Encoders] = None,
) -> str:
    n = []
    for item in val:
        quoted = escape_item(item, charset, mapping)
        n.append(quoted)
    return '(' + ','.join(n) + ')'


def escape_set(val: Any, charset: str, mapping: Optional[Encoders] = None) -> str:
    return ','.join([escape_item(x, charset, mapping) for x in val])


def escape_bool(value: Any, mapping: Optional[Encoders] = None) -> str:
    return str(int(value))


def escape_int(value: Any, mapping: Optional[Encoders] = None) -> str:
    return str(value)


def escape_float(
    value: Any,
    mapping: Optional[Encoders] = None,
    nan_as_null: bool = False,
    inf_as_null: bool = False,
) -> str:
    s = repr(value)
    if s == 'nan':
        if nan_as_null:
            return 'NULL'
        raise ProgrammingError(0, '%s can not be used with SingleStoreDB' % s)
    if s == 'inf':
        if inf_as_null:
            return 'NULL'
        raise ProgrammingError(0, '%s can not be used with SingleStoreDB' % s)
    if 'e' not in s:
        s += 'e0'
    return s


_escape_table = [chr(x) for x in range(128)]
_escape_table[0] = '\\0'
_escape_table[ord('\\')] = '\\\\'
_escape_table[ord('\n')] = '\\n'
_escape_table[ord('\r')] = '\\r'
_escape_table[ord('\032')] = '\\Z'
_escape_table[ord('"')] = '\\"'
_escape_table[ord("'")] = "\\'"


def escape_string(value: str, mapping: Optional[Encoders] = None) -> str:
    """
    Escapes *value* without adding quote.

    Value should be unicode

    """
    return value.translate(_escape_table)


def escape_bytes_prefixed(value: bytes, mapping: Optional[Encoders] = None) -> str:
    return "_binary X'{}'".format(value.hex())


def escape_bytes(value: bytes, mapping: Optional[Encoders] = None) -> str:
    return "X'{}'".format(value.hex())


def escape_str(value: str, mapping: Optional[Encoders] = None) -> str:
    return "'{}'".format(escape_string(str(value), mapping))


def escape_None(value: str, mapping: Optional[Encoders] = None) -> str:
    return 'NULL'


def escape_timedelta(obj: datetime.timedelta, mapping: Optional[Encoders] = None) -> str:
    seconds = int(obj.seconds) % 60
    minutes = int(obj.seconds // 60) % 60
    hours = int(obj.seconds // 3600) % 24 + int(obj.days) * 24
    if obj.microseconds:
        fmt = "'{0:02d}:{1:02d}:{2:02d}.{3:06d}'"
    else:
        fmt = "'{0:02d}:{1:02d}:{2:02d}'"
    return fmt.format(hours, minutes, seconds, obj.microseconds)


def escape_time(obj: datetime.time, mapping: Optional[Encoders] = None) -> str:
    if obj.microsecond:
        fmt = "'{0.hour:02}:{0.minute:02}:{0.second:02}.{0.microsecond:06}'"
    else:
        fmt = "'{0.hour:02}:{0.minute:02}:{0.second:02}'"
    return fmt.format(obj)


def escape_datetime(obj: datetime.datetime, mapping: Optional[Encoders] = None) -> str:
    if obj.microsecond:
        fmt = "'{0.year:04}-{0.month:02}-{0.day:02} " \
              "{0.hour:02}:{0.minute:02}:{0.second:02}.{0.microsecond:06}'"
    else:
        fmt = "'{0.year:04}-{0.month:02}-{0.day:02} " \
              "{0.hour:02}:{0.minute:02}:{0.second:02}'"
    return fmt.format(obj)


def escape_date(obj: datetime.date, mapping: Optional[Encoders] = None) -> str:
    fmt = "'{0.year:04}-{0.month:02}-{0.day:02}'"
    return fmt.format(obj)


def escape_struct_time(obj: Tuple[Any, ...], mapping: Optional[Encoders] = None) -> str:
    return escape_datetime(datetime.datetime(*obj[:6]))


def Decimal2Literal(o: Any, d: Any) -> str:
    return format(o, 'f')


def through(x: Any) -> Any:
    return x


# def convert_bit(b):
#    b = "\x00" * (8 - len(b)) + b # pad w/ zeroes
#    return struct.unpack(">Q", b)[0]
#
#     the snippet above is right, but MySQLdb doesn't process bits,
#     so we shouldn't either
convert_bit = through


encoders: Encoders = {
    bool: escape_bool,
    int: escape_int,
    float: escape_float,
    str: escape_str,
    bytes: escape_bytes,
    tuple: escape_sequence,
    list: escape_sequence,
    set: escape_sequence,
    frozenset: escape_sequence,
    dict: escape_dict,
    type(None): escape_None,
    datetime.date: escape_date,
    datetime.datetime: escape_datetime,
    datetime.timedelta: escape_timedelta,
    datetime.time: escape_time,
    time.struct_time: escape_struct_time,
    Decimal: Decimal2Literal,
}

if has_numpy:

    def escape_numpy(value: Any, mapping: Optional[Encoders] = None) -> str:
        """Convert numpy arrays to vectors of bytes."""
        return escape_bytes(value.tobytes(), mapping=mapping)

    encoders[np.ndarray] = escape_numpy
    encoders[np.float16] = escape_float
    encoders[np.float32] = escape_float
    encoders[np.float64] = escape_float
    if hasattr(np, 'float128'):
        encoders[np.float128] = escape_float
    encoders[np.uint] = escape_int
    encoders[np.uint8] = escape_int
    encoders[np.uint16] = escape_int
    encoders[np.uint32] = escape_int
    encoders[np.uint64] = escape_int
    encoders[np.integer] = escape_int
    encoders[np.int_] = escape_int
    encoders[np.int8] = escape_int
    encoders[np.int16] = escape_int
    encoders[np.int32] = escape_int
    encoders[np.int64] = escape_int

if has_shapely:

    def escape_shapely(value: Any, mapping: Optional[Encoders] = None) -> str:
        """Convert shapely geo objects."""
        return escape_str(shapely.wkt.dumps(value), mapping=mapping)

    encoders[shapely.geometry.Polygon] = escape_shapely
    encoders[shapely.geometry.Point] = escape_shapely
    encoders[shapely.geometry.LineString] = escape_shapely

if has_pygeos:

    def escape_pygeos(value: Any, mapping: Optional[Encoders] = None) -> str:
        """Convert pygeos objects."""
        return escape_str(pygeos.io.to_wkt(value), mapping=mapping)

    encoders[pygeos.Geometry] = escape_pygeos


# for MySQLdb compatibility
conversions = encoders.copy()  # type: ignore
conversions.update(decoders)   # type: ignore
Thing2Literal = escape_str

# Run doctests with `pytest --doctest-modules pymysql/converters.py`
