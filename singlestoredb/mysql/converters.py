# type: ignore
import datetime
import time
from decimal import Decimal

from ..converters import converters as decoders
from .err import ProgrammingError


def escape_item(val, charset, mapping=None):
    if mapping is None:
        mapping = encoders
    encoder = mapping.get(type(val))

    # Fallback to default when no encoder found
    if not encoder:
        try:
            encoder = mapping[str]
        except KeyError:
            raise TypeError('no default type converter defined')

    if encoder in (escape_dict, escape_sequence):
        val = encoder(val, charset, mapping)
    else:
        val = encoder(val, mapping)
    return val


def escape_dict(val, charset, mapping=None):
    n = {}
    for k, v in val.items():
        quoted = escape_item(v, charset, mapping)
        n[k] = quoted
    return n


def escape_sequence(val, charset, mapping=None):
    n = []
    for item in val:
        quoted = escape_item(item, charset, mapping)
        n.append(quoted)
    return '(' + ','.join(n) + ')'


def escape_set(val, charset, mapping=None):
    return ','.join([escape_item(x, charset, mapping) for x in val])


def escape_bool(value, mapping=None):
    return str(int(value))


def escape_int(value, mapping=None):
    return str(value)


def escape_float(value, mapping=None):
    s = repr(value)
    if s in ('inf', 'nan'):
        raise ProgrammingError('%s can not be used with MySQL' % s)
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


def escape_string(value, mapping=None):
    """
    Escapes *value* without adding quote.

    Value should be unicode

    """
    return value.translate(_escape_table)


def escape_bytes_prefixed(value, mapping=None):
    return "_binary'%s'" % value.decode('ascii', 'surrogateescape').translate(
        _escape_table,
    )


def escape_bytes(value, mapping=None):
    return "'%s'" % value.decode('ascii', 'surrogateescape').translate(_escape_table)


def escape_str(value, mapping=None):
    return "'%s'" % escape_string(str(value), mapping)


def escape_None(value, mapping=None):
    return 'NULL'


def escape_timedelta(obj, mapping=None):
    seconds = int(obj.seconds) % 60
    minutes = int(obj.seconds // 60) % 60
    hours = int(obj.seconds // 3600) % 24 + int(obj.days) * 24
    if obj.microseconds:
        fmt = "'{0:02d}:{1:02d}:{2:02d}.{3:06d}'"
    else:
        fmt = "'{0:02d}:{1:02d}:{2:02d}'"
    return fmt.format(hours, minutes, seconds, obj.microseconds)


def escape_time(obj, mapping=None):
    if obj.microsecond:
        fmt = "'{0.hour:02}:{0.minute:02}:{0.second:02}.{0.microsecond:06}'"
    else:
        fmt = "'{0.hour:02}:{0.minute:02}:{0.second:02}'"
    return fmt.format(obj)


def escape_datetime(obj, mapping=None):
    if obj.microsecond:
        fmt = "'{0.year:04}-{0.month:02}-{0.day:02} " \
              "{0.hour:02}:{0.minute:02}:{0.second:02}.{0.microsecond:06}'"
    else:
        fmt = "'{0.year:04}-{0.month:02}-{0.day:02} " \
              "{0.hour:02}:{0.minute:02}:{0.second:02}'"
    return fmt.format(obj)


def escape_date(obj, mapping=None):
    fmt = "'{0.year:04}-{0.month:02}-{0.day:02}'"
    return fmt.format(obj)


def escape_struct_time(obj, mapping=None):
    return escape_datetime(datetime.datetime(*obj[:6]))


def Decimal2Literal(o, d):
    return format(o, 'f')


def through(x):
    return x


# def convert_bit(b):
#    b = "\x00" * (8 - len(b)) + b # pad w/ zeroes
#    return struct.unpack(">Q", b)[0]
#
#     the snippet above is right, but MySQLdb doesn't process bits,
#     so we shouldn't either
convert_bit = through


encoders = {
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


# for MySQLdb compatibility
conversions = encoders.copy()
conversions.update(decoders)
Thing2Literal = escape_str

# Run doctests with `pytest --doctest-modules pymysql/converters.py`
