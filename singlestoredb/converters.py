#!/usr/bin/env python
"""Data value conversion utilities."""
import datetime
import re
from base64 import b64decode
from decimal import Decimal
from json import loads as json_loads
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Set
from typing import Union


# Cache fromisoformat methods if they exist
_dt_datetime_fromisoformat = None
if hasattr(datetime.datetime, 'fromisoformat'):
    _dt_datetime_fromisoformat = datetime.datetime.fromisoformat  # type: ignore
_dt_time_fromisoformat = None
if hasattr(datetime.time, 'fromisoformat'):
    _dt_time_fromisoformat = datetime.time.fromisoformat  # type: ignore
_dt_date_fromisoformat = None
if hasattr(datetime.date, 'fromisoformat'):
    _dt_date_fromisoformat = datetime.date.fromisoformat  # type: ignore


def _convert_second_fraction(s: str) -> int:
    if not s:
        return 0
    # Pad zeros to ensure the fraction length in microseconds
    s = s.ljust(6, '0')
    return int(s[:6])


DATETIME_RE = re.compile(
    r'(\d{1,4})-(\d{1,2})-(\d{1,2})[T ](\d{1,2}):(\d{1,2}):(\d{1,2})(?:.(\d{1,6}))?',
)

ZERO_DATETIMES = set([
    '0000-00-00 00:00:00',
    '0000-00-00 00:00:00.000',
    '0000-00-00 00:00:00.000000',
    '0000-00-00T00:00:00',
    '0000-00-00T00:00:00.000',
    '0000-00-00T00:00:00.000000',
])
ZERO_DATES = set([
    '0000-00-00',
])


def datetime_fromisoformat(
    obj: Union[str, bytes, bytearray],
) -> Union[datetime.datetime, str, None]:
    """Returns a DATETIME or TIMESTAMP column value as a datetime object:

      >>> datetime_fromisoformat('2007-02-25 23:06:20')
      datetime.datetime(2007, 2, 25, 23, 6, 20)
      >>> datetime_fromisoformat('2007-02-25T23:06:20')
      datetime.datetime(2007, 2, 25, 23, 6, 20)

    Illegal values are returned as str or None:

      >>> datetime_fromisoformat('2007-02-31T23:06:20')
      '2007-02-31T23:06:20'
      >>> datetime_fromisoformat('0000-00-00 00:00:00')
      None

    """
    if isinstance(obj, (bytes, bytearray)):
        obj = obj.decode('ascii')

    if obj in ZERO_DATETIMES:
        return None

    # Use datetime methods if possible
    if _dt_datetime_fromisoformat is not None:
        try:
            if ' ' in obj or 'T' in obj:
                return _dt_datetime_fromisoformat(obj)
            if _dt_date_fromisoformat is not None:
                date = _dt_date_fromisoformat(obj)
                return datetime.datetime(date.year, date.month, date.day)
        except ValueError:
            return obj

    m = DATETIME_RE.match(obj)
    if not m:
        mdate = date_fromisoformat(obj)
        if type(mdate) is str:
            return mdate
        return datetime.datetime(mdate.year, mdate.month, mdate.day)  # type: ignore

    try:
        groups = list(m.groups())
        groups[-1] = _convert_second_fraction(groups[-1])
        return datetime.datetime(*[int(x) for x in groups])  # type: ignore
    except ValueError:
        mdate = date_fromisoformat(obj)
        if type(mdate) is str:
            return mdate
        return datetime.datetime(mdate.year, mdate.month, mdate.day)  # type: ignore


TIMEDELTA_RE = re.compile(r'(-)?(\d{1,3}):(\d{1,2}):(\d{1,2})(?:.(\d{1,6}))?')


def timedelta_fromisoformat(
    obj: Union[str, bytes, bytearray],
) -> Union[datetime.timedelta, str, None]:
    """Returns a TIME column as a timedelta object:

      >>> timedelta_fromisoformat('25:06:17')
      datetime.timedelta(days=1, seconds=3977)
      >>> timedelta_fromisoformat('-25:06:17')
      datetime.timedelta(days=-2, seconds=82423)

    Illegal values are returned as string:

      >>> timedelta_fromisoformat('random crap')
      'random crap'

    Note that MySQL always returns TIME columns as (+|-)HH:MM:SS, but
    can accept values as (+|-)DD HH:MM:SS. The latter format will not
    be parsed correctly by this function.
    """
    if isinstance(obj, (bytes, bytearray)):
        obj = obj.decode('ascii')

    m = TIMEDELTA_RE.match(obj)
    if not m:
        return obj

    try:
        groups = list(m.groups())
        groups[-1] = _convert_second_fraction(groups[-1])
        negate = -1 if groups[0] else 1
        hours, minutes, seconds, microseconds = groups[1:]

        tdelta = (
            datetime.timedelta(
                hours=int(hours),
                minutes=int(minutes),
                seconds=int(seconds),
                microseconds=int(microseconds),
            )
            * negate
        )
        return tdelta
    except ValueError:
        return obj


TIME_RE = re.compile(r'(\d{1,2}):(\d{1,2}):(\d{1,2})(?:.(\d{1,6}))?')


def time_fromisoformat(
    obj: Union[str, bytes, bytearray],
) -> Union[datetime.time, str, None]:
    """Returns a TIME column as a time object:

      >>> time_fromisoformat('15:06:17')
      datetime.time(15, 6, 17)

    Illegal values are returned as str:

      >>> time_fromisoformat('-25:06:17')
      '-25:06:17'
      >>> time_fromisoformat('random crap')
      'random crap'

    Note that MySQL always returns TIME columns as (+|-)HH:MM:SS, but
    can accept values as (+|-)DD HH:MM:SS. The latter format will not
    be parsed correctly by this function.

    Also note that MySQL's TIME column corresponds more closely to
    Python's timedelta and not time. However if you want TIME columns
    to be treated as time-of-day and not a time offset, then you can
    use set this function as the converter for FIELD_TYPE.TIME.
    """
    if isinstance(obj, (bytes, bytearray)):
        obj = obj.decode('ascii')

    # Use datetime methods if possible
    if _dt_time_fromisoformat is not None:
        try:
            return _dt_time_fromisoformat(obj)
        except ValueError:
            return obj

    m = TIME_RE.match(obj)
    if not m:
        return obj

    try:
        groups = list(m.groups())
        groups[-1] = _convert_second_fraction(groups[-1])
        hours, minutes, seconds, microseconds = groups
        return datetime.time(
            hour=int(hours),
            minute=int(minutes),
            second=int(seconds),
            microsecond=int(microseconds),
        )
    except ValueError:
        return obj


def date_fromisoformat(
    obj: Union[str, bytes, bytearray],
) -> Union[datetime.date, str, None]:
    """Returns a DATE column as a date object:

      >>> date_fromisoformat('2007-02-26')
      datetime.date(2007, 2, 26)

    Illegal values are returned as str or None:

      >>> date_fromisoformat('2007-02-31')
      '2007-02-31'
      >>> date_fromisoformat('0000-00-00')
      None

    """
    if isinstance(obj, (bytes, bytearray)):
        obj = obj.decode('ascii')

    if obj in ZERO_DATES:
        return None

    # Use datetime methods if possible
    if _dt_date_fromisoformat is not None:
        try:
            return _dt_date_fromisoformat(obj)
        except ValueError:
            return obj

    try:
        return datetime.date(*[int(x) for x in obj.split('-', 2)])
    except ValueError:
        return obj


def identity(x: Any) -> Optional[Any]:
    """Return input value."""
    return x


def bit_or_none(x: Any) -> Optional[bytes]:
    """
    Convert value to bit.

    Parameters
    ----------
    x : Any
        Arbitrary value

    Returns
    -------
    int
        If value can be cast to a bit
    None
        If input value is None

    """
    if x is None:
        return None
    if type(x) is str:
        return b64decode(x)
    return x


def int_or_none(x: Any) -> Optional[int]:
    """
    Convert value to int.

    Parameters
    ----------
    x : Any
        Arbitrary value

    Returns
    -------
    int
        If value can be cast to an int
    None
        If input value is None

    """
    if x is None:
        return None
    return int(x)


def float_or_none(x: Any) -> Optional[float]:
    """
    Convert value to float.

    Parameters
    ----------
    x : Any
        Arbitrary value

    Returns
    -------
    float
        If value can be cast to a float
    None
        If input value is None

    """
    if x is None:
        return None
    return float(x)


def decimal_or_none(x: Any) -> Optional[Decimal]:
    """
    Convert value to decimal.

    Parameters
    ----------
    x : Any
        Arbitrary value

    Returns
    -------
    decimal.Decimal
        If value can be cast to a decimal
    None
        If input value is None

    """
    if x is None:
        return None
    return Decimal(x)


def date_or_none(x: Optional[str]) -> Optional[Union[datetime.date, str]]:
    """
    Convert value to a date.

    Parameters
    ----------
    x : Any
        Arbitrary value

    Returns
    -------
    datetime.date
        If value can be cast to a date
    None
        If input value is None

    """
    if x is None:
        return None
    return date_fromisoformat(x)


def timedelta_or_none(x: Optional[str]) -> Optional[Union[datetime.timedelta, str]]:
    """
    Convert value to a timedelta.

    Parameters
    ----------
    x : Any
        Arbitrary value

    Returns
    -------
    datetime.timedelta
        If value can be cast to a time
    None
        If input value is None

    """
    if x is None:
        return None
    return timedelta_fromisoformat(x)


def time_or_none(x: Optional[str]) -> Optional[Union[datetime.time, str]]:
    """
    Convert value to a time.

    Parameters
    ----------
    x : Any
        Arbitrary value

    Returns
    -------
    datetime.time
        If value can be cast to a time
    None
        If input value is None

    """
    if x is None:
        return None
    return time_fromisoformat(x)


def datetime_or_none(x: Optional[str]) -> Optional[Union[datetime.datetime, str]]:
    """
    Convert value to a datetime.

    Parameters
    ----------
    x : Any
        Arbitrary value

    Returns
    -------
    datetime.time
        If value can be cast to a datetime
    None
        If input value is None

    """
    if x is None:
        return None
    return datetime_fromisoformat(x)


def none(x: Any) -> None:
    """
    Return None.

    Parameters
    ----------
    x : Any
        Arbitrary value

    Returns
    -------
    None

    """
    return None


def json_or_none(x: Optional[str]) -> Optional[Union[Dict[str, Any], List[Any]]]:
    """
    Convert JSON to dict or list.

    Parameters
    ----------
    x : str
        JSON string

    Returns
    -------
    dict
        If JSON string contains an object
    list
        If JSON string contains a list
    None
        If input value is None

    """
    if x is None:
        return None
    return json_loads(x)


def set_or_none(x: Optional[str]) -> Optional[Set[str]]:
    """
    Convert value to set of strings.

    Parameters
    ----------
    x : str
        Input string value

    Returns
    -------
    set of strings
        If value contains a set of strings
    None
        If input value is None

    """
    if x is None:
        return None
    return set(y.strip() for y in x.split(','))


def geometry_or_none(x: Optional[str]) -> Optional[Any]:
    """
    Convert value to geometry coordinates.

    Parameters
    ----------
    x : Any
        Geometry value

    Returns
    -------
    ???
        If value is valid geometry value
    None
        If input value is None

    """
    if x is None:
        return None
    return x


# Map of database types and conversion functions
converters: Dict[int, Callable[..., Any]] = {
    0: decimal_or_none,
    1: int_or_none,
    2: int_or_none,
    3: int_or_none,
    4: float_or_none,
    5: float_or_none,
    6: none,
    7: datetime_or_none,
    8: int_or_none,
    9: int_or_none,
    10: date_or_none,
    11: timedelta_or_none,
    12: datetime_or_none,
    13: int_or_none,
    14: date_or_none,
    #   15: identity,
    16: bit_or_none,
    245: json_or_none,
    246: decimal_or_none,
    #   247: identity,
    248: set_or_none,
    #   249: identity,
    #   250: identity,
    #   251: identity,
    #   252: identity,
    #   253: identity,
    #   254: identity,
    255: geometry_or_none,
}
