#!/usr/bin/env python
"""Data value conversion utilities."""
import datetime
from base64 import b64decode
from datetime import timezone
from decimal import Decimal
from json import loads as json_loads
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Set
from typing import Union


def _parse_isoformat_date(dtstr: str) -> List[Any]:
    # It is assumed that this function will only be called with a
    # string of length exactly 10, and (though this is not used) ASCII-only
    year = int(dtstr[0:4])
    if dtstr[4] != '-':
        raise ValueError('Invalid date separator: %s' % dtstr[4])

    month = int(dtstr[5:7])

    if dtstr[7] != '-':
        raise ValueError('Invalid date separator')

    day = int(dtstr[8:10])

    return [year, month, day]


def _parse_hh_mm_ss_ff(tstr: str) -> List[Any]:
    # Parses things of the form HH[:MM[:SS[.fff[fff]]]]
    len_str = len(tstr)

    time_comps = [0, 0, 0, 0]
    pos = 0
    for comp in range(0, 3):
        if (len_str - pos) < 2:
            raise ValueError('Incomplete time component')

        time_comps[comp] = int(tstr[pos:pos+2])

        pos += 2
        next_char = tstr[pos:pos+1]

        if not next_char or comp >= 2:
            break

        if next_char != ':':
            raise ValueError('Invalid time separator: %c' % next_char)

        pos += 1

    if pos < len_str:
        if tstr[pos] != '.':
            raise ValueError('Invalid microsecond component')
        else:
            pos += 1

            len_remainder = len_str - pos
            if len_remainder not in (3, 6):
                raise ValueError('Invalid microsecond component')

            time_comps[3] = int(tstr[pos:])
            if len_remainder == 3:
                time_comps[3] *= 1000

    return time_comps


def _parse_isoformat_time(tstr: str) -> List[Any]:
    # Format supported is HH[:MM[:SS[.fff[fff]]]][+HH:MM[:SS[.ffffff]]]
    len_str = len(tstr)
    if len_str < 2:
        raise ValueError('Isoformat time too short')

    # This is equivalent to re.search('[+-]', tstr), but faster
    tz_pos = (tstr.find('-') + 1 or tstr.find('+') + 1)
    timestr = tstr[:tz_pos-1] if tz_pos > 0 else tstr

    time_comps = _parse_hh_mm_ss_ff(timestr)

    tzi = None
    if tz_pos > 0:
        tzstr = tstr[tz_pos:]

        # Valid time zone strings are:
        # HH:MM               len: 5
        # HH:MM:SS            len: 8
        # HH:MM:SS.ffffff     len: 15

        if len(tzstr) not in (5, 8, 15):
            raise ValueError('Malformed time zone string')

        tz_comps = _parse_hh_mm_ss_ff(tzstr)
        if all(x == 0 for x in tz_comps):
            tzi = timezone.utc
        else:
            tzsign = -1 if tstr[tz_pos - 1] == '-' else 1

            td = datetime.timedelta(
                hours=tz_comps[0], minutes=tz_comps[1],
                seconds=tz_comps[2], microseconds=tz_comps[3],
            )

            tzi = timezone(tzsign * td)

    time_comps.append(tzi)

    return time_comps


def datetime_fromisoformat(date_string: str) -> datetime.datetime:
    """Construct a datetime from the output of datetime.isoformat()."""
    if not isinstance(date_string, str):
        raise TypeError('fromisoformat: argument must be str')

    # Split this at the separator
    dstr = date_string[0:10]
    tstr = date_string[11:]

    try:
        date_components = _parse_isoformat_date(dstr)
    except ValueError:
        raise ValueError(f'Invalid isoformat string: {date_string!r}')

    if tstr:
        try:
            time_components = _parse_isoformat_time(tstr)
        except ValueError:
            raise ValueError(f'Invalid isoformat string: {date_string!r}')
    else:
        time_components = [0, 0, 0, 0, None]

    return datetime.datetime(*(date_components + time_components))


def date_fromisoformat(date_string: str) -> datetime.date:
    """Construct a date from the output of date.isoformat()."""
    if not isinstance(date_string, str):
        raise TypeError('fromisoformat: argument must be str')

    try:
        assert len(date_string) == 10
        return datetime.date(*_parse_isoformat_date(date_string))
    except Exception:
        raise ValueError(f'Invalid isoformat string: {date_string!r}')


def time_fromisoformat(time_string: str) -> datetime.time:
    """Construct a time from the output of isoformat()."""
    if not isinstance(time_string, str):
        raise TypeError('fromisoformat: argument must be str')

    try:
        return datetime.time(*_parse_isoformat_time(time_string))
    except Exception:
        raise ValueError(f'Invalid isoformat string: {time_string!r}')


# datetime_fromisoformat = datetime.datetime.fromisoformat
# time_fromisoformat = datetime.time.fromisoformat
# date_fromisoformat = datetime.date.fromisoformat
datetime_min = datetime.datetime.min
date_min = datetime.date.min
datetime_combine = datetime.datetime.combine


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


def date_or_none(x: Optional[str]) -> Optional[datetime.date]:
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
    try:
        return date_fromisoformat(x)
    except ValueError:
        return None


def time_or_none(x: Optional[str]) -> Optional[datetime.timedelta]:
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
    try:
        return datetime_combine(date_min, time_fromisoformat(x)) - datetime_min
    except ValueError:
        return None


def datetime_or_none(x: Optional[str]) -> Optional[datetime.datetime]:
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
    try:
        return datetime_fromisoformat(x)
    except ValueError:
        return None


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
    11: time_or_none,
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
