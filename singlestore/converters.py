#!/usr/bin/env python
"""Data value conversion utilities."""
from __future__ import annotations

import datetime
import decimal
import json
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Set


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


def decimal_or_none(x: Any) -> Optional[decimal.Decimal]:
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
    return decimal.Decimal(x)


def date_or_none(x: Any) -> Optional[datetime.date]:
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
    return datetime.date.fromisoformat(x)


def time_or_none(x: Any) -> Optional[datetime.time]:
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
    return datetime.time.fromisoformat(x)


def datetime_or_none(x: Any) -> Optional[datetime.datetime]:
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
    return datetime.datetime.fromisoformat(x)


def bytes_or_none(x: Any, encoding: str = 'utf-8') -> Optional[bytes]:
    """
    Convert value to bytes.

    Parameters
    ----------
    x : Any
        Arbitrary value
    encoding : str
        Character encoding for input strings

    Returns
    -------
    bytes
        If value can be cast to bytes
    None
        If input value is None

    """
    if x is None:
        return None
    if isinstance(x, bytes):
        return x
    return bytes(x, encoding=encoding)


def string_or_none(x: Any, encoding: str = 'utf-8') -> Optional[str]:
    """
    Convert value to str.

    Parameters
    ----------
    x : Any
        Arbitrary value
    encoding : str
        Character encoding for input bytes

    Returns
    -------
    str
        If value can be cast to a str
    None
        If input value is None

    """
    if x is None:
        return None
    if isinstance(x, str):
        return x
    return str(x, encoding=encoding)


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


def json_or_none(x: str) -> Optional[Dict[str, Any] | List[Any]]:
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
    if isinstance(x, (dict, list, tuple)):
        return x
    return json.loads(x)


def set_or_none(x: str) -> Optional[Set[str]]:
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


def geometry_or_none(x: Any) -> Optional[Any]:
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
converters: Dict[str | int, Callable[..., Any]] = {
    'DECIMAL': decimal_or_none,
    0x00: decimal_or_none,
    'TINY': int_or_none,
    0x01: int_or_none,
    'SHORT': int_or_none,
    0x02: int_or_none,
    'LONG': int_or_none,
    0x03: int_or_none,
    'INT': int_or_none,
    'FLOAT': float_or_none,
    0x04: float_or_none,
    'DOUBLE': float_or_none,
    0x05: float_or_none,
    'NULL': none,
    0x06: none,
    'TIMESTAMP': datetime_or_none,
    0x07: datetime_or_none,
    'LONGLONG': int_or_none,
    'BIGINT': int_or_none,
    0x08: int_or_none,
    'INT32': int_or_none,
    0x09: int_or_none,
    'DATE': date_or_none,
    0x0a: date_or_none,
    'TIME': time_or_none,
    0x0b: time_or_none,
    'DATETIME': datetime_or_none,
    0x0c: datetime_or_none,
    'YEAR': int_or_none,
    0x0d: int_or_none,
    'NEWDATE': date_or_none,
    0x0e: date_or_none,
    'VARCHAR': string_or_none,
    0x0f: string_or_none,
    'BIT': int_or_none,
    0x10: int_or_none,
    'JSON': json_or_none,
    0xf5: json_or_none,
    'NEWDECIMAL': decimal_or_none,
    0xf6: decimal_or_none,
    'ENUM': string_or_none,
    0xf7: string_or_none,
    'SET': set_or_none,
    0xf8: set_or_none,
    'TINY_BLOB': bytes_or_none,
    0xf9: bytes_or_none,
    'MEDIUM_BLOB': bytes_or_none,
    0xfa: bytes_or_none,
    'LONG_BLOB': bytes_or_none,
    0xfb: bytes_or_none,
    'BLOB': bytes_or_none,
    0xfc: bytes_or_none,
    'TEXT': bytes_or_none,
    'CHAR': bytes_or_none,
    'VAR_STRING': string_or_none,
    0xfd: string_or_none,
    'STRING': string_or_none,
    0xfe: string_or_none,
    'GEOMETRY': geometry_or_none,
    0xff: geometry_or_none,
}
