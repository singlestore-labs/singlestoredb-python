#!/usr/bin/env python
"""Data value conversion utilities."""
from __future__ import annotations

import base64
import datetime
import decimal
import json
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Set


def bit_or_none(x: Any) -> Optional[int]:
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
    if type(x) == str:
        return int.from_bytes(base64.b64decode(x), 'little')
    return int.from_bytes(x, 'little')


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


def base64_bytes_or_none(x: Any) -> Optional[bytes]:
    """
    Convert value to bytes.

    Parameters
    ----------
    x : Any
        Arbitrary value

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
        x = str(x)
    return base64.b64decode(x)


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
    15: bytes_or_none,
    16: bit_or_none,
    245: json_or_none,
    246: decimal_or_none,
    247: string_or_none,
    248: set_or_none,
    249: bytes_or_none,
    250: bytes_or_none,
    251: bytes_or_none,
    252: bytes_or_none,
    253: string_or_none,
    254: bytes_or_none,
    255: geometry_or_none,
}
