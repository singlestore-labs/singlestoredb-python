#!/usr/bin/env python
"""Data value conversion utilities."""
from __future__ import annotations

import datetime
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


datetime_fromisoformat = datetime.datetime.fromisoformat
time_fromisoformat = datetime.time.fromisoformat
date_fromisoformat = datetime.date.fromisoformat
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
