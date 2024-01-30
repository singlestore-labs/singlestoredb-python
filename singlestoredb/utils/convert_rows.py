#!/usr/bin/env python
"""Data value conversion utilities."""
from typing import Any
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union


def convert_row(
    values: Optional[Union[List[Any], Tuple[Any, ...]]],
    converters: List[Any],
) -> Optional[Tuple[Any, ...]]:
    """
    Convert a row of data values.

    Parameters
    ----------
    values : tuple or None
        Tuple containing values in a row of data
    converters : list[tuple]
        List of two-element tuples containing a column index and a converter
        function. The column index specifies which column to apply the function to.

    Returns
    -------
    tuple or None

    """
    if values is None:
        return None
    if not converters:
        return tuple(values)
    values = list(values)
    for i, conv in enumerate(converters):
        idx, encoding, func = conv
        value = values[idx]
        if encoding is not None and value is not None:
            value = value.decode(encoding)
        if func is not None:
            values[idx] = func(value)
        else:
            values[idx] = value
    return tuple(values)


def convert_rows(rows: List[Any], converters: List[Any]) -> List[Any]:
    """
    Convert rows of data values.

    Parameters
    ----------
    rows : list of tuples or None
        Rows of data from a query
    converters : list[tuple]
        List of two-element tuples containing a column index and a converter
        function. The column index specifies which column to apply the function to.

    Returns
    -------
    list of tuples

    """
    if not rows or not converters:
        return rows
    rows = list(rows)
    for i, row in enumerate(rows):
        rows[i] = convert_row(row, converters=converters)
    return rows
