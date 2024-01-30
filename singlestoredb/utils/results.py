#!/usr/bin/env python
"""SingleStoreDB package utilities."""
import collections
import warnings
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import NamedTuple
from typing import Optional
from typing import Tuple
from typing import Union

try:
    has_pandas = True
    from pandas import DataFrame
except ImportError:
    has_pandas = False
    DataFrame = Any

DBAPIResult = Union[List[Tuple[Any, ...]], Tuple[Any, ...]]
OneResult = Union[Tuple[Any, ...], Dict[str, Any], DataFrame]
ManyResult = Union[List[Tuple[Any, ...]], List[Dict[str, Any]], DataFrame]
Result = Union[OneResult, ManyResult]


class Description(NamedTuple):
    """Column definition."""

    name: str
    type_code: int
    display_size: Optional[int]
    internal_size: Optional[int]
    precision: Optional[int]
    scale: Optional[int]
    null_ok: Optional[bool]
    flags: Optional[int]
    charset: Optional[int]


def results_to_dataframe(
    desc: List[Description],
    res: Optional[DBAPIResult],
    single: Optional[bool] = False,
) -> Optional[Result]:
    """
    Convert results to a DataFrame.

    Parameters
    ----------
    desc : list of Descriptions
        The column metadata
    res : tuple or list of tuples
        The query results
    single : bool, optional
        Is this a single result (i.e., from `fetchone`)?

    Returns
    -------
    DataFrame
        If `pandas` is available
    tuple or list of tuples
        If `pandas` is not available

    """
    if not res:
        return res
    if has_pandas:
        columns = [x[0] for x in desc]
        if single:
            return DataFrame([res], columns=columns)
        return DataFrame(res, columns=columns)
    warnings.warn(
        'pandas is not available; unable to convert to DataFrame',
        RuntimeWarning,
    )
    return res


def results_to_namedtuple(
    desc: List[Description],
    res: Optional[DBAPIResult],
    single: Optional[bool] = False,
) -> Optional[Result]:
    """
    Convert results to namedtuples.

    Parameters
    ----------
    desc : list of Descriptions
        The column metadata
    res : tuple or list of tuples
        The query results
    single : bool, optional
        Is this a single result (i.e., from `fetchone`)?

    Returns
    -------
    namedtuple
        If single is True
    list of namedtuples
        If single is False

    """
    if not res:
        return res
    tup = collections.namedtuple(  # type: ignore
        'Row', list(
            [x[0] for x in desc],
        ), rename=True,
    )
    if single:
        return tup(*res)
    return [tup(*x) for x in res]


def results_to_dict(
    desc: List[Description],
    res: Optional[DBAPIResult],
    single: Optional[bool] = False,
) -> Optional[Result]:
    """
    Convert results to dicts.

    Parameters
    ----------
    desc : list of Descriptions
        The column metadata
    res : tuple or list of tuples
        The query results
    single : bool, optional
        Is this a single result (i.e., from `fetchone`)?

    Returns
    -------
    dict
        If single is True
    list of dicts
        If single is False

    """
    if not res:
        return res
    names = [x[0] for x in desc]
    if single:
        return dict(zip(names, res))
    return [dict(zip(names, x)) for x in res]


def results_to_tuple(
    desc: List[Description],
    res: Optional[DBAPIResult],
    single: Optional[bool] = False,
) -> Optional[Result]:
    """
    Convert results to tuples.

    Parameters
    ----------
    desc : list of Descriptions
        The column metadata
    res : tuple or list of tuples
        The query results
    single : bool, optional
        Is this a single result (i.e., from `fetchone`)?

    Returns
    -------
    tuple
        If single is True
    list of tuples
        If single is False

    """
    if not res:
        return res
    if single:
        if type(res) is tuple:
            return res
        return tuple(res)
    if type(res[0]) is tuple:
        return list(res)
    return [tuple(x) for x in res]


_converters: Dict[
    str, Callable[
        [List[Description], Optional[DBAPIResult], Optional[bool]],
        Optional[Result],
    ],
] = {
    'tuple': results_to_tuple,
    'tuples': results_to_tuple,
    'namedtuple': results_to_namedtuple,
    'namedtuples': results_to_namedtuple,
    'dict': results_to_dict,
    'dicts': results_to_dict,
    'dataframe': results_to_dataframe,
}


def format_results(
    format: str,
    desc: List[Description],
    res: Optional[DBAPIResult],
    single: bool = False,
) -> Optional[Result]:
    """
    Convert results to format specified in the package options.

    Parameters
    ----------
    desc : list of Descriptions
        The column metadata
    res : tuple or list of tuples
        The query results
    single : bool, optional
        Is this a single result (i.e., from `fetchone`)?

    Returns
    -------
    list of (named)tuples, list of dicts or DataFrame
        If single is False
    (named)tuple, dict, or DataFrame
        If single is True

    """
    return _converters[format](desc, res, single)
