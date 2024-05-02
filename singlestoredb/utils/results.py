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

from .dtypes import NUMPY_TYPE_MAP
from .dtypes import POLARS_TYPE_MAP
from .dtypes import PYARROW_TYPE_MAP

UNSIGNED_FLAG = 32
BINARY_FLAG = 128

try:
    has_numpy = True
    import numpy as np
except ImportError:
    has_numpy = False

try:
    has_pandas = True
    import pandas as pd
except ImportError:
    has_pandas = False

try:
    has_polars = True
    import polars as pl
except ImportError:
    has_polars = False

try:
    has_pyarrow = True
    import pyarrow as pa
except ImportError:
    has_pyarrow = False

DBAPIResult = Union[List[Tuple[Any, ...]], Tuple[Any, ...]]
OneResult = Union[
    Tuple[Any, ...], Dict[str, Any],
    'np.ndarray', 'pd.DataFrame', 'pl.DataFrame', 'pa.Table',
]
ManyResult = Union[
    List[Tuple[Any, ...]], List[Dict[str, Any]],
    'np.ndarray', 'pd.DataFrame', 'pl.DataFrame', 'pa.Table',
]
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


if has_numpy:
    # If an int column is nullable, we need to use floats rather than
    # ints for numpy and pandas.
    NUMPY_TYPE_MAP_CAST_FLOAT = NUMPY_TYPE_MAP.copy()
    NUMPY_TYPE_MAP_CAST_FLOAT.update({
        1: np.float32,  # Tiny
        -1: np.float32,  # Unsigned Tiny
        2: np.float32,  # Short
        -2: np.float32,  # Unsigned Short
        3: np.float64,  # Long
        -3: np.float64,  # Unsigned Long
        8: np.float64,  # LongLong
        -8: np.float64,  # Unsigned LongLong
        9: np.float64,  # Int24
        -9: np.float64,  # Unsigned Int24
        13: np.float64,  # Year
    })

if has_polars:
    # Remap date/times to strings; let polars do the parsing
    POLARS_TYPE_MAP = POLARS_TYPE_MAP.copy()
    POLARS_TYPE_MAP.update({
        7: pl.Utf8,
        10: pl.Utf8,
        12: pl.Utf8,
    })


INT_TYPES = set([1, 2, 3, 8, 9])
CHAR_TYPES = set([15, 249, 250, 251, 252, 253, 254])
DECIMAL_TYPES = set([0, 246])


def signed(desc: Description) -> int:
    if ((desc.flags or 0) & UNSIGNED_FLAG and desc.type_code in INT_TYPES) or \
            (desc.charset == 63 and desc.type_code in CHAR_TYPES):
        return -desc.type_code
    return desc.type_code


def _description_to_numpy_schema(desc: List[Description]) -> Dict[str, Any]:
    """Convert description to numpy array schema info."""
    if has_numpy:
        return dict(
            dtype=[
                (
                    x.name,
                    NUMPY_TYPE_MAP_CAST_FLOAT[signed(x)]
                    if x.null_ok else NUMPY_TYPE_MAP[signed(x)],
                )
                for x in desc
            ],
        )
    return {}


def _description_to_pandas_schema(desc: List[Description]) -> Dict[str, Any]:
    """Convert description to pandas DataFrame schema info."""
    if has_pandas:
        return dict(columns=[x.name for x in desc])
    return {}


def _decimalize_polars(desc: Description) -> 'pl.Decimal':
    return pl.Decimal(desc.precision or 10, desc.scale or 0)


def _description_to_polars_schema(desc: List[Description]) -> Dict[str, Any]:
    """Convert description to polars DataFrame schema info."""
    if has_polars:
        with_columns = {}
        for x in desc:
            if x.type_code in [7, 12]:
                if x.scale == 6:
                    with_columns[x.name] = pl.col(x.name).str.to_datetime(
                        '%Y-%m-%d %H:%M:%S.%6f', time_unit='us',
                    )
                else:
                    with_columns[x.name] = pl.col(x.name).str.to_datetime(
                        '%Y-%m-%d %H:%M:%S', time_unit='us',
                    )
            elif x.type_code == 10:
                with_columns[x.name] = pl.col(x.name).str.to_date('%Y-%m-%d')

        return dict(
            schema=dict(
                schema=[
                    (
                        x.name, _decimalize_polars(x)
                        if x.type_code in DECIMAL_TYPES else POLARS_TYPE_MAP[signed(x)],
                    )
                    for x in desc
                ],
            ),
            with_columns=with_columns,
        )
    return {}


def _decimalize_arrow(desc: Description) -> 'pa.Decimal128':
    return pa.decimal128(desc.precision or 10, desc.scale or 0)


def _description_to_arrow_schema(desc: List[Description]) -> Dict[str, Any]:
    """Convert description to Arrow Table schema info."""
    if has_pyarrow:
        return dict(
            schema=pa.schema([
                (
                    x.name, _decimalize_arrow(x)
                    if x.type_code in DECIMAL_TYPES else PYARROW_TYPE_MAP[signed(x)],
                )
                for x in desc
            ]),
        )
    return {}


def results_to_numpy(
    desc: List[Description],
    res: Optional[DBAPIResult],
    single: Optional[bool] = False,
    schema: Optional[Dict[str, Any]] = None,
) -> Optional[Result]:
    """
    Convert results to numpy.

    Parameters
    ----------
    desc : list of Descriptions
        The column metadata
    res : tuple or list of tuples
        The query results
    single : bool, optional
        Is this a single result (i.e., from `fetchone`)?
    schema : Dict[str, Any], optional
        Cached schema for current output format

    Returns
    -------
    numpy.array
        If `numpy` is available
    tuple or list of tuples
        If `numpy` is not available

    """
    if not res:
        return res
    if has_numpy:
        schema = _description_to_numpy_schema(desc) if schema is None else schema
        if single:
            return np.array([res], **schema)
        return np.array(list(res), **schema)
    warnings.warn(
        'numpy is not available; unable to convert to array',
        RuntimeWarning,
    )
    return res


def results_to_pandas(
    desc: List[Description],
    res: Optional[DBAPIResult],
    single: Optional[bool] = False,
    schema: Optional[Dict[str, Any]] = None,
) -> Optional[Result]:
    """
    Convert results to pandas.

    Parameters
    ----------
    desc : list of Descriptions
        The column metadata
    res : tuple or list of tuples
        The query results
    single : bool, optional
        Is this a single result (i.e., from `fetchone`)?
    schema : Dict[str, Any], optional
        Cached schema for current output format

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
        schema = _description_to_pandas_schema(desc) if schema is None else schema
        return pd.DataFrame(results_to_numpy(desc, res, single=single, schema=schema))
    warnings.warn(
        'pandas is not available; unable to convert to DataFrame',
        RuntimeWarning,
    )
    return res


def results_to_polars(
    desc: List[Description],
    res: Optional[DBAPIResult],
    single: Optional[bool] = False,
    schema: Optional[Dict[str, Any]] = None,
) -> Optional[Result]:
    """
    Convert results to polars.

    Parameters
    ----------
    desc : list of Descriptions
        The column metadata
    res : tuple or list of tuples
        The query results
    single : bool, optional
        Is this a single result (i.e., from `fetchone`)?
    schema : Dict[str, Any], optional
        Cached schema for current output format

    Returns
    -------
    DataFrame
        If `polars` is available
    tuple or list of tuples
        If `polars` is not available

    """
    if not res:
        return res
    if has_polars:
        schema = _description_to_polars_schema(desc) if schema is None else schema
        if single:
            out = pl.DataFrame([res], **schema.get('schema', {}))
        else:
            out = pl.DataFrame(res, **schema.get('schema', {}))
        with_columns = schema.get('with_columns')
        if with_columns:
            return out.with_columns(**with_columns)
        return out
    warnings.warn(
        'polars is not available; unable to convert to DataFrame',
        RuntimeWarning,
    )
    return res


def results_to_arrow(
    desc: List[Description],
    res: Optional[DBAPIResult],
    single: Optional[bool] = False,
    schema: Optional[Dict[str, Any]] = None,
) -> Optional[Result]:
    """
    Convert results to Arrow.

    Parameters
    ----------
    desc : list of Descriptions
        The column metadata
    res : tuple or list of tuples
        The query results
    single : bool, optional
        Is this a single result (i.e., from `fetchone`)?
    schema : Dict[str, Any], optional
        Cached schema for current output format

    Returns
    -------
    Table
        If `pyarrow` is available
    tuple or list of tuples
        If `pyarrow` is not available

    """
    if not res:
        return res
    if has_pyarrow:
        names = [x[0] for x in desc]
        schema = _description_to_arrow_schema(desc) if schema is None else schema
        if single:
            if isinstance(res, dict):
                return pa.Table.from_pylist([res], **schema)
            else:
                return pa.Table.from_pylist([dict(zip(names, res))], **schema)
        if isinstance(res[0], dict):
            return pa.Table.from_pylist(res, **schema)
        else:
            return pa.Table.from_pylist([dict(zip(names, x)) for x in res], **schema)
    warnings.warn(
        'pyarrow is not available; unable to convert to Table',
        RuntimeWarning,
    )
    return res


def results_to_namedtuple(
    desc: List[Description],
    res: Optional[DBAPIResult],
    single: Optional[bool] = False,
    schema: Optional[Dict[str, Any]] = None,
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
    schema : Dict[str, Any], optional
        Cached schema for current output format

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
    schema: Optional[Dict[str, Any]] = None,
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
    schema : Dict[str, Any], optional
        Cached schema for current output format

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
    schema: Optional[Dict[str, Any]] = None,
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
    schema : Dict[str, Any], optional
        Cached schema for current output format

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


def _no_schema(desc: List[Description]) -> Optional[Dict[str, Any]]:
    return {}


_converters: Dict[
    str, Callable[
        [
            List[Description], Optional[DBAPIResult],
            Optional[bool], Optional[Dict[str, Any]],
        ],
        Optional[Result],
    ],
] = {
    'tuple': results_to_tuple,
    'tuples': results_to_tuple,
    'namedtuple': results_to_namedtuple,
    'namedtuples': results_to_namedtuple,
    'dict': results_to_dict,
    'dicts': results_to_dict,
    'numpy': results_to_numpy,
    'pandas': results_to_pandas,
    'polars': results_to_polars,
    'arrow': results_to_arrow,
    'pyarrow': results_to_arrow,
}

_schema_converters: Dict[
    str, Callable[
        [List[Description]], Optional[Dict[str, Any]],
    ],
] = {
    'tuple': _no_schema,
    'tuples': _no_schema,
    'namedtuple': _no_schema,
    'namedtuples': _no_schema,
    'dict': _no_schema,
    'dicts': _no_schema,
    'structsequence': _no_schema,
    'structsequences': _no_schema,
    'numpy': _description_to_numpy_schema,
    'pandas': _description_to_numpy_schema,
    'polars': _description_to_polars_schema,
    'arrow': _description_to_arrow_schema,
    'pyarrow': _description_to_arrow_schema,
}


def format_results(
    format: str,
    desc: List[Description],
    res: Optional[DBAPIResult],
    single: bool = False,
    schema: Optional[Dict[str, Any]] = None,
) -> Optional[Result]:
    """
    Convert results to format specified in the package options.

    Parameters
    ----------
    format : str
        Name of the format type
    desc : list of Descriptions
        The column metadata
    res : tuple or list of tuples
        The query results
    single : bool, optional
        Is this a single result (i.e., from `fetchone`)?
    schema : Dict[str, Any], optional
        Cached schema for current output format

    Returns
    -------
    list of (named)tuples, list of dicts or DataFrame
        If single is False
    (named)tuple, dict, or DataFrame
        If single is True

    """
    return _converters[format](desc, res, single, schema)


def get_schema(
    format: str,
    desc: List[Description],
) -> Dict[str, Any]:
    """
    Convert a DB-API description to a format schema.

    Parameters
    ----------
    format : str
        Name of the format type
    desc : list of Descriptions
        The column metadata

    Returns
    -------
    Dict[str, Any]
        A dictionary of function parameters containing schema information
        for the given format type

    """
    if format in _schema_converters:
        return _schema_converters[format](desc) or {}
    return {}
