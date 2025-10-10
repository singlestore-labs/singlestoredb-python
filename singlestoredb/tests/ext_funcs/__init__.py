#!/usr/bin/env python3
# mypy: disable-error-code="type-arg"
import asyncio
import json
import time
import typing
from typing import List
from typing import NamedTuple
from typing import Optional
from typing import Tuple

import numpy as np
import pyarrow.compute as pc

import singlestoredb.functions.sql_types as dt
from singlestoredb.functions import Masked
from singlestoredb.functions import Table
from singlestoredb.functions import udf
from singlestoredb.functions.dtypes import BIGINT
from singlestoredb.functions.dtypes import BLOB
from singlestoredb.functions.dtypes import BOOL
from singlestoredb.functions.dtypes import DOUBLE
from singlestoredb.functions.dtypes import FLOAT
from singlestoredb.functions.dtypes import MEDIUMINT
from singlestoredb.functions.dtypes import SMALLINT
from singlestoredb.functions.dtypes import TEXT
from singlestoredb.functions.dtypes import TINYINT
from singlestoredb.functions.typing import JSON
from singlestoredb.functions.typing import numpy as npt
from singlestoredb.functions.typing import pandas as pdt
from singlestoredb.functions.typing import polars as plt
from singlestoredb.functions.typing import pyarrow as pat


@udf
def add(x: int, y: int) -> int:
    """
    Add two integers.

    Parameters
    ----------
    x : int
        First integer.
    y : int
        Second integer.

    Returns
    -------
    int
        Sum of x and y.

    """
    return x + y


@udf
def add_vec(x: npt.Int64Array, y: npt.Int64Array) -> npt.Int64Array:
    """
    Add two numpy arrays of int64.

    Parameters
    ----------
    x : numpy.ndarray
        First array.
    y : numpy.ndarray
        Second array.

    Returns
    -------
    numpy.ndarray
        Elementwise sum of x and y.

    """
    return x + y


@udf
async def async_add(x: int, y: int) -> int:
    """
    Asynchronously add two integers.

    Parameters
    ----------
    x : int
        First integer.
    y : int
        Second integer.

    Returns
    -------
    int
        Sum of x and y.

    """
    return x + y


@udf
async def async_add_vec_vec(x: npt.Int64Array, y: npt.Int64Array) -> npt.Int64Array:
    """
    Asynchronously add two numpy arrays of int64.

    Parameters
    ----------
    x : numpy.ndarray
        First array.
    y : numpy.ndarray
        Second array.

    Returns
    -------
    numpy.ndarray
        Elementwise sum of x and y.

    """
    return x + y


@udf
def doc_test(x: int, y: float) -> int:
    """
    A simple function to test the decorator and documentation.

    Parameters
    ----------
    x : int
        An integer to be multiplied by 2.
    y : float
        A float that is not used in the computation.

    Examples
    --------
    Basic usage of the function:
    >>> doc_test(
    ...     3, 4.5,
    ... )
    6

    Another example with different values:
    >>> doc_test(5, 2.0)
    10

    SQL Example
    sql> SELECT doc_test(3, 4.5);
    6

    Final text

    Returns
    -------
    int
        The input integer multiplied by 2.

    """
    return x * 2


@udf
def int_mult(x: int, y: int) -> int:
    """
    Multiply two integers.

    Parameters
    ----------
    x : int
        First integer.
    y : int
        Second integer.

    Returns
    -------
    int
        Product of x and y.

    """
    return x * y


@udf
def double_mult(x: float, y: float) -> float:
    """
    Multiply two floats.

    Parameters
    ----------
    x : float
        First float.
    y : float
        Second float.

    Returns
    -------
    float
        Product of x and y.

    """
    return x * y


@udf(timeout=2)
def timeout_double_mult(x: float, y: float) -> float:
    """
    Multiply two floats after a delay.

    Parameters
    ----------
    x : float
        First float.
    y : float
        Second float.

    Returns
    -------
    float
        Product of x and y.

    """
    time.sleep(5)
    return x * y


@udf
async def async_double_mult(x: float, y: float) -> float:
    """
    Asynchronously multiply two floats.

    Parameters
    ----------
    x : float
        First float.
    y : float
        Second float.

    Returns
    -------
    float
        Product of x and y.

    """
    return x * y


@udf(timeout=2)
async def async_timeout_double_mult(x: float, y: float) -> float:
    """
    Asynchronously multiply two floats after a delay.

    Parameters
    ----------
    x : float
        First float.
    y : float
        Second float.

    Returns
    -------
    float
        Product of x and y.

    """
    await asyncio.sleep(5)
    return x * y


@udf(
    args=[DOUBLE(nullable=False), DOUBLE(nullable=False)],
    returns=DOUBLE(nullable=False),
)
def pandas_double_mult(x: pdt.Series, y: pdt.Series) -> pdt.Series:
    """
    Multiply two pandas Series of floats.

    Parameters
    ----------
    x : pandas.Series
        First series.
    y : pandas.Series
        Second series.

    Returns
    -------
    pandas.Series
        Elementwise product of x and y.

    """
    return x * y


@udf
def numpy_double_mult(
    x: npt.Float64Array,
    y: npt.Float64Array,
) -> npt.Float64Array:
    """
    Multiply two numpy arrays of float64.

    Parameters
    ----------
    x : numpy.ndarray
        First array.
    y : numpy.ndarray
        Second array.

    Returns
    -------
    numpy.ndarray
        Elementwise product of x and y.

    """
    return x * y


@udf
async def async_numpy_double_mult(
    x: npt.Float64Array,
    y: npt.Float64Array,
) -> npt.Float64Array:
    """
    Asynchronously multiply two numpy arrays of float64.

    Parameters
    ----------
    x : numpy.ndarray
        First array.
    y : numpy.ndarray
        Second array.

    Returns
    -------
    numpy.ndarray
        Elementwise product of x and y.

    """
    return x * y


@udf(
    args=[DOUBLE(nullable=False), DOUBLE(nullable=False)],
    returns=DOUBLE(nullable=False),
)
def arrow_double_mult(x: pat.Array, y: pat.Array) -> pat.Array:
    """
    Multiply two pyarrow arrays of doubles.

    Parameters
    ----------
    x : pyarrow.Array
        First array.
    y : pyarrow.Array
        Second array.

    Returns
    -------
    pyarrow.Array
        Elementwise product of x and y.

    """
    import pyarrow.compute as pc
    return pc.multiply(x, y)


@udf(
    args=[DOUBLE(nullable=False), DOUBLE(nullable=False)],
    returns=DOUBLE(nullable=False),
)
def polars_double_mult(x: plt.Series, y: plt.Series) -> plt.Series:
    """
    Multiply two polars Series of doubles.

    Parameters
    ----------
    x : polars.Series
        First series.
    y : polars.Series
        Second series.

    Returns
    -------
    polars.Series
        Elementwise product of x and y.

    """
    return x * y


@udf
def nullable_double_mult(x: Optional[float], y: Optional[float]) -> Optional[float]:
    """
    Multiply two optional floats, returning None if either is None.

    Parameters
    ----------
    x : float or None
        First value.
    y : float or None
        Second value.

    Returns
    -------
    float or None
        Product of x and y, or None.

    """
    if x is None or y is None:
        return None
    return x * y


@udf(args=[FLOAT(nullable=False), FLOAT(nullable=False)], returns=FLOAT(nullable=False))
def float_mult(x: float, y: float) -> float:
    """
    Multiply two floats.

    Parameters
    ----------
    x : float
        First float.
    y : float
        Second float.

    Returns
    -------
    float
        Product of x and y.

    """
    return x * y


@udf(args=[FLOAT(nullable=True), FLOAT(nullable=True)], returns=FLOAT(nullable=True))
def nullable_float_mult(x: Optional[float], y: Optional[float]) -> Optional[float]:
    """
    Multiply two optional floats, returning None if either is None.

    Parameters
    ----------
    x : float or None
        First value.
    y : float or None
        Second value.

    Returns
    -------
    float or None
        Product of x and y, or None.

    """
    if x is None or y is None:
        return None
    return x * y


#
# TINYINT
#

tinyint_udf = udf(
    args=[TINYINT(nullable=False), TINYINT(nullable=False)],
    returns=TINYINT(nullable=False),
)


@tinyint_udf
def tinyint_mult(x: Optional[int], y: Optional[int]) -> Optional[int]:
    """
    Multiply two optional tinyints, returning None if either is None.

    Parameters
    ----------
    x : int or None
        First value.
    y : int or None
        Second value.

    Returns
    -------
    int or None
        Product of x and y, or None.

    """
    if x is None or y is None:
        return None
    return x * y


@tinyint_udf
def pandas_tinyint_mult(x: pdt.Series, y: pdt.Series) -> pdt.Series:
    """
    Multiply two pandas Series of tinyints.

    Parameters
    ----------
    x : pandas.Series
        First series.
    y : pandas.Series
        Second series.

    Returns
    -------
    pandas.Series
        Elementwise product of x and y.

    """
    return x * y


@tinyint_udf
def polars_tinyint_mult(x: plt.Series, y: plt.Series) -> plt.Series:
    """
    Multiply two polars Series of tinyints.

    Parameters
    ----------
    x : polars.Series
        First series.
    y : polars.Series
        Second series.

    Returns
    -------
    polars.Series
        Elementwise product of x and y.

    """
    return x * y


@tinyint_udf
def numpy_tinyint_mult(x: np.ndarray, y: np.ndarray) -> np.ndarray:
    """
    Multiply two numpy arrays of tinyints.

    Parameters
    ----------
    x : numpy.ndarray
        First array.
    y : numpy.ndarray
        Second array.

    Returns
    -------
    numpy.ndarray
        Elementwise product of x and y.

    """
    return x * y


@tinyint_udf
def arrow_tinyint_mult(x: pat.Array, y: pat.Array) -> pat.Array:
    """
    Multiply two pyarrow arrays of tinyints.

    Parameters
    ----------
    x : pyarrow.Array
        First array.
    y : pyarrow.Array
        Second array.

    Returns
    -------
    pyarrow.Array
        Elementwise product of x and y.

    """
    import pyarrow.compute as pc
    return pc.multiply(x, y)

#
# SMALLINT
#


smallint_udf = udf(
    args=[SMALLINT(nullable=False), SMALLINT(nullable=False)],
    returns=SMALLINT(nullable=False),
)


@smallint_udf
def smallint_mult(x: Optional[int], y: Optional[int]) -> Optional[int]:
    """
    Multiply two optional smallints, returning None if either is None.

    Parameters
    ----------
    x : int or None
        First value.
    y : int or None
        Second value.

    Returns
    -------
    int or None
        Product of x and y, or None.

    """
    if x is None or y is None:
        return None
    return x * y


@smallint_udf
def pandas_smallint_mult(x: pdt.Series, y: pdt.Series) -> pdt.Series:
    """
    Multiply two pandas Series of smallints.

    Parameters
    ----------
    x : pandas.Series
        First series.
    y : pandas.Series
        Second series.

    Returns
    -------
    pandas.Series
        Elementwise product of x and y.

    """
    return x * y


@smallint_udf
def polars_smallint_mult(x: plt.Series, y: plt.Series) -> plt.Series:
    """
    Multiply two polars Series of smallints.

    Parameters
    ----------
    x : polars.Series
        First series.
    y : polars.Series
        Second series.

    Returns
    -------
    polars.Series
        Elementwise product of x and y.

    """
    return x * y


@smallint_udf
def numpy_smallint_mult(x: np.ndarray, y: np.ndarray) -> np.ndarray:
    """
    Multiply two numpy arrays of smallints.

    Parameters
    ----------
    x : numpy.ndarray
        First array.
    y : numpy.ndarray
        Second array.

    Returns
    -------
    numpy.ndarray
        Elementwise product of x and y.

    """
    return x * y


@smallint_udf
def arrow_smallint_mult(x: pat.Array, y: pat.Array) -> pat.Array:
    """
    Multiply two pyarrow arrays of smallints.

    Parameters
    ----------
    x : pyarrow.Array
        First array.
    y : pyarrow.Array
        Second array.

    Returns
    -------
    pyarrow.Array
        Elementwise product of x and y.

    """
    import pyarrow.compute as pc
    return pc.multiply(x, y)


#
# MEDIUMINT
#


mediumint_udf = udf(
    args=[MEDIUMINT(nullable=False), MEDIUMINT(nullable=False)],
    returns=MEDIUMINT(nullable=False),
)


@mediumint_udf
def mediumint_mult(x: Optional[int], y: Optional[int]) -> Optional[int]:
    """
    Multiply two optional mediumints, returning None if either is None.

    Parameters
    ----------
    x : int or None
        First value.
    y : int or None
        Second value.

    Returns
    -------
    int or None
        Product of x and y, or None.

    """
    if x is None or y is None:
        return None
    return x * y


@mediumint_udf
def pandas_mediumint_mult(x: pdt.Series, y: pdt.Series) -> pdt.Series:
    """
    Multiply two pandas Series of mediumints.

    Parameters
    ----------
    x : pandas.Series
        First series.
    y : pandas.Series
        Second series.

    Returns
    -------
    pandas.Series
        Elementwise product of x and y.

    """
    return x * y


@mediumint_udf
def polars_mediumint_mult(x: plt.Series, y: plt.Series) -> plt.Series:
    """
    Multiply two polars Series of mediumints.

    Parameters
    ----------
    x : polars.Series
        First series.
    y : polars.Series
        Second series.

    Returns
    -------
    polars.Series
        Elementwise product of x and y.

    """
    return x * y


@mediumint_udf
def numpy_mediumint_mult(x: np.ndarray, y: np.ndarray) -> np.ndarray:
    """
    Multiply two numpy arrays of mediumints.

    Parameters
    ----------
    x : numpy.ndarray
        First array.
    y : numpy.ndarray
        Second array.

    Returns
    -------
    numpy.ndarray
        Elementwise product of x and y.

    """
    return x * y


@mediumint_udf
def arrow_mediumint_mult(x: pat.Array, y: pat.Array) -> pat.Array:
    """
    Multiply two pyarrow arrays of mediumints.

    Parameters
    ----------
    x : pyarrow.Array
        First array.
    y : pyarrow.Array
        Second array.

    Returns
    -------
    pyarrow.Array
        Elementwise product of x and y.

    """
    import pyarrow.compute as pc
    return pc.multiply(x, y)


#
# BIGINT
#


bigint_udf = udf(
    args=[BIGINT(nullable=False), BIGINT(nullable=False)],
    returns=BIGINT(nullable=False),
)


@bigint_udf
def bigint_mult(x: Optional[int], y: Optional[int]) -> Optional[int]:
    """
    Multiply two optional bigints, returning None if either is None.

    Parameters
    ----------
    x : int or None
        First value.
    y : int or None
        Second value.

    Returns
    -------
    int or None
        Product of x and y, or None.

    """
    if x is None or y is None:
        return None
    return x * y


@bigint_udf
def pandas_bigint_mult(x: pdt.Series, y: pdt.Series) -> pdt.Series:
    """
    Multiply two pandas Series of bigints.

    Parameters
    ----------
    x : pandas.Series
        First series.
    y : pandas.Series
        Second series.

    Returns
    -------
    pandas.Series
        Elementwise product of x and y.

    """
    return x * y


@bigint_udf
def polars_bigint_mult(x: plt.Series, y: plt.Series) -> plt.Series:
    """
    Multiply two polars Series of bigints.

    Parameters
    ----------
    x : polars.Series
        First series.
    y : polars.Series
        Second series.

    Returns
    -------
    polars.Series
        Elementwise product of x and y.

    """
    return x * y


@bigint_udf
def numpy_bigint_mult(x: np.ndarray, y: np.ndarray) -> np.ndarray:
    """
    Multiply two numpy arrays of bigints.

    Parameters
    ----------
    x : numpy.ndarray
        First array.
    y : numpy.ndarray
        Second array.

    Returns
    -------
    numpy.ndarray
        Elementwise product of x and y.

    """
    return x * y


@bigint_udf
def arrow_bigint_mult(x: pat.Array, y: pat.Array) -> pat.Array:
    """
    Multiply two pyarrow arrays of bigints.

    Parameters
    ----------
    x : pyarrow.Array
        First array.
    y : pyarrow.Array
        Second array.

    Returns
    -------
    pyarrow.Array
        Elementwise product of x and y.

    """
    import pyarrow.compute as pc
    return pc.multiply(x, y)


#
# BOOL - Scalar (non-vector) tests
#

@udf
def bool_and(x: bool, y: bool) -> bool:
    """Scalar bool AND operation."""
    return x and y


@udf
def bool_or(x: bool, y: bool) -> bool:
    """Scalar bool OR operation."""
    return x or y


@udf
def bool_not(x: bool) -> bool:
    """Scalar bool NOT operation."""
    return not x


@udf
def bool_xor(x: bool, y: bool) -> bool:
    """Scalar bool XOR operation."""
    return x != y


#
# BOOL - Vector (non-nullable)
#

bool_udf = udf(
    args=[BOOL(nullable=False), BOOL(nullable=False)],
    returns=BOOL(nullable=False),
)


@bool_udf
def numpy_bool_and(x: np.ndarray, y: np.ndarray) -> np.ndarray:
    """Vector bool AND using numpy."""
    return x & y


@bool_udf
def pandas_bool_and(x: pdt.Series, y: pdt.Series) -> pdt.Series:
    """Vector bool AND using pandas."""
    return x & y


@bool_udf
def polars_bool_and(x: plt.Series, y: plt.Series) -> plt.Series:
    """Vector bool AND using polars."""
    return x & y


@bool_udf
def arrow_bool_and(x: pat.Array, y: pat.Array) -> pat.Array:
    """Vector bool AND using pyarrow."""
    import pyarrow as pa
    import pyarrow.compute as pc
    # Convert TINYINT (0/1) to bool by comparing with 0
    x_bool = pc.not_equal(x, 0)
    y_bool = pc.not_equal(y, 0)
    result_bool = pc.and_(x_bool, y_bool)
    # Convert back to int8 for TINYINT return type
    return pc.cast(result_bool, pa.int8())


#
# BOOL - Nullable scalar
#

@udf
def nullable_bool_and(x: Optional[bool], y: Optional[bool]) -> Optional[bool]:
    """Nullable scalar bool AND operation."""
    if x is None or y is None:
        return None
    return x and y


#
# BOOL - Nullable vector
#

nullable_bool_udf = udf(
    args=[BOOL(nullable=True), BOOL(nullable=True)],
    returns=BOOL(nullable=True),
)


@nullable_bool_udf
def numpy_nullable_bool_and(x: np.ndarray, y: np.ndarray) -> np.ndarray:
    """Nullable vector bool AND using numpy."""
    return x & y


@nullable_bool_udf
def pandas_nullable_bool_and(x: pdt.Series, y: pdt.Series) -> pdt.Series:
    """Nullable vector bool AND using pandas."""
    return x & y


@nullable_bool_udf
def polars_nullable_bool_and(x: plt.Series, y: plt.Series) -> plt.Series:
    """Nullable vector bool AND using polars."""
    return x & y


@nullable_bool_udf
def arrow_nullable_bool_and(x: pat.Array, y: pat.Array) -> pat.Array:
    """Nullable vector bool AND using pyarrow."""
    import pyarrow as pa
    import pyarrow.compute as pc
    # Convert TINYINT (0/1) to bool by comparing with 0
    x_bool = pc.not_equal(x, 0)
    y_bool = pc.not_equal(y, 0)
    result_bool = pc.and_(x_bool, y_bool)
    # Convert back to int8 for TINYINT return type
    return pc.cast(result_bool, pa.int8())


#
# BOOL - Masked variants (with explicit null handling)
#

@udf(
    args=[BOOL(nullable=True), BOOL(nullable=True)],
    returns=BOOL(nullable=True),
)
def numpy_nullable_bool_and_with_masks(
    x: Masked[npt.NDArray[np.bool_]], y: Masked[npt.NDArray[np.bool_]],
) -> Masked[npt.NDArray[np.bool_]]:
    """Nullable vector bool AND with masks using numpy."""
    x_data, x_nulls = x
    y_data, y_nulls = y
    return Masked(x_data & y_data, x_nulls | y_nulls)


@udf(
    args=[BOOL(nullable=True), BOOL(nullable=True)],
    returns=BOOL(nullable=True),
)
def pandas_nullable_bool_and_with_masks(
    x: Masked[pdt.Series], y: Masked[pdt.Series],
) -> Masked[pdt.Series]:
    """Nullable vector bool AND with masks using pandas."""
    x_data, x_nulls = x
    y_data, y_nulls = y
    return Masked(x_data & y_data, x_nulls | y_nulls)


@udf(
    args=[BOOL(nullable=True), BOOL(nullable=True)],
    returns=BOOL(nullable=True),
)
def polars_nullable_bool_and_with_masks(
    x: Masked[plt.Series], y: Masked[plt.Series],
) -> Masked[plt.Series]:
    """Nullable vector bool AND with masks using polars."""
    x_data, x_nulls = x
    y_data, y_nulls = y
    return Masked(x_data & y_data, x_nulls | y_nulls)


@udf(
    args=[BOOL(nullable=True), BOOL(nullable=True)],
    returns=BOOL(nullable=True),
)
def arrow_nullable_bool_and_with_masks(
    x: Masked[pat.Array], y: Masked[pat.Array],
) -> Masked[pat.Array]:
    """Nullable vector bool AND with masks using pyarrow."""
    import pyarrow as pa
    import pyarrow.compute as pc
    x_data, x_nulls = x
    y_data, y_nulls = y
    # Convert TINYINT (0/1) to bool by comparing with 0
    x_bool = pc.not_equal(x_data, 0)
    y_bool = pc.not_equal(y_data, 0)
    result_bool = pc.and_(x_bool, y_bool)
    # Convert back to int8 for TINYINT return type
    result_int = pc.cast(result_bool, pa.int8())
    return Masked(result_int, pc.or_(x_nulls, y_nulls))


#
# NULLABLE TINYINT
#


nullable_tinyint_udf = udf(
    args=[TINYINT(nullable=True), TINYINT(nullable=True)],
    returns=TINYINT(nullable=True),
)


@nullable_tinyint_udf
def nullable_tinyint_mult(x: Optional[int], y: Optional[int]) -> Optional[int]:
    """
    Multiply two optional tinyints, returning None if either is None.

    Parameters
    ----------
    x : int or None
        First value.
    y : int or None
        Second value.

    Returns
    -------
    int or None
        Product of x and y, or None.

    """
    if x is None or y is None:
        return None
    return x * y


@nullable_tinyint_udf
def pandas_nullable_tinyint_mult(x: pdt.Series, y: pdt.Series) -> pdt.Series:
    """
    Multiply two pandas Series of nullable tinyints.

    Parameters
    ----------
    x : pandas.Series
        First series.
    y : pandas.Series
        Second series.

    Returns
    -------
    pandas.Series
        Elementwise product of x and y.

    """
    return x * y


@nullable_tinyint_udf
def polars_nullable_tinyint_mult(x: plt.Series, y: plt.Series) -> plt.Series:
    """
    Multiply two polars Series of nullable tinyints.

    Parameters
    ----------
    x : polars.Series
        First series.
    y : polars.Series
        Second series.

    Returns
    -------
    polars.Series
        Elementwise product of x and y.

    """
    return x * y


@nullable_tinyint_udf
def numpy_nullable_tinyint_mult(x: np.ndarray, y: np.ndarray) -> np.ndarray:
    """
    Multiply two numpy arrays of nullable tinyints.

    Parameters
    ----------
    x : numpy.ndarray
        First array.
    y : numpy.ndarray
        Second array.

    Returns
    -------
    numpy.ndarray
        Elementwise product of x and y.

    """
    return x * y


@nullable_tinyint_udf
def arrow_nullable_tinyint_mult(x: pat.Array, y: pat.Array) -> pat.Array:
    """
    Multiply two pyarrow arrays of nullable tinyints.

    Parameters
    ----------
    x : pyarrow.Array
        First array.
    y : pyarrow.Array
        Second array.

    Returns
    -------
    pyarrow.Array
        Elementwise product of x and y.

    """
    import pyarrow.compute as pc
    return pc.multiply(x, y)

#
# NULLABLE SMALLINT
#


nullable_smallint_udf = udf(
    args=[SMALLINT(nullable=True), SMALLINT(nullable=True)],
    returns=SMALLINT(nullable=True),
)


@nullable_smallint_udf
def nullable_smallint_mult(x: Optional[int], y: Optional[int]) -> Optional[int]:
    """
    Multiply two optional smallints, returning None if either is None.

    Parameters
    ----------
    x : int or None
        First value.
    y : int or None
        Second value.

    Returns
    -------
    int or None
        Product of x and y, or None.

    """
    if x is None or y is None:
        return None
    return x * y


@nullable_smallint_udf
def pandas_nullable_smallint_mult(x: pdt.Series, y: pdt.Series) -> pdt.Series:
    """
    Multiply two pandas Series of nullable smallints.

    Parameters
    ----------
    x : pandas.Series
        First series.
    y : pandas.Series
        Second series.

    Returns
    -------
    pandas.Series
        Elementwise product of x and y.

    """
    return x * y


@nullable_smallint_udf
def polars_nullable_smallint_mult(x: plt.Series, y: plt.Series) -> plt.Series:
    """
    Multiply two polars Series of nullable smallints.

    Parameters
    ----------
    x : polars.Series
        First series.
    y : polars.Series
        Second series.

    Returns
    -------
    polars.Series
        Elementwise product of x and y.

    """
    return x * y


@nullable_smallint_udf
def numpy_nullable_smallint_mult(x: np.ndarray, y: np.ndarray) -> np.ndarray:
    """
    Multiply two numpy arrays of nullable smallints.

    Parameters
    ----------
    x : numpy.ndarray
        First array.
    y : numpy.ndarray
        Second array.

    Returns
    -------
    numpy.ndarray
        Elementwise product of x and y.

    """
    return x * y


@nullable_smallint_udf
def arrow_nullable_smallint_mult(x: pat.Array, y: pat.Array) -> pat.Array:
    """
    Multiply two pyarrow arrays of nullable smallints.

    Parameters
    ----------
    x : pyarrow.Array
        First array.
    y : pyarrow.Array
        Second array.

    Returns
    -------
    pyarrow.Array
        Elementwise product of x and y.

    """
    import pyarrow.compute as pc
    return pc.multiply(x, y)


#
# NULLABLE MEDIUMINT
#


nullable_mediumint_udf = udf(
    args=[MEDIUMINT(nullable=True), MEDIUMINT(nullable=True)],
    returns=MEDIUMINT(nullable=True),
)


@nullable_mediumint_udf
def nullable_mediumint_mult(x: Optional[int], y: Optional[int]) -> Optional[int]:
    """
    Multiply two optional mediumints, returning None if either is None.

    Parameters
    ----------
    x : int or None
        First value.
    y : int or None
        Second value.

    Returns
    -------
    int or None
        Product of x and y, or None.

    """
    if x is None or y is None:
        return None
    return x * y


@nullable_mediumint_udf
def pandas_nullable_mediumint_mult(x: pdt.Series, y: pdt.Series) -> pdt.Series:
    """
    Multiply two pandas Series of nullable mediumints.

    Parameters
    ----------
    x : pandas.Series
        First series.
    y : pandas.Series
        Second series.

    Returns
    -------
    pandas.Series
        Elementwise product of x and y.

    """
    return x * y


@nullable_mediumint_udf
def polars_nullable_mediumint_mult(x: plt.Series, y: plt.Series) -> plt.Series:
    """
    Multiply two polars Series of nullable mediumints.

    Parameters
    ----------
    x : polars.Series
        First series.
    y : polars.Series
        Second series.

    Returns
    -------
    polars.Series
        Elementwise product of x and y.

    """
    return x * y


@nullable_mediumint_udf
def numpy_nullable_mediumint_mult(x: np.ndarray, y: np.ndarray) -> np.ndarray:
    """
    Multiply two numpy arrays of nullable mediumints.

    Parameters
    ----------
    x : numpy.ndarray
        First array.
    y : numpy.ndarray
        Second array.

    Returns
    -------
    numpy.ndarray
        Elementwise product of x and y.

    """
    return x * y


@nullable_mediumint_udf
def arrow_nullable_mediumint_mult(x: pat.Array, y: pat.Array) -> pat.Array:
    """
    Multiply two pyarrow arrays of nullable mediumints.

    Parameters
    ----------
    x : pyarrow.Array
        First array.
    y : pyarrow.Array
        Second array.

    Returns
    -------
    pyarrow.Array
        Elementwise product of x and y.

    """
    import pyarrow.compute as pc
    return pc.multiply(x, y)


#
# NULLABLE BIGINT
#


nullable_bigint_udf = udf(
    args=[BIGINT(nullable=True), BIGINT(nullable=True)],
    returns=BIGINT(nullable=True),
)


@nullable_bigint_udf
def nullable_bigint_mult(x: Optional[int], y: Optional[int]) -> Optional[int]:
    """
    Multiply two optional bigints, returning None if either is None.

    Parameters
    ----------
    x : int or None
        First value.
    y : int or None
        Second value.

    Returns
    -------
    int or None
        Product of x and y, or None.

    """
    if x is None or y is None:
        return None
    return x * y


@nullable_bigint_udf
def pandas_nullable_bigint_mult(x: pdt.Series, y: pdt.Series) -> pdt.Series:
    """
    Multiply two pandas Series of nullable bigints.

    Parameters
    ----------
    x : pandas.Series
        First series.
    y : pandas.Series
        Second series.

    Returns
    -------
    pandas.Series
        Elementwise product of x and y.

    """
    return x * y


@nullable_bigint_udf
def polars_nullable_bigint_mult(x: plt.Series, y: plt.Series) -> plt.Series:
    """
    Multiply two polars Series of nullable bigints.

    Parameters
    ----------
    x : polars.Series
        First series.
    y : polars.Series
        Second series.

    Returns
    -------
    polars.Series
        Elementwise product of x and y.

    """
    return x * y


@nullable_bigint_udf
def numpy_nullable_bigint_mult(x: np.ndarray, y: np.ndarray) -> np.ndarray:
    """
    Multiply two numpy arrays of nullable bigints.

    Parameters
    ----------
    x : numpy.ndarray
        First array.
    y : numpy.ndarray
        Second array.

    Returns
    -------
    numpy.ndarray
        Elementwise product of x and y.

    """
    return x * y


@nullable_bigint_udf
def arrow_nullable_bigint_mult(x: pat.Array, y: pat.Array) -> pat.Array:
    """
    Multiply two pyarrow arrays of nullable bigints.

    Parameters
    ----------
    x : pyarrow.Array
        First array.
    y : pyarrow.Array
        Second array.

    Returns
    -------
    pyarrow.Array
        Elementwise product of x and y.

    """
    import pyarrow.compute as pc
    return pc.multiply(x, y)


@udf
def nullable_int_mult(x: Optional[int], y: Optional[int]) -> Optional[int]:
    """
    Multiply two optional integers, returning None if either is None.

    Parameters
    ----------
    x : int or None
        First value.
    y : int or None
        Second value.

    Returns
    -------
    int or None
        Product of x and y, or None.

    """
    if x is None or y is None:
        return None
    return x * y


@udf
def string_mult(x: str, times: int) -> str:
    """
    Repeat a string a given number of times.

    Parameters
    ----------
    x : str
        String to repeat.
    times : int
        Number of repetitions.

    Returns
    -------
    str
        Repeated string.

    """
    return x * times


@udf(args=[TEXT(nullable=False), BIGINT(nullable=False)], returns=TEXT(nullable=False))
def pandas_string_mult(x: pdt.Series, times: pdt.Series) -> pdt.Series:
    """
    Repeat each string in a pandas Series a given number of times.

    pandas Series do not support annotated element types, so the `args`
    parameter of the `@udf` decorator is used to specify the types.

    Parameters
    ----------
    x : pandas.Series
        Series of strings.
    times : pandas.Series
        Series of repetition counts.

    Returns
    -------
    pandas.Series
        Series of repeated strings.

    """
    return x * times


@udf
def numpy_string_mult(
    x: npt.NDArray[np.str_], times: npt.NDArray[np.int_],
) -> npt.NDArray[np.str_]:
    """
    Repeat each string in a numpy array a given number of times.

    Numpy arrays can be used to specify the element types of vector inputs.

    Parameters
    ----------
    x : numpy.ndarray
        Array of strings.
    times : numpy.ndarray
        Array of repetition counts.

    Returns
    -------
    numpy.ndarray
        Array of repeated strings.

    """
    return x * times


# @udf.polars
# def polars_string_mult(x: str, times: int) -> str:
#     print(type(x), x, type(times), times)
#     return x * times


# @udf.arrow
# def arrow_string_mult(x: str, times: int) -> str:
#     print(type(x), x, type(times), times)
#     import pyarrow.compute as pc
#     return pc.multiply(x, times)
#     return x * times


@udf
def nullable_string_mult(x: Optional[str], times: Optional[int]) -> Optional[str]:
    """
    Repeat a string a given number of times, returning None if either is None.

    Parameters
    ----------
    x : str or None
        String to repeat.
    times : int or None
        Number of repetitions.

    Returns
    -------
    str or None
        Repeated string or None.

    """
    if x is None or times is None:
        return None
    return x * times


@udf(
    args=[TINYINT(nullable=True), TINYINT(nullable=True)],
    returns=TINYINT(nullable=True),
)
def pandas_nullable_tinyint_mult_with_masks(
    x: Masked[pdt.Series], y: Masked[pdt.Series],
) -> Masked[pdt.Series]:
    """
    Multiply two masked pandas Series of nullable tinyints.

    Masks are used to represent null values in vector inputs and
    outputs that do not natively support nulls. Each parameter
    wrapped by the `Masked` type is a tuple of (data, nulls),
    where `data` is the original type and `nulls` is a boolean
    array indicating which elements are null.

    The return value is also wrapped in `Masked`, with the
    returned vectors being represented by a tuple of (data, nulls).

    Parameters
    ----------
    x : Masked[pandas.Series]
        First masked series.
    y : Masked[pandas.Series]
        Second masked series.

    Returns
    -------
    Masked[pandas.Series]
        Masked elementwise product of x and y.

    """
    x_data, x_nulls = x
    y_data, y_nulls = y
    return Masked(x_data * y_data, x_nulls | y_nulls)


@udf
def numpy_nullable_tinyint_mult_with_masks(
    x: Masked[npt.NDArray[np.int8]], y: Masked[npt.NDArray[np.int8]],
) -> Masked[npt.NDArray[np.int8]]:
    """
    Multiply two masked numpy arrays of nullable tinyints.

    Masks are used to represent null values in vector inputs and
    outputs that do not natively support nulls. Each parameter
    wrapped by the `Masked` type is a tuple of (data, nulls),
    where `data` is the original type and `nulls` is a boolean
    array indicating which elements are null.

    The return value is also wrapped in `Masked`, with the
    returned vectors being represented by a tuple of (data, nulls).

    Parameters
    ----------
    x : Masked[numpy.ndarray]
        First masked array.
    y : Masked[numpy.ndarray]
        Second masked array.

    Returns
    -------
    Masked[numpy.ndarray]
        Masked elementwise product of x and y.

    """
    x_data, x_nulls = x
    y_data, y_nulls = y
    return Masked(x_data * y_data, x_nulls | y_nulls)


@udf(
    args=[TINYINT(nullable=True), TINYINT(nullable=True)],
    returns=TINYINT(nullable=True),
)
def polars_nullable_tinyint_mult_with_masks(
    x: Masked[plt.Series], y: Masked[plt.Series],
) -> Masked[plt.Series]:
    """
    Multiply two masked polars Series of nullable tinyints.

    This function demonstrates how to handle masks in polars Series,
    which do not natively support nulls. Each parameter wrapped by the
    `Masked` type is a tuple of (data, nulls), where `data` is the
    original polars Series and `nulls` is a boolean Series indicating
    which elements are null.

    In addition, the element types of the polars Series are annotated
    using the `args` parameter of the `@udf` decorator, since polars
    Series do not support annotated element types directly.

    Parameters
    ----------
    x : Masked[polars.Series]
        First masked series.
    y : Masked[polars.Series]
        Second masked series.

    Returns
    -------
    Masked[polars.Series]
        Masked elementwise product of x and y.

    """
    x_data, x_nulls = x
    y_data, y_nulls = y
    return Masked(x_data * y_data, x_nulls | y_nulls)


@udf(
    args=[TINYINT(nullable=True), TINYINT(nullable=True)],
    returns=TINYINT(nullable=True),
)
def arrow_nullable_tinyint_mult_with_masks(
    x: Masked[pat.Array], y: Masked[pat.Array],
) -> Masked[pat.Array]:
    """
    Multiply two masked pyarrow arrays of nullable tinyints.

    Parameters
    ----------
    x : Masked[pyarrow.Array]
        First masked array.
    y : Masked[pyarrow.Array]
        Second masked array.

    Returns
    -------
    Masked[pyarrow.Array]
        Masked elementwise product of x and y.

    """
    import pyarrow.compute as pc
    x_data, x_nulls = x
    y_data, y_nulls = y
    return Masked(pc.multiply(x_data, y_data), pc.or_(x_nulls, y_nulls))


@udf(returns=[TEXT(nullable=False, name='res')])
def numpy_fixed_strings() -> Table[npt.StrArray]:
    """
    Return a table of fixed-length numpy strings.

    Table-valued functions must use a `Table` annotation which
    encapsulates the return type. The return type can be one or
    more vectors. If the return type is more than one native
    Python type, they must be wrapped in a `List[Tuple[...]]`.

    The return value mult also be wrapped in a `Table` instance.

    Returns
    -------
    Table[numpy.ndarray]
        Table containing fixed-length strings.

    """
    out = np.array(
        [
            'hello',
            'hi there 😜',
            '😜 bye',
        ], dtype=np.str_,
    )
    assert str(out.dtype) == '<U10'
    return Table(out)


@udf(returns=[TEXT(nullable=False, name='res'), TINYINT(nullable=False, name='res2')])
def numpy_fixed_strings_2() -> Table[npt.StrArray, npt.Int8Array]:
    """
    Return a table of fixed-length numpy strings and int8s.

    Returns
    -------
    Table[numpy.ndarray, numpy.ndarray]
        Table containing fixed-length strings and int8s.

    """
    out = np.array(
        [
            'hello',
            'hi there 😜',
            '😜 bye',
        ], dtype=np.str_,
    )
    assert str(out.dtype) == '<U10'
    return Table(out, out)


@udf(returns=[BLOB(nullable=False, name='res')])
def numpy_fixed_binary() -> Table[npt.BytesArray]:
    """
    Return a table of fixed-length numpy binary strings.

    Returns
    -------
    Table[numpy.ndarray]
        Table containing fixed-length binary strings.

    """
    out = np.array(
        [
            'hello'.encode('utf8'),
            'hi there 😜'.encode('utf8'),
            '😜 bye'.encode('utf8'),
        ], dtype=np.bytes_,
    )
    assert str(out.dtype) == '|S13'
    return Table(out)


@udf
def no_args_no_return_value() -> None:
    """Function with no arguments and no return value."""
    pass


@udf
def table_function(n: int) -> Table[List[int]]:
    """
    Return a table of n tens.

    When returning native Python types from a table-valued function,
    the return type must be wrapped in a `Table[List[...]]` annotation.

    Parameters
    ----------
    n : int
        Number of tens.

    Returns
    -------
    Table[List[int]]
        Table containing n tens.

    """
    return Table([10] * n)


@udf
async def async_table_function(n: int) -> Table[List[int]]:
    """
    Asynchronously return a table of n tens.

    Parameters
    ----------
    n : int
        Number of tens.

    Returns
    -------
    Table[List[int]]
        Table containing n tens.
    """
    return Table([10] * n)


@udf(
    returns=[
        dt.INT(name='c_int', nullable=False),
        dt.DOUBLE(name='c_float', nullable=False),
        dt.TEXT(name='c_str', nullable=False),
    ],
)
def table_function_tuple(n: int) -> Table[List[Tuple[int, float, str]]]:
    """
    Return a table of tuples (int, float, str).

    To return multiple native Python types from a table-valued function,
    the return type must be wrapped in a `Table[List[Tuple[...]]]` annotation

    Parameters
    ----------
    n : int
        Number of tuples.

    Returns
    -------
    Table[List[Tuple[int, float, str]]]
        Table containing n tuples.

    """
    return Table([(10, 10.0, 'ten')] * n)


class MyTable(NamedTuple):
    c_int: int
    c_float: float
    c_str: str


@udf
def table_function_struct(n: int) -> Table[List[MyTable]]:
    """
    Return a table of MyTable namedtuples.

    Multiple return values can also be represented using
    a NamedTuple, pydantic model, or dataclass.  Each
    field of the NamedTuple, pydantic model, or dataclass
    will be mapped to a column in the returned table.

    Parameters
    ----------
    n : int
        Number of tuples.

    Returns
    -------
    Table[List[MyTable]]
        Table containing n MyTable tuples.

    """
    return Table([MyTable(10, 10.0, 'ten')] * n)


@udf
def vec_function(
    x: npt.Float64Array, y: npt.Float64Array,
) -> npt.Float64Array:
    """
    Multiply two numpy arrays of float64.

    Parameters
    ----------
    x : numpy.ndarray
        First array.
    y : numpy.ndarray
        Second array.

    Returns
    -------
    numpy.ndarray
        Elementwise product of x and y.

    """
    return x * y


class VecInputs(typing.NamedTuple):
    x: np.int8
    y: np.int8


class VecOutputs(typing.NamedTuple):
    res: np.int16


@udf(args=VecInputs, returns=VecOutputs)
def vec_function_ints(
    x: npt.IntArray, y: npt.IntArray,
) -> npt.IntArray:
    """
    Multiply two numpy arrays of int.

    You can specify the types of arguments or return values
    using a NamedTuple, pydantic model, or dataclass. This is
    especially useful for vector inputs and outputs, where you
    may want to specify the element type of a numpy array or
    pandas Series.

    Parameters
    ----------
    x : numpy.ndarray
        First array.
    y : numpy.ndarray
        Second array.

    Returns
    -------
    numpy.ndarray
        Elementwise product of x and y.

    """
    return x * y


class DFOutputs(typing.NamedTuple):
    res: np.int16
    res2: np.float64


@udf(args=VecInputs, returns=DFOutputs)
def vec_function_df(
    x: npt.IntArray, y: npt.IntArray,
) -> Table[pdt.DataFrame]:
    """
    Return a pandas DataFrame with two columns.

    When using a `DataFrame` return type, the return type of the UDF
    must be wrapped in a `Table` annotation. The columns of the DataFrame
    are determined by the fields of the return type NamedTuple, pydantic
    model, or dataclass specified in the `returns` parameter of the `@udf`
    decorator.

    Parameters
    ----------
    x : numpy.ndarray
        First array.
    y : numpy.ndarray
        Second array.

    Returns
    -------
    Table[pandas.DataFrame]
        Table containing a DataFrame with columns 'res' and 'res2'.

    """
    return pdt.DataFrame(dict(res=[1, 2, 3], res2=[1.1, 2.2, 3.3]))


@udf(args=VecInputs, returns=DFOutputs)
async def async_vec_function_df(
    x: npt.IntArray, y: npt.IntArray,
) -> Table[pdt.DataFrame]:
    """
    Asynchronously return a pandas DataFrame with two columns.

    Parameters
    ----------
    x : numpy.ndarray
        First array.
    y : numpy.ndarray
        Second array.

    Returns
    -------
    Table[pandas.DataFrame]
        Table containing a DataFrame with columns 'res' and 'res2'.

    """
    return pdt.DataFrame(dict(res=[1, 2, 3], res2=[1.1, 2.2, 3.3]))


class MaskOutputs(typing.NamedTuple):
    res: Optional[np.int16]


@udf(args=VecInputs, returns=MaskOutputs)
def vec_function_ints_masked(
    x: Masked[npt.IntArray], y: Masked[npt.IntArray],
) -> Table[Masked[npt.IntArray]]:
    """
    Multiply two masked numpy arrays of int.

    Masked vectors can also be used in table-valued functions.
    The return type must be wrapped in a `Table` annotation.
    Each masked vector is represented by a `Masked` type, which
    encapsulates a tuple of (data, nulls), where `data` is the
    original vector type and `nulls` is a boolean array indicating
    which elements are null.

    Parameters
    ----------
    x : Masked[numpy.ndarray]
        First masked array.
    y : Masked[numpy.ndarray]
        Second masked array.

    Returns
    -------
    Table[Masked[numpy.ndarray]]
        Table containing masked elementwise product.

    """
    x_data, x_nulls = x
    y_data, y_nulls = y
    return Table(Masked(x_data * y_data, x_nulls | y_nulls))


class MaskOutputs2(typing.NamedTuple):
    res: Optional[np.int16]
    res2: Optional[np.int16]


@udf(args=VecInputs, returns=MaskOutputs2)
def vec_function_ints_masked2(
    x: Masked[npt.IntArray], y: Masked[npt.IntArray],
) -> Table[Masked[npt.IntArray], Masked[npt.IntArray]]:
    """
    Multiply two masked numpy arrays of int, returning two masked outputs.

    Parameters
    ----------
    x : Masked[numpy.ndarray]
        First masked array.
    y : Masked[numpy.ndarray]
        Second masked array.

    Returns
    -------
    Table[Masked[numpy.ndarray], Masked[numpy.ndarray]]
        Table containing two masked elementwise products.

    """
    x_data, x_nulls = x
    y_data, y_nulls = y
    return Table(
        Masked(x_data * y_data, x_nulls | y_nulls),
        Masked(x_data * y_data, x_nulls | y_nulls),
    )


#
# Begin JSON UDFs
#

# numpy

@udf
def json_object_numpy(
    x: npt.IntArray,
    y: npt.JSONArray,
) -> npt.JSONArray:
    """
    Create a numpy array of JSON objects from int and JSON arrays.

    The JSON type is used to represent JSON objects and lists.
    The JSON type has argument and return transformers which will
    automatically convert JSON strings from the database into
    native Python types (dicts and lists) when passed into a UDF,
    and convert native Python types back into JSON strings when
    returning from a UDF.

    Parameters
    ----------
    x : numpy.ndarray
        Array of integers.
    y : numpy.ndarray
        Array of JSON objects.

    Returns
    -------
    numpy.ndarray
        Array of JSON objects.

    """
    return npt.array([
        None if a == 0 and b is None else dict(
            x=a * 2 if a is not None else None,
            y=b['foo'] if b is not None else None,
        ) for a, b in zip(x, y)
    ])


@udf
def json_object_numpy_masked(
    x: Masked[npt.IntArray],
    y: Masked[npt.JSONArray],
) -> Masked[npt.JSONArray]:
    """
    Create a masked numpy array of JSON objects from masked int and JSON arrays.

    Just as with other types, the JSON type can be used with masked
    vectors to represent null values in vector inputs and outputs
    that do not natively support nulls. Each parameter wrapped by the
    `Masked` type is a tuple of (data, nulls), where `data` is the original
    type and `nulls` is a boolean array indicating which elements are null.

    Parameters
    ----------
    x : Masked[numpy.ndarray]
        Masked array of integers.
    y : Masked[numpy.ndarray]
        Masked array of JSON objects.

    Returns
    -------
    Masked[numpy.ndarray]
        Masked array of JSON objects.

    """
    (x_data, x_nulls), (y_data, y_nulls) = x, y
    return Masked(
        npt.array([
            dict(
                x=a * 2 if a is not None else 0,
                y=b['foo'] if b is not None else None,
            ) for a, b in zip(x_data, y_data)
        ]),
        x_nulls & y_nulls,
    )


@udf
def json_list_numpy(x: npt.IntArray, y: npt.JSONArray) -> npt.JSONArray:
    """
    Create a numpy array of JSON objects from int and JSON arrays, using list indexing.

    Parameters
    ----------
    x : numpy.ndarray
        Array of integers.
    y : numpy.ndarray
        Array of JSON lists.

    Returns
    -------
    numpy.ndarray
        Array of JSON objects.

    """
    return npt.array([
        None if a == 0 and b is None else dict(
            x=a * 2 if a is not None else None,
            y=b[0] if b is not None else None,
        )
        for a, b in zip(x, y)
    ])


@udf
def json_list_numpy_masked(
    x: Masked[npt.IntArray],
    y: Masked[npt.JSONArray],
) -> Masked[npt.JSONArray]:
    """
    Create a masked numpy array of JSON objects from masked int and JSON arrays.

    Parameters
    ----------
    x : Masked[numpy.ndarray]
        Masked array of integers.
    y : Masked[numpy.ndarray]
        Masked array of JSON lists.

    Returns
    -------
    Masked[numpy.ndarray]
        Masked array of JSON objects.

    """
    (x_data, x_nulls), (y_data, y_nulls) = x, y
    return Masked(
        npt.array([
            dict(
                x=a * 2 if a is not None else 0,
                y=b[0] if b is not None else None,
            ) for a, b in zip(x_data, y_data)
        ]),
        x_nulls & y_nulls,
    )


@udf
def json_object_numpy_tvf(
    x: npt.IntArray,
    y: npt.JSONArray,
) -> Table[npt.IntArray, npt.JSONArray]:
    """
    Return a table of int and JSON arrays based on input arrays.

    Parameters
    ----------
    x : numpy.ndarray
        Array of integers.
    y : numpy.ndarray
        Array of JSON objects.

    Returns
    -------
    Table[numpy.ndarray, numpy.ndarray]
        Table containing int and JSON arrays.

    """
    return Table(
        npt.array([x[0] * i for i in range(5)]),
        npt.array([
            dict(x=x[0] * i, y=y[0]['foo'] if y[0] is not None else None)
            for i in range(5)
        ]),
    )


@udf
def json_object_numpy_tvf_masked(
    x: Masked[npt.IntArray],
    y: Masked[npt.JSONArray],
) -> Table[Masked[npt.IntArray], Masked[npt.JSONArray]]:
    """
    Return a table of masked int and JSON arrays based on input arrays.

    Parameters
    ----------
    x : Masked[numpy.ndarray]
        Masked array of integers.
    y : Masked[numpy.ndarray]
        Masked array of JSON objects.

    Returns
    -------
    Table[Masked[numpy.ndarray], Masked[numpy.ndarray]]
        Table containing masked int and JSON arrays.

    """
    (x_data, _), (y_data, _) = x, y
    return Table(
        Masked(
            npt.array([
                0 if x_data[0] == 20 else x_data[0] * i for i in range(5)
            ]),
            npt.array([False, False, True, False, False]),
        ),
        Masked(
            npt.array([
                dict(
                    x=x_data[0] * i, y=y_data[0]['foo']
                    if i != 4 and y_data[0] is not None else None,
                )
                for i in range(5)
            ]),
            npt.array([False, False, False, False, True]),
        ),
    )


# pandas


@udf
def json_object_pandas(
    x: pdt.IntSeries,
    y: pdt.JSONSeries,
) -> pdt.JSONSeries:
    """
    Create a pandas Series of JSON objects from int and JSON series.

    Parameters
    ----------
    x : pandas.Series
        Series of integers.
    y : pandas.Series
        Series of JSON objects.

    Returns
    -------
    pandas.Series
        Series of JSON objects.

    """
    return pdt.Series([
        None if a == 0 and b is None else dict(
            x=a * 2 if a is not None else None,
            y=b['foo'] if b is not None else None,
        ) for a, b in zip(x, y)
    ])


@udf
def json_object_pandas_masked(
    x: Masked[pdt.IntSeries],
    y: Masked[pdt.JSONSeries],
) -> Masked[pdt.JSONSeries]:
    """
    Create a masked pandas Series of JSON objects from masked int and JSON series.

    Parameters
    ----------
    x : Masked[pandas.Series]
        Masked series of integers.
    y : Masked[pandas.Series]
        Masked series of JSON objects.

    Returns
    -------
    Masked[pandas.Series]
        Masked series of JSON objects.

    """
    (x_data, x_nulls), (y_data, y_nulls) = x, y
    return Masked(
        pdt.Series([
            dict(
                x=a * 2 if a is not None else 0,
                y=b['foo'] if b is not None else None,
            ) for a, b in zip(x_data, y_data)
        ]),
        x_nulls & y_nulls,
    )


@udf
def json_list_pandas(x: pdt.IntSeries, y: pdt.JSONSeries) -> pdt.JSONSeries:
    """
    Create a pandas Series of JSON objects from int and JSON series.

    Parameters
    ----------
    x : pandas.Series
        Series of integers.
    y : pandas.Series
        Series of JSON lists.

    Returns
    -------
    pandas.Series
        Series of JSON objects.

    """
    return pdt.Series([
        None if a == 0 and b is None else dict(
            x=a * 2 if a is not None else None,
            y=b[0] if b is not None else None,
        )
        for a, b in zip(x, y)
    ])


@udf
def json_list_pandas_masked(
    x: Masked[pdt.IntSeries],
    y: Masked[pdt.JSONSeries],
) -> Masked[pdt.JSONSeries]:
    """
    Create a masked pandas Series of JSON objects from masked int and JSON series.

    Parameters
    ----------
    x : Masked[pandas.Series]
        Masked series of integers.
    y : Masked[pandas.Series]
        Masked series of JSON lists.

    Returns
    -------
    Masked[pandas.Series]
        Masked series of JSON objects.

    """
    (x_data, x_nulls), (y_data, y_nulls) = x, y
    return Masked(
        pdt.Series([
            dict(
                x=a * 2 if a is not None else 0,
                y=b[0] if b is not None else None,
            ) for a, b in zip(x_data, y_data)
        ]),
        x_nulls & y_nulls,
    )


@udf
def json_object_pandas_tvf(
    x: pdt.IntSeries,
    y: pdt.JSONSeries,
) -> Table[pdt.IntSeries, pdt.JSONSeries]:
    """
    Return a table of int and JSON arrays based on input series.

    Parameters
    ----------
    x : pandas.Series
        Series of integers.
    y : pandas.Series
        Series of JSON objects.

    Returns
    -------
    Table[pandas.Series, pandas.Series]
        Table containing int and JSON series.

    """
    return Table(
        pdt.Series([x[0] * i for i in range(5)]),
        pdt.Series([
            dict(x=x[0] * i, y=y[0]['foo'] if y[0] is not None else None)
            for i in range(5)
        ]),
    )


@udf
def json_object_pandas_tvf_masked(
    x: Masked[pdt.IntSeries],
    y: Masked[pdt.JSONSeries],
) -> Table[Masked[pdt.IntSeries], Masked[pdt.JSONSeries]]:
    """
    Return a table of masked int and JSON arrays based on input series.

    Parameters
    ----------
    x : Masked[pandas.Series]
        Masked series of integers.
    y : Masked[pandas.Series]
        Masked series of JSON objects.

    Returns
    -------
    Table[Masked[pandas.Series], Masked[pandas.Series]]
        Table containing masked int and JSON series.

    """
    (x_data, _), (y_data, _) = x, y
    return Table(
        Masked(
            pdt.Series([
                0 if x_data[0] == 20 else x_data[0] * i for i in range(5)
            ]),
            pdt.Series([False, False, True, False, False]),
        ),
        Masked(
            pdt.Series([
                dict(
                    x=x_data[0] * i, y=y_data[0]['foo']
                    if i != 4 and y_data[0] is not None else None,
                )
                for i in range(5)
            ]),
            pdt.Series([False, False, False, False, True]),
        ),
    )


# polars


@udf
def json_object_polars(
    x: plt.IntSeries,
    y: plt.JSONSeries,
) -> plt.JSONSeries:
    """
    Create a polars Series of JSON objects from int and JSON series.

    Parameters
    ----------
    x : polars.Series
        Series of integers.
    y : polars.Series
        Series of JSON objects.

    Returns
    -------
    polars.Series
        Series of JSON objects.

    """
    return plt.Series([
        None if a == 0 and b is None else dict(
            x=a * 2 if a is not None else None,
            y=b['foo'] if b is not None else None,
        ) for a, b in zip(x, y)
    ])


@udf
def json_object_polars_masked(
    x: Masked[plt.IntSeries],
    y: Masked[plt.JSONSeries],
) -> Masked[plt.JSONSeries]:
    """
    Create a masked polars Series of JSON objects from masked int and JSON series.

    Parameters
    ----------
    x : Masked[polars.Series]
        Masked series of integers.
    y : Masked[polars.Series]
        Masked series of JSON objects.

    Returns
    -------
    Masked[polars.Series]
        Masked series of JSON objects.

    """
    (x_data, x_nulls), (y_data, y_nulls) = x, y
    return Masked(
        plt.Series([
            dict(
                x=a * 2 if a is not None else 0,
                y=b['foo'] if b is not None else None,
            ) for a, b in zip(x_data, y_data)
        ]),
        x_nulls & y_nulls,
    )


@udf
def json_list_polars(x: plt.IntSeries, y: plt.JSONSeries) -> plt.JSONSeries:
    """
    Create a polars Series of JSON objects from int and JSON series, using list indexing.

    Parameters
    ----------
    x : polars.Series
        Series of integers.
    y : polars.Series
        Series of JSON lists.

    Returns
    -------
    polars.Series
        Series of JSON objects.

    """
    return plt.Series([
        None if a == 0 and b is None else dict(
            x=a * 2 if a is not None else None,
            y=b[0] if b is not None else None,
        )
        for a, b in zip(x, y)
    ])


@udf
def json_list_polars_masked(
    x: Masked[plt.IntSeries],
    y: Masked[plt.JSONSeries],
) -> Masked[plt.JSONSeries]:
    """
    Create a masked polars Series of JSON objects from masked int and JSON series.

    Parameters
    ----------
    x : Masked[polars.Series]
        Masked series of integers.
    y : Masked[polars.Series]
        Masked series of JSON lists.

    Returns
    -------
    Masked[polars.Series]
        Masked series of JSON objects.

    """
    (x_data, x_nulls), (y_data, y_nulls) = x, y
    return Masked(
        plt.Series([
            dict(
                x=a * 2 if a is not None else 0,
                y=b[0] if b is not None else None,
            ) for a, b in zip(x_data, y_data)
        ]),
        x_nulls & y_nulls,
    )


@udf
def json_object_polars_tvf(
    x: plt.IntSeries,
    y: plt.JSONSeries,
) -> Table[plt.IntSeries, plt.JSONSeries]:
    """
    Return a table of int and JSON arrays based on input series.

    Parameters
    ----------
    x : polars.Series
        Series of integers.
    y : polars.Series
        Series of JSON objects.

    Returns
    -------
    Table[polars.Series, polars.Series]
        Table containing int and JSON series.

    """
    return Table(
        plt.Series([x[0] * i for i in range(5)]),
        plt.Series([
            dict(x=x[0] * i, y=y[0]['foo'] if y[0] is not None else None)
            for i in range(5)
        ]),
    )


@udf
def json_object_polars_tvf_masked(
    x: Masked[plt.IntSeries],
    y: Masked[plt.JSONSeries],
) -> Table[Masked[plt.IntSeries], Masked[plt.JSONSeries]]:
    """
    Return a table of masked int and JSON arrays based on input series.

    Parameters
    ----------
    x : Masked[polars.Series]
        Masked series of integers.
    y : Masked[polars.Series]
        Masked series of JSON objects.

    Returns
    -------
    Table[Masked[polars.Series], Masked[polars.Series]]
        Table containing masked int and JSON series.

    """
    (x_data, _), (y_data, _) = x, y
    return Table(
        Masked(
            plt.Series([
                0 if x_data[0] == 20 else x_data[0] * i for i in range(5)
            ]),
            plt.Series([False, False, True, False, False]),
        ),
        Masked(
            plt.Series([
                dict(
                    x=x_data[0] * i, y=y_data[0]['foo']
                    if i != 4 and y_data[0] is not None else None,
                )
                for i in range(5)
            ]),
            plt.Series([False, False, False, False, True]),
        ),
    )


# pyarrow


@udf
def json_object_pyarrow(
    x: pat.IntArray,
    y: pat.JSONArray,
) -> pat.JSONArray:
    """
    Create a pyarrow array of JSON objects from int and JSON arrays.

    Parameters
    ----------
    x : pyarrow.Array
        Array of integers.
    y : pyarrow.Array
        Array of JSON objects.

    Returns
    -------
    pyarrow.Array
        Array of JSON objects.

    """
    return pat.array([
        None if a == 0 and b.as_py() is None else dict(
            x=pc.multiply(a, 2) if a is not None else None,
            y=json.loads(b.as_py())['foo'] if b.as_py() is not None else None,
        ) for a, b in zip(x, y)
    ])


@udf
def json_object_pyarrow_masked(
    x: Masked[pat.IntArray],
    y: Masked[pat.JSONArray],
) -> Masked[pat.JSONArray]:
    """
    Create a masked pyarrow array of JSON objects from masked int and JSON arrays.

    Parameters
    ----------
    x : Masked[pyarrow.Array]
        Masked array of integers.
    y : Masked[pyarrow.Array]
        Masked array of JSON objects.

    Returns
    -------
    Masked[pyarrow.Array]
        Masked array of JSON objects.

    """
    (x_data, x_nulls), (y_data, y_nulls) = x, y
    return Masked(
        pat.array([
            dict(
                x=pc.multiply(a, 2) if a is not None else 0,
                y=json.loads(b.as_py())['foo'] if b.as_py() is not None else None,
            ) for a, b in zip(x_data, y_data)
        ]),
        pc.and_(x_nulls, y_nulls),
    )


@udf
def json_list_pyarrow(x: pat.IntArray, y: pat.JSONArray) -> pat.JSONArray:
    """
    Create a pyarrow array of JSON objects from int and JSON arrays.

    Parameters
    ----------
    x : pyarrow.Array
        Array of integers.
    y : pyarrow.Array
        Array of JSON lists.

    Returns
    -------
    pyarrow.Array
        Array of JSON objects.

    """
    return pat.array([
        None if a == 0 and b is None else dict(
            x=pc.multiply(a, 2) if a is not None else None,
            y=json.loads(b.as_py())[0] if b.as_py() is not None else None,
        )
        for a, b in zip(x, y)
    ])


@udf
def json_list_pyarrow_masked(
    x: Masked[pat.IntArray],
    y: Masked[pat.JSONArray],
) -> Masked[pat.JSONArray]:
    """
    Create a masked pyarrow array of JSON objects from masked int and JSON arrays.

    Parameters
    ----------
    x : Masked[pyarrow.Array]
        Masked array of integers.
    y : Masked[pyarrow.Array]
        Masked array of JSON lists.

    Returns
    -------
    Masked[pyarrow.Array]
        Masked array of JSON objects.

    """
    (x_data, x_nulls), (y_data, y_nulls) = x, y
    return Masked(
        pat.array([
            dict(
                x=pc.multiply(a, 2) if a is not None else 0,
                y=json.loads(b.as_py())[0] if b.as_py() is not None else None,
            ) for a, b in zip(x_data, y_data)
        ]),
        pc.and_(x_nulls, y_nulls),
    )


@udf
def json_object_pyarrow_tvf(
    x: pat.IntArray,
    y: pat.JSONArray,
) -> Table[pat.IntArray, pat.JSONArray]:
    """
    Return a table of int and JSON arrays based on input arrays.

    Parameters
    ----------
    x : pyarrow.Array
        Array of integers.
    y : pyarrow.Array
        Array of JSON objects.

    Returns
    -------
    Table[pyarrow.Array, pyarrow.Array]
        Table containing int and JSON arrays.

    """
    return Table(
        pat.array([pc.multiply(x[0], i) for i in range(5)]),
        pat.array([
            dict(
                x=pc.multiply(x[0], i),
                y=json.loads(y[0].as_py())['foo'] if y[0].as_py() is not None else None,
            )
            for i in range(5)
        ]),
    )


@udf
def json_object_pyarrow_tvf_masked(
    x: Masked[pat.IntArray],
    y: Masked[pat.JSONArray],
) -> Table[Masked[pat.IntArray], Masked[pat.JSONArray]]:
    """
    Return a table of masked int and JSON arrays based on input arrays.

    Parameters
    ----------
    x : Masked[pyarrow.Array]
        Masked array of integers.
    y : Masked[pyarrow.Array]
        Masked array of JSON objects.

    Returns
    -------
    Table[Masked[pyarrow.Array], Masked[pyarrow.Array]]
        Table containing masked int and JSON arrays.

    """
    (x_data, _), (y_data, _) = x, y
    return Table(
        Masked(
            pat.array([
                0 if x_data[0] == 20 else pc.multiply(x_data[0], i) for i in range(5)
            ]),
            pat.array([False, False, True, False, False]),
        ),
        Masked(
            pat.array([
                dict(
                    x=pc.multiply(x_data[0], i),
                    y=json.loads(str(y_data[0]))['foo']
                    if i != 4 and y_data[0].as_py() is not None else None,
                )
                for i in range(5)
            ]),
            pat.array([False, False, False, False, True]),
        ),
    )


@udf
def json_object_list(x: List[int], y: List[JSON]) -> List[JSON]:
    """
    Create a list of JSON objects from int and JSON lists.

    Parameters
    ----------
    x : list of int
        List of integers.
    y : list of JSON
        List of JSON objects or arrays.

    Returns
    -------
    list of JSON
        List of JSON objects.

    """
    return [dict(x=x * 2, y=y['foo']) for x, y in zip(x, y)]  # type: ignore


@udf
def json_list_list(x: List[int], y: List[JSON]) -> List[JSON]:
    """
    Create a list of JSON objects from int and JSON lists.

    Parameters
    ----------
    x : list of int
        List of integers.
    y : list of JSON
        List of JSON objects or arrays.

    Returns
    -------
    list of JSON
        List of JSON objects.

    """
    return [dict(x=x * 2, y=y[0] if isinstance(y, list) else None) for x, y in zip(x, y)]


@udf
def json_object_list_tvf(
    x: List[int], y: List[JSON],
) -> Table[List[Tuple[int, JSON]]]:
    """
    Return a table of transformed values from lists of int and JSON objects.

    Parameters
    ----------
    x : list of int
        List of integers.
    y : list of JSON
        List of JSON objects or arrays.

    Returns
    -------
    Table[List[Tuple[int, JSON]]]
        Table containing transformed values.

    """
    out: List[Tuple[int, JSON]] = []
    for i in range(5):
        out.append((
            x[0] * i,
            dict(x=x[0] * i, y=y[0]['foo'] if isinstance(y[0], dict) else None),
        ))
    return Table(out)


@udf
def json_object_nonvector(x: int, y: JSON) -> JSON:
    """
    Extract and transform values from a JSON object.

    Parameters
    ----------
    x : int
        An integer value.
    y : JSON
        A JSON object.

    Returns
    -------
    JSON
        A JSON object with transformed values.

    """
    if not isinstance(y, dict):
        raise ValueError('Expected dict for JSON object')
    return dict(x=x * 2, y=y['foo'])


@udf
def json_list_nonvector(x: int, y: JSON) -> JSON:
    """
    Extract and transform values from a JSON array.

    Parameters
    ----------
    x : int
        An integer value.
    y : JSON
        A JSON array.

    Returns
    -------
    JSON
        A JSON object with transformed values.

    """
    if not isinstance(y, list):
        raise ValueError('Expected list for JSON array')
    return dict(x=x * 2, y=y[0])


@udf
def json_object_nonvector_tvf(
    x: int, y: JSON,
) -> Table[List[Tuple[int, JSON]]]:
    """
    Return a table of transformed values from a JSON object.

    Parameters
    ----------
    x : int
        An integer value.
    y : JSON
        A JSON object.

    Returns
    -------
    Table[List[Tuple[int, JSON]]]
        Table containing transformed values.

    """
    out: List[Tuple[int, JSON]] = []
    for i in range(5):
        out.append((x * i, dict(x=x * i, y=y['foo'] if isinstance(y, dict) else None)))
    return Table(out)
