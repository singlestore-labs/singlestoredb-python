#!/usr/bin/env python3
# mypy: disable-error-code="type-arg"
import asyncio
import time
import typing
from typing import List
from typing import NamedTuple
from typing import Optional
from typing import Tuple

import numpy as np

import singlestoredb.functions.dtypes as dt
from singlestoredb.functions import Masked
from singlestoredb.functions import Table
from singlestoredb.functions import udf
from singlestoredb.functions.dtypes import BIGINT
from singlestoredb.functions.dtypes import BLOB
from singlestoredb.functions.dtypes import DOUBLE
from singlestoredb.functions.dtypes import FLOAT
from singlestoredb.functions.dtypes import MEDIUMINT
from singlestoredb.functions.dtypes import SMALLINT
from singlestoredb.functions.dtypes import TEXT
from singlestoredb.functions.dtypes import TINYINT
from singlestoredb.functions.typing import numpy as npt
from singlestoredb.functions.typing import pandas as pdt
from singlestoredb.functions.typing import polars as plt
from singlestoredb.functions.typing import pyarrow as pat


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
    return x * y


@udf
def double_mult(x: float, y: float) -> float:
    return x * y


@udf(timeout=2)
def timeout_double_mult(x: float, y: float) -> float:
    time.sleep(5)
    return x * y


@udf
async def async_double_mult(x: float, y: float) -> float:
    return x * y


@udf(timeout=2)
async def async_timeout_double_mult(x: float, y: float) -> float:
    await asyncio.sleep(5)
    return x * y


@udf(
    args=[DOUBLE(nullable=False), DOUBLE(nullable=False)],
    returns=DOUBLE(nullable=False),
)
def pandas_double_mult(x: pdt.Series, y: pdt.Series) -> pdt.Series:
    return x * y


@udf
def numpy_double_mult(
    x: npt.Float64Array,
    y: npt.Float64Array,
) -> npt.Float64Array:
    return x * y


@udf
async def async_numpy_double_mult(
    x: npt.Float64Array,
    y: npt.Float64Array,
) -> npt.Float64Array:
    return x * y


@udf(
    args=[DOUBLE(nullable=False), DOUBLE(nullable=False)],
    returns=DOUBLE(nullable=False),
)
def arrow_double_mult(x: pat.Array, y: pat.Array) -> pat.Array:
    import pyarrow.compute as pc
    return pc.multiply(x, y)


@udf(
    args=[DOUBLE(nullable=False), DOUBLE(nullable=False)],
    returns=DOUBLE(nullable=False),
)
def polars_double_mult(x: plt.Series, y: plt.Series) -> plt.Series:
    return x * y


@udf
def nullable_double_mult(x: Optional[float], y: Optional[float]) -> Optional[float]:
    if x is None or y is None:
        return None
    return x * y


@udf(args=[FLOAT(nullable=False), FLOAT(nullable=False)], returns=FLOAT(nullable=False))
def float_mult(x: float, y: float) -> float:
    return x * y


@udf(args=[FLOAT(nullable=True), FLOAT(nullable=True)], returns=FLOAT(nullable=True))
def nullable_float_mult(x: Optional[float], y: Optional[float]) -> Optional[float]:
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
    if x is None or y is None:
        return None
    return x * y


@tinyint_udf
def pandas_tinyint_mult(x: pdt.Series, y: pdt.Series) -> pdt.Series:
    return x * y


@tinyint_udf
def polars_tinyint_mult(x: plt.Series, y: plt.Series) -> plt.Series:
    return x * y


@tinyint_udf
def numpy_tinyint_mult(x: np.ndarray, y: np.ndarray) -> np.ndarray:
    return x * y


@tinyint_udf
def arrow_tinyint_mult(x: pat.Array, y: pat.Array) -> pat.Array:
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
    if x is None or y is None:
        return None
    return x * y


@smallint_udf
def pandas_smallint_mult(x: pdt.Series, y: pdt.Series) -> pdt.Series:
    return x * y


@smallint_udf
def polars_smallint_mult(x: plt.Series, y: plt.Series) -> plt.Series:
    return x * y


@smallint_udf
def numpy_smallint_mult(x: np.ndarray, y: np.ndarray) -> np.ndarray:
    return x * y


@smallint_udf
def arrow_smallint_mult(x: pat.Array, y: pat.Array) -> pat.Array:
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
    if x is None or y is None:
        return None
    return x * y


@mediumint_udf
def pandas_mediumint_mult(x: pdt.Series, y: pdt.Series) -> pdt.Series:
    return x * y


@mediumint_udf
def polars_mediumint_mult(x: plt.Series, y: plt.Series) -> plt.Series:
    return x * y


@mediumint_udf
def numpy_mediumint_mult(x: np.ndarray, y: np.ndarray) -> np.ndarray:
    return x * y


@mediumint_udf
def arrow_mediumint_mult(x: pat.Array, y: pat.Array) -> pat.Array:
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
    if x is None or y is None:
        return None
    return x * y


@bigint_udf
def pandas_bigint_mult(x: pdt.Series, y: pdt.Series) -> pdt.Series:
    return x * y


@bigint_udf
def polars_bigint_mult(x: plt.Series, y: plt.Series) -> plt.Series:
    return x * y


@bigint_udf
def numpy_bigint_mult(x: np.ndarray, y: np.ndarray) -> np.ndarray:
    return x * y


@bigint_udf
def arrow_bigint_mult(x: pat.Array, y: pat.Array) -> pat.Array:
    import pyarrow.compute as pc
    return pc.multiply(x, y)


#
# NULLABLE TINYINT
#


nullable_tinyint_udf = udf(
    args=[TINYINT(nullable=True), TINYINT(nullable=True)],
    returns=TINYINT(nullable=True),
)


@nullable_tinyint_udf
def nullable_tinyint_mult(x: Optional[int], y: Optional[int]) -> Optional[int]:
    if x is None or y is None:
        return None
    return x * y


@nullable_tinyint_udf
def pandas_nullable_tinyint_mult(x: pdt.Series, y: pdt.Series) -> pdt.Series:
    return x * y


@nullable_tinyint_udf
def polars_nullable_tinyint_mult(x: plt.Series, y: plt.Series) -> plt.Series:
    return x * y


@nullable_tinyint_udf
def numpy_nullable_tinyint_mult(x: np.ndarray, y: np.ndarray) -> np.ndarray:
    return x * y


@nullable_tinyint_udf
def arrow_nullable_tinyint_mult(x: pat.Array, y: pat.Array) -> pat.Array:
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
    if x is None or y is None:
        return None
    return x * y


@nullable_smallint_udf
def pandas_nullable_smallint_mult(x: pdt.Series, y: pdt.Series) -> pdt.Series:
    return x * y


@nullable_smallint_udf
def polars_nullable_smallint_mult(x: plt.Series, y: plt.Series) -> plt.Series:
    return x * y


@nullable_smallint_udf
def numpy_nullable_smallint_mult(x: np.ndarray, y: np.ndarray) -> np.ndarray:
    return x * y


@nullable_smallint_udf
def arrow_nullable_smallint_mult(x: pat.Array, y: pat.Array) -> pat.Array:
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
    if x is None or y is None:
        return None
    return x * y


@nullable_mediumint_udf
def pandas_nullable_mediumint_mult(x: pdt.Series, y: pdt.Series) -> pdt.Series:
    return x * y


@nullable_mediumint_udf
def polars_nullable_mediumint_mult(x: plt.Series, y: plt.Series) -> plt.Series:
    return x * y


@nullable_mediumint_udf
def numpy_nullable_mediumint_mult(x: np.ndarray, y: np.ndarray) -> np.ndarray:
    return x * y


@nullable_mediumint_udf
def arrow_nullable_mediumint_mult(x: pat.Array, y: pat.Array) -> pat.Array:
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
    if x is None or y is None:
        return None
    return x * y


@nullable_bigint_udf
def pandas_nullable_bigint_mult(x: pdt.Series, y: pdt.Series) -> pdt.Series:
    return x * y


@nullable_bigint_udf
def polars_nullable_bigint_mult(x: plt.Series, y: plt.Series) -> plt.Series:
    return x * y


@nullable_bigint_udf
def numpy_nullable_bigint_mult(x: np.ndarray, y: np.ndarray) -> np.ndarray:
    return x * y


@nullable_bigint_udf
def arrow_nullable_bigint_mult(x: pat.Array, y: pat.Array) -> pat.Array:
    import pyarrow.compute as pc
    return pc.multiply(x, y)


@udf
def nullable_int_mult(x: Optional[int], y: Optional[int]) -> Optional[int]:
    if x is None or y is None:
        return None
    return x * y


@udf
def string_mult(x: str, times: int) -> str:
    return x * times


@udf(args=[TEXT(nullable=False), BIGINT(nullable=False)], returns=TEXT(nullable=False))
def pandas_string_mult(x: pdt.Series, times: pdt.Series) -> pdt.Series:
    return x * times


@udf
def numpy_string_mult(
    x: npt.NDArray[np.str_], times: npt.NDArray[np.int_],
) -> npt.NDArray[np.str_]:
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
    x_data, x_nulls = x
    y_data, y_nulls = y
    return Masked(x_data * y_data, x_nulls | y_nulls)


@udf
def numpy_nullable_tinyint_mult_with_masks(
    x: Masked[npt.NDArray[np.int8]], y: Masked[npt.NDArray[np.int8]],
) -> Masked[npt.NDArray[np.int8]]:
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
    import pyarrow.compute as pc
    x_data, x_nulls = x
    y_data, y_nulls = y
    return Masked(pc.multiply(x_data, y_data), pc.or_(x_nulls, y_nulls))


@udf(returns=[TEXT(nullable=False, name='res')])
def numpy_fixed_strings() -> Table[npt.StrArray]:
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
    pass


@udf
def table_function(n: int) -> Table[List[int]]:
    return Table([10] * n)


@udf
async def async_table_function(n: int) -> Table[List[int]]:
    return Table([10] * n)


@udf(
    returns=[
        dt.INT(name='c_int', nullable=False),
        dt.DOUBLE(name='c_float', nullable=False),
        dt.TEXT(name='c_str', nullable=False),
    ],
)
def table_function_tuple(n: int) -> Table[List[Tuple[int, float, str]]]:
    return Table([(10, 10.0, 'ten')] * n)


class MyTable(NamedTuple):
    c_int: int
    c_float: float
    c_str: str


@udf
def table_function_struct(n: int) -> Table[List[MyTable]]:
    return Table([MyTable(10, 10.0, 'ten')] * n)


@udf
def vec_function(
    x: npt.Float64Array, y: npt.Float64Array,
) -> npt.Float64Array:
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
    return x * y


class DFOutputs(typing.NamedTuple):
    res: np.int16
    res2: np.float64


@udf(args=VecInputs, returns=DFOutputs)
def vec_function_df(
    x: npt.IntArray, y: npt.IntArray,
) -> Table[pdt.DataFrame]:
    return pdt.DataFrame(dict(res=[1, 2, 3], res2=[1.1, 2.2, 3.3]))


@udf(args=VecInputs, returns=DFOutputs)
async def async_vec_function_df(
    x: npt.IntArray, y: npt.IntArray,
) -> Table[pdt.DataFrame]:
    return pdt.DataFrame(dict(res=[1, 2, 3], res2=[1.1, 2.2, 3.3]))


class MaskOutputs(typing.NamedTuple):
    res: Optional[np.int16]


@udf(args=VecInputs, returns=MaskOutputs)
def vec_function_ints_masked(
    x: Masked[npt.IntArray], y: Masked[npt.IntArray],
) -> Table[Masked[npt.IntArray]]:
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
    x_data, x_nulls = x
    y_data, y_nulls = y
    return Table(
        Masked(x_data * y_data, x_nulls | y_nulls),
        Masked(x_data * y_data, x_nulls | y_nulls),
    )
