#!/usr/bin/env python3
# type: ignore
from typing import Optional
from typing import Tuple

from singlestoredb.functions.decorator import udf
from singlestoredb.functions.dtypes import BIGINT
from singlestoredb.functions.dtypes import FLOAT
from singlestoredb.functions.dtypes import MEDIUMINT
from singlestoredb.functions.dtypes import SMALLINT
from singlestoredb.functions.dtypes import TINYINT
from singlestoredb.functions.dtypes import VARCHAR


@udf
def double_mult(x: float, y: float) -> float:
    return x * y


@udf.pandas
def pandas_double_mult(x: float, y: float) -> float:
    return x * y


@udf.numpy
def numpy_double_mult(x: float, y: float) -> float:
    return x * y


@udf.arrow
def arrow_double_mult(x: float, y: float) -> float:
    import pyarrow.compute as pc
    return pc.multiply(x, y)


@udf.polars
def polars_double_mult(x: float, y: float) -> float:
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


def _int_mult(x: int, y: int) -> int:
    if x is None or y is None:
        return None
    return x * y


def _arrow_int_mult(x: int, y: int) -> int:
    import pyarrow.compute as pc
    return pc.multiply(x, y)


def _int_mult_with_masks(x: Tuple[int, bool], y: Tuple[int, bool]) -> Tuple[int, bool]:
    x_data, x_nulls = x
    y_data, y_nulls = y
    return (x_data * y_data, x_nulls | y_nulls)


def _arrow_int_mult_with_masks(
    x: Tuple[int, bool],
    y: Tuple[int, bool],
) -> Tuple[int, bool]:
    import pyarrow.compute as pc
    x_data, x_nulls = x
    y_data, y_nulls = y
    return (pc.multiply(x_data, y_data), pc.or_(x_nulls, y_nulls))


int_mult = udf(_int_mult, name='int_mult')

tinyint_mult = udf(
    _int_mult,
    name='tinyint_mult',
    args=[TINYINT(nullable=False), TINYINT(nullable=False)],
    returns=TINYINT(nullable=False),
)

pandas_tinyint_mult = udf.pandas(
    _int_mult,
    name='pandas_tinyint_mult',
    args=[TINYINT(nullable=False), TINYINT(nullable=False)],
    returns=TINYINT(nullable=False),
)

polars_tinyint_mult = udf.polars(
    _int_mult,
    name='polars_tinyint_mult',
    args=[TINYINT(nullable=False), TINYINT(nullable=False)],
    returns=TINYINT(nullable=False),
)

numpy_tinyint_mult = udf.numpy(
    _int_mult,
    name='numpy_tinyint_mult',
    args=[TINYINT(nullable=False), TINYINT(nullable=False)],
    returns=TINYINT(nullable=False),
)

arrow_tinyint_mult = udf.arrow(
    _arrow_int_mult,
    name='arrow_tinyint_mult',
    args=[TINYINT(nullable=False), TINYINT(nullable=False)],
    returns=TINYINT(nullable=False),
)

smallint_mult = udf(
    _int_mult,
    name='smallint_mult',
    args=[SMALLINT(nullable=False), SMALLINT(nullable=False)],
    returns=SMALLINT(nullable=False),
)

pandas_smallint_mult = udf.pandas(
    _int_mult,
    name='pandas_smallint_mult',
    args=[SMALLINT(nullable=False), SMALLINT(nullable=False)],
    returns=SMALLINT(nullable=False),
)

polars_smallint_mult = udf.polars(
    _int_mult,
    name='polars_smallint_mult',
    args=[SMALLINT(nullable=False), SMALLINT(nullable=False)],
    returns=SMALLINT(nullable=False),
)

numpy_smallint_mult = udf.numpy(
    _int_mult,
    name='numpy_smallint_mult',
    args=[SMALLINT(nullable=False), SMALLINT(nullable=False)],
    returns=SMALLINT(nullable=False),
)

arrow_smallint_mult = udf.arrow(
    _arrow_int_mult,
    name='arrow_smallint_mult',
    args=[SMALLINT(nullable=False), SMALLINT(nullable=False)],
    returns=SMALLINT(nullable=False),
)

mediumint_mult = udf(
    _int_mult,
    name='mediumint_mult',
    args=[MEDIUMINT(nullable=False), MEDIUMINT(nullable=False)],
    returns=MEDIUMINT(nullable=False),
)

pandas_mediumint_mult = udf.pandas(
    _int_mult,
    name='pandas_mediumint_mult',
    args=[MEDIUMINT(nullable=False), MEDIUMINT(nullable=False)],
    returns=MEDIUMINT(nullable=False),
)

polars_mediumint_mult = udf.polars(
    _int_mult,
    name='polars_mediumint_mult',
    args=[MEDIUMINT(nullable=False), MEDIUMINT(nullable=False)],
    returns=MEDIUMINT(nullable=False),
)

numpy_mediumint_mult = udf.numpy(
    _int_mult,
    name='numpy_mediumint_mult',
    args=[MEDIUMINT(nullable=False), MEDIUMINT(nullable=False)],
    returns=MEDIUMINT(nullable=False),
)

arrow_mediumint_mult = udf.arrow(
    _arrow_int_mult,
    name='arrow_mediumint_mult',
    args=[MEDIUMINT(nullable=False), MEDIUMINT(nullable=False)],
    returns=MEDIUMINT(nullable=False),
)

bigint_mult = udf(
    _int_mult,
    name='bigint_mult',
    args=[BIGINT(nullable=False), BIGINT(nullable=False)],
    returns=BIGINT(nullable=False),
)

pandas_bigint_mult = udf.pandas(
    _int_mult,
    name='pandas_bigint_mult',
    args=[BIGINT(nullable=False), BIGINT(nullable=False)],
    returns=BIGINT(nullable=False),
)

polars_bigint_mult = udf.polars(
    _int_mult,
    name='polars_bigint_mult',
    args=[BIGINT(nullable=False), BIGINT(nullable=False)],
    returns=BIGINT(nullable=False),
)

numpy_bigint_mult = udf.numpy(
    _int_mult,
    name='numpy_bigint_mult',
    args=[BIGINT(nullable=False), BIGINT(nullable=False)],
    returns=BIGINT(nullable=False),
)

arrow_bigint_mult = udf.arrow(
    _arrow_int_mult,
    name='arrow_bigint_mult',
    args=[BIGINT(nullable=False), BIGINT(nullable=False)],
    returns=BIGINT(nullable=False),
)

nullable_tinyint_mult = udf(
    _int_mult,
    name='nullable_tinyint_mult',
    args=[TINYINT, TINYINT],
    returns=TINYINT,
)

pandas_nullable_tinyint_mult = udf.pandas(
    _int_mult,
    name='pandas_nullable_tinyint_mult',
    args=[TINYINT, TINYINT],
    returns=TINYINT,
)

pandas_nullable_tinyint_mult_with_masks = udf.pandas(
    _int_mult_with_masks,
    name='pandas_nullable_tinyint_mult_with_masks',
    args=[TINYINT, TINYINT],
    returns=TINYINT,
    include_masks=True,
)

polars_nullable_tinyint_mult = udf.polars(
    _int_mult,
    name='polars_nullable_tinyint_mult',
    args=[TINYINT, TINYINT],
    returns=TINYINT,
)

polars_nullable_tinyint_mult_with_masks = udf.polars(
    _int_mult_with_masks,
    name='polars_nullable_tinyint_mult_with_masks',
    args=[TINYINT, TINYINT],
    returns=TINYINT,
    include_masks=True,
)

numpy_nullable_tinyint_mult = udf.numpy(
    _int_mult,
    name='numpy_nullable_tinyint_mult',
    args=[TINYINT, TINYINT],
    returns=TINYINT,
)

numpy_nullable_tinyint_mult_with_masks = udf.numpy(
    _int_mult_with_masks,
    name='numpy_nullable_tinyint_mult_with_masks',
    args=[TINYINT, TINYINT],
    returns=TINYINT,
    include_masks=True,
)

arrow_nullable_tinyint_mult = udf.arrow(
    _arrow_int_mult,
    name='arrow_nullable_tinyint_mult',
    args=[TINYINT, TINYINT],
    returns=TINYINT,
)

arrow_nullable_tinyint_mult_with_masks = udf.arrow(
    _arrow_int_mult_with_masks,
    name='arrow_nullable_tinyint_mult_with_masks',
    args=[TINYINT, TINYINT],
    returns=TINYINT,
    include_masks=True,
)

nullable_smallint_mult = udf(
    _int_mult,
    name='nullable_smallint_mult',
    args=[SMALLINT, SMALLINT],
    returns=SMALLINT,
)

nullable_mediumint_mult = udf(
    _int_mult,
    name='nullable_mediumint_mult',
    args=[MEDIUMINT, MEDIUMINT],
    returns=MEDIUMINT,
)

nullable_bigint_mult = udf(
    _int_mult,
    name='nullable_bigint_mult',
    args=[BIGINT, BIGINT],
    returns=BIGINT,
)

numpy_nullable_bigint_mult = udf.numpy(
    _int_mult,
    name='numpy_nullable_bigint_mult',
    args=[BIGINT, BIGINT],
    returns=BIGINT,
)

numpy_nullable_bigint_mult_with_masks = udf.numpy(
    _int_mult_with_masks,
    name='numpy_nullable_bigint_mult',
    args=[BIGINT, BIGINT],
    returns=BIGINT,
    include_masks=True,
)


@udf
def nullable_int_mult(x: Optional[int], y: Optional[int]) -> Optional[int]:
    if x is None or y is None:
        return None
    return x * y


@udf
def string_mult(x: str, times: int) -> str:
    return x * times


@udf.pandas
def pandas_string_mult(x: str, times: int) -> str:
    return x * times


@udf.numpy
def numpy_string_mult(x: str, times: int) -> str:
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


@udf(args=dict(x=VARCHAR(20, nullable=False)))
def varchar_mult(x: str, times: int) -> str:
    return x * times


@udf(args=dict(x=VARCHAR(20, nullable=True)))
def nullable_varchar_mult(x: Optional[str], times: Optional[int]) -> Optional[str]:
    if x is None or times is None:
        return None
    return x * times
