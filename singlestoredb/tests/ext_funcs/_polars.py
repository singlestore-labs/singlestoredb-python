#!/usr/bin/env python3
# mypy: disable-error-code="type-arg,has-type"
"""The polars UDFs of the parent package; it imports them only if polars does."""
from singlestoredb.functions import Masked
from singlestoredb.functions import udf
from singlestoredb.functions.dtypes import BOOL
from singlestoredb.functions.dtypes import DOUBLE
from singlestoredb.functions.dtypes import TINYINT
from singlestoredb.functions.typing import polars as plt
from singlestoredb.tests.ext_funcs import bigint_udf
from singlestoredb.tests.ext_funcs import bool_udf
from singlestoredb.tests.ext_funcs import mediumint_udf
from singlestoredb.tests.ext_funcs import nullable_bigint_udf
from singlestoredb.tests.ext_funcs import nullable_bool_udf
from singlestoredb.tests.ext_funcs import nullable_mediumint_udf
from singlestoredb.tests.ext_funcs import nullable_smallint_udf
from singlestoredb.tests.ext_funcs import nullable_tinyint_udf
from singlestoredb.tests.ext_funcs import smallint_udf
from singlestoredb.tests.ext_funcs import tinyint_udf


@udf(
    args=[DOUBLE(nullable=False), DOUBLE(nullable=False)],
    returns=DOUBLE(nullable=False),
)
def polars_double_mult(x: plt.Series, y: plt.Series) -> plt.Series:
    return x * y


@tinyint_udf
def polars_tinyint_mult(x: plt.Series, y: plt.Series) -> plt.Series:
    return x * y


@smallint_udf
def polars_smallint_mult(x: plt.Series, y: plt.Series) -> plt.Series:
    return x * y


@mediumint_udf
def polars_mediumint_mult(x: plt.Series, y: plt.Series) -> plt.Series:
    return x * y


@bigint_udf
def polars_bigint_mult(x: plt.Series, y: plt.Series) -> plt.Series:
    return x * y


@bool_udf
def polars_bool_and(x: plt.Series, y: plt.Series) -> plt.Series:
    """Vector bool AND using polars."""
    return x & y


@nullable_bool_udf
def polars_nullable_bool_and(x: plt.Series, y: plt.Series) -> plt.Series:
    """Nullable vector bool AND using polars."""
    return x & y


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


@nullable_tinyint_udf
def polars_nullable_tinyint_mult(x: plt.Series, y: plt.Series) -> plt.Series:
    return x * y


@nullable_smallint_udf
def polars_nullable_smallint_mult(x: plt.Series, y: plt.Series) -> plt.Series:
    return x * y


@nullable_mediumint_udf
def polars_nullable_mediumint_mult(x: plt.Series, y: plt.Series) -> plt.Series:
    return x * y


@nullable_bigint_udf
def polars_nullable_bigint_mult(x: plt.Series, y: plt.Series) -> plt.Series:
    return x * y


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


__all__ = [name for name in list(globals()) if name.startswith('polars_')]
