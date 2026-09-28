# mypy: disable-error-code="attr-defined,type-arg,valid-type,var-annotated"
"""
UDF annotations written with a PEP 695 type alias.

numpy 2.5 defines ``npt.NDArray`` as an alias of
``ndarray[_AnyShape, dtype[ScalarT]]`` rather than as a subscripted generic, so
``typing.get_origin`` of an NDArray annotation returns the alias object instead
of ``numpy.ndarray``, and ``typing.get_args`` returns the scalar type instead of
the ``(shape, dtype)`` pair the signature machinery reads. The aliases below are
built with ``typing.TypeAliasType`` directly rather than taken from ``npt``, so
the alias form is covered whatever numpy is installed.

These live in their own module because the type checker cannot follow an alias
built at runtime -- the file-level suppressions above would otherwise apply to
the hand-written annotations in ``test_udf_returns.py``.

"""
import sys
import typing
import unittest
from typing import Any
from typing import Callable
from typing import Optional

import numpy as np
import numpy.typing as npt

from singlestoredb.functions import udf
from singlestoredb.functions.signature import get_signature
from singlestoredb.functions.signature import signature_to_sql


def to_sql(func: Callable[..., Any]) -> str:
    """Convert a function signature to SQL."""
    out = signature_to_sql(get_signature(func))
    return out.split('EXTERNAL FUNCTION ')[1].split('AS REMOTE')[0].strip()


@unittest.skipIf(
    sys.version_info < (3, 12),
    'PEP 695 type aliases require Python 3.12+',
)
class TypeAliasTest(unittest.TestCase):

    def test_subscripted_alias(self) -> None:
        ScalarT = typing.TypeVar('ScalarT')
        NDArrayAlias = typing.TypeAliasType(  # noqa: TYP006
            'NDArrayAlias',
            np.ndarray[Any, np.dtype[ScalarT]],
            type_params=(ScalarT,),
        )

        @udf
        def foo_a(x: NDArrayAlias[np.str_]) -> NDArrayAlias[np.str_]:
            return np.array([f'{i}: {v}' for i, v in enumerate(x)])

        assert to_sql(foo_a) == '`foo_a`(`x` TEXT NOT NULL) RETURNS TEXT NOT NULL'

    def test_bare_alias(self) -> None:
        Vec = typing.TypeAliasType('Vec', npt.NDArray[np.float64])  # noqa: TYP006

        @udf
        def foo_b(x: Vec) -> Vec:
            return x * 2

        assert to_sql(foo_b) == '`foo_b`(`x` DOUBLE NOT NULL) RETURNS DOUBLE NOT NULL'

    def test_optional_subscripted_alias(self) -> None:
        ScalarT = typing.TypeVar('ScalarT')
        NDArrayAlias = typing.TypeAliasType(  # noqa: TYP006
            'NDArrayAlias',
            np.ndarray[Any, np.dtype[ScalarT]],
            type_params=(ScalarT,),
        )

        @udf
        def foo_c(
            x: Optional[NDArrayAlias[np.str_]],
        ) -> Optional[NDArrayAlias[np.str_]]:
            return x

        # NOT NULL despite the Optional: the numpy branch of `get_schema` does
        # not thread `is_optional` into its ParamSpec. Pre-existing on every
        # numpy version, and asserted here so the alias expansion above is
        # provably nullability-neutral.
        assert to_sql(foo_c) == '`foo_c`(`x` TEXT NOT NULL) RETURNS TEXT NOT NULL'

    def test_optional_bare_alias(self) -> None:
        Vec = typing.TypeAliasType('Vec', npt.NDArray[np.float64])  # noqa: TYP006

        @udf
        def foo_d(x: Optional[Vec]) -> Optional[Vec]:
            return x

        assert to_sql(foo_d) == '`foo_d`(`x` DOUBLE NOT NULL) RETURNS DOUBLE NOT NULL'


if __name__ == '__main__':
    unittest.main()
