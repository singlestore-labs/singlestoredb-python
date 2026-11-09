#!/usr/bin/env python
# type: ignore
"""UDF signatures in a module using postponed evaluation of annotations."""
from __future__ import annotations

import dataclasses
import datetime
import unittest
from datetime import date
from typing import List
from typing import NamedTuple
from typing import Optional

import numpy as np
import numpy.typing as npt

from ..functions import dtypes as dt
from ..functions import Masked
from ..functions import signature as sig
from ..functions import Table
from ..functions import udf
from ..functions.ext.asgi import get_masked_params
from ..functions.ext.plugin import registry


def to_sql(x):
    out = sig.signature_to_sql(sig.get_signature(x))
    out = out.split('EXTERNAL FUNCTION ')[1]
    return out.split('AS REMOTE')[0].strip()


class Row(NamedTuple):
    a: int
    b: Optional[str]


@dataclasses.dataclass
class DataclassRow:
    a: int
    b: Optional[str] = None


class TestFutureAnnotations(unittest.TestCase):

    def test_scalars(self):
        @udf
        def foo(x: Optional[int], y: float = 1.5) -> Optional[str]: ...
        assert to_sql(foo) == \
            '`foo`(`x` BIGINT NULL, `y` DOUBLE NOT NULL DEFAULT 1.5e0) ' \
            'RETURNS TEXT NULL'

    def test_masked_vectors(self):
        @udf
        def foo(
            x: npt.NDArray[np.float32],
        ) -> Masked[npt.NDArray[np.float32]]: ...
        assert to_sql(foo) == '`foo`(`x` FLOAT NOT NULL) RETURNS FLOAT NULL'

    def test_masked_params(self):
        @udf
        def foo(
            x: Masked[npt.NDArray[np.float32]],
            y: npt.NDArray[np.float32],
        ) -> npt.NDArray[np.float32]: ...
        assert to_sql(foo) == \
            '`foo`(`x` FLOAT NULL, `y` FLOAT NOT NULL) RETURNS FLOAT NOT NULL'
        assert get_masked_params(foo) == [True, False]
        assert registry._get_masked_params(foo) == [True, False]

    def test_table(self):
        @udf
        def foo(x: int) -> Table[List[Row]]: ...
        assert to_sql(foo) == \
            '`foo`(`x` BIGINT NOT NULL) ' \
            'RETURNS TABLE(`a` BIGINT NOT NULL, `b` TEXT NULL)'

    def test_dataclass_table(self):
        @udf
        def foo(x: int) -> Table[List[DataclassRow]]: ...
        assert to_sql(foo) == \
            '`foo`(`x` BIGINT NOT NULL) ' \
            'RETURNS TABLE(`a` BIGINT NOT NULL, `b` TEXT NULL)'

    def test_dtype_strings(self):
        @udf
        def foo(x: 'str') -> 'array[int64]': ...  # noqa: F821
        assert to_sql(foo) == \
            '`foo`(`x` TEXT NOT NULL) RETURNS ARRAY(BIGINT NOT NULL) NOT NULL'

    def test_unsupported_types(self):
        with self.assertRaisesRegex(TypeError, 'datetime.date'):
            @udf
            def foo(x: datetime.date) -> int: ...

        with self.assertRaisesRegex(TypeError, 'datetime.date'):
            @udf
            def foo_short(x: date) -> int: ...

        @udf(args=dt.TEXT, returns=dt.TEXT)
        def bar(x: datetime.date) -> datetime.date: ...
        assert to_sql(bar) == '`bar`(`x` TEXT NULL) RETURNS TEXT NULL'

        @udf(args=dt.TEXT, returns=dt.TEXT)
        def bar_short(x: date) -> date: ...
        assert to_sql(bar_short) == '`bar_short`(`x` TEXT NULL) RETURNS TEXT NULL'

    def test_unresolvable(self):
        class Local(NamedTuple):
            a: int

        with self.assertRaisesRegex(TypeError, 'could not resolve.*Local'):
            @udf
            def foo(x: int) -> Table[List[Local]]: ...
