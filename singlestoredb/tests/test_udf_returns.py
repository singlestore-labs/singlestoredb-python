# mypy: disable-error-code="type-arg"
# from __future__ import annotations
import unittest
from typing import Any
from typing import Callable
from typing import List
from typing import NamedTuple
from typing import Optional
from typing import TypedDict

import numpy as np
import numpy.typing as npt
import pandas as pd
import polars as pl
import pyarrow as pa
from pydantic import BaseModel

from singlestoredb.functions import Table
from singlestoredb.functions import udf
from singlestoredb.functions.signature import get_signature
from singlestoredb.functions.signature import signature_to_sql


def to_sql(func: Callable[..., Any]) -> str:
    """Convert a function signature to SQL."""
    out = signature_to_sql(get_signature(func))
    return out.split('EXTERNAL FUNCTION ')[1].split('AS REMOTE')[0].strip()


class Parameters(NamedTuple):
    x: Optional[str] = ''


class UDFTuple(NamedTuple):
    value: str


class TVFTuple(NamedTuple):
    idx: int
    value: Optional[str]


class TVFDict(TypedDict):
    idx: int
    value: Optional[str]


class TVFBaseModel(BaseModel):
    idx: int
    value: Optional[str]


class UDFResultsTest(unittest.TestCase):

    def test_udf_returns(self) -> None:
        # Plain UDF
        @udf
        def foo_a(x: str) -> str:
            return f'0: {x}'

        foo_a_out = foo_a('cat')

        assert type(foo_a_out) is str
        assert foo_a_out == '0: cat'
        assert to_sql(foo_a) == '`foo_a`(`x` TEXT NOT NULL) RETURNS TEXT NOT NULL'

        # Vectorized UDF using lists
        @udf
        def foo_b(x: List[str]) -> List[str]:
            return [f'{i}: {y}' for i, y in enumerate(x)]

        foo_b_out = foo_b(['cat', 'dog', 'monkey'])

        assert type(foo_b_out) is list
        assert foo_b_out == ['0: cat', '1: dog', '2: monkey']
        assert to_sql(foo_b) == '`foo_b`(`x` TEXT NOT NULL) RETURNS TEXT NOT NULL'

        # Illegal return type for UDF
        @udf
        def foo_c(x: List[str]) -> List[UDFTuple]:
            return [UDFTuple(value='invalid')]

        # Vectorized UDF using pandas Series
        @udf(args=Parameters, returns=UDFTuple)
        def foo_d(x: pd.Series) -> pd.Series:
            return pd.Series([f'{i}: {y}' for i, y in enumerate(x)])

        foo_d_out = foo_d(pd.Series(['cat', 'dog', 'monkey']))

        assert type(foo_d_out) is pd.Series
        assert list(foo_d_out) == ['0: cat', '1: dog', '2: monkey']
        assert to_sql(foo_d) == "`foo_d`(`x` TEXT NULL DEFAULT '') RETURNS TEXT NOT NULL"

        # Vectorized UDF using polars Series
        @udf(args=Parameters, returns=UDFTuple)
        def foo_e(x: pl.Series) -> pl.Series:
            return pl.Series([f'{i}: {y}' for i, y in enumerate(x)])

        foo_e_out = foo_e(pl.Series(['cat', 'dog', 'monkey']))

        assert type(foo_e_out) is pl.Series
        assert list(foo_e_out) == ['0: cat', '1: dog', '2: monkey']
        assert to_sql(foo_e) == "`foo_e`(`x` TEXT NULL DEFAULT '') RETURNS TEXT NOT NULL"

        # Vectorized UDF using numpy arrays
        @udf(args=Parameters, returns=UDFTuple)
        def foo_f(x: np.ndarray) -> np.ndarray:
            return np.array([f'{i}: {y}' for i, y in enumerate(x)])

        foo_f_out = foo_f(np.array(['cat', 'dog', 'monkey']))

        assert type(foo_f_out) is np.ndarray
        assert list(foo_f_out) == ['0: cat', '1: dog', '2: monkey']
        assert to_sql(foo_f) == "`foo_f`(`x` TEXT NULL DEFAULT '') RETURNS TEXT NOT NULL"

        # Vectorized UDF using typed numpy arrays
        @udf
        def foo_g(x: npt.NDArray[np.str_]) -> npt.NDArray[np.str_]:
            return np.array([f'{i}: {y}' for i, y in enumerate(x)])

        foo_g_out = foo_g(np.array(['cat', 'dog', 'monkey']))

        assert type(foo_g_out) is np.ndarray
        assert list(foo_g_out) == ['0: cat', '1: dog', '2: monkey']
        assert to_sql(foo_g) == '`foo_g`(`x` TEXT NOT NULL) RETURNS TEXT NOT NULL'

        # Plain TVF using one list
        @udf
        def foo_h_(x: str) -> Table[List[str]]:
            return Table([x] * 3)

        foo_h__out = foo_h_('cat')

        assert type(foo_h__out) is Table
        assert foo_h__out == Table(['cat', 'cat', 'cat'])

        assert to_sql(foo_h_) == \
            '`foo_h_`(`x` TEXT NOT NULL) RETURNS TABLE(`a` TEXT NOT NULL)'

        # Plain TVF using multiple lists -- Illegal!
        @udf
        def foo_h(x: str) -> Table[List[int], List[str]]:
            return Table(list(range(3)), [x] * 3)

        foo_h_out = foo_h('cat')

        assert type(foo_h_out) is Table
        assert foo_h_out == Table([0, 1, 2], ['cat', 'cat', 'cat'])

        with self.assertRaises(TypeError):
            to_sql(foo_h)

        # Plain TVF using lists of NamedTuples
        @udf
        def foo_i(x: str) -> Table[List[TVFTuple]]:
            return Table([
                TVFTuple(idx=0, value=x),
                TVFTuple(idx=1, value=x),
                TVFTuple(idx=2, value=x),
            ])

        foo_i_out = foo_i('cat')

        assert type(foo_i_out) is Table
        assert foo_i_out == Table([
            TVFTuple(idx=0, value='cat'),
            TVFTuple(idx=1, value='cat'),
            TVFTuple(idx=2, value='cat'),
        ])
        assert to_sql(foo_i) == (
            '`foo_i`(`x` TEXT NOT NULL) '
            'RETURNS TABLE(`idx` BIGINT NOT NULL, `value` TEXT NULL)'
        )

        # Plain TVF using lists of TypedDicts
        @udf
        def foo_j(x: str) -> Table[List[TVFDict]]:
            return Table([
                dict(idx=0, value=x),
                dict(idx=1, value=x),
                dict(idx=2, value=x),
            ])

        foo_j_out = foo_j('cat')

        assert type(foo_j_out) is Table
        assert foo_j_out == Table([
            dict(idx=0, value='cat'),
            dict(idx=1, value='cat'),
            dict(idx=2, value='cat'),
        ])
        assert to_sql(foo_j) == (
            '`foo_j`(`x` TEXT NOT NULL) '
            'RETURNS TABLE(`idx` BIGINT NOT NULL, `value` TEXT NULL)'
        )

        # Plain TVF using lists of pydantic BaseModels
        @udf
        def foo_k(x: str) -> Table[List[TVFBaseModel]]:
            return Table([
                TVFBaseModel(idx=0, value=x),
                TVFBaseModel(idx=1, value=x),
                TVFBaseModel(idx=2, value=x),
            ])

        foo_k_out = foo_k('cat')

        assert type(foo_k_out) is Table
        assert foo_k_out == Table([
            TVFBaseModel(idx=0, value='cat'),
            TVFBaseModel(idx=1, value='cat'),
            TVFBaseModel(idx=2, value='cat'),
        ])
        assert to_sql(foo_k) == (
            '`foo_k`(`x` TEXT NOT NULL) '
            'RETURNS TABLE(`idx` BIGINT NOT NULL, `value` TEXT NULL)'
        )

        # Plain TVF using pandas Series
        @udf(returns=TVFTuple)
        def foo_l(x: str) -> Table[pd.Series, pd.Series]:
            return Table(pd.Series(range(3)), pd.Series([x] * 3))

        foo_l_out = foo_l('cat')

        assert type(foo_l_out) is Table
        assert len(foo_l_out) == 2
        assert type(foo_l_out[0]) is pd.Series
        assert list(foo_l_out[0]) == [0, 1, 2]
        assert type(foo_l_out[1]) is pd.Series
        assert list(foo_l_out[1]) == ['cat', 'cat', 'cat']
        assert to_sql(foo_l) == (
            '`foo_l`(`x` TEXT NOT NULL) '
            'RETURNS TABLE(`idx` BIGINT NOT NULL, `value` TEXT NULL)'
        )

        # Plain TVF using polars Series
        @udf(returns=TVFTuple)
        def foo_m(x: str) -> Table[pl.Series, pl.Series]:
            return Table(pl.Series(range(3)), pl.Series([x] * 3))

        foo_m_out = foo_m('cat')

        assert type(foo_m_out) is Table
        assert len(foo_m_out) == 2
        assert type(foo_m_out[0]) is pl.Series
        assert list(foo_m_out[0]) == [0, 1, 2]
        assert type(foo_m_out[1]) is pl.Series
        assert list(foo_m_out[1]) == ['cat', 'cat', 'cat']
        assert to_sql(foo_m) == (
            '`foo_m`(`x` TEXT NOT NULL) '
            'RETURNS TABLE(`idx` BIGINT NOT NULL, `value` TEXT NULL)'
        )

        # Plain TVF using pyarrow Array
        @udf(returns=TVFTuple)
        def foo_n(x: str) -> Table[pa.Array, pa.Array]:
            return Table(pa.array(range(3)), pa.array([x] * 3))

        foo_n_out = foo_n('cat')

        assert type(foo_n_out) is Table
        assert foo_n_out == Table(pa.array([0, 1, 2]), pa.array(['cat', 'cat', 'cat']))
        assert to_sql(foo_n) == (
            '`foo_n`(`x` TEXT NOT NULL) '
            'RETURNS TABLE(`idx` BIGINT NOT NULL, `value` TEXT NULL)'
        )

        # Plain TVF using numpy arrays
        @udf(returns=TVFTuple)
        def foo_o(x: str) -> Table[np.ndarray, np.ndarray]:
            return Table(np.array(range(3)), np.array([x] * 3))

        foo_o_out = foo_o('cat')

        assert type(foo_o_out) is Table
        assert len(foo_o_out) == 2
        assert type(foo_o_out[0]) is np.ndarray
        assert list(foo_o_out[0]) == [0, 1, 2]
        assert type(foo_o_out[1]) is np.ndarray
        assert list(foo_o_out[1]) == ['cat', 'cat', 'cat']
        assert to_sql(foo_o) == (
            '`foo_o`(`x` TEXT NOT NULL) '
            'RETURNS TABLE(`idx` BIGINT NOT NULL, `value` TEXT NULL)'
        )

        # Plain TVF using typed numpy arrays
        @udf
        def foo_p(x: str) -> Table[npt.NDArray[np.int_], npt.NDArray[np.str_]]:
            return Table(np.array(range(3)), np.array([x] * 3))

        foo_p_out = foo_p('cat')

        assert type(foo_p_out) is Table
        assert len(foo_p_out) == 2
        assert type(foo_p_out[0]) is np.ndarray
        assert list(foo_p_out[0]) == [0, 1, 2]
        assert type(foo_p_out[1]) is np.ndarray
        assert list(foo_p_out[1]) == ['cat', 'cat', 'cat']
        assert to_sql(foo_p) == (
            '`foo_p`(`x` TEXT NOT NULL) '
            'RETURNS TABLE(`a` BIGINT NOT NULL, `b` TEXT NOT NULL)'
        )

        # Plain TVF using pandas DataFrame
        @udf(returns=TVFTuple)
        def foo_q(x: str) -> Table[pd.DataFrame]:
            return Table(pd.DataFrame([[0, x], [1, x], [2, x]]))  # columns???

        foo_q_out = foo_q('cat')

        assert type(foo_q_out) is Table
        assert len(foo_q_out) == 1
        assert list(foo_q_out[0].iloc[:, 0]) == [0, 1, 2]
        assert list(foo_q_out[0].iloc[:, 1]) == ['cat', 'cat', 'cat']
        assert to_sql(foo_q) == (
            '`foo_q`(`x` TEXT NOT NULL) '
            'RETURNS TABLE(`idx` BIGINT NOT NULL, `value` TEXT NULL)'
        )

        # Plain TVF using polars DataFrame
        @udf(returns=TVFTuple)
        def foo_r(x: str) -> Table[pl.DataFrame]:
            return Table(pl.DataFrame([[0, 1, 2], [x] * 3]))  # columns???

        foo_r_out = foo_r('cat')

        assert type(foo_r_out) is Table
        assert len(foo_r_out) == 1
        assert list(foo_r_out[0][:, 0]) == [0, 1, 2]
        assert list(foo_r_out[0][:, 1]) == ['cat', 'cat', 'cat']
        assert to_sql(foo_r) == (
            '`foo_r`(`x` TEXT NOT NULL) '
            'RETURNS TABLE(`idx` BIGINT NOT NULL, `value` TEXT NULL)'
        )

        # Plain TVF using pyarrow Table
        @udf(returns=TVFTuple)
        def foo_s(x: str) -> Table[pa.Table]:
            return Table(
                pa.Table.from_pylist([
                    dict(idx=0, value='cat'),
                    dict(idx=1, value='cat'),
                    dict(idx=2, value='cat'),
                ]),
            )  # columns???

        foo_s_out = foo_s('cat')

        assert type(foo_s_out) is Table
        assert foo_s_out == Table(
            pa.Table.from_pylist([
                dict(idx=0, value='cat'),
                dict(idx=1, value='cat'),
                dict(idx=2, value='cat'),
            ]),
        )
        assert to_sql(foo_s) == (
            '`foo_s`(`x` TEXT NOT NULL) '
            'RETURNS TABLE(`idx` BIGINT NOT NULL, `value` TEXT NULL)'
        )

        # Vectorized TVF using lists -- Illegal!
        @udf
        def foo_t(x: List[str]) -> Table[List[int], List[str]]:
            return Table(list(range(len(x))), x)

        foo_t_out = foo_t(['cat', 'dog', 'monkey'])

        assert type(foo_t_out) is Table
        assert foo_t_out == Table([0, 1, 2], ['cat', 'dog', 'monkey'])
        with self.assertRaises(TypeError):
            to_sql(foo_t)

        # Vectorized TVF using pandas Series
        @udf(args=Parameters, returns=TVFTuple)
        def foo_u(x: pd.Series) -> Table[pd.Series, pd.Series]:
            return Table(pd.Series(range(len(x))), pd.Series(x))

        foo_u_out = foo_u(pd.Series(['cat', 'dog', 'monkey']))

        assert type(foo_u_out) is Table
        assert len(foo_u_out) == 2
        assert list(foo_u_out[0]) == [0, 1, 2]
        assert list(foo_u_out[1]) == ['cat', 'dog', 'monkey']
        assert to_sql(foo_u) == (
            "`foo_u`(`x` TEXT NULL DEFAULT '') "
            'RETURNS TABLE(`idx` BIGINT NOT NULL, `value` TEXT NULL)'
        )

        # Vectorized TVF using polars Series
        @udf(args=Parameters, returns=TVFTuple)
        def foo_v(x: pl.Series) -> Table[pl.Series, pl.Series]:
            return Table(pl.Series(range(len(x))), pl.Series(x))

        foo_v_out = foo_v(pl.Series(['cat', 'dog', 'monkey']))

        assert type(foo_v_out) is Table
        assert len(foo_v_out) == 2
        assert list(foo_v_out[0]) == [0, 1, 2]
        assert list(foo_v_out[1]) == ['cat', 'dog', 'monkey']
        assert to_sql(foo_v) == (
            "`foo_v`(`x` TEXT NULL DEFAULT '') "
            'RETURNS TABLE(`idx` BIGINT NOT NULL, `value` TEXT NULL)'
        )

        # Vectorized TVF using pyarrow Array
        @udf(args=Parameters, returns=TVFTuple)
        def foo_w(x: pa.Array) -> Table[pa.Array, pa.Array]:
            return Table(pa.array(range(len(x))), pa.array(x))

        foo_w_out = foo_w(pa.array(['cat', 'dog', 'monkey']))

        assert type(foo_w_out) is Table
        assert foo_w_out == Table(
            pa.array([0, 1, 2]), pa.array(['cat', 'dog', 'monkey']),
        )
        assert to_sql(foo_w) == (
            "`foo_w`(`x` TEXT NULL DEFAULT '') "
            'RETURNS TABLE(`idx` BIGINT NOT NULL, `value` TEXT NULL)'
        )

        # Vectorized TVF using numpy arrays
        @udf(args=Parameters, returns=TVFTuple)
        def foo_x(x: np.ndarray) -> Table[np.ndarray, np.ndarray]:
            return Table(np.array(range(len(x))), np.array(x))

        foo_x_out = foo_x(np.array(['cat', 'dog', 'monkey']))

        assert type(foo_x_out) is Table
        assert len(foo_x_out) == 2
        assert list(foo_x_out[0]) == [0, 1, 2]
        assert list(foo_x_out[1]) == ['cat', 'dog', 'monkey']
        assert to_sql(foo_x) == (
            "`foo_x`(`x` TEXT NULL DEFAULT '') "
            'RETURNS TABLE(`idx` BIGINT NOT NULL, `value` TEXT NULL)'
        )

        # Vectorized TVF using typed numpy arrays
        @udf
        def foo_y(
            x: npt.NDArray[np.str_],
        ) -> Table[npt.NDArray[np.int_], npt.NDArray[np.str_]]:
            return Table(np.array(range(len(x))), np.array(x))

        foo_y_out = foo_y(np.array(['cat', 'dog', 'monkey']))

        assert type(foo_y_out) is Table
        assert len(foo_y_out) == 2
        assert list(foo_y_out[0]) == [0, 1, 2]
        assert list(foo_y_out[1]) == ['cat', 'dog', 'monkey']
        assert to_sql(foo_y) == (
            '`foo_y`(`x` TEXT NOT NULL) '
            'RETURNS TABLE(`a` BIGINT NOT NULL, `b` TEXT NOT NULL)'
        )


if __name__ == '__main__':
    unittest.main()
