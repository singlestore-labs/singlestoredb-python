"""Test for utility functions."""
from typing import Any

from singlestoredb.docstring.common import DocstringReturns
from singlestoredb.docstring.util import combine_docstrings


def test_combine_docstrings() -> None:
    """Test combine_docstrings wrapper."""

    def fun1(arg_a: Any, arg_b: Any, arg_c: Any, arg_d: Any) -> None:
        """short_description: fun1

        :param arg_a: fun1
        :param arg_b: fun1
        :return: fun1
        """
        assert arg_a and arg_b and arg_c and arg_d

    def fun2(arg_b: Any, arg_c: Any, arg_d: Any, arg_e: Any) -> None:
        """short_description: fun2

        long_description: fun2

        :param arg_b: fun2
        :param arg_c: fun2
        :param arg_e: fun2
        """
        assert arg_b and arg_c and arg_d and arg_e

    @combine_docstrings(fun1, fun2)
    def decorated1(
        arg_a: Any, arg_b: Any, arg_c: Any,
        arg_d: Any, arg_e: Any, arg_f: Any,
    ) -> None:
        """
        :param arg_e: decorated
        :param arg_f: decorated
        """
        assert arg_a and arg_b and arg_c and arg_d and arg_e and arg_f

    assert decorated1.__doc__ == (
        'short_description: fun2\n'
        '\n'
        'long_description: fun2\n'
        '\n'
        ':param arg_a: fun1\n'
        ':param arg_b: fun1\n'
        ':param arg_c: fun2\n'
        ':param arg_e: fun2\n'
        ':param arg_f: decorated\n'
        ':returns: fun1'
    )

    @combine_docstrings(fun1, fun2, exclude=[DocstringReturns])
    def decorated2(
        arg_a: Any, arg_b: Any, arg_c: Any, arg_d: Any, arg_e: Any, arg_f: Any,
    ) -> None:
        assert arg_a and arg_b and arg_c and arg_d and arg_e and arg_f

    assert decorated2.__doc__ == (
        'short_description: fun2\n'
        '\n'
        'long_description: fun2\n'
        '\n'
        ':param arg_a: fun1\n'
        ':param arg_b: fun1\n'
        ':param arg_c: fun2\n'
        ':param arg_e: fun2'
    )
