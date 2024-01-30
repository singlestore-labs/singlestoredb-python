#!/usr/bin/env python
# type: ignore
"""Test SingleStoreDB exceptions."""
import unittest

import singlestoredb as s2


class TestExceptions(unittest.TestCase):

    def test_str(self):
        exc = s2.Error()
        assert str(exc) == 'Unknown error', str(exc)

        exc = s2.Error(errno=1)
        assert str(exc) == '1', str(exc)

        exc = s2.Error(msg='hi there')
        assert str(exc) == 'hi there', str(exc)

        exc = s2.Error(errno=1, msg='hi there')
        assert str(exc) == '1: hi there', str(exc)

        exc = s2.Error(errno=1, msg='hi there', sqlstate=9)
        assert str(exc) == '1 (9): hi there'

        exc = s2.Error(msg='hi there', sqlstate=9)
        assert str(exc) == '(9): hi there'

        exc = s2.Error(sqlstate=9)
        assert str(exc) == '(9)'

    def test_repr(self):
        exc = s2.Error(errno=1, msg='hi there')
        assert str(exc) == '1: hi there', str(exc)
        assert str(exc) == repr(exc)

    def test_msg(self):
        exc = s2.Error(errno=1, msg='hi there')
        assert exc.msg == exc.errmsg


if __name__ == '__main__':
    import nose2
    nose2.main()
