#!/usr/bin/env python
# type: ignore
"""SingleStore HTTP connection testing."""
from __future__ import annotations

import base64
import os
import unittest

import singlestore as s2
from singlestore import http
from singlestore.tests import utils
# import traceback


class TestHTTP(unittest.TestCase):

    dbname: str = ''

    @classmethod
    def setUpClass(cls):
        sql_file = os.path.join(os.path.dirname(__file__), 'test.sql')
        cls.dbname = utils.load_sql(sql_file)

    @classmethod
    def tearDownClass(cls):
        utils.drop_database(cls.dbname)

    def setUp(self):
        self.conn = s2.connect(database=type(self).dbname)
        self.cur = self.conn.cursor()
        self.driver = self.conn._driver.dbapi.__name__

    def tearDown(self):
        try:
            if self.cur is not None:
                self.cur.close()
        except Exception:
            # traceback.print_exc()
            pass

        try:
            if self.conn is not None:
                self.conn.close()
        except Exception:
            # traceback.print_exc()
            pass

    def test_get_exc_type(self):
        exc = http.get_exc_type(0)
        assert exc is s2.InterfaceError, exc

        exc = http.get_exc_type(2012)
        assert exc is s2.InterfaceError, exc

        exc = http.get_exc_type(1230)
        assert exc is s2.DataError, exc

        exc = http.get_exc_type(1110)
        assert exc is s2.ProgrammingError, exc

        exc = http.get_exc_type(1452)
        assert exc is s2.IntegrityError, exc

        exc = http.get_exc_type(9999)
        assert exc is s2.OperationalError, exc

        exc = http.get_exc_type(222)
        assert exc is s2.InternalError, exc

    def test_identity(self):
        out = http.identity(1)
        assert out == 1, out

        out = http.identity('hi')
        assert out == 'hi', out

    def test_b64decode_converter(self):
        data = base64.b64encode(b'hi there')
        assert type(data) is bytes, type(data)

        out = http.b64decode_converter(http.identity, None)
        assert out is None, out

        out = http.b64decode_converter(http.identity, data)
        assert out == b'hi there', out

        out = http.b64decode_converter(http.identity, str(data, 'utf8'))
        assert out == b'hi there', out


if __name__ == '__main__':
    import nose2
    nose2.main()
