#!/usr/bin/env python
# type: ignore
"""SingleStoreDB HTTP connection testing."""
import base64
import os
import unittest

import singlestoredb.connection as sc
from singlestoredb import config
from singlestoredb import http
from singlestoredb.tests import utils
# import traceback


class TestHTTP(unittest.TestCase):

    dbname: str = ''
    dbexisted: bool = False

    @classmethod
    def setUpClass(cls):
        sql_file = os.path.join(os.path.dirname(__file__), 'test.sql')
        cls.dbname, cls.dbexisted = utils.load_sql(sql_file)

    @classmethod
    def tearDownClass(cls):
        if not cls.dbexisted:
            utils.drop_database(cls.dbname)

    def setUp(self):
        self.conn = self._connect()
        self.cur = self.conn.cursor()
        if self.params['driver'] not in ['http', 'https']:
            self.skipTest('Tests must be run using HTTP connection')
        self.driver = self.params['driver'] or 'http'

    def _connect(self):
        params = sc.build_params(host=config.get_option('host'))
        self.params = {
            k: v for k, v in dict(
                host=params.get('host'),
                port=params.get('port'),
                user=params.get('user'),
                password=params.get('password'),
                driver=params.get('driver'),
            ).items() if v is not None
        }
        return http.connect(database=type(self).dbname, **self.params)

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
        exc = http.connection.get_exc_type(0)
        assert exc is http.InterfaceError, exc

        exc = http.connection.get_exc_type(2012)
        assert exc is http.InterfaceError, exc

        exc = http.connection.get_exc_type(1230)
        assert exc is http.DataError, exc

        exc = http.connection.get_exc_type(1110)
        assert exc is http.ProgrammingError, exc

        exc = http.connection.get_exc_type(1452)
        assert exc is http.IntegrityError, exc

        exc = http.connection.get_exc_type(9999)
        assert exc is http.OperationalError, exc

        exc = http.connection.get_exc_type(222)
        assert exc is http.InternalError, exc

    def test_identity(self):
        out = http.connection.identity(1)
        assert out == 1, out

        out = http.connection.identity('hi')
        assert out == 'hi', out

    def test_b64decode_converter(self):
        data = base64.b64encode(b'hi there')
        assert type(data) is bytes, type(data)

        out = http.connection.b64decode_converter(http.connection.identity, None)
        assert out is None, out

        out = http.connection.b64decode_converter(http.connection.identity, data)
        assert out == b'hi there', out

        out = http.connection.b64decode_converter(
            http.connection.identity,
            str(data, 'utf8'),
        )
        assert out == b'hi there', out

    def test_executemany(self):
        self.cur.executemany(
            'select * from data where id < %(id)s',
            [dict(id='d'), dict(id='e')],
        )

        assert self.cur.rownumber == 0, self.cur.rownumber

        # First set
        out = self.cur.fetchall()

        assert self.cur.rownumber == 3, self.cur.rownumber

        desc = self.cur.description
        rowcount = self.cur.rowcount
        lastrowid = self.cur.lastrowid

        assert sorted(out) == sorted([
            ('a', 'antelopes', 2),
            ('b', 'bears', 2),
            ('c', 'cats', 5),
        ]), out

        assert rowcount == 7, rowcount
        assert lastrowid is None, lastrowid
        assert len(desc) == 3, desc
        assert desc[0][0] == 'id', desc[0][0]
        assert desc[0][1] in [253, 15], desc[0][1]
        assert desc[1][0] == 'name', desc[1][0]
        assert desc[1][1] in [253, 15], desc[1][1]
        assert desc[2][0] == 'value', desc[2][0]
        assert desc[2][1] == 8, desc[2][1]

        # Second set
        self.cur.nextset()

        out = self.cur.fetchall()

        desc = self.cur.description
        rowcount = self.cur.rowcount
        lastrowid = self.cur.lastrowid

        assert sorted(out) == sorted([
            ('a', 'antelopes', 2),
            ('b', 'bears', 2),
            ('c', 'cats', 5),
            ('d', 'dogs', 4),
        ]), out

        assert rowcount == 4, rowcount
        assert lastrowid is None, lastrowid
        assert len(desc) == 3, desc
        assert desc[0][0] == 'id', desc[0][0]
        assert desc[0][1] in [253, 15], desc[0][1]
        assert desc[1][0] == 'name', desc[1][0]
        assert desc[1][1] in [253, 15], desc[1][1]
        assert desc[2][0] == 'value', desc[2][0]
        assert desc[2][1] == 8, desc[2][1]

        out = self.cur.nextset()
        assert out is None, out

    def test_executemany_no_args(self):
        self.cur.executemany('select * from data where id < "d"')

        # First set
        out = self.cur.fetchall()

        desc = self.cur.description
        rowcount = self.cur.rowcount
        lastrowid = self.cur.lastrowid

        assert sorted(out) == sorted([
            ('a', 'antelopes', 2),
            ('b', 'bears', 2),
            ('c', 'cats', 5),
        ]), out

        assert rowcount == 3, rowcount
        assert lastrowid is None, lastrowid
        assert len(desc) == 3, desc
        assert desc[0][0] == 'id', desc[0][0]
        assert desc[0][1] in [253, 15], desc[0][1]
        assert desc[1][0] == 'name', desc[1][0]
        assert desc[1][1] in [253, 15], desc[1][1]
        assert desc[2][0] == 'value', desc[2][0]
        assert desc[2][1] == 8, desc[2][1]

        out = self.cur.nextset()
        assert out is None, out

    def test_is_connected(self):
        assert self.cur.is_connected() is True
        self.cur.close()
        assert self.cur.is_connected() is False

    def test_close(self):
        self.cur.close()
        assert self.cur.is_connected() is False

        with self.assertRaises(http.ProgrammingError):
            self.cur.execute('select 1')

        with self.assertRaises(http.ProgrammingError):
            self.cur.executemany('select 1')

        with self.assertRaises(http.ProgrammingError):
            self.cur.callproc('get_animal', ['cats'])

#   def test_callproc(self):
#       with self.assertRaises(NotImplementedError):
#           self.cur.callproc('get_animal', ['cats'])

    def test_iter(self):
        self.cur.execute('select * from data')

        out = list(self.cur)

        assert sorted(out) == sorted([
            ('a', 'antelopes', 2),
            ('b', 'bears', 2),
            ('c', 'cats', 5),
            ('d', 'dogs', 4),
            ('e', 'elephants', 0),
        ]), out

    def test_next(self):
        self.cur.execute('select * from data')

        out = [next(self.cur) for i in range(5)]

        assert sorted(out) == sorted([
            ('a', 'antelopes', 2),
            ('b', 'bears', 2),
            ('c', 'cats', 5),
            ('d', 'dogs', 4),
            ('e', 'elephants', 0),
        ]), out

        with self.assertRaises(StopIteration):
            next(self.cur)

    def test_context_manager(self):
        with self._connect() as conn:
            with conn.cursor() as cur:
                assert conn.is_connected()
                assert cur.is_connected()
        assert not conn.is_connected()
        assert not cur.is_connected()

    def test_commit(self):
        self.conn.autocommit(True)
        assert self.conn.commit() is None
        self.conn.autocommit(False)
        with self.assertRaises(http.NotSupportedError):
            self.conn.commit()

    def test_rollback(self):
        self.conn.autocommit(True)
        assert self.conn.rollback() is None
        self.conn.autocommit(False)
        with self.assertRaises(http.NotSupportedError):
            self.conn.rollback()

    def test_http_error(self):
        # Break content type
        self.conn._sess.headers.update({
            'Content-Type': 'GaRbAge',
        })
        with self.assertRaises(http.InternalError) as cm:
            self.cur.execute('select 1')
        exc = cm.exception
        assert exc.errno == 415, exc.errno
        assert 'Content-Type' in exc.msg, exc.msg


if __name__ == '__main__':
    import nose2
    nose2.main()
