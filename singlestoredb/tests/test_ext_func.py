#!/usr/bin/env python
# type: ignore
"""Test SingleStoreDB external functions."""
import os
import socket
import subprocess
import time
import unittest

import requests

import singlestoredb as s2
import singlestoredb.mysql.constants.FIELD_TYPE as ft
from . import ext_funcs
from . import utils
from singlestoredb.functions.ext import create_app


try:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(('8.8.8.8', 80))
    HTTP_HOST = s.getsockname()[0]
except Exception:
    HTTP_HOST = '127.0.0.1'
finally:
    s.close()


def get_open_port() -> int:
    """Find an open port number."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('', 0))
    s.listen(1)
    port = s.getsockname()[1]
    s.close()
    return port


def start_http_server(database, data_format='rowdat_1'):
    """Start an external function server."""
    port = get_open_port()
    print(f'Start UDF HTTP server on http://{HTTP_HOST}:{port}')
    proc = subprocess.Popen(
        ['uvicorn', 'singlestoredb.functions.ext:create_app'],
        env=dict(
            PATH=os.environ['PATH'],
            PYTHONPATH=os.environ.get('PYTHONPATH', ''),
            UVICORN_HOST=str(HTTP_HOST),
            UVICORN_PORT=str(port),
            UVICORN_FACTORY='1',
            SINGLESTOREDB_EXT_FUNCTIONS='singlestoredb.tests.ext_funcs',
            SINGLESTOREDB_PURE_PYTHON=os.environ.get('SINGLESTOREDB_PURE_PYTHON', '0'),
        ),
    )

    # Wait for server to be available
    retries = 10
    while retries > 0:
        try:
            out = requests.get(f'http://{HTTP_HOST}:{port}/show/create_function')
            if out.status_code == 200:
                break
        except Exception:
            pass
        time.sleep(3)
        retries -= 1

    app = create_app(ext_funcs)
    app.register_functions(
        base_url=f'http://{HTTP_HOST}:{port}',
        database=database,
        data_format=data_format,
    )

    with s2.connect(database=database) as conn:
        with conn.cursor() as cur:
            cur.execute('set global enable_external_functions=on')
            cur.execute('show functions')
            for item in list(cur):
                cur.execute(f'show create function `{item[0]}`')
                for func in list(cur):
                    print(*func)

    return proc, HTTP_HOST, port


def stop_http_server(proc, database):
    """Stop the external function server."""
    proc.terminate()
    app = create_app(ext_funcs)
    app.drop_functions(database=database)


class TestExtFunc(unittest.TestCase):

    dbname: str = ''
    dbexisted: bool = False
    http_server = None
    http_host = '127.0.0.1'
    http_port = 0

    @classmethod
    def setUpClass(cls):
        sql_file = os.path.join(os.path.dirname(__file__), 'test.sql')
        cls.dbname, cls.dbexisted = utils.load_sql(sql_file)
        cls.http_server, cls.http_host, cls.http_port = \
            start_http_server(cls.dbname, 'rowdat_1')

    @classmethod
    def tearDownClass(cls):
        stop_http_server(cls.http_server, cls.dbname)
        cls.http_server = None
        cls.http_host = '127.0.0.1'
        cls.http_port = 0
        if not cls.dbexisted:
            utils.drop_database(cls.dbname)

    def setUp(self):
        self.conn = s2.connect(database=type(self).dbname)
        self.cur = self.conn.cursor()

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

    def test_show_create_function(self):
        out = requests.get(
            f'http://{type(self).http_host}:{type(self).http_port}'
            '/show/create_function',
        )
        print(out.text)

    def test_double_mult(self):
        self.cur.execute('select double_mult(value, 100) as res from data order by id')

        assert [tuple(x) for x in self.cur] == \
               [(200.0,), (200.0,), (500.0,), (400.0,), (0.0,)]

        desc = self.cur.description
        assert len(desc) == 1
        assert desc[0].name == 'res'
        assert desc[0].type_code == ft.DOUBLE
        assert desc[0].null_ok is False

        # NULL is not valid
        with self.assertRaises(self.conn.OperationalError):
            self.cur.execute(
                'select double_mult(value, NULL) as res '
                'from data order by id',
            )

    def test_pandas_double_mult(self):
        self.cur.execute(
            'select pandas_double_mult(value, 100) as res '
            'from data order by id',
        )

        assert [tuple(x) for x in self.cur] == \
               [(200.0,), (200.0,), (500.0,), (400.0,), (0.0,)]

        desc = self.cur.description
        assert len(desc) == 1
        assert desc[0].name == 'res'
        assert desc[0].type_code == ft.DOUBLE
        assert desc[0].null_ok is False

        # NULL is not valid
        with self.assertRaises(self.conn.OperationalError):
            self.cur.execute(
                'select pandas_double_mult(value, NULL) as res '
                'from data order by id',
            )

    def test_numpy_double_mult(self):
        self.cur.execute(
            'select numpy_double_mult(value, 100) as res '
            'from data order by id',
        )

        assert [tuple(x) for x in self.cur] == \
               [(200.0,), (200.0,), (500.0,), (400.0,), (0.0,)]

        desc = self.cur.description
        assert len(desc) == 1
        assert desc[0].name == 'res'
        assert desc[0].type_code == ft.DOUBLE
        assert desc[0].null_ok is False

        # NULL is not valid
        with self.assertRaises(self.conn.OperationalError):
            self.cur.execute(
                'select numpy_double_mult(value, NULL) as res '
                'from data order by id',
            )

    def test_arrow_double_mult(self):
        self.cur.execute(
            'select arrow_double_mult(value, 100) as res '
            'from data order by id',
        )

        assert [tuple(x) for x in self.cur] == \
               [(200.0,), (200.0,), (500.0,), (400.0,), (0.0,)]

        desc = self.cur.description
        assert len(desc) == 1
        assert desc[0].name == 'res'
        assert desc[0].type_code == ft.DOUBLE
        assert desc[0].null_ok is False

        # NULL is not valid
        with self.assertRaises(self.conn.OperationalError):
            self.cur.execute(
                'select arrow_double_mult(value, NULL) as res '
                'from data order by id',
            )

    def test_polars_double_mult(self):
        self.cur.execute(
            'select polars_double_mult(value, 100) as res '
            'from data order by id',
        )

        assert [tuple(x) for x in self.cur] == \
               [(200.0,), (200.0,), (500.0,), (400.0,), (0.0,)]

        desc = self.cur.description
        assert len(desc) == 1
        assert desc[0].name == 'res'
        assert desc[0].type_code == ft.DOUBLE
        assert desc[0].null_ok is False

        # NULL is not valid
        with self.assertRaises(self.conn.OperationalError):
            self.cur.execute(
                'select polars_double_mult(value, NULL) as res '
                'from data order by id',
            )

    def test_nullable_double_mult(self):
        self.cur.execute(
            'select nullable_double_mult(value, 100) as res from '
            'data_with_nulls order by id',
        )

        assert [tuple(x) for x in self.cur] == \
               [(200.0,), (200.0,), (500.0,), (None,), (0.0,)]

        desc = self.cur.description
        assert len(desc) == 1
        assert desc[0].name == 'res'
        assert desc[0].type_code == ft.DOUBLE
        assert desc[0].null_ok is True

        self.cur.execute(
            'select nullable_double_mult(value, NULL) as res '
            'from data_with_nulls order by id',
        )

        assert [tuple(x) for x in self.cur] == \
               [(None,), (None,), (None,), (None,), (None,)]

    def test_float_mult(self):
        self.cur.execute(
            'select float_mult(value, 100) as res '
            'from data order by id',
        )

        assert [tuple(x) for x in self.cur] == \
               [(200.0,), (200.0,), (500.0,), (400.0,), (0.0,)]

        desc = self.cur.description
        assert len(desc) == 1
        assert desc[0].name == 'res'
        assert desc[0].type_code == ft.FLOAT
        assert desc[0].null_ok is False

        # NULL is not valid
        with self.assertRaises(self.conn.OperationalError):
            self.cur.execute(
                'select float_mult(value, NULL) as res '
                'from data order by id',
            )

    def test_nullable_float_mult(self):
        self.cur.execute(
            'select nullable_float_mult(value, 100) as res '
            'from data_with_nulls order by id',
        )

        assert [tuple(x) for x in self.cur] == \
               [(200.0,), (200.0,), (500.0,), (None,), (0.0,)]

        desc = self.cur.description
        assert len(desc) == 1
        assert desc[0].name == 'res'
        assert desc[0].type_code == ft.FLOAT
        assert desc[0].null_ok is True

        self.cur.execute(
            'select nullable_float_mult(value, NULL) as res '
            'from data_with_nulls order by id',
        )

        assert [tuple(x) for x in self.cur] == \
               [(None,), (None,), (None,), (None,), (None,)]

    def test_int_mult(self):
        self.cur.execute(
            'select int_mult(value, 100) as res '
            'from data order by id',
        )

        assert [tuple(x) for x in self.cur] == \
               [(200,), (200,), (500,), (400,), (0,)]

        desc = self.cur.description
        assert len(desc) == 1
        assert desc[0].name == 'res'
        assert desc[0].type_code == ft.LONGLONG
        assert desc[0].null_ok is False

        # NULL is not valid
        with self.assertRaises(self.conn.OperationalError):
            self.cur.execute(
                'select int_mult(value, NULL) as res '
                'from data order by id',
            )

    def test_tinyint_mult(self):
        self.cur.execute(
            'select tinyint_mult(value, 100) as res '
            'from data order by id',
        )

        assert [tuple(x) for x in self.cur] == \
               [(127,), (127,), (127,), (127,), (0,)]

        desc = self.cur.description
        assert len(desc) == 1
        assert desc[0].name == 'res'
        assert desc[0].type_code == ft.TINY
        assert desc[0].null_ok is False

        # NULL is not valid
        with self.assertRaises(self.conn.OperationalError):
            self.cur.execute(
                'select tinyint_mult(value, NULL) as res '
                'from data order by id',
            )

    def test_pandas_tinyint_mult(self):
        self.cur.execute(
            'select pandas_tinyint_mult(value, 100) as res '
            'from data order by id',
        )

        assert [tuple(x) for x in self.cur] == \
               [(127,), (127,), (127,), (127,), (0,)]

        desc = self.cur.description
        assert len(desc) == 1
        assert desc[0].name == 'res'
        assert desc[0].type_code == ft.TINY
        assert desc[0].null_ok is False

        # NULL is not valid
        with self.assertRaises(self.conn.OperationalError):
            self.cur.execute(
                'select pandas_tinyint_mult(value, NULL) as res '
                'from data order by id',
            )

    def test_polars_tinyint_mult(self):
        self.cur.execute(
            'select polars_tinyint_mult(value, 100) as res '
            'from data order by id',
        )

        assert [tuple(x) for x in self.cur] == \
               [(127,), (127,), (127,), (127,), (0,)]

        desc = self.cur.description
        assert len(desc) == 1
        assert desc[0].name == 'res'
        assert desc[0].type_code == ft.TINY
        assert desc[0].null_ok is False

        # NULL is not valid
        with self.assertRaises(self.conn.OperationalError):
            self.cur.execute(
                'select polars_tinyint_mult(value, NULL) as res '
                'from data order by id',
            )

    def test_numpy_tinyint_mult(self):
        self.cur.execute(
            'select numpy_tinyint_mult(value, 100) as res '
            'from data order by id',
        )

        assert [tuple(x) for x in self.cur] == \
               [(127,), (127,), (127,), (127,), (0,)]

        desc = self.cur.description
        assert len(desc) == 1
        assert desc[0].name == 'res'
        assert desc[0].type_code == ft.TINY
        assert desc[0].null_ok is False

        # NULL is not valid
        with self.assertRaises(self.conn.OperationalError):
            self.cur.execute(
                'select numpy_tinyint_mult(value, NULL) as res '
                'from data order by id',
            )

    def test_arrow_tinyint_mult(self):
        self.cur.execute(
            'select arrow_tinyint_mult(value, 100) as res from '
            'data order by id',
        )

        assert [tuple(x) for x in self.cur] == \
               [(127,), (127,), (127,), (127,), (0,)]

        desc = self.cur.description
        assert len(desc) == 1
        assert desc[0].name == 'res'
        assert desc[0].type_code == ft.TINY
        assert desc[0].null_ok is False

        # NULL is not valid
        with self.assertRaises(self.conn.OperationalError):
            self.cur.execute(
                'select arrow_tinyint_mult(value, NULL) as res '
                'from data order by id',
            )

    def test_nullable_tinyint_mult(self):
        self.cur.execute(
            'select nullable_tinyint_mult(value, 100) as res from '
            'data_with_nulls order by id',
        )

        assert [tuple(x) for x in self.cur] == \
               [(127,), (127,), (127,), (None,), (0,)]

        desc = self.cur.description
        assert len(desc) == 1
        assert desc[0].name == 'res'
        assert desc[0].type_code == ft.TINY
        assert desc[0].null_ok is True

    def test_pandas_nullable_tinyint_mult(self):
        self.cur.execute(
            'select pandas_nullable_tinyint_mult(value, 100) as res '
            'from data_with_nulls order by id',
        )

        assert [tuple(x) for x in self.cur] == \
               [(127,), (127,), (127,), (0,), (0,)]

        desc = self.cur.description
        assert len(desc) == 1
        assert desc[0].name == 'res'
        assert desc[0].type_code == ft.TINY
        assert desc[0].null_ok is True

    def test_pandas_nullable_tinyint_mult_with_masks(self):
        self.cur.execute(
            'select pandas_nullable_tinyint_mult_with_masks(value, 100) '
            'as res from data_with_nulls order by id',
        )

        assert [tuple(x) for x in self.cur] == \
               [(127,), (127,), (127,), (None,), (0,)]

        desc = self.cur.description
        assert len(desc) == 1
        assert desc[0].name == 'res'
        assert desc[0].type_code == ft.TINY
        assert desc[0].null_ok is True

    def test_polars_nullable_tinyint_mult(self):
        self.cur.execute(
            'select polars_nullable_tinyint_mult(value, 100) as res '
            'from data_with_nulls order by id',
        )

        assert [tuple(x) for x in self.cur] == \
               [(127,), (127,), (127,), (0,), (0,)]

        desc = self.cur.description
        assert len(desc) == 1
        assert desc[0].name == 'res'
        assert desc[0].type_code == ft.TINY
        assert desc[0].null_ok is True

    def test_polars_nullable_tinyint_mult_with_masks(self):
        self.cur.execute(
            'select polars_nullable_tinyint_mult_with_masks(value, 100) '
            'as res from data_with_nulls order by id',
        )

        assert [tuple(x) for x in self.cur] == \
               [(127,), (127,), (127,), (None,), (0,)]

        desc = self.cur.description
        assert len(desc) == 1
        assert desc[0].name == 'res'
        assert desc[0].type_code == ft.TINY
        assert desc[0].null_ok is True

    def test_numpy_nullable_tinyint_mult(self):
        self.cur.execute(
            'select numpy_nullable_tinyint_mult(value, 100) as res '
            'from data_with_nulls order by id',
        )

        assert [tuple(x) for x in self.cur] == \
               [(127,), (127,), (127,), (0,), (0,)]

        desc = self.cur.description
        assert len(desc) == 1
        assert desc[0].name == 'res'
        assert desc[0].type_code == ft.TINY
        assert desc[0].null_ok is True

    def test_numpy_nullable_tinyint_mult_with_masks(self):
        self.cur.execute(
            'select numpy_nullable_tinyint_mult_with_masks(value, 100) '
            'as res from data_with_nulls order by id',
        )

        assert [tuple(x) for x in self.cur] == \
               [(127,), (127,), (127,), (None,), (0,)]

        desc = self.cur.description
        assert len(desc) == 1
        assert desc[0].name == 'res'
        assert desc[0].type_code == ft.TINY
        assert desc[0].null_ok is True

    def test_arrow_nullable_tinyint_mult(self):
        self.cur.execute(
            'select arrow_nullable_tinyint_mult(value, 100) as res '
            'from data_with_nulls order by id',
        )

        assert [tuple(x) for x in self.cur] == \
               [(127,), (127,), (127,), (None,), (0,)]

        desc = self.cur.description
        assert len(desc) == 1
        assert desc[0].name == 'res'
        assert desc[0].type_code == ft.TINY
        assert desc[0].null_ok is True

    def test_arrow_nullable_tinyint_mult_with_masks(self):
        self.cur.execute(
            'select arrow_nullable_tinyint_mult_with_masks(value, 100) '
            'as res from data_with_nulls order by id',
        )

        assert [tuple(x) for x in self.cur] == \
               [(127,), (127,), (127,), (None,), (0,)]

        desc = self.cur.description
        assert len(desc) == 1
        assert desc[0].name == 'res'
        assert desc[0].type_code == ft.TINY
        assert desc[0].null_ok is True

    def test_smallint_mult(self):
        self.cur.execute(
            'select smallint_mult(value, 100) as res '
            'from data order by id',
        )

        assert [tuple(x) for x in self.cur] == \
               [(200,), (200,), (500,), (400,), (0,)]

        desc = self.cur.description
        assert len(desc) == 1
        assert desc[0].name == 'res'
        assert desc[0].type_code == ft.SHORT
        assert desc[0].null_ok is False

        # NULL is not valid
        with self.assertRaises(self.conn.OperationalError):
            self.cur.execute(
                'select smallint_mult(value, NULL) as res '
                'from data order by id',
            )

    def test_pandas_smallint_mult(self):
        self.cur.execute(
            'select pandas_smallint_mult(value, 100) as res '
            'from data order by id',
        )

        assert [tuple(x) for x in self.cur] == \
               [(200,), (200,), (500,), (400,), (0,)]

        desc = self.cur.description
        assert len(desc) == 1
        assert desc[0].name == 'res'
        assert desc[0].type_code == ft.SHORT
        assert desc[0].null_ok is False

        # NULL is not valid
        with self.assertRaises(self.conn.OperationalError):
            self.cur.execute(
                'select pandas_smallint_mult(value, NULL) as res '
                'from data order by id',
            )

    def test_polars_smallint_mult(self):
        self.cur.execute(
            'select polars_smallint_mult(value, 100) as res '
            'from data order by id',
        )

        assert [tuple(x) for x in self.cur] == \
               [(200,), (200,), (500,), (400,), (0,)]

        desc = self.cur.description
        assert len(desc) == 1
        assert desc[0].name == 'res'
        assert desc[0].type_code == ft.SHORT
        assert desc[0].null_ok is False

        # NULL is not valid
        with self.assertRaises(self.conn.OperationalError):
            self.cur.execute(
                'select polars_smallint_mult(value, NULL) as res '
                'from data order by id',
            )

    def test_numpy_smallint_mult(self):
        self.cur.execute(
            'select numpy_smallint_mult(value, 100) as res '
            'from data order by id',
        )

        assert [tuple(x) for x in self.cur] == \
               [(200,), (200,), (500,), (400,), (0,)]

        desc = self.cur.description
        assert len(desc) == 1
        assert desc[0].name == 'res'
        assert desc[0].type_code == ft.SHORT
        assert desc[0].null_ok is False

        # NULL is not valid
        with self.assertRaises(self.conn.OperationalError):
            self.cur.execute(
                'select numpy_smallint_mult(value, NULL) as res '
                'from data order by id',
            )

    def test_arrow_smallint_mult(self):
        self.cur.execute(
            'select arrow_smallint_mult(value, 100) as res '
            'from data order by id',
        )

        assert [tuple(x) for x in self.cur] == \
               [(200,), (200,), (500,), (400,), (0,)]

        desc = self.cur.description
        assert len(desc) == 1
        assert desc[0].name == 'res'
        assert desc[0].type_code == ft.SHORT
        assert desc[0].null_ok is False

        # NULL is not valid
        with self.assertRaises(self.conn.OperationalError):
            self.cur.execute(
                'select arrow_smallint_mult(value, NULL) as res '
                'from data order by id',
            )

    def test_nullable_smallint_mult(self):
        self.cur.execute(
            'select nullable_smallint_mult(value, 100) as res '
            'from data_with_nulls order by id',
        )

        assert [tuple(x) for x in self.cur] == \
               [(200,), (200,), (500,), (None,), (0,)]

        desc = self.cur.description
        assert len(desc) == 1
        assert desc[0].name == 'res'
        assert desc[0].type_code == ft.SHORT
        assert desc[0].null_ok is True

    def test_mediumint_mult(self):
        self.cur.execute(
            'select mediumint_mult(value, 100) as res '
            'from data order by id',
        )

        assert [tuple(x) for x in self.cur] == \
               [(200,), (200,), (500,), (400,), (0,)]

        desc = self.cur.description
        assert len(desc) == 1
        assert desc[0].name == 'res'
        assert desc[0].type_code == ft.INT24
        assert desc[0].null_ok is False

        # NULL is not valid
        with self.assertRaises(self.conn.OperationalError):
            self.cur.execute(
                'select mediumint_mult(value, NULL) as res '
                'from data order by id',
            )

    def test_pandas_mediumint_mult(self):
        self.cur.execute(
            'select pandas_mediumint_mult(value, 100) as res '
            'from data order by id',
        )

        assert [tuple(x) for x in self.cur] == \
               [(200,), (200,), (500,), (400,), (0,)]

        desc = self.cur.description
        assert len(desc) == 1
        assert desc[0].name == 'res'
        assert desc[0].type_code == ft.INT24
        assert desc[0].null_ok is False

        # NULL is not valid
        with self.assertRaises(self.conn.OperationalError):
            self.cur.execute(
                'select pandas_mediumint_mult(value, NULL) as res '
                'from data order by id',
            )

    def test_polars_mediumint_mult(self):
        self.cur.execute(
            'select polars_mediumint_mult(value, 100) as res '
            'from data order by id',
        )

        assert [tuple(x) for x in self.cur] == \
               [(200,), (200,), (500,), (400,), (0,)]

        desc = self.cur.description
        assert len(desc) == 1
        assert desc[0].name == 'res'
        assert desc[0].type_code == ft.INT24
        assert desc[0].null_ok is False

        # NULL is not valid
        with self.assertRaises(self.conn.OperationalError):
            self.cur.execute(
                'select polars_mediumint_mult(value, NULL) as res '
                'from data order by id',
            )

    def test_numpy_mediumint_mult(self):
        self.cur.execute(
            'select numpy_mediumint_mult(value, 100) as res '
            'from data order by id',
        )

        assert [tuple(x) for x in self.cur] == \
               [(200,), (200,), (500,), (400,), (0,)]

        desc = self.cur.description
        assert len(desc) == 1
        assert desc[0].name == 'res'
        assert desc[0].type_code == ft.INT24
        assert desc[0].null_ok is False

        # NULL is not valid
        with self.assertRaises(self.conn.OperationalError):
            self.cur.execute(
                'select numpy_mediumint_mult(value, NULL) as res '
                'from data order by id',
            )

    def test_arrow_mediumint_mult(self):
        self.cur.execute(
            'select arrow_mediumint_mult(value, 100) as res '
            'from data order by id',
        )

        assert [tuple(x) for x in self.cur] == \
               [(200,), (200,), (500,), (400,), (0,)]

        desc = self.cur.description
        assert len(desc) == 1
        assert desc[0].name == 'res'
        assert desc[0].type_code == ft.INT24
        assert desc[0].null_ok is False

        # NULL is not valid
        with self.assertRaises(self.conn.OperationalError):
            self.cur.execute(
                'select arrow_mediumint_mult(value, NULL) as res '
                'from data order by id',
            )

    def test_nullable_mediumint_mult(self):
        self.cur.execute(
            'select nullable_mediumint_mult(value, 100) as res '
            'from data_with_nulls order by id',
        )

        assert [tuple(x) for x in self.cur] == \
               [(200,), (200,), (500,), (None,), (0,)]

        desc = self.cur.description
        assert len(desc) == 1
        assert desc[0].name == 'res'
        assert desc[0].type_code == ft.INT24
        assert desc[0].null_ok is True

    def test_bigint_mult(self):
        self.cur.execute(
            'select bigint_mult(value, 100) as res '
            'from data order by id',
        )

        assert [tuple(x) for x in self.cur] == \
               [(200,), (200,), (500,), (400,), (0,)]

        desc = self.cur.description
        assert len(desc) == 1
        assert desc[0].name == 'res'
        assert desc[0].type_code == ft.LONGLONG
        assert desc[0].null_ok is False

        # NULL is not valid
        with self.assertRaises(self.conn.OperationalError):
            self.cur.execute(
                'select bigint_mult(value, NULL) as res '
                'from data order by id',
            )

    def test_pandas_bigint_mult(self):
        self.cur.execute(
            'select pandas_bigint_mult(value, 100) as res '
            'from data order by id',
        )

        assert [tuple(x) for x in self.cur] == \
               [(200,), (200,), (500,), (400,), (0,)]

        desc = self.cur.description
        assert len(desc) == 1
        assert desc[0].name == 'res'
        assert desc[0].type_code == ft.LONGLONG
        assert desc[0].null_ok is False

        # NULL is not valid
        with self.assertRaises(self.conn.OperationalError):
            self.cur.execute(
                'select pandas_bigint_mult(value, NULL) as res '
                'from data order by id',
            )

    def test_polars_bigint_mult(self):
        self.cur.execute(
            'select polars_bigint_mult(value, 100) as res '
            'from data order by id',
        )

        assert [tuple(x) for x in self.cur] == \
               [(200,), (200,), (500,), (400,), (0,)]

        desc = self.cur.description
        assert len(desc) == 1
        assert desc[0].name == 'res'
        assert desc[0].type_code == ft.LONGLONG
        assert desc[0].null_ok is False

        # NULL is not valid
        with self.assertRaises(self.conn.OperationalError):
            self.cur.execute(
                'select polars_bigint_mult(value, NULL) as res '
                'from data order by id',
            )

    def test_numpy_bigint_mult(self):
        self.cur.execute(
            'select numpy_bigint_mult(value, 100) as res '
            'from data order by id',
        )

        assert [tuple(x) for x in self.cur] == \
               [(200,), (200,), (500,), (400,), (0,)]

        desc = self.cur.description
        assert len(desc) == 1
        assert desc[0].name == 'res'
        assert desc[0].type_code == ft.LONGLONG
        assert desc[0].null_ok is False

        # NULL is not valid
        with self.assertRaises(self.conn.OperationalError):
            self.cur.execute(
                'select numpy_bigint_mult(value, NULL) as res '
                'from data order by id',
            )

    def test_numpy_nullable_bigint_mult(self):
        self.cur.execute(
            'select numpy_nullable_bigint_mult(value, 100) as res '
            'from data_with_nulls order by id',
        )

        assert [tuple(x) for x in self.cur] == \
               [(200,), (200,), (500,), (None,), (0,)]

        desc = self.cur.description
        assert len(desc) == 1
        assert desc[0].name == 'res'
        assert desc[0].type_code == ft.LONGLONG
        assert desc[0].null_ok is True

    def test_arrow_bigint_mult(self):
        self.cur.execute(
            'select arrow_bigint_mult(value, 100) as res '
            'from data order by id',
        )

        assert [tuple(x) for x in self.cur] == \
               [(200,), (200,), (500,), (400,), (0,)]

        desc = self.cur.description
        assert len(desc) == 1
        assert desc[0].name == 'res'
        assert desc[0].type_code == ft.LONGLONG
        assert desc[0].null_ok is False

        # NULL is not valid
        with self.assertRaises(self.conn.OperationalError):
            self.cur.execute(
                'select arrow_bigint_mult(value, NULL) as res '
                'from data order by id',
            )

    def test_nullable_bigint_mult(self):
        self.cur.execute(
            'select nullable_bigint_mult(value, 100) as res '
            'from data_with_nulls order by id',
        )

        assert [tuple(x) for x in self.cur] == \
               [(200,), (200,), (500,), (None,), (0,)]

        desc = self.cur.description
        assert len(desc) == 1
        assert desc[0].name == 'res'
        assert desc[0].type_code == ft.LONGLONG
        assert desc[0].null_ok is True

    def test_nullable_int_mult(self):
        self.cur.execute(
            'select nullable_int_mult(value, 100) as res '
            'from data_with_nulls order by id',
        )

        assert [tuple(x) for x in self.cur] == \
               [(200,), (200,), (500,), (None,), (0,)]

        desc = self.cur.description
        assert len(desc) == 1
        assert desc[0].name == 'res'
        assert desc[0].type_code == ft.LONGLONG
        assert desc[0].null_ok is True

    def test_string_mult(self):
        self.cur.execute(
            'select string_mult(name, value) as res '
            'from data order by id',
        )

        assert [tuple(x) for x in self.cur] == [
            ('antelopesantelopes',),
            ('bearsbears',),
            ('catscatscatscatscats',),
            ('dogsdogsdogsdogs',),
            ('',),
        ]

        desc = self.cur.description
        assert len(desc) == 1
        assert desc[0].name == 'res'
        assert desc[0].type_code == ft.BLOB
        assert desc[0].null_ok is False

        # NULL is not valid
        with self.assertRaises(self.conn.OperationalError):
            self.cur.execute(
                'select string_mult(NULL, value) as res '
                'from data order by id',
            )

    def test_pandas_string_mult(self):
        self.cur.execute(
            'select pandas_string_mult(name, value) as res '
            'from data order by id',
        )

        assert [tuple(x) for x in self.cur] == [
            ('antelopesantelopes',),
            ('bearsbears',),
            ('catscatscatscatscats',),
            ('dogsdogsdogsdogs',),
            ('',),
        ]

        desc = self.cur.description
        assert len(desc) == 1
        assert desc[0].name == 'res'
        assert desc[0].type_code == ft.BLOB
        assert desc[0].null_ok is False

        # NULL is not valid
        with self.assertRaises(self.conn.OperationalError):
            self.cur.execute(
                'select pandas_string_mult(NULL, value) as res '
                'from data order by id',
            )

    def test_numpy_string_mult(self):
        self.cur.execute(
            'select numpy_string_mult(name, value) as res '
            'from data order by id',
        )

        assert [tuple(x) for x in self.cur] == [
            ('antelopesantelopes',),
            ('bearsbears',),
            ('catscatscatscatscats',),
            ('dogsdogsdogsdogs',),
            ('',),
        ]

        desc = self.cur.description
        assert len(desc) == 1
        assert desc[0].name == 'res'
        assert desc[0].type_code == ft.BLOB
        assert desc[0].null_ok is False

        # NULL is not valid
        with self.assertRaises(self.conn.OperationalError):
            self.cur.execute(
                'select numpy_string_mult(NULL, value) as res '
                'from data order by id',
            )

    def _test_polars_string_mult(self):
        self.cur.execute(
            'select polars_string_mult(name, value) as res '
            'from data order by id',
        )

        assert [tuple(x) for x in self.cur] == [
            ('antelopesantelopes',),
            ('bearsbears',),
            ('catscatscatscatscats',),
            ('dogsdogsdogsdogs',),
            ('',),
        ]

        desc = self.cur.description
        assert len(desc) == 1
        assert desc[0].name == 'res'
        assert desc[0].type_code == ft.BLOB
        assert desc[0].null_ok is False

        # NULL is not valid
        with self.assertRaises(self.conn.OperationalError):
            self.cur.execute(
                'select polars_string_mult(NULL, value) as res '
                'from data order by id',
            )

    def _test_arrow_string_mult(self):
        self.cur.execute(
            'select arrow_string_mult(name, value) as res '
            'from data order by id',
        )

        assert [tuple(x) for x in self.cur] == [
            ('antelopesantelopes',),
            ('bearsbears',),
            ('catscatscatscatscats',),
            ('dogsdogsdogsdogs',),
            ('',),
        ]

        desc = self.cur.description
        assert len(desc) == 1
        assert desc[0].name == 'res'
        assert desc[0].type_code == ft.BLOB
        assert desc[0].null_ok is False

        # NULL is not valid
        with self.assertRaises(self.conn.OperationalError):
            self.cur.execute(
                'select arrow_string_mult(NULL, value) as res '
                'from data order by id',
            )

    def test_nullable_string_mult(self):
        self.cur.execute(
            'select nullable_string_mult(name, value) as res '
            'from data_with_nulls order by id',
        )

        assert [tuple(x) for x in self.cur] == [
            ('antelopesantelopes',),
            (None,),
            (None,),
            (None,),
            ('',),
        ]

        desc = self.cur.description
        assert len(desc) == 1
        assert desc[0].name == 'res'
        assert desc[0].type_code == ft.BLOB
        assert desc[0].null_ok is True

    def test_varchar_mult(self):
        self.cur.execute(
            'select varchar_mult(name, value) as res '
            'from data order by id',
        )

        assert [tuple(x) for x in self.cur] == [
            ('antelopesantelopes',),
            ('bearsbears',),
            ('catscatscatscatscats',),
            ('dogsdogsdogsdogs',),
            ('',),
        ]

        desc = self.cur.description
        assert len(desc) == 1
        assert desc[0].name == 'res'
        assert desc[0].type_code == ft.BLOB
        assert desc[0].null_ok is False

        # NULL is not valid
        with self.assertRaises(self.conn.OperationalError):
            self.cur.execute(
                'select varchar_mult(NULL, value) as res '
                'from data order by id',
            )

    def test_nullable_varchar_mult(self):
        self.cur.execute(
            'select nullable_varchar_mult(name, value) as res '
            'from data_with_nulls order by id',
        )

        assert [tuple(x) for x in self.cur] == [
            ('antelopesantelopes',),
            (None,),
            (None,),
            (None,),
            ('',),
        ]

        desc = self.cur.description
        assert len(desc) == 1
        assert desc[0].name == 'res'
        assert desc[0].type_code == ft.BLOB
        assert desc[0].null_ok is True
