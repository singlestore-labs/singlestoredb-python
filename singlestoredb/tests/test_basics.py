#!/usr/bin/env python
# type: ignore
"""Basic SingleStoreDB connection testing."""
import datetime
import decimal
import math
import os
import unittest

from requests.exceptions import InvalidJSONError

try:
    import numpy as np
    has_numpy = True
except ImportError:
    has_numpy = False

try:
    import shapely.wkt
    has_shapely = True
except ImportError:
    has_shapely = False

try:
    import pygeos
    from pygeos.testing import assert_geometries_equal
    has_pygeos = True
except ImportError:
    has_pygeos = False

import singlestoredb as s2
from . import utils
# import traceback


class TestBasics(unittest.TestCase):

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

    def test_connection(self):
        self.cur.execute('show databases')
        dbs = set([x[0] for x in self.cur.fetchall()])
        assert type(self).dbname in dbs, dbs

    def test_fetchall(self):
        self.cur.execute('select * from data')

        out = self.cur.fetchall()

        desc = self.cur.description
        rowcount = self.cur.rowcount
        rownumber = self.cur.rownumber
        lastrowid = self.cur.lastrowid

        assert sorted(out) == sorted([
            ('a', 'antelopes', 2),
            ('b', 'bears', 2),
            ('c', 'cats', 5),
            ('d', 'dogs', 4),
            ('e', 'elephants', 0),
        ]), out

        assert rowcount in (5, -1), rowcount
        assert rownumber == 5, rownumber
        assert lastrowid is None, lastrowid
        assert len(desc) == 3, desc
        assert desc[0].name == 'id', desc[0].name
        assert desc[0].type_code in [253, 15], desc[0].type_code
        assert desc[1].name == 'name', desc[1].name
        assert desc[1].type_code in [253, 15], desc[1].type_code
        assert desc[2].name == 'value', desc[2].name
        assert desc[2].type_code == 8, desc[2].type_code

    def test_fetchone(self):
        self.cur.execute('select * from data')

        out = []
        while True:
            row = self.cur.fetchone()
            if row is None:
                break
            out.append(row)
            assert self.cur.rownumber == len(out), self.cur.rownumber

        desc = self.cur.description
        rowcount = self.cur.rowcount
        rownumber = self.cur.rownumber
        lastrowid = self.cur.lastrowid

        assert sorted(out) == sorted([
            ('a', 'antelopes', 2),
            ('b', 'bears', 2),
            ('c', 'cats', 5),
            ('d', 'dogs', 4),
            ('e', 'elephants', 0),
        ]), out

        assert rowcount in (5, -1), rowcount
        assert rownumber == 5, rownumber
        assert lastrowid is None, lastrowid
        assert len(desc) == 3, desc
        assert desc[0].name == 'id', desc[0].name
        assert desc[0].type_code in [253, 15], desc[0].type_code
        assert desc[1].name == 'name', desc[1].name
        assert desc[1].type_code in [253, 15], desc[1].type_code
        assert desc[2].name == 'value', desc[2].name
        assert desc[2].type_code == 8, desc[2].type_code

    def test_fetchmany(self):
        self.cur.execute('select * from data')

        out = []
        while True:
            rows = self.cur.fetchmany(size=3)
            assert len(rows) <= 3, rows
            if not rows:
                break
            out.extend(rows)
            assert self.cur.rownumber == len(out), self.cur.rownumber

        desc = self.cur.description
        rowcount = self.cur.rowcount
        rownumber = self.cur.rownumber
        lastrowid = self.cur.lastrowid

        assert sorted(out) == sorted([
            ('a', 'antelopes', 2),
            ('b', 'bears', 2),
            ('c', 'cats', 5),
            ('d', 'dogs', 4),
            ('e', 'elephants', 0),
        ]), out

        assert rowcount in (5, -1), rowcount
        assert rownumber == 5, rownumber
        assert lastrowid is None, lastrowid
        assert len(desc) == 3, desc
        assert desc[0].name == 'id'
        assert desc[0].type_code in [253, 15]
        assert desc[1].name == 'name'
        assert desc[1].type_code in [253, 15]
        assert desc[2].name == 'value'
        assert desc[2].type_code == 8

    def test_arraysize(self):
        self.cur.execute('select * from data')

        self.cur.arraysize = 3
        assert self.cur.arraysize == 3

        rows = self.cur.fetchmany()
        assert len(rows) == 3, rows
        assert self.cur.rownumber == 3, self.cur.rownumber

        self.cur.arraysize = 1
        assert self.cur.arraysize == 1

        rows = self.cur.fetchmany()
        assert len(rows) == 1, rows
        assert self.cur.rownumber == 4, self.cur.rownumber

        rows = self.cur.fetchmany()
        assert len(rows) == 1, rows
        assert self.cur.rownumber == 5, self.cur.rownumber

        rows = self.cur.fetchall()
        assert len(rows) == 0, rows
        assert self.cur.rownumber == 5, self.cur.rownumber

    def test_execute_with_dict_params(self):
        self.cur.execute('select * from data where id < %(name)s', dict(name='d'))
        out = self.cur.fetchall()

        desc = self.cur.description
        rowcount = self.cur.rowcount
        lastrowid = self.cur.lastrowid

        assert sorted(out) == sorted([
            ('a', 'antelopes', 2),
            ('b', 'bears', 2),
            ('c', 'cats', 5),
        ]), out

        assert rowcount in (3, -1), rowcount
        assert lastrowid is None, lastrowid
        assert len(desc) == 3, desc
        assert desc[0].name == 'id', desc[0].name
        assert desc[0].type_code in [253, 15], desc[0].type_code
        assert desc[1].name == 'name', desc[1].name
        assert desc[1].type_code in [253, 15], desc[1].type_code
        assert desc[2].name == 'value', desc[2].name
        assert desc[2].type_code == 8, desc[2].type_code

        with self.assertRaises(KeyError):
            self.cur.execute('select * from data where id < %(name)s', dict(foo='d'))

    def test_execute_with_positional_params(self):
        self.cur.execute('select * from data where id < %s', ['d'])
        out = self.cur.fetchall()

        desc = self.cur.description
        rowcount = self.cur.rowcount
        lastrowid = self.cur.lastrowid

        assert sorted(out) == sorted([
            ('a', 'antelopes', 2),
            ('b', 'bears', 2),
            ('c', 'cats', 5),
        ]), out

        assert rowcount in (3, -1), rowcount
        assert lastrowid is None, lastrowid
        assert len(desc) == 3, desc
        assert desc[0].name == 'id', desc[0].name
        assert desc[0].type_code in [253, 15], desc[0].type_code
        assert desc[1].name == 'name', desc[1].name
        assert desc[1].type_code in [253, 15], desc[1].type_code
        assert desc[2].name == 'value', desc[2].name
        assert desc[2].type_code == 8, desc[2].type_code

        with self.assertRaises(TypeError):
            self.cur.execute(
                'select * from data where id < %s and id > %s', ['d', 'e', 'f'],
            )

        with self.assertRaises(TypeError):
            self.cur.execute('select * from data where id < %s and id > %s', ['d'])

    def test_execute_with_escaped_positional_substitutions(self):
        self.cur.execute(
            'select `id`, `time` from alltypes where `time` = %s', ['00:07:00'],
        )
        out = self.cur.fetchall()
        assert out[0] == (0, datetime.timedelta(seconds=420)), out[0]

        self.cur.execute('select `id`, `time` from alltypes where `time` = "00:07:00"')
        out = self.cur.fetchall()
        assert out[0] == (0, datetime.timedelta(seconds=420)), out[0]

#       with self.assertRaises(IndexError):
#           self.cur.execute(
#               'select `id`, `time` from alltypes where `id` = %1s '
#               'or `time` = "00:07:00"', [0],
#           )

        self.cur.execute(
            'select `id`, `time` from alltypes where `id` = %s '
            'or `time` = "00:07:00"', [0],
        )
        out = self.cur.fetchall()
        assert out[0] == (0, datetime.timedelta(seconds=420)), out[0]

    def test_execute_with_escaped_substitutions(self):
        self.cur.execute(
            'select `id`, `time` from alltypes where `time` = %(time)s',
            dict(time='00:07:00'),
        )
        out = self.cur.fetchall()
        assert out[0] == (0, datetime.timedelta(seconds=420)), out[0]

        self.cur.execute(
            'select `id`, `time` from alltypes where `time` = %(time)s',
            dict(time='00:07:00'),
        )
        out = self.cur.fetchall()
        assert len(out) == 1, out

        with self.assertRaises(KeyError):
            self.cur.execute(
                'select `id`, `time`, `char_100` from alltypes '
                'where `time` = %(time)s or `char_100` like "foo:bar"',
                dict(x='00:07:00'),
            )

        self.cur.execute(
            'select `id`, `time`, `char_100` from alltypes '
            'where `time` = %(time)s or `char_100` like "foo::bar"',
            dict(time='00:07:00'),
        )
        out = self.cur.fetchall()
        assert out[0][:2] == (0, datetime.timedelta(seconds=420)), out[0]

    def test_is_connected(self):
        assert self.conn.is_connected()
        assert self.cur.is_connected()
        self.cur.close()
        assert not self.cur.is_connected()
        assert self.conn.is_connected()
        self.conn.close()
        assert not self.cur.is_connected()
        assert not self.conn.is_connected()

    def test_connection_attr(self):
        # Use context manager to get to underlying object (self.conn is a weakref.proxy)
        with self.conn as conn:
            assert conn is self.conn

    def test_executemany(self):
        # NOTE: Doesn't actually do anything since no rows match
        self.cur.executemany(
            'delete from data where id > %(name)s',
            [dict(name='z'), dict(name='y')],
        )

    def test_executemany_no_args(self):
        self.cur.executemany('select * from data where id > "z"')

    def test_context_managers(self):
        with s2.connect() as conn:
            with conn.cursor() as cur:
                assert cur.is_connected()
                assert conn.is_connected()
        assert not cur.is_connected()
        assert not conn.is_connected()

    def test_iterator(self):
        self.cur.execute('select * from data')

        out = []
        for row in self.cur:
            out.append(row)

        assert sorted(out) == sorted([
            ('a', 'antelopes', 2),
            ('b', 'bears', 2),
            ('c', 'cats', 5),
            ('d', 'dogs', 4),
            ('e', 'elephants', 0),
        ]), out

    def test_urls(self):
        from singlestoredb.connection import build_params
        from singlestoredb.config import get_option

        # Full URL (without scheme)
        url = 'me:p455w0rd@s2host.com:3307/mydb'
        out = build_params(host=url)
        assert out['driver'] == get_option('driver'), out['driver']
        assert out['host'] == 's2host.com', out['host']
        assert out['port'] == 3307, out['port']
        assert out['database'] == 'mydb', out['database']
        assert out['user'] == 'me', out['user']
        assert out['password'] == 'p455w0rd', out['password']

        # Full URL (with scheme)
        url = 'http://me:p455w0rd@s2host.com:3307/mydb'
        out = build_params(host=url)
        assert out['driver'] == 'http', out['driver']
        assert out['host'] == 's2host.com', out['host']
        assert out['port'] == 3307, out['port']
        assert out['database'] == 'mydb', out['database']
        assert out['user'] == 'me', out['user']
        assert out['password'] == 'p455w0rd', out['password']

        # No port
        url = 'me:p455w0rd@s2host.com/mydb'
        out = build_params(host=url)
        assert out['driver'] == get_option('driver'), out['driver']
        assert out['host'] == 's2host.com', out['host']
        if out['driver'] in ['http', 'https']:
            assert out['port'] in [get_option('http_port'), 80, 443], out['port']
        else:
            assert out['port'] in [get_option('port'), 3306], out['port']
        assert out['database'] == 'mydb', out['database']
        assert out['user'] == 'me', out['user']
        assert out['password'] == 'p455w0rd', out['password']

        # No http port
        url = 'http://me:p455w0rd@s2host.com/mydb'
        out = build_params(host=url)
        assert out['driver'] == 'http', out['driver']
        assert out['host'] == 's2host.com', out['host']
        assert out['port'] in [get_option('http_port'), 80], out['port']
        assert out['database'] == 'mydb', out['database']
        assert out['user'] == 'me', out['user']
        assert out['password'] == 'p455w0rd', out['password']

        # No https port
        url = 'https://me:p455w0rd@s2host.com/mydb'
        out = build_params(host=url)
        assert out['driver'] == 'https', out['driver']
        assert out['host'] == 's2host.com', out['host']
        assert out['port'] in [get_option('http_port'), 443], out['port']
        assert out['database'] == 'mydb', out['database']
        assert out['user'] == 'me', out['user']
        assert out['password'] == 'p455w0rd', out['password']

        # Invalid port
        url = 'https://me:p455w0rd@s2host.com:foo/mydb'
        with self.assertRaises(ValueError):
            build_params(host=url)

        # Empty password
        url = 'me:@s2host.com/mydb'
        out = build_params(host=url)
        assert out['driver'] == get_option('driver'), out['driver']
        assert out['host'] == 's2host.com', out['host']
        if out['driver'] in ['http', 'https']:
            assert out['port'] in [get_option('http_port'), 80, 443], out['port']
        else:
            assert out['port'] in [get_option('port'), 3306], out['port']
        assert out['database'] == 'mydb', out['database']
        assert out['user'] == 'me', out['user']
        assert out['password'] == '', out['password']

        # No user/password
        url = 's2host.com/mydb'
        out = build_params(host=url)
        assert out['driver'] == get_option('driver'), out['driver']
        assert out['host'] == 's2host.com', out['host']
        if out['driver'] in ['http', 'https']:
            assert out['port'] in [get_option('http_port'), 80, 443], out['port']
        else:
            assert out['port'] in [get_option('port'), 3306], out['port']
        assert out['database'] == 'mydb', out['database']
        assert 'user' not in out or out['user'] == get_option('user'), out['user']
        assert 'password' not in out or out['password'] == get_option(
            'password',
        ), out['password']

        # Just hostname
        url = 's2host.com'
        out = build_params(host=url)
        assert out['driver'] == get_option('driver'), out['driver']
        assert out['host'] == 's2host.com', out['host']
        if out['driver'] in ['http', 'https']:
            assert out['port'] in [get_option('http_port'), 80, 443], out['port']
        else:
            assert out['port'] in [get_option('port'), 3306], out['port']
        assert 'database' not in out
        assert 'user' not in out or out['user'] == get_option('user'), out['user']
        assert 'password' not in out or out['password'] == get_option(
            'password',
        ), out['password']

        # Just hostname and port
        url = 's2host.com:1000'
        out = build_params(host=url)
        assert out['driver'] == get_option('driver'), out['driver']
        assert out['host'] == 's2host.com', out['host']
        assert out['port'] == 1000, out['port']
        assert 'database' not in out
        assert 'user' not in out or out['user'] == get_option('user'), out['user']
        assert 'password' not in out or out['password'] == get_option(
            'password',
        ), out['password']

        # Query options
        url = 's2host.com:1000?local_infile=1&charset=utf8'
        out = build_params(host=url)
        assert out['driver'] == get_option('driver'), out['driver']
        assert out['host'] == 's2host.com', out['host']
        assert out['port'] == 1000, out['port']
        assert 'database' not in out
        assert 'user' not in out or out['user'] == get_option('user'), out['user']
        assert 'password' not in out or out['password'] == get_option(
            'password',
        ), out['password']
        assert out['local_infile'] is True, out['local_infile']
        assert out['charset'] == 'utf8', out['charset']

        # Bad query option
        url = 's2host.com:1000?bad_param=10'
        with self.assertRaises(ValueError):
            build_params(host=url)

    def test_wrap_exc(self):
        with self.assertRaises(s2.ProgrammingError) as cm:
            self.cur.execute('garbage syntax')

        exc = cm.exception
        assert exc.errno == 1064, exc.errno
        assert 'You have an error in your SQL syntax' in exc.errmsg, exc.errmsg

    def test_extended_types(self):
        if not has_numpy or not has_pygeos or not has_shapely:
            self.skipTest('Test requires numpy, pygeos, and shapely')

        import uuid

        key = str(uuid.uuid4())

        # shapely data
        data = [
            (
                1, 'POLYGON((1 1, 2 1, 2 2, 1 2, 1 1))', 'POINT(1.5 1.5)',
                [0.5, 0.6], datetime.datetime(1950, 1, 2, 12, 13, 14),
                datetime.date(1950, 1, 2), datetime.time(12, 13, 14),
                datetime.timedelta(seconds=123456), key,
            ),
            (
                2, 'POLYGON((5 1, 6 1, 6 2, 5 2, 5 1))', 'POINT(5.5 1.5)',
                [1.3, 2.5], datetime.datetime(1960, 3, 4, 15, 16, 17),
                datetime.date(1960, 3, 4), datetime.time(15, 16, 17),
                datetime.timedelta(seconds=2), key,
            ),
            (
                3, 'POLYGON((5 5, 6 5, 6 6, 5 6, 5 5))', 'POINT(5.5 5.5)',
                [10.3, 11.1], datetime.datetime(1970, 6, 7, 18, 19, 20),
                datetime.date(1970, 5, 6), datetime.time(18, 19, 20),
                datetime.timedelta(seconds=-2), key,
            ),
            (
                4, 'POLYGON((1 5, 2 5, 2 6, 1 6, 1 5))', 'POINT(1.5 5.5)',
                [3.3, 3.4], datetime.datetime(1980, 8, 9, 21, 22, 23),
                datetime.date(1980, 7, 8), datetime.time(21, 22, 23),
                datetime.timedelta(seconds=-123456), key,
            ),
            (
                5, 'POLYGON((3 3, 4 3, 4 4, 3 4, 3 3))', 'POINT(3.5 3.5)',
                [2.9, 9.5], datetime.datetime(2010, 10, 11, 1, 2, 3),
                datetime.date(2010, 8, 9), datetime.time(1, 2, 3),
                datetime.timedelta(seconds=0), key,
            ),
        ]

        new_data = []
        for i, row in enumerate(data):
            row = list(row)
            row[1] = shapely.wkt.loads(row[1])
            row[2] = shapely.wkt.loads(row[2])
            if 'http' in self.conn.driver:
                row[3] = ''
            else:
                row[3] = np.array(row[3], dtype='<f4')
            new_data.append(row)

        self.cur.executemany(
            'INSERT INTO extended_types '
            '(id, geography, geographypoint, vectors, dt, d, t, td, testkey) '
            'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)', new_data,
        )

        self.cur.execute(
            'SELECT * FROM extended_types WHERE testkey = %s ORDER BY id', [key],
        )

        for data_row, row in zip(new_data, self.cur):
            assert data_row[0] == row[0]
            assert data_row[1].equals_exact(shapely.wkt.loads(row[1]), 1e-4)
            assert data_row[2].equals_exact(shapely.wkt.loads(row[2]), 1e-4)
            if 'http' in self.conn.driver:
                assert row[3] == b''
            else:
                assert (data_row[3] == np.frombuffer(row[3], dtype='<f4')).all()

        # pygeos data
        data = [
            (
                6, 'POLYGON((1 1, 2 1, 2 2, 1 2, 1 1))', 'POINT(1.5 1.5)',
                [0.5, 0.6], datetime.datetime(1950, 1, 2, 12, 13, 14),
                datetime.date(1950, 1, 2), datetime.time(12, 13, 14),
                datetime.timedelta(seconds=123456), key,
            ),
            (
                7, 'POLYGON((5 1, 6 1, 6 2, 5 2, 5 1))', 'POINT(5.5 1.5)',
                [1.3, 2.5], datetime.datetime(1960, 3, 4, 15, 16, 17),
                datetime.date(1960, 3, 4), datetime.time(15, 16, 17),
                datetime.timedelta(seconds=2), key,
            ),
            (
                8, 'POLYGON((5 5, 6 5, 6 6, 5 6, 5 5))', 'POINT(5.5 5.5)',
                [10.3, 11.1], datetime.datetime(1970, 6, 7, 18, 19, 20),
                datetime.date(1970, 5, 6), datetime.time(18, 19, 20),
                datetime.timedelta(seconds=-2), key,
            ),
            (
                9, 'POLYGON((1 5, 2 5, 2 6, 1 6, 1 5))', 'POINT(1.5 5.5)',
                [3.3, 3.4], datetime.datetime(1980, 8, 9, 21, 22, 23),
                datetime.date(1980, 7, 8), datetime.time(21, 22, 23),
                datetime.timedelta(seconds=-123456), key,
            ),
            (
                10, 'POLYGON((3 3, 4 3, 4 4, 3 4, 3 3))', 'POINT(3.5 3.5)',
                [2.9, 9.5], datetime.datetime(2010, 10, 11, 1, 2, 3),
                datetime.date(2010, 8, 9), datetime.time(1, 2, 3),
                datetime.timedelta(seconds=0), key,
            ),
        ]

        new_data = []
        for i, row in enumerate(data):
            row = list(row)
            row[1] = pygeos.io.from_wkt(row[1])
            row[2] = pygeos.io.from_wkt(row[2])
            if 'http' in self.conn.driver:
                row[3] = ''
            else:
                row[3] = np.array(row[3], dtype='<f4')
            new_data.append(row)

        self.cur.executemany(
            'INSERT INTO extended_types '
            '(id, geography, geographypoint, vectors, dt, d, t, td, testkey) '
            'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)', new_data,
        )

        self.cur.execute(
            'SELECT * FROM extended_types WHERE id >= 6 and testkey = %s ORDER BY id', [
                key,
            ],
        )

        for data_row, row in zip(new_data, self.cur):
            assert data_row[0] == row[0]
            assert_geometries_equal(data_row[1], pygeos.io.from_wkt(row[1]))
            assert_geometries_equal(data_row[2], pygeos.io.from_wkt(row[2]))
            if 'http' in self.conn.driver:
                assert row[3] == b''
            else:
                assert (data_row[3] == np.frombuffer(row[3], dtype='<f4')).all()

    def test_alltypes(self):
        self.cur.execute('select * from alltypes where id = 0')
        names = [x[0] for x in self.cur.description]
        types = [x[1] for x in self.cur.description]
        out = self.cur.fetchone()
        row = dict(zip(names, out))
        typ = dict(zip(names, types))

        bits = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]

        def otype(x):
            return x

        assert row['id'] == 0, row['id']
        assert typ['id'] == otype(3), typ['id']

        assert row['tinyint'] == 80, row['tinyint']
        assert typ['tinyint'] == otype(1), typ['tinyint']

        assert row['bool'] == 0, row['bool']
        assert typ['bool'] == otype(1), typ['bool']

        assert row['boolean'] == 1, row['boolean']
        assert typ['boolean'] == otype(1), typ['boolean']

        assert row['smallint'] == -27897, row['smallint']
        assert typ['smallint'] == otype(2), typ['smallint']

        assert row['mediumint'] == 104729, row['mediumint']
        assert typ['mediumint'] == otype(9), typ['mediumint']

        assert row['int24'] == -200899, row['int24']
        assert typ['int24'] == otype(9), typ['int24']

        assert row['int'] == -1295369311, row['int']
        assert typ['int'] == otype(3), typ['int']

        assert row['integer'] == -1741727421, row['integer']
        assert typ['integer'] == otype(3), typ['integer']

        assert row['bigint'] == -266883847, row['bigint']
        assert typ['bigint'] == otype(8), typ['bigint']

        assert row['float'] == -146487000.0, row['float']
        assert typ['float'] == otype(4), typ['float']

        assert row['double'] == -474646154.719356, row['double']
        assert typ['double'] == otype(5), typ['double']

        assert row['real'] == -901409776.279346, row['real']
        assert typ['real'] == otype(5), typ['real']

        assert row['decimal'] == decimal.Decimal('28111097.610822'), row['decimal']
        assert typ['decimal'] == otype(246), typ['decimal']

        assert row['dec'] == decimal.Decimal('389451155.931428'), row['dec']
        assert typ['dec'] == otype(246), typ['dec']

        assert row['fixed'] == decimal.Decimal('-143773416.044092'), row['fixed']
        assert typ['fixed'] == otype(246), typ['fixed']

        assert row['numeric'] == decimal.Decimal('866689461.300046'), row['numeric']
        assert typ['numeric'] == otype(246), typ['numeric']

        assert row['date'] == datetime.date(8524, 11, 10), row['date']
        assert typ['date'] == 10, typ['date']

        assert row['time'] == datetime.timedelta(minutes=7), row['time']
        assert typ['time'] == 11, typ['time']

        assert row['time_6'] == datetime.timedelta(
            hours=1, minutes=10, microseconds=2,
        ), row['time_6']
        assert typ['time_6'] == 11, typ['time_6']

        assert row['datetime'] == datetime.datetime(
            9948, 3, 11, 15, 29, 22,
        ), row['datetime']
        assert typ['datetime'] == 12, typ['datetime']

        assert row['datetime_6'] == datetime.datetime(
            1756, 10, 29, 2, 2, 42, 8,
        ), row['datetime_6']
        assert typ['datetime_6'] == 12, typ['datetime_6']

        assert row['timestamp'] == datetime.datetime(
            1980, 12, 31, 1, 10, 23,
        ), row['timestamp']
        assert typ['timestamp'] == otype(7), typ['timestamp']

        assert row['timestamp_6'] == datetime.datetime(
            1991, 1, 2, 22, 15, 10, 6,
        ), row['timestamp_6']
        assert typ['timestamp_6'] == otype(7), typ['timestamp_6']

        assert row['year'] == 1923, row['year']
        assert typ['year'] == otype(13), typ['year']

        assert row['char_100'] == \
            'This is a test of a 100 character column.', row['char_100']
        assert typ['char_100'] == otype(254), typ['char_100']

        assert row['binary_100'] == bytearray(bits + [0] * 84), row['binary_100']
        assert typ['binary_100'] == otype(254), typ['binary_100']

        assert row['varchar_200'] == \
            'This is a test of a variable character column.', row['varchar_200']
        assert typ['varchar_200'] == otype(253), typ['varchar_200']  # why not 15?

        assert row['varbinary_200'] == bytearray(bits * 2), row['varbinary_200']
        assert typ['varbinary_200'] == otype(253), typ['varbinary_200']  # why not 15?

        assert row['longtext'] == 'This is a longtext column.', row['longtext']
        assert typ['longtext'] == otype(251), typ['longtext']

        assert row['mediumtext'] == 'This is a mediumtext column.', row['mediumtext']
        assert typ['mediumtext'] == otype(250), typ['mediumtext']

        assert row['text'] == 'This is a text column.', row['text']
        assert typ['text'] == otype(252), typ['text']

        assert row['tinytext'] == 'This is a tinytext column.'
        assert typ['tinytext'] == otype(249), typ['tinytext']

        assert row['longblob'] == bytearray(bits * 3), row['longblob']
        assert typ['longblob'] == otype(251), typ['longblob']

        assert row['mediumblob'] == bytearray(bits * 2), row['mediumblob']
        assert typ['mediumblob'] == otype(250), typ['mediumblob']

        assert row['blob'] == bytearray(bits), row['blob']
        assert typ['blob'] == otype(252), typ['blob']

        assert row['tinyblob'] == bytearray([10, 11, 12, 13, 14, 15]), row['tinyblob']
        assert typ['tinyblob'] == otype(249), typ['tinyblob']

        assert row['json'] == {'a': 10, 'b': 2.75, 'c': 'hello world'}, row['json']
        assert typ['json'] == otype(245), typ['json']

        assert row['enum'] == 'one', row['enum']
        assert typ['enum'] == otype(253), typ['enum']  # mysql code: 247

        # TODO: HTTP sees this as a varchar, so it doesn't become a set.
        assert row['set'] in [{'two'}, 'two'], row['set']
        assert typ['set'] == otype(253), typ['set']  # mysql code: 248

        assert row['bit'] == b'\x00\x00\x00\x00\x00\x00\x00\x80', row['bit']
        assert typ['bit'] == otype(16), typ['bit']

    def test_alltypes_nulls(self):
        self.cur.execute('select * from alltypes where id = 1')
        names = [x[0] for x in self.cur.description]
        types = [x[1] for x in self.cur.description]
        out = self.cur.fetchone()
        row = dict(zip(names, out))
        typ = dict(zip(names, types))

        def otype(x):
            return x

        assert row['id'] == 1, row['id']
        assert typ['id'] == otype(3), typ['id']

        assert row['tinyint'] is None, row['tinyint']
        assert typ['tinyint'] == otype(1), typ['tinyint']

        assert row['bool'] is None, row['bool']
        assert typ['bool'] == otype(1), typ['bool']

        assert row['boolean'] is None, row['boolean']
        assert typ['boolean'] == otype(1), typ['boolean']

        assert row['smallint'] is None, row['smallint']
        assert typ['smallint'] == otype(2), typ['smallint']

        assert row['mediumint'] is None, row['mediumint']
        assert typ['mediumint'] == otype(9), typ['mediumint']

        assert row['int24'] is None, row['int24']
        assert typ['int24'] == otype(9), typ['int24']

        assert row['int'] is None, row['int']
        assert typ['int'] == otype(3), typ['int']

        assert row['integer'] is None, row['integer']
        assert typ['integer'] == otype(3), typ['integer']

        assert row['bigint'] is None, row['bigint']
        assert typ['bigint'] == otype(8), typ['bigint']

        assert row['float'] is None, row['float']
        assert typ['float'] == otype(4), typ['float']

        assert row['double'] is None, row['double']
        assert typ['double'] == otype(5), typ['double']

        assert row['real'] is None, row['real']
        assert typ['real'] == otype(5), typ['real']

        assert row['decimal'] is None, row['decimal']
        assert typ['decimal'] == otype(246), typ['decimal']

        assert row['dec'] is None, row['dec']
        assert typ['dec'] == otype(246), typ['dec']

        assert row['fixed'] is None, row['fixed']
        assert typ['fixed'] == otype(246), typ['fixed']

        assert row['numeric'] is None, row['numeric']
        assert typ['numeric'] == otype(246), typ['numeric']

        assert row['date'] is None, row['date']
        assert typ['date'] == 10, typ['date']

        assert row['time'] is None, row['time']
        assert typ['time'] == 11, typ['time']

        assert row['time'] is None, row['time']
        assert typ['time_6'] == 11, typ['time_6']

        assert row['datetime'] is None, row['datetime']
        assert typ['datetime'] == 12, typ['datetime']

        assert row['datetime_6'] is None, row['datetime_6']
        assert typ['datetime'] == 12, typ['datetime']

        assert row['timestamp'] is None, row['timestamp']
        assert typ['timestamp'] == otype(7), typ['timestamp']

        assert row['timestamp_6'] is None, row['timestamp_6']
        assert typ['timestamp_6'] == otype(7), typ['timestamp_6']

        assert row['year'] is None, row['year']
        assert typ['year'] == otype(13), typ['year']

        assert row['char_100'] is None, row['char_100']
        assert typ['char_100'] == otype(254), typ['char_100']

        assert row['binary_100'] is None, row['binary_100']
        assert typ['binary_100'] == otype(254), typ['binary_100']

        assert row['varchar_200'] is None, typ['varchar_200']
        assert typ['varchar_200'] == otype(253), typ['varchar_200']  # why not 15?

        assert row['varbinary_200'] is None, row['varbinary_200']
        assert typ['varbinary_200'] == otype(253), typ['varbinary_200']  # why not 15?

        assert row['longtext'] is None, row['longtext']
        assert typ['longtext'] == otype(251), typ['longtext']

        assert row['mediumtext'] is None, row['mediumtext']
        assert typ['mediumtext'] == otype(250), typ['mediumtext']

        assert row['text'] is None, row['text']
        assert typ['text'] == otype(252), typ['text']

        assert row['tinytext'] is None, row['tinytext']
        assert typ['tinytext'] == otype(249), typ['tinytext']

        assert row['longblob'] is None, row['longblob']
        assert typ['longblob'] == otype(251), typ['longblob']

        assert row['mediumblob'] is None, row['mediumblob']
        assert typ['mediumblob'] == otype(250), typ['mediumblob']

        assert row['blob'] is None, row['blob']
        assert typ['blob'] == otype(252), typ['blob']

        assert row['tinyblob'] is None, row['tinyblob']
        assert typ['tinyblob'] == otype(249), typ['tinyblob']

        assert row['json'] is None, row['json']
        assert typ['json'] == otype(245), typ['json']

        assert row['enum'] is None, row['enum']
        assert typ['enum'] == otype(253), typ['enum']  # mysql code: 247

        assert row['set'] is None, row['set']
        assert typ['set'] == otype(253), typ['set']  # mysql code: 248

        assert row['bit'] is None, row['bit']
        assert typ['bit'] == otype(16), typ['bit']

    def test_alltypes_mins(self):
        self.cur.execute('select * from alltypes where id = 2')
        names = [x[0] for x in self.cur.description]
        out = self.cur.fetchone()
        row = dict(zip(names, out))

        expected = dict(
            id=2,
            tinyint=-128,
            unsigned_tinyint=0,
            bool=-128,
            boolean=-128,
            smallint=-32768,
            unsigned_smallint=0,
            mediumint=-8388608,
            unsigned_mediumint=0,
            int24=-8388608,
            unsigned_int24=0,
            int=-2147483648,
            unsigned_int=0,
            integer=-2147483648,
            unsigned_integer=0,
            bigint=-9223372036854775808,
            unsigned_bigint=0,
            float=0,
            double=-1.7976931348623158e308,
            real=-1.7976931348623158e308,
            decimal=decimal.Decimal('-99999999999999.999999'),
            dec=-decimal.Decimal('99999999999999.999999'),
            fixed=decimal.Decimal('-99999999999999.999999'),
            numeric=decimal.Decimal('-99999999999999.999999'),
            date=datetime.date(1000, 1, 1),
            time=-1 * datetime.timedelta(hours=838, minutes=59, seconds=59),
            time_6=-1 * datetime.timedelta(hours=838, minutes=59, seconds=59),
            datetime=datetime.datetime(1000, 1, 1, 0, 0, 0),
            datetime_6=datetime.datetime(1000, 1, 1, 0, 0, 0, 0),
            timestamp=datetime.datetime(1970, 1, 1, 0, 0, 1),
            timestamp_6=datetime.datetime(1970, 1, 1, 0, 0, 1, 0),
            year=1901,
            char_100='',
            binary_100=b'\x00' * 100,
            varchar_200='',
            varbinary_200=b'',
            longtext='',
            mediumtext='',
            text='',
            tinytext='',
            longblob=b'',
            mediumblob=b'',
            blob=b'',
            tinyblob=b'',
            json={},
            enum='one',
            set='two',
            bit=b'\x00\x00\x00\x00\x00\x00\x00\x00',
        )

        for k, v in sorted(row.items()):
            assert v == expected[k], '{} != {} in key {}'.format(v, expected[k], k)

    def test_alltypes_maxs(self):
        self.cur.execute('select * from alltypes where id = 3')
        names = [x[0] for x in self.cur.description]
        out = self.cur.fetchone()
        row = dict(zip(names, out))

        expected = dict(
            id=3,
            tinyint=127,
            unsigned_tinyint=255,
            bool=127,
            boolean=127,
            smallint=32767,
            unsigned_smallint=65535,
            mediumint=8388607,
            unsigned_mediumint=16777215,
            int24=8388607,
            unsigned_int24=16777215,
            int=2147483647,
            unsigned_int=4294967295,
            integer=2147483647,
            unsigned_integer=4294967295,
            bigint=9223372036854775807,
            unsigned_bigint=18446744073709551615,
            float=0,
            double=1.7976931348623158e308,
            real=1.7976931348623158e308,
            decimal=decimal.Decimal('99999999999999.999999'),
            dec=decimal.Decimal('99999999999999.999999'),
            fixed=decimal.Decimal('99999999999999.999999'),
            numeric=decimal.Decimal('99999999999999.999999'),
            date=datetime.date(9999, 12, 31),
            time=datetime.timedelta(hours=838, minutes=59, seconds=59),
            time_6=datetime.timedelta(hours=838, minutes=59, seconds=59),
            datetime=datetime.datetime(9999, 12, 31, 23, 59, 59),
            datetime_6=datetime.datetime(9999, 12, 31, 23, 59, 59, 999999),
            timestamp=datetime.datetime(2038, 1, 19, 3, 14, 7),
            timestamp_6=datetime.datetime(2038, 1, 19, 3, 14, 7, 999999),
            year=2155,
            char_100='',
            binary_100=b'\x00' * 100,
            varchar_200='',
            varbinary_200=b'',
            longtext='',
            mediumtext='',
            text='',
            tinytext='',
            longblob=b'',
            mediumblob=b'',
            blob=b'',
            tinyblob=b'',
            json={},
            enum='one',
            set='two',
            bit=b'\xff\xff\xff\xff\xff\xff\xff\xff',
        )

        for k, v in sorted(row.items()):
            # TODO: Figure out how to get time zones working
            if 'timestamp' in k:
                continue
            assert v == expected[k], '{} != {} in key {}'.format(v, expected[k], k)

    def test_alltypes_zeros(self):
        self.cur.execute('select * from alltypes where id = 4')
        names = [x[0] for x in self.cur.description]
        out = self.cur.fetchone()
        row = dict(zip(names, out))

        expected = dict(
            id=4,
            tinyint=0,
            unsigned_tinyint=0,
            bool=0,
            boolean=0,
            smallint=0,
            unsigned_smallint=0,
            mediumint=0,
            unsigned_mediumint=0,
            int24=0,
            unsigned_int24=0,
            int=0,
            unsigned_int=0,
            integer=0,
            unsigned_integer=0,
            bigint=0,
            unsigned_bigint=0,
            float=0,
            double=0,
            real=0,
            decimal=decimal.Decimal('0.0'),
            dec=decimal.Decimal('0.0'),
            fixed=decimal.Decimal('0.0'),
            numeric=decimal.Decimal('0.0'),
            date=None,
            time=datetime.timedelta(hours=0, minutes=0, seconds=0),
            time_6=datetime.timedelta(hours=0, minutes=0, seconds=0, microseconds=0),
            datetime=None,
            datetime_6=None,
            timestamp=None,
            timestamp_6=None,
            year=None,
            char_100='',
            binary_100=b'\x00' * 100,
            varchar_200='',
            varbinary_200=b'',
            longtext='',
            mediumtext='',
            text='',
            tinytext='',
            longblob=b'',
            mediumblob=b'',
            blob=b'',
            tinyblob=b'',
            json={},
            enum='one',
            set='two',
            bit=b'\x00\x00\x00\x00\x00\x00\x00\x00',
        )

        for k, v in sorted(row.items()):
            assert v == expected[k], '{} != {} in key {}'.format(v, expected[k], k)

    def _test_MySQLdb(self):
        try:
            import json
            import MySQLdb
        except (ModuleNotFoundError, ImportError):
            self.skipTest('MySQLdb is not installed')

        self.cur.execute('select * from alltypes order by id')
        s2_out = self.cur.fetchall()

        port = self.conn.connection_params['port']
        if 'http' in self.conn.driver:
            port = 3306

        args = dict(
            host=self.conn.connection_params['host'],
            port=port,
            user=self.conn.connection_params['user'],
            password=self.conn.connection_params['password'],
            database=type(self).dbname,
        )

        with MySQLdb.connect(**args) as conn:
            conn.converter[245] = json.loads
            with conn.cursor() as cur:
                cur.execute('select * from alltypes order by id')
                mydb_out = cur.fetchall()

        for a, b in zip(s2_out, mydb_out):
            assert a == b, (a, b)

    def test_int_string(self):
        string = 'a' * 48
        self.cur.execute(f"SELECT 1, '{string}'")
        self.assertEqual((1, string), self.cur.fetchone())

    def test_double_string(self):
        string = 'a' * 49
        self.cur.execute(f"SELECT 1.2 :> DOUBLE, '{string}'")
        self.assertEqual((1.2, string), self.cur.fetchone())

    def test_year_string(self):
        string = 'a' * 49
        self.cur.execute(f"SELECT 1999 :> YEAR, '{string}'")
        self.assertEqual((1999, string), self.cur.fetchone())

    def test_nan_as_null(self):
        with self.assertRaises((s2.ProgrammingError, InvalidJSONError)):
            self.cur.execute('SELECT %s :> DOUBLE AS X', [math.nan])

        with s2.connect(database=type(self).dbname, nan_as_null=True) as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT %s :> DOUBLE AS X', [math.nan])
                self.assertEqual(None, list(cur)[0][0])

        with s2.connect(database=type(self).dbname, nan_as_null=True) as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT %s :> DOUBLE AS X', [1.234])
                self.assertEqual(1.234, list(cur)[0][0])

    def test_inf_as_null(self):
        with self.assertRaises((s2.ProgrammingError, InvalidJSONError)):
            self.cur.execute('SELECT %s :> DOUBLE AS X', [math.inf])

        with s2.connect(database=type(self).dbname, inf_as_null=True) as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT %s :> DOUBLE AS X', [math.inf])
                self.assertEqual(None, list(cur)[0][0])

        with s2.connect(database=type(self).dbname, inf_as_null=True) as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT %s :> DOUBLE AS X', [1.234])
                self.assertEqual(1.234, list(cur)[0][0])

    def test_encoding_errors(self):
        with s2.connect(
            database=type(self).dbname,
            encoding_errors='strict',
        ) as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT * FROM badutf8')
                list(cur)

        with s2.connect(
            database=type(self).dbname,
            encoding_errors='backslashreplace',
        ) as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT * FROM badutf8')
                list(cur)


if __name__ == '__main__':
    import nose2
    nose2.main()
