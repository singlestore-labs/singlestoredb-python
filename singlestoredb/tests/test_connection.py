#!/usr/bin/env python
# type: ignore
"""Basic SingleStoreDB connection testing."""
from __future__ import annotations

import datetime
import decimal
import os
import unittest
import uuid
import warnings

import pandas as pd

import singlestoredb as s2
from singlestoredb import connection as sc
from singlestoredb.tests import utils
# import traceback


class TestConnection(unittest.TestCase):

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
        self.conn = s2.connect(database=type(self).dbname, local_infile=True)
        self.cur = self.conn.cursor()
        self.driver = self.conn._driver.dbapi.__name__.replace(
            'singlestoredb.', '',
        )

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

    def test_cast_bool_param(self):
        cbp = sc.cast_bool_param

        assert cbp(0) is False, cbp(0)
        assert cbp(1) is True, cbp(1)
        with self.assertRaises(ValueError):
            cbp(10)

        assert cbp(True) is True, cbp(True)
        assert cbp(False) is False, cbp(False)
        assert cbp(None) is False, cbp(None)

        assert cbp('true') is True, cbp('true')
        assert cbp('t') is True, cbp('t')
        assert cbp('True') is True, cbp('True')
        assert cbp('T') is True, cbp('T')
        assert cbp('TRUE') is True, cbp('TRUE')

        assert cbp('on') is True, cbp('on')
        assert cbp('yes') is True, cbp('yes')
        assert cbp('enable') is True, cbp('enable')
        assert cbp('enabled') is True, cbp('enabled')

        assert cbp('false') is True, cbp('false')
        assert cbp('f') is True, cbp('f')
        assert cbp('False') is True, cbp('False')
        assert cbp('F') is True, cbp('F')
        assert cbp('FALSE') is True, cbp('FALSE')

        assert cbp('off') is True, cbp('off')
        assert cbp('no') is True, cbp('no')
        assert cbp('disable') is True, cbp('disable')
        assert cbp('disabled') is True, cbp('disabled')

        with self.assertRaises(ValueError):
            cbp('nein')

        with self.assertRaises(ValueError):
            cbp(b'no')

        with self.assertRaises(ValueError):
            cbp(['no'])

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

        assert rowcount == 5, rowcount
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

        assert rowcount == 5, rowcount
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

        assert rowcount == 5, rowcount
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
        self.cur.execute('select * from data where id < :name', dict(name='d'))
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
        assert desc[0].name == 'id', desc[0].name
        assert desc[0].type_code in [253, 15], desc[0].type_code
        assert desc[1].name == 'name', desc[1].name
        assert desc[1].type_code in [253, 15], desc[1].type_code
        assert desc[2].name == 'value', desc[2].name
        assert desc[2].type_code == 8, desc[2].type_code

    def test_execute_with_positional_params(self):
        self.cur.execute('select * from data where id < :1', ['d'])
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
        assert desc[0].name == 'id', desc[0].name
        assert desc[0].type_code in [253, 15], desc[0].type_code
        assert desc[1].name == 'name', desc[1].name
        assert desc[1].type_code in [253, 15], desc[1].type_code
        assert desc[2].name == 'value', desc[2].name
        assert desc[2].type_code == 8, desc[2].type_code

    def test_execute_with_escaped_positional_substitutions(self):
        self.cur.execute(
            'select `id`, `time` from alltypes where `time` = :1', [
                '00:07:00',
            ],
        )
        out = self.cur.fetchall()
        assert out[0] == (0, datetime.timedelta(seconds=420)), out[0]

        self.cur.execute(
            'select `id`, `time` from alltypes where `time` = "00:07:00"',
        )
        out = self.cur.fetchall()
        assert out[0] == (0, datetime.timedelta(seconds=420)), out[0]

        with self.assertRaises(IndexError):
            self.cur.execute(
                'select `id`, `time` from alltypes where `id` = :1 '
                'or `time` = "00:07:00"', [0],
            )

        self.cur.execute(
            'select `id`, `time` from alltypes where `id` = :1 '
            'or `time` = "00::07::00"', [0],
        )
        out = self.cur.fetchall()
        assert out[0] == (0, datetime.timedelta(seconds=420)), out[0]

    def test_execute_with_escaped_substitutions(self):
        self.cur.execute(
            'select `id`, `time` from alltypes where `time` = :time',
            dict(time='00:07:00'),
        )
        out = self.cur.fetchall()
        assert out[0] == (0, datetime.timedelta(seconds=420)), out[0]

        self.cur.execute(
            'select `id`, `time` from alltypes where `time` = :time',
            dict(time='00::07::00'),
        )
        out = self.cur.fetchall()
        assert len(out) == 0, out

        with self.assertRaises(KeyError):
            self.cur.execute(
                'select `id`, `time`, `char_100` from alltypes '
                'where `time` = :time or `char_100` like "foo:bar"',
                dict(time='00:07:00'),
            )

        self.cur.execute(
            'select `id`, `time`, `char_100` from alltypes '
            'where `time` = :time or `char_100` like "foo::bar"',
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
            'delete from data where id > :name',
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
            assert out['port'] in [
                get_option(
                    'http_port',
                ), 80, 443,
            ], out['port']
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
            assert out['port'] in [
                get_option(
                    'http_port',
                ), 80, 443,
            ], out['port']
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
            assert out['port'] in [
                get_option(
                    'http_port',
                ), 80, 443,
            ], out['port']
        else:
            assert out['port'] in [get_option('port'), 3306], out['port']
        assert out['database'] == 'mydb', out['database']
        assert 'user' not in out or out['user'] == get_option(
            'user',
        ), out['user']
        assert 'password' not in out or out['password'] == get_option(
            'password',
        ), out['password']

        # Just hostname
        url = 's2host.com'
        out = build_params(host=url)
        assert out['driver'] == get_option('driver'), out['driver']
        assert out['host'] == 's2host.com', out['host']
        if out['driver'] in ['http', 'https']:
            assert out['port'] in [
                get_option(
                    'http_port',
                ), 80, 443,
            ], out['port']
        else:
            assert out['port'] in [get_option('port'), 3306], out['port']
        assert 'database' not in out
        assert 'user' not in out or out['user'] == get_option(
            'user',
        ), out['user']
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
        assert 'user' not in out or out['user'] == get_option(
            'user',
        ), out['user']
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
        assert 'user' not in out or out['user'] == get_option(
            'user',
        ), out['user']
        assert 'password' not in out or out['password'] == get_option(
            'password',
        ), out['password']
        assert out['local_infile'] is True, out['local_infile']
        assert out['charset'] == 'utf8', out['charset']

    def test_exception(self):
        with self.assertRaises(s2.ProgrammingError) as cm:
            self.cur.execute('garbage syntax')
        exc = cm.exception
        if self.driver != 'pyodbc':
            assert exc.errno == 1064, exc.errno
        assert 'You have an error in your SQL syntax' in exc.errmsg, exc.errmsg

    def test_alltypes(self):
        self.cur.execute('select * from alltypes where id = 0')
        names = [x[0] for x in self.cur.description]
        types = [x[1] for x in self.cur.description]
        out = self.cur.fetchone()
        row = dict(zip(names, out))
        typ = dict(zip(names, types))

        bits = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]

        if self.driver == 'pyodbc':
            odbc_types = {
                # int -> bigint
                3: 8, 1: 8, 2: 8, 9: 8,
                # float -> double
                4: 5,
                # timestamp -> datetime
                7: 12,
                # year -> bigint
                13: 8,
                # char/binary -> varchar/varbinary
                249: 15, 250: 15, 251: 15, 252: 15, 253: 15, 254: 15, 255: 15,
                # newdecimal -> decimal
                246: 0,
                # json -> varchar
                245: 15,
                # bit -> varchar
                16: 15,
            }
        else:
            odbc_types = {}

        def otype(x):
            return odbc_types.get(x, x)

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

        assert row['decimal'] == decimal.Decimal(
            '28111097.610822',
        ), row['decimal']
        assert typ['decimal'] == otype(246), typ['decimal']

        assert row['dec'] == decimal.Decimal('389451155.931428'), row['dec']
        assert typ['dec'] == otype(246), typ['dec']

        assert row['fixed'] == decimal.Decimal(
            '-143773416.044092',
        ), row['fixed']
        assert typ['fixed'] == otype(246), typ['fixed']

        assert row['numeric'] == decimal.Decimal(
            '866689461.300046',
        ), row['numeric']
        assert typ['numeric'] == otype(246), typ['numeric']

        assert row['date'] == datetime.date(8524, 11, 10), row['date']
        assert typ['date'] == 10, typ['date']

        assert row['time'] == datetime.timedelta(minutes=7), row['time']
        assert typ['time'] == 11, typ['time']

        # pyodbc doesn't support microseconds on times
        if not self.driver == 'pyodbc':
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
        assert typ['datetime'] == 12, typ['datetime']

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

        assert row['binary_100'] == bytearray(
            bits + [0] * 84,
        ), row['binary_100']
        assert typ['binary_100'] == otype(254), typ['binary_100']

        assert row['varchar_200'] == \
            'This is a test of a variable character column.', row['varchar_200']
        assert typ['varchar_200'] == otype(
            253,
        ), typ['varchar_200']  # why not 15?

        assert row['varbinary_200'] == bytearray(
            bits * 2,
        ), row['varbinary_200']
        assert typ['varbinary_200'] == otype(
            253,
        ), typ['varbinary_200']  # why not 15?

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

        assert row['tinyblob'] == bytearray(
            [10, 11, 12, 13, 14, 15],
        ), row['tinyblob']
        assert typ['tinyblob'] == otype(249), typ['tinyblob']

        # pyodbc surfaces json as varchar
        if self.driver == 'pyodbc':
            assert row['json'] == '{"a":10,"b":2.75,"c":"hello world"}', row['json']
        else:
            assert row['json'] == {
                'a': 10, 'b': 2.75,
                'c': 'hello world',
            }, row['json']
        assert typ['json'] == otype(245), typ['json']

        assert row['enum'] == 'one', row['enum']
        assert typ['enum'] == otype(253), typ['enum']  # mysql code: 247

        assert row['set'] == 'two', row['set']
        assert typ['set'] == otype(253), typ['set']  # mysql code: 248

        if self.driver == 'pyodbc':
            assert row['bit'] == b'\x80\x00\x00\x00\x00\x00\x00\x00', row['bit']
        else:
            assert row['bit'] == b'\x00\x00\x00\x00\x00\x00\x00\x80', row['bit']
        assert typ['bit'] == otype(16), typ['bit']

    def test_alltypes_nulls(self):
        self.cur.execute('select * from alltypes where id = 1')
        names = [x[0] for x in self.cur.description]
        types = [x[1] for x in self.cur.description]
        out = self.cur.fetchone()
        row = dict(zip(names, out))
        typ = dict(zip(names, types))

        if self.driver == 'pyodbc':
            odbc_types = {
                # int -> bigint
                3: 8, 1: 8, 2: 8, 9: 8,
                # float -> double
                4: 5,
                # timestamp -> datetime
                7: 12,
                # year -> bigint
                13: 8,
                # char/binary -> varchar/varbinary
                249: 15, 250: 15, 251: 15, 252: 15, 253: 15, 254: 15, 255: 15,
                # newdecimal -> decimal
                246: 0,
                # json -> varchar
                245: 15,
                # bit -> varchar
                16: 15,
            }
        else:
            odbc_types = {}

        def otype(x):
            return odbc_types.get(x, x)

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
        assert typ['varchar_200'] == otype(
            253,
        ), typ['varchar_200']  # why not 15?

        assert row['varbinary_200'] is None, row['varbinary_200']
        assert typ['varbinary_200'] == otype(
            253,
        ), typ['varbinary_200']  # why not 15?

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

    def test_convert_exception(self):
        driver = self.conn._driver
        dbapi = driver.dbapi

        if self.driver in ['mysql.connector', 'http', 'https']:
            exc_args = tuple()
            exc_kwargs = dict(errno=-1, msg='hi there')
        else:
            exc_args = (-1, 'hi there')
            exc_kwargs = {}

        exc = driver.convert_exception(
            dbapi.NotSupportedError(*exc_args, **exc_kwargs),
        )
        assert exc.args[0] == -1
        assert exc.args[1] == 'hi there'
        assert exc.errno == -1
        assert exc.errmsg == 'hi there'
        assert exc.msg == 'hi there'

        with self.assertRaises(s2.NotSupportedError):
            raise driver.convert_exception(
                dbapi.NotSupportedError(*exc_args, **exc_kwargs),
            )

        with self.assertRaises(s2.ProgrammingError):
            raise driver.convert_exception(
                dbapi.ProgrammingError(*exc_args, **exc_kwargs),
            )

        with self.assertRaises(s2.InternalError):
            raise driver.convert_exception(
                dbapi.InternalError(*exc_args, **exc_kwargs),
            )

        with self.assertRaises(s2.IntegrityError):
            raise driver.convert_exception(
                dbapi.IntegrityError(*exc_args, **exc_kwargs),
            )

        with self.assertRaises(s2.OperationalError):
            raise driver.convert_exception(
                dbapi.OperationalError(*exc_args, **exc_kwargs),
            )

        with self.assertRaises(s2.DataError):
            raise driver.convert_exception(
                dbapi.DataError(*exc_args, **exc_kwargs),
            )

        with self.assertRaises(s2.DatabaseError):
            raise driver.convert_exception(
                dbapi.DatabaseError(*exc_args, **exc_kwargs),
            )

        with self.assertRaises(s2.InterfaceError):
            raise driver.convert_exception(
                dbapi.InterfaceError(*exc_args, **exc_kwargs),
            )

        with self.assertRaises(s2.Error):
            raise driver.convert_exception(
                dbapi.Error(*exc_args, **exc_kwargs),
            )

        if self.driver == 'mariadb':
            with self.assertRaises(s2.Error):
                raise driver.convert_exception(dbapi.Warning('hi there'))
        else:
            with self.assertRaises(s2.Warning):
                raise driver.convert_exception(dbapi.Warning('hi there'))

    def test_name_check(self):
        nc = sc._name_check
        assert nc('foo') == 'foo'
        assert nc('Foo') == 'Foo'
        assert nc('Foo_Bar') == 'Foo_Bar'
        assert nc('Foo_Bar2') == 'Foo_Bar2'

        with self.assertRaises(ValueError):
            assert nc('foo.bar')

        with self.assertRaises(ValueError):
            assert nc('2foo')

        with self.assertRaises(ValueError):
            assert nc('')

    def test_echo(self):
        self.cur.execute('echo return_int()')

        out = self.cur.fetchall()
        assert out == [(1234567890,)], out

        # These take an extra `nextset` for some reason
        if self.driver in ['pymysql', 'MySQLdb', 'cymysql', 'pyodbc']:
            self.cur.nextset()

        out = self.cur.nextset()
        assert out is False, out

    def test_echo_with_result_set(self):
        self.cur.execute('echo result_set_and_return_int()')

        out = self.cur.fetchall()
        assert out == [(5,)], out

        if self.driver == 'mysql.connector' and s2.get_option('pure_python'):
            warnings.warn(
                'The mysql.connector in pure python mode does not '
                'support multiple result sets.',
            )
            return

        out = self.cur.nextset()
        assert out is True, out

        out = self.cur.fetchall()
        assert out == [(1, 2, 3)], out

        out = self.cur.nextset()
        assert out is True, out

        out = self.cur.fetchall()
        assert out == [(1234567890,)], out

        # These take an extra `nextset` for some reason
        if self.driver in ['pymysql', 'MySQLdb', 'cymysql', 'pyodbc']:
            self.cur.nextset()

        out = self.cur.nextset()
        assert out is False, out

    def test_callproc(self):
        self.cur.callproc('get_animal', ['cats'])

        out = self.cur.fetchall()
        assert list(out) == [(5,)], out

        if self.driver == 'mysql.connector' and s2.get_option('pure_python'):
            warnings.warn(
                'The mysql.connector in pure python mode does not '
                'support multiple result sets.',
            )
            return

        out = self.cur.nextset()
        assert out is True, out

        out = self.cur.fetchall()
        assert list(out) == [(1, 2, 3)], out

        # These take an extra `nextset` for some reason
        if self.driver in ['pymysql', 'MySQLdb', 'cymysql', 'pyodbc']:
            self.cur.nextset()

        out = self.cur.nextset()
        assert out is False, out

    def test_callproc_no_args(self):
        self.cur.callproc('no_args')

        out = self.cur.fetchall()
        assert list(out) == [(4, 5, 6)], out

        # These take an extra `nextset` for some reason
        if self.driver in ['pymysql', 'MySQLdb', 'cymysql', 'pyodbc']:
            self.cur.nextset()

        out = self.cur.nextset()
        assert out is False, out

    def test_callproc_return_int(self):
        self.cur.callproc('result_set_and_return_int')

        out = self.cur.fetchall()
        assert out == [(5,)], out

        if self.driver == 'mysql.connector' and s2.get_option('pure_python'):
            warnings.warn(
                'The mysql.connector in pure python mode does not '
                'support multiple result sets.',
            )
            return

        out = self.cur.nextset()
        assert out is True, out

        out = self.cur.fetchall()
        assert out == [(1, 2, 3)], out

        # These take an extra `nextset` for some reason
        if self.driver in ['pymysql', 'MySQLdb', 'cymysql', 'pyodbc']:
            self.cur.nextset()

        out = self.cur.nextset()
        assert out is False, out

    def test_callproc_bad_args(self):
        self.cur.callproc('get_animal', [10])

        out = self.cur.fetchall()
        assert list(out) == [], out

        if self.driver == 'mysql.connector' and s2.get_option('pure_python'):
            warnings.warn(
                'The mysql.connector in pure python mode does not '
                'support multiple result sets.',
            )
            return

        out = self.cur.nextset()
        assert out is True, out

        out = self.cur.fetchall()
        assert list(out) == [(1, 2, 3)], out

        # These take an extra `nextset` for some reason
        if self.driver in ['pymysql', 'MySQLdb', 'cymysql', 'pyodbc']:
            self.cur.nextset()

        out = self.cur.nextset()
        assert out is False, out

    def test_callproc_too_many_args(self):
        with self.assertRaises((
            s2.ProgrammingError,
            s2.OperationalError,
            s2.InternalError,
        )):
            self.cur.callproc('get_animal', ['cats', 'dogs'])

        with self.assertRaises((
            s2.ProgrammingError,
            s2.OperationalError,
            s2.InternalError,
        )):
            self.cur.callproc('get_animal', [])

        with self.assertRaises((
            s2.ProgrammingError,
            s2.OperationalError,
            s2.InternalError,
        )):
            self.cur.callproc('get_animal')

    def test_cursor_close(self):
        self.cur.close()

        with self.assertRaises(s2.InterfaceError):
            self.cur.close()

        with self.assertRaises(s2.InterfaceError):
            self.cur.callproc('foo')

        with self.assertRaises(s2.InterfaceError):
            self.cur.execute('select 1')

        with self.assertRaises(s2.InterfaceError):
            self.cur.executemany('select 1')

        with self.assertRaises(s2.InterfaceError):
            self.cur.fetchone()

        with self.assertRaises(s2.InterfaceError):
            self.cur.fetchall()

        with self.assertRaises(s2.InterfaceError):
            self.cur.fetchmany()

        with self.assertRaises(s2.InterfaceError):
            self.cur.nextset()

        with self.assertRaises(s2.InterfaceError):
            self.cur.setinputsizes([])

        with self.assertRaises(s2.InterfaceError):
            self.cur.setoutputsize(10)

        with self.assertRaises(s2.InterfaceError):
            self.cur.scroll(2)

        with self.assertRaises(s2.InterfaceError):
            self.cur.next()

        # The following attributes are still accessible after close.

        assert isinstance(self.cur.messages, list), self.cur.messages

        assert isinstance(self.cur.rowcount, (int, type(None))), self.cur.rowcount

        assert isinstance(self.cur.lastrowid, (int, type(None))), self.cur.lastrowid

    def test_setinputsizes(self):
        self.cur.setinputsizes([10, 20, 30])

    def test_setoutputsize(self):
        if self.driver in ['MySQLdb', 'pymysql', 'cymysql']:
            self.skipTest('outputsize is not supported')

        self.cur.setoutputsize(100)

    def test_scroll(self):
        if self.driver in ['mysql.connector', 'pyodbc', 'mariadb', 'cymysql']:
            self.skipTest('scroll is not supported')

        self.cur.execute('select * from data order by name')

        out = self.cur.fetchone()
        assert out[1] == 'antelopes', out[1]
        assert self.cur.rownumber == 1, self.cur.rownumber

        self.cur.scroll(3)

        out = self.cur.fetchone()
        assert out[1] == 'elephants', out[1]
        assert self.cur.rownumber == 5, self.cur.rownumber

        self.cur.scroll(0, mode='absolute')

        assert self.cur.rownumber == 0, self.cur.rownumber

        out = self.cur.fetchone()
        assert out[1] == 'antelopes', out[1]
        assert self.cur.rownumber == 1, self.cur.rownumber

        with self.assertRaises((ValueError, s2.ProgrammingError)):
            self.cur.scroll(0, mode='badmode')

    def test_autocommit(self):
        if self.driver in ['http', 'https']:
            self.skipTest('Can not set autocommit in HTTP')

        orig = self.conn.locals.autocommit

        self.conn.autocommit(True)
        val = self.conn.locals.autocommit
        assert val is True, val

        self.conn.autocommit(False)
        val = self.conn.locals.autocommit
        assert val is False, val

        self.conn.locals.autocommit = orig

    def test_conn_close(self):
        self.conn.close()

        self.conn.close()

        with self.assertRaises(s2.InterfaceError):
            self.conn.autocommit()

        with self.assertRaises(s2.InterfaceError):
            self.conn.commit()

        with self.assertRaises(s2.InterfaceError):
            self.conn.rollback()

        with self.assertRaises(s2.InterfaceError):
            self.conn.cursor()

        with self.assertRaises(s2.InterfaceError):
            self.conn.messages

        with self.assertRaises(s2.InterfaceError):
            self.conn.globals.autocommit = True

        with self.assertRaises(s2.InterfaceError):
            self.conn.globals.autocommit

        with self.assertRaises(s2.InterfaceError):
            self.conn.locals.autocommit = True

        with self.assertRaises(s2.InterfaceError):
            self.conn.locals.autocommit

        with self.assertRaises(s2.InterfaceError):
            self.conn.enable_data_api()

        with self.assertRaises(s2.InterfaceError):
            self.conn.disable_data_api()

    def test_rollback(self):
        if self.driver in ['http', 'https']:
            self.skipTest('Can not set autocommit in HTTP')

        self.conn.autocommit(False)

        self.cur.execute('select * from data')
        out = self.cur.fetchall()
        assert len(out) == 5, len(out)

        self.cur.execute("INSERT INTO data SET id='f', name='frogs', value=3")

        self.cur.execute('select * from data')
        out = self.cur.fetchall()
        assert len(out) == 6, len(out)

        self.conn.rollback()

        self.cur.execute('select * from data')
        out = self.cur.fetchall()
        assert len(out) == 5, len(out)

    def test_commit(self):
        if self.driver in ['http', 'https']:
            self.skipTest('Can not set autocommit in HTTP')

        self.conn.autocommit(False)

        self.cur.execute('select * from data')
        out = self.cur.fetchall()
        assert len(out) == 5, len(out)

        self.cur.execute("INSERT INTO data SET id='f', name='frogs', value=3")

        self.cur.execute('select * from data')
        out = self.cur.fetchall()
        assert len(out) == 6, len(out)

        self.conn.commit()

        self.cur.execute('select * from data')
        out = self.cur.fetchall()
        assert len(out) == 6, len(out)

        self.cur.execute("delete from data where id='f'")

        self.cur.execute('select * from data')
        out = self.cur.fetchall()
        assert len(out) == 5, len(out)

        self.conn.commit()

        self.cur.execute('select * from data')
        out = self.cur.fetchall()
        assert len(out) == 5, len(out)

    def test_global_var(self):
        orig = self.conn.globals.enable_external_functions

        self.conn.globals.enable_external_functions = True
        val = self.conn.globals.enable_external_functions
        assert val is True, val

        self.conn.globals.enable_external_functions = False
        val = self.conn.globals.enable_external_functions
        assert val is False, val

        self.conn.globals.enable_external_functions = orig
        val = self.conn.globals.enable_external_functions
        assert val == orig, val

    def test_session_var(self):
        if self.driver in ['http', 'https']:
            self.skipTest('Can not change session variable in HTTP')

        orig = self.conn.locals.enable_multipartition_queries

        self.conn.locals.enable_multipartition_queries = True
        val = self.conn.locals.enable_multipartition_queries
        assert val is True, val

        self.conn.locals.enable_multipartition_queries = False
        val = self.conn.locals.enable_multipartition_queries
        assert val is False, val

        self.conn.locals.enable_multipartition_queries = orig
        val = self.conn.locals.enable_multipartition_queries
        assert val == orig, val

    def test_local_infile(self):
        if self.driver in ['http', 'https']:
            self.skipTest('Can not load local files in HTTP')

        path = os.path.join(os.path.dirname(__file__), 'local_infile.csv')
        tblname = ('TEST_' + str(uuid.uuid4())).replace('-', '_')

        self.cur.execute(f'''
            create table `{tblname}` (
                first_name char(20) not null,
                last_name char(30) not null,
                age  int not null
            ) collate="utf8_unicode_ci";
        ''')

        try:
            self.cur.execute(
                f'load data local infile :1 into table {tblname} '
                'fields terminated by "," lines terminated by "\n";', [path],
            )

            self.cur.execute(f'select * from {tblname};')
            out = list(self.cur)
            assert out == [
                ('John', 'Doe', 34),
                ('Sandy', 'Smith', 24),
                ('Patty', 'Jones', 57),
            ], out

        finally:
            self.cur.execute(f'drop table {tblname};')

    def test_converters(self):
        def upper(x):
            if isinstance(x, str):
                return x.upper()
            return x

        convs = {
            15: upper,
            249: upper,
            250: upper,
            251: upper,
            252: upper,
            253: upper,
            254: upper,
        }

        with s2.connect(database=type(self).dbname, converters=convs) as conn:
            with conn.cursor() as cur:
                cur.execute('select * from alltypes where id = 0')
                names = [x[0] for x in cur.description]
                out = cur.fetchone()
                row = dict(zip(names, out))
                assert row['longtext'] == 'THIS IS A LONGTEXT COLUMN.', \
                    row['longtext']
                assert row['mediumtext'] == 'THIS IS A MEDIUMTEXT COLUMN.', \
                    row['mediumtext']
                assert row['text'] == 'THIS IS A TEXT COLUMN.', \
                    row['text']
                assert row['tinytext'] == 'THIS IS A TINYTEXT COLUMN.', \
                    row['tinytext']

        with s2.connect(database=type(self).dbname) as conn:
            with conn.cursor() as cur:
                cur.execute('select * from alltypes where id = 0')
                names = [x[0] for x in cur.description]
                out = cur.fetchone()
                row = dict(zip(names, out))
                assert row['longtext'] == 'This is a longtext column.', \
                    row['longtext']
                assert row['mediumtext'] == 'This is a mediumtext column.', \
                    row['mediumtext']
                assert row['text'] == 'This is a text column.', \
                    row['text']
                assert row['tinytext'] == 'This is a tinytext column.', \
                    row['tinytext']

    def test_results_format(self):
        columns = [
            'id', 'tinyint', 'bool', 'boolean', 'smallint', 'mediumint',
            'int24', 'int', 'integer', 'bigint', 'float', 'double', 'real',
            'decimal', 'dec', 'fixed', 'numeric', 'date', 'time', 'time_6',
            'datetime', 'datetime_6', 'timestamp', 'timestamp_6', 'year',
            'char_100', 'binary_100', 'varchar_200', 'varbinary_200',
            'longtext', 'mediumtext', 'text', 'tinytext', 'longblob',
            'mediumblob', 'blob', 'tinyblob', 'json', 'enum', 'set', 'bit',
        ]

        with s2.connect(database=type(self).dbname, results_format='tuple') as conn:
            with conn.cursor() as cur:
                cur.execute('select * from alltypes')
                out = cur.fetchall()
                assert type(out[0]) is tuple, type(out[0])
                assert len(out[0]) == len(columns), len(out[0])

        with s2.connect(database=type(self).dbname, results_format='namedtuple') as conn:
            with conn.cursor() as cur:
                cur.execute('select * from alltypes')
                out = cur.fetchall()
                assert type(out[0]).__name__ == 'Row', type(out)
                assert list(out[0]._fields) == columns, out[0]._fields

        with s2.connect(database=type(self).dbname, results_format='dict') as conn:
            with conn.cursor() as cur:
                cur.execute('select * from alltypes')
                out = cur.fetchall()
                assert type(out[0]) is dict, type(out)
                assert list(out[0].keys()) == columns, out[0].keys()

        with s2.connect(database=type(self).dbname, results_format='dataframe') as conn:
            with conn.cursor() as cur:
                cur.execute('select * from alltypes')
                out = cur.fetchall()
                assert type(out) is pd.DataFrame, type(out)
                assert list(out.columns) == columns, out.columns


if __name__ == '__main__':
    import nose2
    nose2.main()
