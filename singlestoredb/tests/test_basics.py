#!/usr/bin/env python
# type: ignore
"""Basic SingleStoreDB connection testing."""
from __future__ import annotations

import datetime
import decimal
import os
import unittest

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
            'select `id`, `time` from alltypes where `time` = :1', ['00:07:00'],
        )
        out = self.cur.fetchall()
        if self.driver == 'pyodbc':
            assert out[0] == (0, datetime.time(0, 7)), out[0]
        else:
            assert out[0] == (0, datetime.timedelta(seconds=420)), out[0]

        self.cur.execute('select `id`, `time` from alltypes where `time` = "00:07:00"')
        out = self.cur.fetchall()
        if self.driver == 'pyodbc':
            assert out[0] == (0, datetime.time(0, 7)), out[0]
        else:
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
        if self.driver == 'pyodbc':
            assert out[0] == (0, datetime.time(0, 7)), out[0]
        else:
            assert out[0] == (0, datetime.timedelta(seconds=420)), out[0]

    def test_execute_with_escaped_substitutions(self):
        self.cur.execute(
            'select `id`, `time` from alltypes where `time` = :time',
            dict(time='00:07:00'),
        )
        out = self.cur.fetchall()
        if self.driver == 'pyodbc':
            assert out[0] == (0, datetime.time(0, 7)), out[0]
        else:
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
        if self.driver == 'pyodbc':
            assert out[0][:2] == (0, datetime.time(0, 7)), out[0]
        else:
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

        # pyodbc uses time rather than timedelta. In addition, if you try to
        # put your own converter in, it changes the type code to 15!!!
        if self.driver == 'pyodbc':
            assert row['time'] == datetime.time(0, 7, 0), row['time']
        else:
            assert row['time'] == datetime.timedelta(minutes=7), row['time']
        assert typ['time'] == 11, typ['time']

        # Same as above
        if self.driver == 'pyodbc':
            assert row['time'] == datetime.time(0, 7, 0), row['time']
        else:
            assert row['time_6'] == datetime.timedelta(
                hours=1, minutes=10, microseconds=2,
            ), row['time']
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

        # pyodbc surfaces json as varchar
        if self.driver == 'pyodbc':
            assert row['json'] == '{"a":10,"b":2.75,"c":"hello world"}', row['json']
        else:
            assert row['json'] == {'a': 10, 'b': 2.75, 'c': 'hello world'}, row['json']
        assert typ['json'] == otype(245), typ['json']

        assert row['enum'] == 'one', row['enum']
        assert typ['enum'] == otype(253), typ['enum']  # mysql code: 247

        # TODO: HTTP sees this as a varchar, so it doesn't become a set.
        assert row['set'] in [{'two'}, 'two'], row['set']
        assert typ['set'] == otype(253), typ['set']  # mysql code: 248

        # pyodbc uses the opposite endianness of all other drivers
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


if __name__ == '__main__':
    import nose2
    nose2.main()
