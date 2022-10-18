#!/usr/bin/env python
# type: ignore
"""Test SingleStoreDB data types."""
import datetime
import unittest

import singlestoredb.types as st


class TestTypes(unittest.TestCase):

    def test_date_from_ticks(self):
        dt = st.DateFromTicks(9999)
        ts = datetime.date.fromtimestamp(9999)
        assert dt == ts

    def test_time_from_ticks(self):
        tm = st.TimeFromTicks(9999)
        ts = datetime.datetime.fromtimestamp(9999)
        ts = datetime.time(hour=ts.hour, minute=ts.minute, second=ts.second)
        assert tm == ts

    def test_timestamp_from_ticks(self):
        tm = st.TimestampFromTicks(9999)
        ts = datetime.datetime.fromtimestamp(9999)
        assert tm == ts

    def test_dbapitype_eq(self):
        ct = st.ColumnType

        assert st.STRING == ct.CHAR
        assert st.STRING == 'CHAR'
        assert st.STRING == 254
        assert st.STRING == ct.VARCHAR
        assert st.STRING == 'VARCHAR'
        assert st.STRING == 15
        assert st.STRING == ct.VARSTRING
        assert st.STRING == 'VARSTRING'
        assert st.STRING == 253
        assert st.STRING == ct.STRING
        assert st.STRING == 'STRING'
        assert st.STRING != 'BINARY'
        assert st.STRING != 'VARBINARY'

        # Because MySQL uses the same type code for strings and binary
        # we can't tell the difference between these.
        # assert st.STRING != ct.BINARY

        assert st.BINARY == ct.BINARY
        assert st.BINARY == ct.VARBINARY

        assert st.NUMBER == ct.DECIMAL
        assert st.NUMBER == ct.TINY
        assert st.NUMBER == ct.LONG
        assert st.NUMBER == ct.FLOAT
        assert st.NUMBER == ct.DOUBLE
        assert st.NUMBER != ct.STRING
        assert st.NUMBER != ct.BINARY

        assert st.DATETIME == ct.DATETIME
        assert st.DATETIME != ct.YEAR
        assert st.DATETIME != ct.DATE
        assert st.DATETIME != ct.TIME

        assert st.ROWID != ct.LONG
        assert st.ROWID != ct.STRING
        assert st.ROWID != ct.BINARY

    def test_str(self):
        out = str(st.NUMBER)
        assert 'BIGINT' in out
        assert 'BIT' in out
        assert 'BOOL' in out
        assert 'DECIMAL' in out
        assert 'LONG' in out
        assert '246' in out

    def test_repr(self):
        assert str(st.NUMBER) == repr(st.NUMBER)

    def test_name(self):
        ct = st.ColumnType
        assert ct.DEC.name == 'DECIMAL', ct.DEC.name
        assert ct.BOOL.name == 'TINY', ct.BOOL.name
        assert ct.BIT.name == 'BIT', ct.BIT.name
        assert ct.LONGBLOB.name == 'LONGBLOB', ct.LONGBLOB.name
        assert ct.LONGTEXT.name == 'LONGTEXT', ct.LONGTEXT.name

    def test_code(self):
        ct = st.ColumnType
        assert ct.DEC.code == 0, ct.DEC.code
        assert ct.BOOL.code == 1, ct.BOOL.code
        assert ct.BIT.code == 16, ct.BIT.code
        assert ct.LONGBLOB.code == 251, ct.LONGBLOB.code
        assert ct.LONGTEXT.code == 251, ct.LONGTEXT.code

    def test_get_code(self):
        ct = st.ColumnType
        assert ct.get_code('DECIMAL') == 0, ct.get_code('DECIMAL')
        assert ct.get_code('TINY') == 1, ct.get_code('TINY')
        assert ct.get_code('BIT') == 16, ct.get_code('BIT')
        assert ct.get_code('LONGBLOB') == 251, ct.get_code('LONGBLOB')
        assert ct.get_code('LONGTEXT') == 251, ct.get_code('LONGTEXT')

        assert ct.get_code(0) == 0, ct.get_code(0)
        assert ct.get_code(1) == 1, ct.get_code(1)
        assert ct.get_code(16) == 16, ct.get_code(16)

        assert ct.get_code(int) == 8, ct.get_code(int)
        assert ct.get_code(float) == 5, ct.get_code(float)
        assert ct.get_code(str) == 15, ct.get_code(str)

    def test_get_name(self):
        ct = st.ColumnType
        assert ct.get_name(0) == 'DECIMAL', ct.get_name(0)
        assert ct.get_name(1) == 'TINY', ct.get_name(1)
        assert ct.get_name(16) == 'BIT', ct.get_name(16)
        assert ct.get_name(251) == 'LONGBLOB', ct.get_name(251)
        assert ct.get_name(251) == 'LONGBLOB', ct.get_name(251)

        assert ct.get_name('dec') == 'DECIMAL', ct.get_name('dec')
        assert ct.get_name('bool') == 'TINY', ct.get_name('bool')
        assert ct.get_name('bigint') == 'BIGINT', ct.get_name('bigint')

        assert ct.get_name(int) == 'BIGINT', ct.get_name(int)
        assert ct.get_name(float) == 'DOUBLE', ct.get_name(float)
        assert ct.get_name(str) == 'VARBINARY', ct.get_name(str)


if __name__ == '__main__':
    import nose2
    nose2.main()
