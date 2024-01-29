#!/usr/bin/env python
# type: ignore
"""SingleStoreDB Pytest Plugin testing

Each of these tests performs the same simple operation which
would fail if any other test had been run on the same database.
"""
from singlestoredb.connection import Cursor

# pytest_plugins = ('singlestoredb.pytest',)

CREATE_TABLE_STATEMENT = 'create table test_dict (a text)'


def test_tempdb1(singlestoredb_tempdb: Cursor):
    # alias the fixture
    cursor = singlestoredb_tempdb

    cursor.execute(CREATE_TABLE_STATEMENT)


def test_tempdb2(singlestoredb_tempdb: Cursor):
    # alias the fixture
    cursor = singlestoredb_tempdb

    cursor.execute(CREATE_TABLE_STATEMENT)


def test_tempdb3(singlestoredb_tempdb: Cursor):
    # alias the fixture
    cursor = singlestoredb_tempdb

    cursor.execute(CREATE_TABLE_STATEMENT)
