#!/usr/bin/env python
# type: ignore
"""SingleStoreDB Pytest Plugin testing

Each of these tests performs the same simple operation which
would fail if any other test had been run on the same database.
"""
import os

import pytest

from singlestoredb.connection import Cursor

# pytest_plugins = ('singlestoredb.pytest',)

# Skip all tests in this module when using HTTP Data API
# The singlestoredb_tempdb fixture uses 'USE database' which doesn't work with HTTP
pytestmark = pytest.mark.skipif(
    'http://' in os.environ.get('SINGLESTOREDB_URL', '').lower() or
    'https:/' in os.environ.get('SINGLESTOREDB_URL', '').lower(),
    reason='Plugin tests require MySQL protocol (USE database not supported via HTTP)',
)

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
