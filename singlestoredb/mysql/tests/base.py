# type: ignore
import json
import os
import platform
import re
import unittest
import warnings

import singlestoredb.mysql as sv
from singlestoredb.connection import build_params

DBNAME_BASE = 'singlestoredb__test_%s_%s_%s_%s_' % \
              (
                  *platform.python_version_tuple()[:2],
                  platform.system(), platform.machine(),
              )


class PyMySQLTestCase(unittest.TestCase):
    # You can specify your test environment creating a file named
    #  "databases.json" or editing the `databases` variable below.
    fname = os.path.join(os.path.dirname(__file__), 'databases.json')
    if os.path.exists(fname):
        with open(fname) as f:
            databases = json.load(f)
    else:
        params = build_params()
        databases = [
            {
                'host': params['host'],
                'port': params['port'],
                'user': params['user'],
                'passwd': params['password'],
                'database': DBNAME_BASE + '1',
                'use_unicode': True,
                'local_infile': True,
                'buffered': params['buffered'],
            },
            {
                'host': params['host'], 'user': params['user'],
                'port': params['port'], 'passwd': params['password'],
                'database': DBNAME_BASE + '2',
                'buffered': params['buffered'],
            },
        ]

    def mysql_server_is(self, conn, version_tuple):
        """
        Return True if the given connection is on the version given or greater.

        This only checks the server version string provided when the
        connection is established, therefore any check for a version tuple
        greater than (5, 5, 5) will always fail on MariaDB, as it always
        starts with 5.5.5, e.g. 5.5.5-10.7.1-MariaDB-1:10.7.1+maria~focal.

        Examples
        --------

            if self.mysql_server_is(conn, (5, 6, 4)):
                # do something for MySQL 5.6.4 and above

        """
        server_version = conn.get_server_info()
        server_version_tuple = tuple(
            (int(dig) if dig is not None else 0)
            for dig in re.match(r'(\d+)\.(\d+)\.(\d+)', server_version).group(1, 2, 3)
        )
        return server_version_tuple >= version_tuple

    _connections = None

    @property
    def connections(self):
        if self._connections is None:
            self._connections = []
            for params in self.databases:
                conn = sv.connect(**params)
                self._connections.append(conn)
            self.addCleanup(self._teardown_connections)
        return self._connections

    def connect(self, **params):
        p = self.databases[0].copy()
        p.update(params)
        conn = sv.connect(**p)

        @self.addCleanup
        def teardown():
            if conn.open:
                conn.close()

        return conn

    def _teardown_connections(self):
        if self._connections:
            for connection in self._connections:
                if connection.open:
                    connection.close()
            self._connections = None

    def safe_create_table(self, connection, tablename, ddl, cleanup=True):
        """
        Create a table.

        Ensures any existing version of that table is first dropped.

        Also adds a cleanup rule to drop the table after the test
        completes.

        """
        cursor = connection.cursor()

        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            cursor.execute('drop table if exists `%s`' % (tablename,))
        cursor.execute(ddl)
        cursor.close()
        if cleanup:
            self.addCleanup(self.drop_table, connection, tablename)

    def drop_table(self, connection, tablename):
        cursor = connection.cursor()
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            cursor.execute('drop table if exists `%s`' % (tablename,))
        cursor.close()
