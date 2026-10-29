#!/usr/bin/env python
# type: ignore
"""Loading the C extension when a connection asks for pure_python=False."""
import contextlib
import io
import sys
import unittest
from unittest.mock import patch

from singlestoredb.mysql import connection as mysql_connection
from singlestoredb.mysql import err


class _FailingAccelFinder:
    """Make importing _singlestoredb_accel fail the way a broken .so would."""

    def find_spec(self, name, path=None, target=None):
        if name == '_singlestoredb_accel':
            raise ImportError('simulated: GLIBC_2.28 not found')
        return None


class TestAccelImport(unittest.TestCase):

    def _connect(self):
        return mysql_connection.Connection(
            host='127.0.0.1', pure_python=False, defer_connect=True,
        )

    @unittest.skipIf(
        mysql_connection._singlestoredb_accel is None,
        'C extension is not available',
    )
    def test_loads_when_available(self):
        # Binding the extension's name inside __init__ (e.g. with an import
        # statement) makes it local there, so every connection would raise
        # UnboundLocalError before reaching the server.
        conn = self._connect()
        self.assertIs(conn.resultclass, mysql_connection.MySQLResultSV)

    def test_reports_the_real_import_error(self):
        # The module-level import swallows the failure, so the connection
        # retries it only to print why. That retry must load the real module,
        # or the reason is replaced by a ModuleNotFoundError for another name.
        saved = sys.modules.pop('_singlestoredb_accel', None)
        meta_path = [_FailingAccelFinder(), *sys.meta_path]
        stderr = io.StringIO()
        try:
            with patch.object(mysql_connection, '_singlestoredb_accel', None), \
                    patch.object(sys, 'meta_path', meta_path), \
                    contextlib.redirect_stderr(stderr):
                with self.assertRaises(err.NotSupportedError):
                    self._connect()
        finally:
            if saved is not None:
                sys.modules['_singlestoredb_accel'] = saved

        self.assertIn('simulated: GLIBC_2.28 not found', stderr.getvalue())


if __name__ == '__main__':
    unittest.main()
