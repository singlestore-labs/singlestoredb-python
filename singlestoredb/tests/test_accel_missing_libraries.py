#!/usr/bin/env python
# type: ignore
"""Optional libraries that are not installed, with the C extension."""
import os
import subprocess
import sys
import textwrap
import unittest

import singlestoredb as s2
from singlestoredb.mysql import connection as mysql_connection

has_accel = mysql_connection._singlestoredb_accel is not None
pure_python = bool(s2.get_option('pure_python'))


def run_isolated(script):
    """Run script in a fresh interpreter and fail the test if it fails.

    The extension caches a library once it has imported it, so in this
    process an earlier test could already have done so.
    """
    proc = subprocess.run(
        [sys.executable, '-c', textwrap.dedent(script)],
        capture_output=True, text=True, env=os.environ.copy(),
    )
    assert proc.returncode == 0, proc.stderr


@unittest.skipIf(not has_accel, 'C extension is not available')
@unittest.skipIf(pure_python, 'C extension is disabled')
class TestAccelMissingLibraries(unittest.TestCase):

    def setUp(self):
        with s2.connect() as conn:
            if 'http' in conn.driver:
                self.skipTest('HTTP interface does not use the C extension')

    def test_numpy_imported_once_per_result_set(self):
        """Without numpy, vector columns must not retry the import per cell.

        The import is attempted once per result set that has a vector
        column, and not at all for one that has none. Vectors still decode,
        to a tuple (binary) or a list (JSON).
        """
        with s2.connect() as conn:
            with conn.cursor() as cur:
                cur.execute('SHOW VARIABLES LIKE "enable_extended_types_metadata"')
                out = list(cur)
        if not out or out[0][1].lower() == 'off':
            self.skipTest('Vector columns need extended types metadata')

        run_isolated('''
            import sys

            class BlockNumpy:
                """Fail every import of numpy, and count the attempts."""
                attempts = 0

                def find_spec(self, name, path=None, target=None):
                    if name == 'numpy':
                        BlockNumpy.attempts += 1
                        raise ImportError('numpy is blocked for this test')
                    return None

            sys.meta_path.insert(0, BlockNumpy())

            import singlestoredb as s2

            digits = ' UNION ALL '.join(f'SELECT {i} AS x' for i in range(10))
            query = (
                "SELECT a.x, '[1,2,3]' :> VECTOR(3) AS v "
                f'FROM ({digits}) a, ({digits}) b'
            )

            for fmt in ('binary', 'json'):
                for buffered in (True, False):
                    with s2.connect(
                        pure_python=False, vector_data_format=fmt,
                        buffered=buffered,
                    ) as conn:
                        with conn.cursor() as cur:
                            BlockNumpy.attempts = 0
                            cur.execute(query)
                            rows = cur.fetchall()
                            case = (fmt, buffered, BlockNumpy.attempts)
                            assert len(rows) == 100, case
                            assert list(rows[0][1]) == [1, 2, 3], (case, rows[0])
                            assert BlockNumpy.attempts == 1, case

                            BlockNumpy.attempts = 0
                            cur.execute("SELECT 1, 'x'")
                            cur.fetchall()
                            assert BlockNumpy.attempts == 0, (fmt, buffered)
        ''')


if __name__ == '__main__':
    import nose2
    nose2.main()
