#!/usr/bin/env python
# type: ignore
"""Test that the C accelerator does not leak memory per query."""
import gc
import os
import sys
import unittest

import singlestoredb as s2
from singlestoredb.mysql import connection as mysql_connection

# The leak in issue #135 was one allocation per column per query, so a wide
# result makes it unmistakable: it shows up as ~N_COLS blocks per query, three
# orders of magnitude above the noise floor of a few blocks over the run.
N_COLS = 100
WIDE_QUERY = 'SELECT ' + ', '.join(f'{i} AS c{i}' for i in range(N_COLS))

WARMUP = 50
ITERATIONS = 200

# Per-query budget, in allocated blocks. Zero is what a fixed accelerator
# actually measures; this leaves room for caches that fill on the first few
# queries while staying far below the N_COLS a per-column leak would cost.
MAX_BLOCKS_PER_QUERY = 5.0

has_accel = mysql_connection._singlestoredb_accel is not None
pure_python = bool(int(os.environ.get('SINGLESTOREDB_PURE_PYTHON', '0')))


@unittest.skipIf(not has_accel, 'C extension is not available')
@unittest.skipIf(pure_python, 'C extension is disabled')
class TestAccelLeaks(unittest.TestCase):

    def setUp(self):
        self.conn = s2.connect()
        if 'http' in self.conn.driver:
            self.skipTest('HTTP interface does not use the C extension')

    def tearDown(self):
        try:
            self.conn.close()
        except Exception:
            pass

    def blocks_per_query(self, results_type):
        """Return the allocated blocks retained per query of WIDE_QUERY."""
        with s2.connect(results_type=results_type, pure_python=False) as conn:
            with conn.cursor() as cur:
                for _ in range(WARMUP):
                    cur.execute(WIDE_QUERY)
                    cur.fetchall()

                gc.collect()
                before = sys.getallocatedblocks()

                for _ in range(ITERATIONS):
                    cur.execute(WIDE_QUERY)
                    cur.fetchall()

                gc.collect()
                after = sys.getallocatedblocks()

        return (after - before) / ITERATIONS

    def test_no_leak_per_query(self):
        for results_type in ('tuples', 'dicts', 'namedtuples', 'structsequences'):
            with self.subTest(results_type=results_type):
                leaked = self.blocks_per_query(results_type)
                assert leaked < MAX_BLOCKS_PER_QUERY, \
                    f'{results_type} leaks {leaked} blocks per query'

    def test_rows_outlive_the_result_state(self):
        """Struct sequence rows must survive the state that created them.

        The type does not copy its field names, it stores the pointers and
        reads them again in repr, so the names have to outlive every row
        rather than the query that built them.
        """
        with s2.connect(
            results_type='structsequences', pure_python=False,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(WIDE_QUERY)
                rows = cur.fetchall()

                # Discard the state that built the rows, several times over.
                for _ in range(10):
                    cur.execute('SELECT 1')
                    cur.fetchall()

        gc.collect()

        assert len(rows[0]) == N_COLS, len(rows[0])
        assert rows[0].c0 == 0, rows[0].c0
        assert getattr(rows[0], f'c{N_COLS - 1}') == N_COLS - 1
        assert f'c{N_COLS - 1}=' in repr(rows[0]), repr(rows[0])


if __name__ == '__main__':
    import nose2
    nose2.main()
