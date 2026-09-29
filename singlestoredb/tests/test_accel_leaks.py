#!/usr/bin/env python
# type: ignore
"""Test that the C accelerator does not leak memory per query."""
import gc
import sys
import unittest

import singlestoredb as s2
from singlestoredb.mysql import connection as mysql_connection

# The leak in issue #135 was one allocation per column per query, so the
# signal to look for is retention that grows with the width of the result.
# Measuring two widths and taking the difference is what makes this robust:
# anything a query costs that is flat in the column count drops out, and one
# such cost is unavoidable here. Under coverage.py's sys.monitoring backend
# every code object ever seen is retained forever, deliberately, keyed by
# id() (see `code_objects` in coverage/sysmon.py). collections.namedtuple
# compiles a fresh __new__ on each call and the accelerator builds one Row
# class per query, so a coverage run retains ~15 blocks per query on the
# namedtuples path however narrow the result is. That is the tracer's
# accounting, not our allocation, and CI runs under --cov.
NARROW_COLS = 10
WIDE_COLS = 100

WARMUP = 50
ITERATIONS = 200


def query_for(n_cols):
    return 'SELECT ' + ', '.join(f'{i} AS c{i}' for i in range(n_cols))


# Per-column budget, in allocated blocks. A fixed accelerator measures zero;
# the leak this guards against cost one block per column, so anything above
# the noise floor of a fraction of a block is the bug coming back.
MAX_BLOCKS_PER_COLUMN = 0.05

# Per-query budget for the width-independent part, in allocated blocks. Room
# for caches that fill on the first few queries, plus the tracer overhead
# above, which is measured rather than assumed so the budget stays tight when
# nothing is tracing.
MAX_BLOCKS_PER_QUERY = 5.0

N_COLS = WIDE_COLS
WIDE_QUERY = query_for(WIDE_COLS)


def blocks_retained_per_namedtuple():
    """Return the blocks a tracer retains per collections.namedtuple() call.

    Zero when nothing is tracing. Non-zero under coverage, which the
    accelerator then pays once per query on the namedtuples path.
    """
    import collections

    fields = [f'c{i}' for i in range(WIDE_COLS)]

    def build(n):
        for _ in range(n):
            collections.namedtuple('Row', fields, rename=True)

    build(WARMUP)
    gc.collect()
    before = sys.getallocatedblocks()

    build(ITERATIONS)
    gc.collect()
    after = sys.getallocatedblocks()

    return max(0.0, (after - before) / ITERATIONS)


has_accel = mysql_connection._singlestoredb_accel is not None
# Read the parsed option rather than the environment variable: the option's
# validator already accepts true/yes/on, which int() would choke on.
pure_python = bool(s2.get_option('pure_python'))


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

    def blocks_per_query(self, results_type, n_cols=WIDE_COLS):
        """Return the allocated blocks retained per query of n_cols columns."""
        query = query_for(n_cols)
        with s2.connect(results_type=results_type, pure_python=False) as conn:
            with conn.cursor() as cur:
                for _ in range(WARMUP):
                    cur.execute(query)
                    cur.fetchall()

                gc.collect()
                before = sys.getallocatedblocks()

                for _ in range(ITERATIONS):
                    cur.execute(query)
                    cur.fetchall()

                gc.collect()
                after = sys.getallocatedblocks()

        return (after - before) / ITERATIONS

    def test_no_leak_per_column(self):
        """Retention must not grow with the width of the result.

        This is the shape of the issue #135 leak, and differencing two widths
        cancels every per-query cost that is flat in the column count -- see
        the note on the tracer overhead at the top of this module.
        """
        for results_type in ('tuples', 'dicts', 'namedtuples', 'structsequences'):
            with self.subTest(results_type=results_type):
                narrow = self.blocks_per_query(results_type, NARROW_COLS)
                wide = self.blocks_per_query(results_type, WIDE_COLS)

                per_column = (wide - narrow) / (WIDE_COLS - NARROW_COLS)

                assert per_column < MAX_BLOCKS_PER_COLUMN, \
                    f'{results_type} leaks {per_column} blocks per column ' \
                    f'({narrow} blocks/query at {NARROW_COLS} columns, ' \
                    f'{wide} at {WIDE_COLS})'

    def test_no_leak_per_query(self):
        """Retention must not grow per query either.

        The per-column check above cannot see a leak of something allocated
        once per query, so budget that separately. The namedtuples path is
        allowed the tracer's per-class overhead on top, measured here so the
        budget stays tight when nothing is tracing.
        """
        tracer_overhead = blocks_retained_per_namedtuple()

        for results_type in ('tuples', 'dicts', 'namedtuples', 'structsequences'):
            with self.subTest(results_type=results_type):
                budget = MAX_BLOCKS_PER_QUERY
                if results_type == 'namedtuples':
                    budget += tracer_overhead

                leaked = self.blocks_per_query(results_type)

                assert leaked < budget, \
                    f'{results_type} leaks {leaked} blocks per query ' \
                    f'(budget {budget}, of which {tracer_overhead} is ' \
                    f'tracer overhead)'

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

    def test_field_names_survive_stripping_the_type_dict(self):
        """No class attribute may own the field name storage.

        The names are read again by repr, so a deletable attribute holding
        the only reference would turn `delattr` into a use-after-free.
        """
        with s2.connect(
            results_type='structsequences', pure_python=False,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(WIDE_QUERY)
                rows = cur.fetchall()

        row_type = type(rows[0])

        # Nothing in the type's dict may be the owner: every entry there is
        # reachable, and most of them are deletable.
        for name, value in vars(row_type).items():
            assert type(value).__name__ != 'PyCapsule', name

        # Held so the field names are still read out of the type after the
        # loop below deletes the type's own __repr__ entry.
        row_repr = row_type.__repr__

        # CPython reads these three back out of the dict itself, so deleting
        # them breaks a struct sequence whatever owns its names.
        keep = ('n_fields', 'n_sequence_fields', 'n_unnamed_fields')

        for name in list(vars(row_type)):
            if name in keep:
                continue
            try:
                delattr(row_type, name)
            except (AttributeError, TypeError):
                pass

        gc.collect()

        # repr reads the names out of the C field table, not the type dict.
        assert f'c{N_COLS - 1}=' in row_repr(rows[0]), row_repr(rows[0])


if __name__ == '__main__':
    import nose2
    nose2.main()
