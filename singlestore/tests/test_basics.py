#!/usr/bin/env python
# type: ignore
"""Basic SingleStore connection testing."""
from __future__ import annotations

import unittest

import singlestore as s2


class BasicTests(unittest.TestCase):

    def test_connection(self):
        conn = s2.connect()
        cur = conn.cursor()
        cur.execute('show databases')

        dbs = set([x[0] for x in cur.fetchall()])
        assert dbs == set(['app', 'cluster', 'information_schema', 'memsql']), str(dbs)


if __name__ == '__main__':
    import nose2
    nose2.main()
