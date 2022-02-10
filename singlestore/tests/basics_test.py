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
        assert list(cur) == ['app']
