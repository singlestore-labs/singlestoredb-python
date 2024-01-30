#!/usr/bin/env python
# type: ignore
"""Test SingleStoreDB results."""
import os
import unittest

import pandas as pd

import singlestoredb as s2
from singlestoredb.tests import utils
# import traceback


class TestResults(unittest.TestCase):

    dbname: str = ''
    dbexisted: bool = False

    @classmethod
    def setUpClass(cls):
        sql_file = os.path.join(os.path.dirname(__file__), 'test.sql')
        cls.dbname, cls.dbexisted = utils.load_sql(sql_file)

    @classmethod
    def tearDownClass(cls):
        if not cls.dbexisted:
            utils.drop_database(cls.dbname)

    def setUp(self):
        self.conn = s2.connect(database=type(self).dbname)
        self.cur = self.conn.cursor()

    def tearDown(self):
        try:
            if self.cur is not None:
                self.cur.close()
        except Exception:
            # traceback.print_exc()
            pass

        try:
            if self.conn is not None:
                self.conn.close()
        except Exception:
            # traceback.print_exc()
            pass

    def test_tuples(self):
        with s2.options(('results.type', 'tuples')):
            with s2.connect(database=type(self).dbname) as conn:
                with conn.cursor() as cur:
                    cur.execute('select * from data')
                    out = cur.fetchone()
                    assert type(out) is tuple, type(out)
                    assert len(out) == 3, len(out)
                    cur.fetchall()

                    cur.execute('select * from data')
                    out = cur.fetchall()
                    assert len(out) == 5, len(out)
                    assert len(out[0]) == 3, len(out[0])
                    assert type(out[0]) is tuple, type(out[0])
                    assert sorted(out) == sorted([
                        ('a', 'antelopes', 2),
                        ('b', 'bears', 2),
                        ('c', 'cats', 5),
                        ('d', 'dogs', 4),
                        ('e', 'elephants', 0),
                    ]), out

                    out = cur.fetchall()
                    assert len(out) == 0, len(out)

    def test_namedtuples(self):
        with s2.options(('results.type', 'namedtuples')):
            with s2.connect(database=type(self).dbname) as conn:
                with conn.cursor() as cur:
                    cur.execute('select * from data')
                    out = cur.fetchone()
                    assert isinstance(out, tuple), type(out)
                    assert len(out) == 3, len(out)
                    assert type(out).__name__ == 'Row', type(out).__name__
                    assert hasattr(out, 'id')
                    assert hasattr(out, 'name')
                    assert hasattr(out, 'value')
                    cur.fetchall()

                    cur.execute('select * from data')
                    out = cur.fetchall()
                    assert len(out) == 5, len(out)
                    assert len(out[0]) == 3, len(out[0])
                    assert isinstance(out[0], tuple), type(out[0])
                    assert type(out[0]).__name__ == 'Row', type(out[0]).__name__
                    assert hasattr(out[0], 'id')
                    assert hasattr(out[0], 'name')
                    assert hasattr(out[0], 'value')
                    assert sorted(out) == sorted([
                        ('a', 'antelopes', 2),
                        ('b', 'bears', 2),
                        ('c', 'cats', 5),
                        ('d', 'dogs', 4),
                        ('e', 'elephants', 0),
                    ]), out

                    out = cur.fetchall()
                    assert len(out) == 0, len(out)

    def test_dict(self):
        with s2.options(('results.type', 'dicts')):
            with s2.connect(database=type(self).dbname) as conn:
                with conn.cursor() as cur:
                    cur.execute('select * from data')
                    out = cur.fetchone()
                    assert type(out) is dict, type(out)
                    assert len(out) == 3, len(out)
                    cur.fetchall()

                    cur.execute('select * from data')
                    out = cur.fetchall()
                    assert type(out[0]) is dict, type(out[0])
                    assert len(out) == 5, len(out)
                    assert len(out[0]) == 3, len(out[0])
                    assert sorted(out, key=lambda x: x['id']) == sorted(
                        [
                            dict(id='a', name='antelopes', value=2),
                            dict(id='b', name='bears', value=2),
                            dict(id='c', name='cats', value=5),
                            dict(id='d', name='dogs', value=4),
                            dict(id='e', name='elephants', value=0),
                        ], key=lambda x: x['id'],
                    )

                    out = cur.fetchall()
                    assert len(out) == 0, len(out)

    def _test_dataframe(self):
        with s2.options(('results.type', 'dataframe')):
            with s2.connect(database=type(self).dbname) as conn:
                with conn.cursor() as cur:
                    cur.execute('select * from data')
                    out = cur.fetchone()
                    assert type(out) is pd.DataFrame, type(out)
                    assert len(out) == 1, len(out)
                    cur.fetchall()

                    cur.execute('select * from data')
                    out = cur.fetchall()
                    assert type(out) is pd.DataFrame, type(out)
                    assert len(out) == 5, len(out)
                    out = out.sort_values('id').reset_index(drop=True)
                    exp = pd.DataFrame(
                        data=[
                            ('a', 'antelopes', 2),
                            ('b', 'bears', 2),
                            ('c', 'cats', 5),
                            ('d', 'dogs', 4),
                            ('e', 'elephants', 0),
                        ], columns=['id', 'name', 'value'],
                    ).sort_values('id').reset_index(drop=True)
                    assert list(out.columns) == list(exp.columns), list(out.columns)
                    assert [list(x) for x in list(out.values)] == \
                           [list(x) for x in list(exp.values)], \
                           [list(x) for x in list(out.values)]

                    out = cur.fetchall()
                    assert len(out) == 0, len(out)


if __name__ == '__main__':
    import nose2
    nose2.main()
