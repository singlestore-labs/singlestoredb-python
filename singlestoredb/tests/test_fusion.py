#!/usr/bin/env python
# type: ignore
"""SingleStoreDB Fusion testing."""
import os
import unittest

import singlestoredb as s2
from singlestoredb.tests import utils


class TestFusion(unittest.TestCase):

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
        self.enabled = os.environ.get('SINGLESTOREDB_ENABLE_FUSION')
        os.environ['SINGLESTOREDB_ENABLE_FUSION'] = '1'
        self.conn = s2.connect(database=type(self).dbname, local_infile=True)
        self.cur = self.conn.cursor()

    def tearDown(self):
        if self.enabled:
            os.environ['SINGLESTOREDB_ENABLE_FUSION'] = self.enabled
        else:
            del os.environ['SINGLESTOREDB_ENABLE_FUSION']

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

    def test_env_var(self):
        os.environ['SINGLESTOREDB_ENABLE_FUSION'] = '0'

        with self.assertRaises(s2.ProgrammingError):
            self.cur.execute('show fusion commands')

        del os.environ['SINGLESTOREDB_ENABLE_FUSION']

        with self.assertRaises(s2.ProgrammingError):
            self.cur.execute('show fusion commands')

        os.environ['SINGLESTOREDB_ENABLE_FUSION'] = 'yes'

        self.cur.execute('show fusion commands')
        assert list(self.cur)

    def test_show_commands(self):
        self.cur.execute('show fusion commands')
        cmds = [x[0] for x in self.cur.fetchall()]
        assert cmds
        assert [x for x in cmds if x.strip().startswith('SHOW FUSION GRAMMAR')], cmds

        self.cur.execute('show fusion commands like "create%"')
        cmds = [x[0] for x in self.cur.fetchall()]
        assert cmds
        assert [x for x in cmds if x.strip().startswith('CREATE')] == cmds, cmds

    def test_show_grammar(self):
        self.cur.execute('show fusion grammar for "create workspace"')
        cmds = [x[0] for x in self.cur.fetchall()]
        assert cmds
        assert [x for x in cmds if x.strip().startswith('CREATE WORKSPACE')], cmds
