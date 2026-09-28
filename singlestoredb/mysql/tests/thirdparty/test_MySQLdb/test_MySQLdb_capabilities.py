# type: ignore
import warnings

import singlestoredb.mysql as sv
from . import capabilities
from singlestoredb.mysql.tests import base


class test_MySQLdb(capabilities.DatabaseTest):

    db_module = sv
    connect_args = ()
    connect_kwargs = base.PyMySQLTestCase.databases[0].copy()
    connect_kwargs.update(
        dict(
            read_default_file='~/.my.cnf',
            use_unicode=True,
            binary_prefix=True,
            charset='utf8mb4',
            sql_mode='ANSI,STRICT_TRANS_TABLES,TRADITIONAL',
        ),
    )

    leak_test = False

    # These tests want warnings raised as exceptions -- test_truncation asks the
    # server for an over-long column and expects the driver to complain. Scoped
    # to the test rather than set at module import: this package's __init__
    # imports this module, so a module-level warnings.filterwarnings('error')
    # was installed process-wide the moment anything imported one of these
    # classes (singlestoredb/tests/test_dbapi.py does), and every later warning
    # in the session -- a DeprecationWarning from a dependency, say -- became a
    # fatal error in an unrelated test.
    def setUp(self):
        self._warnings = warnings.catch_warnings()
        self._warnings.__enter__()
        warnings.simplefilter('error')
        try:
            super().setUp()
        except Exception:
            self._warnings.__exit__(None, None, None)
            raise

    def tearDown(self):
        try:
            super().tearDown()
        finally:
            self._warnings.__exit__(None, None, None)

    def quote_identifier(self, ident):
        return '`%s`' % ident

    def test_TIME(self):
        from datetime import timedelta

        def generator(row, col):
            return timedelta(0, row * 8000)

        self.check_data_integrity(('col1 TIME',), generator)

    def test_TINYINT(self):
        # Number data
        def generator(row, col):
            v = (row * row) % 256
            if v > 127:
                v = v - 256
            return v

        self.check_data_integrity(('col1 TINYINT',), generator)

    def test_stored_procedures(self):
        db = self.connection
        c = self.cursor
        try:
            self.create_table(('pos INT', 'tree CHAR(20)'))
            c.executemany(
                'INSERT INTO %s (pos,tree) VALUES (%%s,%%s)' % self.table,
                list(enumerate('ash birch cedar larch pine'.split())),
            )
            db.commit()

            c.execute(
                """
            CREATE PROCEDURE test_sp(t VARCHAR(255)) AS
            BEGIN
                ECHO SELECT pos FROM %s WHERE tree = t;
            END
            """
                % self.table,
            )
            db.commit()

            c.callproc('test_sp', ('larch',))
            rows = c.fetchall()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0][0], 3)
            c.nextset()
        finally:
            c.execute('DROP PROCEDURE IF EXISTS test_sp')
            c.execute('drop table %s' % (self.table))

    def test_small_CHAR(self):
        # Character data
        def generator(row, col):
            i = ((row + 1) * (col + 1) + 62) % 256
            if i == 62:
                return ''
            if i == 63:
                return None
            return chr(i)

        self.check_data_integrity(('col1 char(1)', 'col2 char(1)'), generator)

    def test_bug_2671682(self):
        from singlestoredb.mysql.constants import ER

        try:
            self.cursor.execute('describe some_non_existent_table')
        except self.connection.ProgrammingError as msg:
            self.assertEqual(msg.args[0], ER.NO_SUCH_TABLE)

    def test_ping(self):
        self.connection.ping()

    def test_literal_int(self):
        self.assertTrue('2' == self.connection.literal(2))

    def test_literal_float(self):
        self.assertEqual('3.1415e0', self.connection.literal(3.1415))

    def test_literal_string(self):
        self.assertTrue("'foo'" == self.connection.literal('foo'))
