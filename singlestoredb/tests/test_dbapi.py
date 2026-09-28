# type: ignore
import importlib
import os
import unittest
import warnings

import singlestoredb as s2
from . import utils
from singlestoredb.mysql.tests.thirdparty.test_MySQLdb import test_MySQLdb_capabilities
from singlestoredb.mysql.tests.thirdparty.test_MySQLdb import test_MySQLdb_dbapi20


class TestDBAPI(test_MySQLdb_dbapi20.test_MySQLdb):

    driver = s2

    dbname: str = ''
    dbexisted: bool = False

    @classmethod
    def setUpClass(cls):
        sql_file = os.path.join(os.path.dirname(__file__), 'empty.sql')
        cls.dbname, cls.dbexisted = utils.load_sql(sql_file)

    @classmethod
    def tearDownClass(cls):
        if not cls.dbexisted:
            utils.drop_database(cls.dbname)

    def _connect(self):
        return s2.connect(database=type(self).dbname)


class TestWarningFilters(unittest.TestCase):
    """Importing the vendored MySQLdb tests must not escalate warnings."""

    def test_capabilities_import_leaves_filters_alone(self):
        # The capabilities module wants warnings raised as errors, but it must
        # scope that to its own tests. It used to call
        # warnings.filterwarnings('error') at module level, and since this
        # module imports that package, the filter was installed process-wide
        # and turned any later warning -- a DeprecationWarning from a
        # dependency, say -- into a failure in an unrelated test. Reload to
        # re-run the module body against a known set of filters.
        before = list(warnings.filters)
        importlib.reload(test_MySQLdb_capabilities)
        self.assertEqual(warnings.filters, before)
