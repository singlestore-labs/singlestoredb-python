# type: ignore
import os

import singlestoredb as s2
from . import utils
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
