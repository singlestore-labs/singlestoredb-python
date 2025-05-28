import os
import unittest

import singlestoredb as s2
from . import utils

from vectorstore import VectorDB

class TestVectorDB(unittest.TestCase):

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

    def test_vectordb_from_params(self):
        db: VectorDB = s2.vector_db(database=type(self).dbname)
        index = db.create_index(name="test_index", dimension=3, tags={"name": "test_tag"})
        assert index.name == "test_index"
        assert index.dimension == 3
        assert index.tags == {"name": "test_tag"}
        assert db.has_index("test_index")

    def test_vectordb_from_connection(self):
        with s2.connect(database=type(self).dbname) as conn:
            db: VectorDB = conn.vector_db
            index = db.create_index(name="test_index_1", dimension=4, tags={"name": "test_tag"})
            assert index.name == "test_index_1"
            assert index.dimension == 4
            assert index.tags == {"name": "test_tag"}
            assert db.has_index("test_index_1")