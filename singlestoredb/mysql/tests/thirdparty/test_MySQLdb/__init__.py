# type: ignore
from .test_MySQLdb_capabilities import test_MySQLdb as test_capabilities  # noqa: F401
from .test_MySQLdb_dbapi20 import test_MySQLdb as test_dbapi2  # noqa: F401
from .test_MySQLdb_nonstandard import *  # noqa: F401, F403

if __name__ == '__main__':
    import unittest

    unittest.main()
