# type: ignore
# Sorted by alphabetical order
from singlestoredb.mysql.tests.test_basic import *  # noqa: F403, F401
from singlestoredb.mysql.tests.test_connection import *  # noqa: F403, F401
from singlestoredb.mysql.tests.test_converters import *  # noqa: F403, F401
from singlestoredb.mysql.tests.test_cursor import *  # noqa: F403, F401
from singlestoredb.mysql.tests.test_DictCursor import *  # noqa: F403, F401
from singlestoredb.mysql.tests.test_err import *  # noqa: F403, F401
from singlestoredb.mysql.tests.test_issues import *  # noqa: F403, F401
from singlestoredb.mysql.tests.test_load_local import *  # noqa: F403, F401
from singlestoredb.mysql.tests.test_nextset import *  # noqa: F403, F401
from singlestoredb.mysql.tests.test_optionfile import *  # noqa: F403, F401
from singlestoredb.mysql.tests.test_SSCursor import *  # noqa: F403, F401
from singlestoredb.mysql.tests.thirdparty import *  # noqa: F403, F401

if __name__ == '__main__':
    import unittest

    unittest.main()
