from __future__ import annotations

import os
from pathlib import Path

import ibis
from ibis.backends.tests.base import BackendTest
from ibis.backends.tests.base import RoundHalfToEven

from ...connection import Connection


class TestConf(BackendTest, RoundHalfToEven):
    # singlestore has the same rounding behavior as postgres
    check_dtype = False
    supports_window_operations = False
    returned_timestamp_unit = 's'
    supports_arrays = False
    supports_arrays_outside_of_select = supports_arrays
    bool_is_int = True

    def __init__(self, data_directory: Path) -> None:
        super().__init__(data_directory)
        # mariadb supports window operations after version 10.2
        # but the sqlalchemy version string looks like:
        # 5.5.5.10.2.12.MariaDB.10.2.12+maria~jessie
        # or 10.4.12.MariaDB.1:10.4.12+maria~bionic
        # example of possible results:
        # https://github.com/sqlalchemy/sqlalchemy/blob/rel_1_3/
        # test/dialect/mysql/test_dialect.py#L244-L268
        self.__class__.supports_window_operations = True

    @staticmethod
    def connect(data_directory: Path) -> Connection:
        user = os.environ.get('IBIS_TEST_SINGLESTORE_USER', 'ibis')
        password = os.environ.get('IBIS_TEST_SINGLESTORE_PASSWORD', 'ibis')
        host = os.environ.get('IBIS_TEST_SINGLESTORE_HOST', 'localhost')
        port = os.environ.get('IBIS_TEST_SINGLESTORE_PORT', 3306)
        database = os.environ.get('IBIS_TEST_SINGLESTORE_DATABASE', 'ibis_testing')
        return ibis.singlestore.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
        )
