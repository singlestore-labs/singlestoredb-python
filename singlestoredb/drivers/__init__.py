from __future__ import annotations

import re
from typing import Any
from typing import Dict

from . import base
from .cymysql import CyMySQLDriver
from .http import HTTPDriver
from .http import HTTPSDriver
from .mariadb import MariaDBDriver
from .mysqlconnector import MySQLConnectorDriver
from .mysqldb import MySQLdbDriver
from .pymysql import PyMySQLDriver
from .pyodbc import PyODBCDriver


def get_driver(name: str, params: Dict[str, Any]) -> base.Driver:
    """
    Return the driver with the given name.

    Parameters
    ----------
    name : str
        Name of the driver. All non-letters/digits will be removed
        from the name when matching. For example, 'mysqlconnector'
        will match 'mysql.connector' or 'mysqlconnector'. Matches
        are also case-insensitive.
    params : dict
        Dictionary of connection parameters

    Returns
    -------
    Driver

    """
    rm_symbols = re.compile(r'[^A-Z0-9]', flags=re.I)
    new_name = rm_symbols.sub(r'', name).lower()
    for item in globals().values():
        if type(item) is not type:
            continue
        if not issubclass(item, base.Driver):
            continue
        if new_name == rm_symbols.sub(r'', item.name.lower()):
            return item(**params)
    raise RuntimeError("Could not locate driver with name '{}'".format(name))
