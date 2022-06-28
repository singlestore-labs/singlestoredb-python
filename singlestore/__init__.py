#!/usr/bin/env python
"""
SingleStore database module.

Examples
--------
>>> import singlestore as s2
>>> conn = s2.connect('user:password@host/dbname')
>>> cur = conn.cursor()
>>> cur.execute('select * from customers')
>>> for row in cur:
...     print(row)

"""
from __future__ import annotations

import ssl
# Always import this first to get the system libssl.
# mysql.connector bundles its own and we don't want that.

__version__ = '0.1.0'

from .config import options, get_option, set_option, describe_option
from .connection import connect, apilevel, threadsafety, paramstyle
from .exceptions import (
    Warning, Error, InterfaceError, DatabaseError, OperationalError,
    IntegrityError, InternalError, ProgrammingError, NotSupportedError,
    DataError, ManagementError,
)
from .management import (
    manage_cluster, manage_workspace,
)
from .types import (
    Date, Time, Timestamp, DateFromTicks, TimeFromTicks, TimestampFromTicks,
    Binary, STRING, BINARY, NUMBER, DATETIME, ROWID,
)
