#!/usr/bin/env python
"""
SingleStoreDB module.

Examples
--------
>>> import singlestoredb as s2
>>> conn = s2.connect('user:password@host/dbname')
>>> cur = conn.cursor()
>>> cur.execute('select * from customers')
>>> for row in cur:
...     print(row)

"""

__version__ = '0.5.2'

from .config import options, get_option, set_option, describe_option
from .connection import connect, apilevel, threadsafety, paramstyle
from .exceptions import (
    Warning, Error, InterfaceError, DatabaseError, OperationalError,
    IntegrityError, InternalError, ProgrammingError, NotSupportedError,
    DataError, ManagementError,
)
from .management import (
    manage_cluster, manage_workspaces,
)
from .types import (
    Date, Time, Timestamp, DateFromTicks, TimeFromTicks, TimestampFromTicks,
    Binary, STRING, BINARY, NUMBER, DATETIME, ROWID,
)
