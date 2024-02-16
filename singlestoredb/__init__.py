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

__version__ = '1.0.1'

from typing import Any

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


#
# This function is defined here to prevent the side-effect of
# attempting to load the SQLAlchemy dialect in the core SDK.
#
def create_engine(*args: Any, **kwargs: Any) -> Any:
    """
    Create an SQLAlchemy engine for SingleStoreDB.

    Parameters
    ----------
    **kwargs : Any
        The parameters taken here are the same as for
        `sqlalchemy.create_engine`. However, this function can be
        called without any parameters in order to inherit parameters
        set by environment variables or parameters set in by
        options in Python code.

    See Also
    --------
    `sqlalchemy.create_engine`

    Returns
    -------
    SQLAlchemy engine

    """
    from .alchemy import create_engine
    return create_engine(*args, **kwargs)
