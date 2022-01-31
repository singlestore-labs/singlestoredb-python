#!/usr/bin/env python
'''
SingleStore database module

'''
from __future__ import annotations

__version__ = '0.1.0'

from .connection import connect, apilevel, threadsafety, paramstyle
from .exceptions import (
    Warning, Error, InterfaceError, DatabaseError, OperationalError,
    IntegrityError, InternalError, ProgrammingError, NotSupportedError,
)
from .manager import manage_cluster
from .types import (
    Date, Time, Timestamp, DateFromTicks, TimeFromTicks, TimestampFromTicks,
    Binary, STRING, BINARY, NUMBER, DATETIME, ROWID,
)
