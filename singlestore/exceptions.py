#!/usr/bin/env python
"""Database exeception classes."""
from __future__ import annotations

from typing import Optional


class Error(Exception):
    """
    Generic database exception.

    Parameters
    ----------
    errno : int
        Database error code
    msg : str
        Database error message
    sqlstate : str, optional
        SQL engine state code

    """

    def __init__(self, errno: int, message: str, sqlstate: Optional[int] = None):
        self.errno = errno
        self.errmsg = message
        self.sqlstate = sqlstate
        super(Exception, self).__init__(errno, message, sqlstate)

    def __str__(self) -> str:
        """Return string representation."""
        if self.sqlstate:
            return '{} ({}): {}'.format(self.errno, self.sqlstate, self.errmsg)
        return '{}: {}'.format(self.errno, self.errmsg)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)

    @property
    def msg(self) -> str:
        """Return error message."""
        return self.errmsg


class Warning(Exception):
    """Exception for important warnings like data truncations, etc."""


class InterfaceError(Error):
    """Exception for errors in the database interface rather than the database."""


class DatabaseError(Error):
    """General exception for errors in the database."""


class InternalError(DatabaseError):
    """Exception for internal database errors such as out of sync transactions."""


class OperationalError(DatabaseError):
    """Exception for operational errors such as unexpected disconnections."""


class ProgrammingError(DatabaseError):
    """Exception for programming errors."""


class IntegrityError(DatabaseError):
    """Exception for relational integrity errors."""


class DataError(DatabaseError):
    """Exception for problems with processed data like division by zero."""


class NotSupportedError(DatabaseError):
    """Exception for using unsupported features of the database."""


class ClusterManagerError(Error):
    """Exception for errors in the cluster management API."""
