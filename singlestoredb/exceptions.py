#!/usr/bin/env python
"""Database exeception classes."""
from typing import Optional


class MySQLError(Exception):
    """All MySQL-related exceptions."""


class Error(MySQLError):
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

    def __init__(
        self, errno: Optional[int] = None, msg: Optional[str] = None,
        sqlstate: Optional[int] = None,
    ):
        self.errno = errno
        self.errmsg = msg
        self.sqlstate = sqlstate
        super(Exception, self).__init__(errno, msg, sqlstate)

    def __str__(self) -> str:
        """Return string representation."""
        prefix = []
        if self.errno is not None:
            prefix.append(f'{self.errno}')
        if self.sqlstate is not None:
            prefix.append(f'({self.sqlstate})')
        if prefix and self.errmsg:
            return ' '.join(prefix) + ': ' + self.errmsg
        elif prefix:
            return ' '.join(prefix)
        elif self.errmsg:
            return f'{self.errmsg}'
        return 'Unknown error'

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)

    @property
    def msg(self) -> Optional[str]:
        """Return error message."""
        return self.errmsg


class Warning(Warning, MySQLError):  # type: ignore
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


class ManagementError(Error):
    """Exception for errors in the management API."""

    def __init__(
        self, errno: Optional[int] = None, msg: Optional[str] = None,
        response: Optional[str] = None,
    ):
        self.errno = errno
        self.errmsg = msg
        self.response = response
        super(Exception, self).__init__(errno, msg)

    def __str__(self) -> str:
        """Return string representation."""
        prefix = []
        if self.errno is not None:
            prefix.append(f'{self.errno}')
        if self.response is not None:
            prefix.append(f'({self.response})')
        if prefix and self.errmsg:
            return ' '.join(prefix) + ': ' + self.errmsg
        elif prefix:
            return ' '.join(prefix)
        elif self.errmsg:
            return f'{self.errmsg}'
        return 'Unknown error'
