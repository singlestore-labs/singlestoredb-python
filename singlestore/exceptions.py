#!/usr/bin/env python
"""Database exeception classes."""
from __future__ import annotations


class Error(Exception):
    """
    Generic database exception.

    Parameters
    ----------
    code : int
        The database error code
    message : str
        The database error message

    """

    def __init__(self, code: int, message: str):
        self.code = code
        self.message = message
        super(Exception, self).__init__(code, message)

    def __str__(self) -> str:
        """Return string representation."""
        return '[{}] {}'.format(self.code, self.message)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)


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
