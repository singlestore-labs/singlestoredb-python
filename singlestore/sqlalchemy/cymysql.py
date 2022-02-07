#!/usr/bin/env python
"""SingleStore CyMySQL driver."""
from __future__ import annotations

from typing import Any
from typing import Callable
from typing import Sequence

from sqlalchemy import util
from sqlalchemy.engine.base import Connection
from sqlalchemy.engine.interfaces import Dialect

from ..connection import Cursor as S2Cursor
from .base import BIT
from .base import SingleStoreDialect
from .mysqldb import SingleStoreDialect_mysqldb


class _cymysqlBIT(BIT):
    """BIT value for CyMySQL driver."""

    def result_processor(
        self,
        dialect: Dialect,
        coltype: int,
    ) -> Callable[[Sequence[int]], int]:
        """Convert SingleStore's 64 bit, variable length binary string to a long."""

        def process(value: Sequence[int]) -> int:
            if value is not None:
                v = 0
                for i in iter(value):
                    v = v << 8 | i
                return v
            return value

        return process


class SingleStoreDialect_cymysql(SingleStoreDialect_mysqldb):
    """SingleStore CyMySQL dialect."""

    driver = 'cymysql'
    supports_statement_cache = True

    description_encoding = None
    supports_sane_rowcount = True
    supports_sane_multi_rowcount = False
    supports_unicode_statements = True

    colspecs = util.update_copy(SingleStoreDialect.colspecs, {BIT: _cymysqlBIT})

    @classmethod
    def dbapi(cls) -> Any:
        """Return DB-API module."""
        return __import__('cymysql')

    def _detect_charset(self, connection: Connection) -> str:
        """
        Return charset of database connection.

        Returns
        -------
        str

        """
        return connection.connection.charset

    def _extract_error_code(self, exception: Exception) -> int:
        """
        Return error code from an exception.

        Returns
        -------
        int

        """
        return exception.errno  # type: ignore

    def is_disconnect(
        self,
        e: Exception,
        connection: Connection,
        cursor: S2Cursor,
    ) -> bool:
        """
        Check if the server has been disconnected.

        Parameters
        ----------
        e : Exception
            Exception value to inspect
        connection : Connection
            Connection to check
        cursor : Cursor
            Cursor to check

        Returns
        -------
        bool

        """
        if isinstance(e, self.dbapi.OperationalError):  # type: ignore
            return self._extract_error_code(e) in (
                2006,
                2013,
                2014,
                2045,
                2055,
            )
        elif isinstance(e, self.dbapi.InterfaceError):  # type: ignore
            # if underlying connection is closed,
            # this is the error you get
            return True
        else:
            return False


dialect = SingleStoreDialect_cymysql
