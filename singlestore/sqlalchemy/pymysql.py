#!/usr/bin/env python
"""SingleStore SQLAlchemy PyMySQL dialect."""
from __future__ import annotations

from typing import Any
from typing import Optional

from sqlalchemy.engine import URL
from sqlalchemy.engine.base import Connection
from sqlalchemy.util import langhelpers

from ..connection import Cursor as S2Cursor
from .mysqldb import SingleStoreDialect_mysqldb


class SingleStoreDialect_pymysql(SingleStoreDialect_mysqldb):
    """SingleStore SQLAlchemy PyMySQL dialect."""

    driver = 'pymysql'
    supports_statement_cache = True

    description_encoding = None

    @langhelpers.memoized_property
    def supports_server_side_cursors(self) -> bool:
        """Check if server-side cursors are supported."""
        try:
            cursors = __import__('pymysql.cursors').cursors
            self._sscursor = cursors.SSCursor
            return True
        except (ImportError, AttributeError):
            return False

    @classmethod
    def dbapi(cls) -> Any:
        """Return DB-API module."""
        return __import__('pymysql')

    def create_connect_args(
        self,
        url: URL,
        _translate_args: Optional[dict[str, Any]] = None,
    ) -> list[Any]:
        """
        Map connection parameters.

        Parameters
        ----------
        url : URL
            SQLAlchemy connection URL
        _translate_args : dict, optional
            Dictionary of parameter mappings

        Returns
        -------
        list
            List of ??? and updated options

        """
        # TODO: update docstring
        if _translate_args is None:
            _translate_args = dict(username='user')
        return super(SingleStoreDialect_pymysql, self).create_connect_args(
            url, _translate_args=_translate_args,
        )

    def is_disconnect(
        self,
        e: Exception,
        connection: Connection,
        cursor: Optional[S2Cursor],
    ) -> bool:
        """
        Check if server is disconnected.

        Parameters
        ----------
        e : Exception
            Exception to check for disconnection
        connection : Connection
            Connection to check
        cursor : Cursor
            Cursor to check

        Returns
        -------
        bool

        """
        if super(SingleStoreDialect_pymysql, self).is_disconnect(
            e, connection, cursor,
        ):
            return True
        elif isinstance(e, self.dbapi.Error):  # type: ignore
            str_e = str(e).lower()
            return (
                'already closed' in str_e or 'connection was killed' in str_e
            )
        else:
            return False

    def _extract_error_code(self, exception: Exception) -> int:
        """Extract error code from an exception."""
        if isinstance(exception.args[0], Exception):
            exception = exception.args[0]
        return exception.args[0]


dialect = SingleStoreDialect_pymysql
