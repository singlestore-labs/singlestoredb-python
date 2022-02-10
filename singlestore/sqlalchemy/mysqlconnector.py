#!/usr/bin/env python
"""SingleStore SQLAlchemy mysql.connector driver."""
from __future__ import annotations

import re
from typing import Any
from typing import Callable
from typing import Optional

from sqlalchemy import util
from sqlalchemy.engine import URL
from sqlalchemy.engine.base import Connection
from sqlalchemy.sql.elements import BinaryExpression

from ..connection import Connection as S2Connection
from ..connection import Cursor as S2Cursor
from .base import BIT
from .base import SingleStoreCompiler
from .base import SingleStoreDialect
from .base import SingleStoreIdentifierPreparer


class SingleStoreCompiler_mysqlconnector(SingleStoreCompiler):
    """SingleStore SQLAlchemy mysql.connector compiler."""

    def visit_mod_binary(
        self,
        binary: BinaryExpression,
        operator: Callable[[Any, Any], Any],
        **kw: Any,
    ) -> str:
        """
        Compile binary expression to SQL.

        Parameters
        ----------
        binary : BinaryExpression
            The expression to convert
        operator : Callable
            Operator to apply
        **kw : keyword arguments
            Additional arguments to pass to operation

        Returns
        -------
        str
            SQL code

        """
        return (
            self.process(binary.left, **kw)
            + ' % '
            + self.process(binary.right, **kw)
        )


class SingleStoreIdentifierPreparer_mysqlconnector(SingleStoreIdentifierPreparer):
    """SingleStore SQLAlchemy mysql.connector identifier preparer."""

    @property
    def _double_percents(self) -> bool:
        """
        Get double-percents flag.

        Should percent signs be doubled-up?

        """
        return False

    @_double_percents.setter
    def _double_percents(self, value: str) -> None:
        """Set double-percents flag."""

    def _escape_identifier(self, value: str) -> str:
        """
        Escape identifier for use in SQL statement.

        Parameters
        ----------
        value : str
            Input value to escape

        Returns
        -------
        str

        """
        value = value.replace(self.escape_quote, self.escape_to_quote)
        return value


class _myconnpyBIT(BIT):
    """mysql.connector BIT value."""

    def result_processor(self, dialect: SingleStoreDialect, coltype: int) -> None:
        """MySQL-connector already converts mysql bits, so."""
        return None


class SingleStoreDialect_mysqlconnector(SingleStoreDialect):
    """SingleStore SQLAlchemy dialect."""

    driver = 'mysqlconnector'
    supports_statement_cache = True

    supports_sane_rowcount = True
    supports_sane_multi_rowcount = True

    supports_native_decimal = True

    default_paramstyle = 'format'
    statement_compiler = SingleStoreCompiler_mysqlconnector

    preparer = SingleStoreIdentifierPreparer_mysqlconnector

    colspecs = util.update_copy(SingleStoreDialect.colspecs, {BIT: _myconnpyBIT})

    @classmethod
    def dbapi(cls) -> Any:
        """Return the DB-API module."""
        from mysql import connector
        return connector

    def do_ping(self, dbapi_connection: S2Connection) -> bool:
        """
        Check if the server is still available.

        Parameters
        ----------
        dbapi_connection : Connection
            Connection to test

        Returns
        -------
        bool

        """
        try:
            dbapi_connection.ping(False)
        except self.dbapi.Error as err:  # type: ignore
            if self.is_disconnect(err, dbapi_connection, None):
                return False
            else:
                raise
        else:
            return True

    def create_connect_args(self, url: URL) -> list[Any]:
        """
        Map connection parameters.

        Parameters
        ----------
        url : URL
            SQLAlchemy connection URL

        Returns
        -------
        list
            List containing ??? and updated options

        """
        # TODO: fix docstring
        opts = url.translate_connect_args(username='user')

        opts.update(url.query)

        util.coerce_kw_type(opts, 'allow_local_infile', bool)
        util.coerce_kw_type(opts, 'autocommit', bool)
        util.coerce_kw_type(opts, 'buffered', bool)
        util.coerce_kw_type(opts, 'compress', bool)
        util.coerce_kw_type(opts, 'connection_timeout', int)
        util.coerce_kw_type(opts, 'connect_timeout', int)
        util.coerce_kw_type(opts, 'consume_results', bool)
        util.coerce_kw_type(opts, 'force_ipv6', bool)
        util.coerce_kw_type(opts, 'get_warnings', bool)
        util.coerce_kw_type(opts, 'pool_reset_session', bool)
        util.coerce_kw_type(opts, 'pool_size', int)
        util.coerce_kw_type(opts, 'raise_on_warnings', bool)
        util.coerce_kw_type(opts, 'raw', bool)
        util.coerce_kw_type(opts, 'ssl_verify_cert', bool)
        util.coerce_kw_type(opts, 'use_pure', bool)
        util.coerce_kw_type(opts, 'use_unicode', bool)

        # unfortunately, MySQL/connector python refuses to release a
        # cursor without reading fully, so non-buffered isn't an option
        opts.setdefault('buffered', True)

        # FOUND_ROWS must be set in ClientFlag to enable
        # supports_sane_rowcount.
        if self.dbapi is not None:
            try:
                from mysql.connector.constants import ClientFlag

                client_flags = opts.get(
                    'client_flags', ClientFlag.get_default(),
                )
                client_flags |= ClientFlag.FOUND_ROWS
                opts['client_flags'] = client_flags
            except Exception:
                pass
        return [[], opts]

    @util.memoized_property
    def _mysqlconnector_version_info(self) -> Optional[tuple[int, ...]]:
        """Return mysql.connector version."""
        if self.dbapi and hasattr(self.dbapi, '__version__'):  # type: ignore
            m = re.match(
                r'(\d+)\.(\d+)(?:\.(\d+))?',
                self.dbapi.__version__,  # type: ignore
            )
            if m:
                return tuple(int(x) for x in m.group(1, 2, 3) if x is not None)
        return None

    def _detect_charset(self, connection: Connection) -> str:
        """Detect server charset."""
        return connection.connection.charset

    def _extract_error_code(self, exception: Exception) -> int:
        """Return the error code associated with an exception."""
        return exception.errno  # type: ignore

    def is_disconnect(
        self,
        e: Exception,
        connection: Connection,
        cursor: Optional[S2Cursor],
    ) -> bool:
        """
        Check if the server disconnected.

        Parameters
        ----------
        e : Exception
            Exception to check for disconnection
        connection : Connection
            Connection to check
        cursor : Cursor, optional
            Cursor to check

        Returns
        -------
        bool

        """
        errnos = (2006, 2013, 2014, 2045, 2055, 2048)
        exceptions = (
            self.dbapi.OperationalError,  # type: ignore
            self.dbapi.InterfaceError,  # type: ignore
        )
        if isinstance(e, exceptions):
            return (
                e.errno in errnos
                or 'SingleStore Connection not available.' in str(e)
                or 'Connection to SingleStore is not available' in str(e)
            )
        else:
            return False

    def _compat_fetchall(
        self,
        rp: S2Cursor,
        charset: Optional[str] = None,
    ) -> Any:
        """Return all results from a cursor."""
        return rp.fetchall()

    def _compat_fetchone(
        self,
        rp: S2Cursor,
        charset: Optional[str] = None,
    ) -> Any:
        """Return one result from a cursor."""
        return rp.fetchone()

    _isolation_lookup = set(
        [
            'SERIALIZABLE',
            'READ UNCOMMITTED',
            'READ COMMITTED',
            'REPEATABLE READ',
            'AUTOCOMMIT',
        ],
    )

    def _set_isolation_level(self, connection: Connection, level: str) -> None:
        """
        Set transaction isolation level.

        Parameters
        ----------
        connection : Connection
            Connection to set level on
        level : str
            Isolation level to set

        See Also
        --------
        `_isolation_lookup`


        """
        if level == 'AUTOCOMMIT':
            connection.autocommit = True
        else:
            connection.autocommit = False
            super(SingleStoreDialect_mysqlconnector, self)._set_isolation_level(
                connection, level,
            )


dialect = SingleStoreDialect_mysqlconnector
