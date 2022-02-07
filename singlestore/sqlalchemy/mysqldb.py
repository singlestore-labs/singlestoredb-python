#!/usr/bin/env python
"""SingleStore SQLAlchemy MySQLdb dialect."""
from __future__ import annotations

import re
from typing import Any
from typing import Callable
from typing import Optional

from sqlalchemy import sql
from sqlalchemy import util
from sqlalchemy.engine import URL
from sqlalchemy.engine.base import Connection

from ..connection import Connection as S2Connection
from ..connection import Cursor as S2Cursor
from .base import SingleStoreCompiler
from .base import SingleStoreDialect
from .base import SingleStoreExecutionContext
from .base import SingleStoreIdentifierPreparer
from .base import TEXT


class SingleStoreExecutionContext_mysqldb(SingleStoreExecutionContext):
    """SingleStore SQLAlchemy MySQLdb execution context."""

    @property
    def rowcount(self) -> int:
        """Return row count of last operation."""
        if hasattr(self, '_rowcount'):
            return self._rowcount
        else:
            return self.cursor.rowcount


class SingleStoreCompiler_mysqldb(SingleStoreCompiler):
    """SingleStore SQLAlchemy MySQLdb compiler."""


class SingleStoreDialect_mysqldb(SingleStoreDialect):
    """SingleStore SQLAlchemy MySQLdb dialect."""

    driver = 'mysqldb'
    supports_statement_cache = True
    supports_unicode_statements = True
    supports_sane_rowcount = True
    supports_sane_multi_rowcount = True

    supports_native_decimal = True

    default_paramstyle = 'format'
    execution_ctx_cls = SingleStoreExecutionContext_mysqldb
    statement_compiler = SingleStoreCompiler_mysqldb
    preparer = SingleStoreIdentifierPreparer

    def __init__(self, **kwargs: Any):
        super(SingleStoreDialect_mysqldb, self).__init__(**kwargs)
        self._mysql_dbapi_version = (
            self._parse_dbapi_version(self.dbapi.__version__)  # type: ignore
            if self.dbapi is not None and hasattr(self.dbapi, '__version__')
            else (0, 0, 0)
        )

    def _parse_dbapi_version(self, version: str) -> tuple[int, ...]:
        """
        Parse version number.

        Parameters
        ----------
        version : str
            Version string to parse

        Returns
        -------
        tuple of ints

        """
        m = re.match(r'(\d+)\.(\d+)(?:\.(\d+))?', version)
        if m:
            return tuple(int(x) for x in m.group(1, 2, 3) if x is not None)
        else:
            return (0, 0, 0)

    @util.langhelpers.memoized_property
    def supports_server_side_cursors(self) -> bool:
        """Indicate whether connection supports server-side cursors."""
        try:
            cursors = __import__('MySQLdb.cursors').cursors
            self._sscursor = cursors.SSCursor
            return True
        except (ImportError, AttributeError):
            return False

    @classmethod
    def dbapi(cls) -> Any:
        """Return DB-API module."""
        return __import__('MySQLdb')

    def on_connect(self) -> Callable[[Connection], None]:
        """Return function to call when server is connected."""
        super_ = super(SingleStoreDialect_mysqldb, self).on_connect()

        def on_connect(conn: Connection) -> None:
            if super_ is not None:
                super_(conn)

            charset_name = conn.character_set_name()

            if charset_name is not None:
                cursor = conn.cursor()
                cursor.execute('SET NAMES %s' % charset_name)
                cursor.close()

        return on_connect

    def do_ping(self, dbapi_connection: S2Connection) -> bool:
        """
        Determine if server is still available.

        Parameters
        ----------
        dbapi_connection : Connection
            The connection to check

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

    def do_executemany(
        self,
        cursor: S2Cursor,
        statement: str,
        parameters: Optional[Any],
        context: Optional[Any] = None,
    ) -> None:
        """
        Execute statement using multiple sets of parameters.

        Parameters
        ----------
        cursor : Cursor
            The cursor to execute statements on
        statement : str
            SQL code
        parameters : dict or list, optional
            Parameters to substitute into SQL code
        context : SingleStoreExecutionContext, optional
            Execution context

        """
        rowcount = cursor.executemany(statement, parameters)
        if context is not None:
            context._rowcount = rowcount

    def _check_unicode_returns(self, connection: Connection) -> bool:
        """Check if connection returns unicode."""
        # work around issue fixed in
        # https://github.com/farcepest/MySQLdb1/commit/cd44524fef63bd3fcb71947392326e9742d520e8
        # specific issue w/ the utf8mb4_bin collation and unicode returns

        collation = connection.exec_driver_sql(
            "show collation where %s = 'utf8mb4' and %s = 'utf8mb4_bin'"
            % (
                self.identifier_preparer.quote('Charset'),
                self.identifier_preparer.quote('Collation'),
            ),
        ).scalar()
        has_utf8mb4_bin = self.server_version_info > (5,) and collation
        if has_utf8mb4_bin:
            additional_tests = [
                sql.collate(
                    sql.cast(
                        sql.literal_column("'test collated returns'"),
                        TEXT(charset='utf8mb4'),
                    ),
                    'utf8mb4_bin',
                ),
            ]
        else:
            additional_tests = []
        return super(SingleStoreDialect_mysqldb, self)._check_unicode_returns(
            connection, additional_tests,
        )

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
            Dictionary mapping of parameters

        Returns
        -------
        list
            List containing ??? and updated connection options

        """
        # TODO: fix docstring
        if _translate_args is None:
            _translate_args = dict(
                database='db', username='user', password='passwd',
            )

        opts = url.translate_connect_args(**_translate_args)
        opts.update(url.query)

        util.coerce_kw_type(opts, 'compress', bool)
        util.coerce_kw_type(opts, 'connect_timeout', int)
        util.coerce_kw_type(opts, 'read_timeout', int)
        util.coerce_kw_type(opts, 'write_timeout', int)
        util.coerce_kw_type(opts, 'client_flag', int)
        util.coerce_kw_type(opts, 'local_infile', int)
        # Note: using either of the below will cause all strings to be
        # returned as Unicode, both in raw SQL operations and with column
        # types like String and MSString.
        util.coerce_kw_type(opts, 'use_unicode', bool)
        util.coerce_kw_type(opts, 'charset', str)

        # Rich values 'cursorclass' and 'conv' are not supported via
        # query string.

        ssl = {}
        keys = [
            ('ssl_ca', str),
            ('ssl_key', str),
            ('ssl_cert', str),
            ('ssl_capath', str),
            ('ssl_cipher', str),
            ('ssl_check_hostname', bool),
        ]
        for key, kw_type in keys:
            if key in opts:
                ssl[key[4:]] = opts[key]
                util.coerce_kw_type(ssl, key[4:], kw_type)
                del opts[key]
        if ssl:
            opts['ssl'] = ssl

        # FOUND_ROWS must be set in CLIENT_FLAGS to enable
        # supports_sane_rowcount.
        client_flag = opts.get('client_flag', 0)

        client_flag_found_rows = self._found_rows_client_flag()
        if client_flag_found_rows is not None:
            client_flag |= client_flag_found_rows
            opts['client_flag'] = client_flag
        return [[], opts]

    def _found_rows_client_flag(self) -> Optional[int]:
        """Return found_rows client flag."""
        if self.dbapi is not None:
            try:
                CLIENT_FLAGS = __import__(
                    self.dbapi.__name__ + '.constants.CLIENT',
                ).constants.CLIENT
            except (AttributeError, ImportError):
                return None
            else:
                return CLIENT_FLAGS.FOUND_ROWS
        else:
            return None

    def _extract_error_code(self, exception: Exception) -> int:
        """Extract error code from exception."""
        return exception.args[0]

    def _detect_charset(self, connection: Connection) -> str:
        """Sniff out the character set in use for connection results."""
        try:
            # note: the SQL here would be
            # "SHOW VARIABLES LIKE 'character_set%%'"
            cset_name = connection.connection.character_set_name
        except AttributeError:
            util.warn(
                "No 'character_set_name' can be detected with "
                'this MySQL-Python version; '
                'please upgrade to a recent version of MySQL-Python.  '
                'Assuming latin1.',
            )
            return 'latin1'
        else:
            return cset_name()

    def get_isolation_level_values(
        self,
        dbapi_connection: S2Connection,
    ) -> tuple[str, ...]:
        """Return valid transaction isolation level values."""
        return (
            'SERIALIZABLE',
            'READ UNCOMMITTED',
            'READ COMMITTED',
            'REPEATABLE READ',
            'AUTOCOMMIT',
        )

    def set_isolation_level(
        self,
        dbapi_connection: S2Connection,
        level: str,
    ) -> None:
        """
        Set transaction isolation level.

        Parameters
        ----------
        dbapi_connection : Connection
            Connection to set level on
        level : str
            Level to set

        See Also
        --------
        `get_isolation_level_values`


        """
        if level == 'AUTOCOMMIT':
            dbapi_connection.autocommit(True)
        else:
            dbapi_connection.autocommit(False)
            super(SingleStoreDialect_mysqldb, self).set_isolation_level(
                dbapi_connection, level,
            )


dialect = SingleStoreDialect_mysqldb
