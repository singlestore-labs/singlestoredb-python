#!/usr/bin/env python
from __future__ import annotations

from typing import Any
from typing import Optional

from sqlalchemy.engine import URL

from .. import http
from .base import SingleStoreDialect


class SingleStoreDialect_http(SingleStoreDialect):
    driver = 'http'
    default_paramstyle = 'qmark'

    @classmethod
    def dbapi(cls) -> Any:
        return http

    def create_connect_args(self, url: URL) -> list[Any]:
        opts = url.translate_connect_args(username='user')
        opts.update(url.query)
        return [[], opts]

    def do_ping(self, dbapi_connection: http.Connection) -> bool:
        try:
            dbapi_connection.ping(False)
        except self.dbapi.Error as err:  # type: ignore
            if self.is_disconnect(err, dbapi_connection, None):
                return False
            else:
                raise
        else:
            return True

    def is_disconnect(
        self,
        e: Any,
        connection: http.Connection,
        cursor: Optional[http.Cursor],
    ) -> bool:
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

    def _detect_charset(self, connection: http.Connection) -> str:
        """Sniff out the character set in use for connection results."""
        return 'utf-8'


dialect = SingleStoreDialect_http
