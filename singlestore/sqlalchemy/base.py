#!/usr/bin/env python
from __future__ import annotations

from sqlalchemy import util
from sqlalchemy.dialects.mysql.base import BIT  # noqa: F401
from sqlalchemy.dialects.mysql.base import MySQLCompiler
from sqlalchemy.dialects.mysql.base import MySQLDDLCompiler
from sqlalchemy.dialects.mysql.base import MySQLDialect
from sqlalchemy.dialects.mysql.base import MySQLExecutionContext
from sqlalchemy.dialects.mysql.base import MySQLIdentifierPreparer
from sqlalchemy.dialects.mysql.base import MySQLTypeCompiler
from sqlalchemy.dialects.mysql.base import TEXT  # noqa: F401

from . import reflection


class SingleStoreExecutionContext(MySQLExecutionContext):
    pass


class SingleStoreCompiler(MySQLCompiler):
    pass


class SingleStoreDDLCompiler(MySQLDDLCompiler):
    pass


class SingleStoreTypeCompiler(MySQLTypeCompiler):
    pass


class SingleStoreIdentifierPreparer(MySQLIdentifierPreparer):
    pass


class SingleStoreDialect(MySQLDialect):

    name = 'singlestore'

    statement_compiler = SingleStoreCompiler
    ddl_compiler = SingleStoreDDLCompiler
    type_compiler = SingleStoreTypeCompiler
    preparer = SingleStoreIdentifierPreparer

    @util.memoized_property
    def _tabledef_parser(self) -> reflection.SingleStoreTableDefinitionParser:
        """return the MySQLTableDefinitionParser, generate if needed.

        The deferred creation ensures that the dialect has
        retrieved server version information first.

        """
        from . import reflection
        preparer = self.identifier_preparer
        return reflection.SingleStoreTableDefinitionParser(self, preparer)
