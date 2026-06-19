"""Mixin classes for SingleStoreDB extensions."""
from __future__ import annotations

from typing import Any
from typing import Literal
from typing import Protocol
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from contextlib import AbstractContextManager

    import ibis.expr.types as ir

    class _BackendProtocol(Protocol):
        """Protocol defining backend interface used by BackendExtensionsMixin."""

        _client: Any

        @property
        def current_database(self) -> str: ...
        def sql(self, query: str) -> ir.Table: ...
        def raw_sql(self, query: str) -> AbstractContextManager[Any]: ...

    class _TableProtocol(Protocol):
        """Protocol defining table interface used by TableExtensionsMixin."""

        def get_name(self) -> str: ...
        def op(self) -> Any: ...

    _BackendBase: type = _BackendProtocol
    _TableBase: type = _TableProtocol
else:
    _BackendBase = object
    _TableBase = object


def _quote_identifier(name: str) -> str:
    """Quote an identifier (table, database, column name) for safe SQL usage."""
    # Escape backticks by doubling them (MySQL/SingleStore convention)
    escaped = name.replace('`', '``')
    return f'`{escaped}`'


def _escape_string_literal(value: str) -> str:
    """Escape a string value for use in SQL string literals."""
    # Escape single quotes by doubling them, and escape backslashes
    return value.replace('\\', '\\\\').replace("'", "''")


def _get_table_backend_and_db(
    table: ir.Table,
    *,
    escape: Literal['identifier', 'literal'] | None = None,
) -> tuple[BackendExtensionsMixin, str]:
    """Get SingleStoreDB backend and database from table.

    Parameters
    ----------
    table
        The Ibis table object.
    escape
        How to escape the database name:
        - None: return unescaped
        - 'identifier': escape for SQL identifiers (backticks)
        - 'literal': escape for string literals (quotes)
    """
    op = table.op()
    if hasattr(op, 'source') and op.source.name == 'singlestoredb':
        db = getattr(getattr(op, 'namespace', None), 'database', None)
        db = db or op.source.current_database
        if escape == 'identifier':
            db = _quote_identifier(db)
        elif escape == 'literal':
            db = _escape_string_literal(db)
        return op.source, db  # type: ignore[return-value]
    raise TypeError(
        f'This method only works with SingleStoreDB tables, '
        f"got {getattr(op.source, 'name', 'unknown')} backend",
    )


class BackendExtensionsMixin(_BackendBase):
    """Mixin for SingleStoreDB Backend extensions."""

    __slots__ = ()

    # --- Variable/Show accessors from old ibis_singlestoredb package ---

    @property
    def show(self) -> Any:
        """Access to SHOW commands on the server."""
        return self._client.show

    @property
    def globals(self) -> Any:
        """Accessor for global variables in the server."""
        return self._client.globals

    @property
    def locals(self) -> Any:
        """Accessor for local variables in the server."""
        return self._client.locals

    @property
    def cluster_globals(self) -> Any:
        """Accessor for cluster global variables in the server."""
        return self._client.cluster_globals

    @property
    def cluster_locals(self) -> Any:
        """Accessor for cluster local variables in the server."""
        return self._client.cluster_locals

    @property
    def vars(self) -> Any:
        """Accessor for variables in the server."""
        return self._client.vars

    @property
    def cluster_vars(self) -> Any:
        """Accessor for cluster variables in the server."""
        return self._client.cluster_vars

    # --- New extension methods ---

    def get_storage_info(self, database: str | None = None) -> ir.Table:
        """Get storage statistics for tables in a database.

        Parameters
        ----------
        database
            Database name. Defaults to current database.

        Returns
        -------
        ir.Table
            Table with storage statistics.
        """
        db = _escape_string_literal(database or self.current_database)
        # S608: db is escaped via _escape_string_literal
        query = f"""
            SELECT * FROM information_schema.table_statistics
            WHERE database_name = '{db}'
        """  # noqa: S608
        return self.sql(query)

    def get_workload_metrics(self) -> ir.Table:
        """Get workload management metrics."""
        return self.sql(
            'SELECT * FROM information_schema.mv_workload_management_events',
        )


class TableExtensionsMixin(_TableBase):
    """Mixin for ir.Table extensions (SingleStoreDB only)."""

    __slots__ = ()

    def optimize(self) -> None:
        """Optimize this table (SingleStoreDB only)."""
        backend, db = _get_table_backend_and_db(self, escape='identifier')
        table = _quote_identifier(self.get_name())
        with backend.raw_sql(f'OPTIMIZE TABLE {db}.{table} FULL'):
            pass

    def get_stats(self) -> ir.Table:
        """Get statistics for this table (SingleStoreDB only)."""
        backend, db = _get_table_backend_and_db(self, escape='literal')
        table = _escape_string_literal(self.get_name())
        # S608: db and table are escaped via _escape_string_literal
        return backend.sql(
            f"""
            SELECT * FROM information_schema.table_statistics
            WHERE database_name = '{db}' AND table_name = '{table}'
        """,  # noqa: S608
        )

    def get_column_statistics(self, column: str | None = None) -> ir.Table:
        """Get column statistics (SingleStoreDB only).

        Parameters
        ----------
        column
            Specific column name, or None for all columns.
        """
        backend, db = _get_table_backend_and_db(self, escape='identifier')
        table = _quote_identifier(self.get_name())
        query = f'SHOW COLUMNAR_SEGMENT_INDEX ON {db}.{table}'
        if column:
            query += f' COLUMNS ({_quote_identifier(column)})'
        return backend.sql(query)
