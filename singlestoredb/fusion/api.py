#!/usr/bin/env python3
import os
import re
from typing import List
from typing import Optional
from typing import Tuple
from typing import Type
from typing import Union

from . import result
from .. import connection
from .. import manage_workspaces
from .handler import SQLHandler

# Handlers must be sorted from longest to shortest key string
_handlers: List[Tuple[str, Type[SQLHandler]]] = []


def _get_handler(sql: str) -> Optional[Type[SQLHandler]]:
    """
    Find a handler for the given SQL query.

    Parameters
    ----------
    sql : str
        The SQL query to search on

    Returns
    -------
    SQLHandler - if a matching one is found
    None - if no matching handler was found

    """
    m = re.match(r'^\s*((?:\w+(?:\s+|;|$))+)', sql)
    if not m:
        return None
    words = re.sub(r'\s+', r' ', m.group(1).strip()).upper()
    if words.endswith(';'):
        words = words[:-1]
    words = f'{words} '
    for k, v in _handlers:
        if words.startswith(k):
            return v
    return None


def register_handler(handler: Type[SQLHandler], overwrite: bool = False) -> None:
    """Register a new SQL handler."""
    handlers_dict = dict(_handlers)
    key = ' '.join(x.upper() for x in handler.command_key) + ' '
    if not overwrite and key in handlers_dict:
        raise ValueError(f'command already exists, use overwrite=True to override: {key}')
    handlers_dict[key] = handler
    _handlers[:] = list(sorted(handlers_dict.items(), key=lambda x: -len(x[0])))


def is_fusion_query(sql: Union[str, bytes]) -> Optional[Type[SQLHandler]]:
    """
    Is the SQL query part of the fusion interface?

    Parameters
    ----------
    sql : str or bytes
        The SQL query

    Returns
    -------
    SQLHandler - if a matching one exists
    None - if no matching handler could be found

    """
    if not os.environ.get('SINGLESTOREDB_ENABLE_FUSION', None):
        return None

    if isinstance(sql, (bytes, bytearray)):
        sql = sql.decode('utf-8')

    return _get_handler(sql)


def execute(
    connection: connection.Connection,
    sql: str,
    handler: Optional[Type[SQLHandler]] = None,
) -> result.DummySQLResult:
    """
    Execute a SQL query in the management interface.

    Parameters
    ----------
    connection : Connection
        The SingleStoreDB connection object
    sql : str
        The SQL query
    handler : SQLHandler, optional
        The handler to use for the commands. If not supplied, one will be
        looked up in the registry.

    Returns
    -------
    DummySQLResult

    """
    if not os.environ.get('SINGLESTOREDB_ENABLE_FUSION', None):
        raise RuntimeError('management API queries have not been enabled')

    if handler is None:
        handler = _get_handler(sql)
        if handler is None:
            raise RuntimeError(f'could not find handler for query: {sql}')

    manager = manage_workspaces(os.environ['SINGLESTOREDB_MANAGEMENT_TOKEN'])

    return handler(connection, manager).execute(sql)
