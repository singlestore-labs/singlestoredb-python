#!/usr/bin/env python3
import re
import sys
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Type
from typing import Union

from . import result
from .. import connection
from ..config import get_option
from .handler import SQLHandler

_handlers: Dict[str, Type[SQLHandler]] = {}
_handlers_re: Optional[Any] = None


def register_handler(handler: Type[SQLHandler], overwrite: bool = False) -> None:
    """
    Register a new SQL handler.

    Parameters
    ----------
    handler : SQLHandler subclass
        The handler class to register
    overwrite : bool, optional
        Should an existing handler be overwritten if it uses the same command key?

    """
    global _handlers
    global _handlers_re

    # Build key for handler
    key = ' '.join(x.upper() for x in handler.command_key)

    # Check for existing handler with same key
    if not overwrite and key in _handlers:
        raise ValueError(f'command already exists, use overwrite=True to override: {key}')

    # Add handler to registry
    _handlers[key] = handler

    # Build regex to detect fusion query
    keys = sorted(_handlers.keys(), key=lambda x: (-len(x), x))
    keys_str = '|'.join(x.replace(' ', '\\s+') for x in keys)
    _handlers_re = re.compile(f'^\\s*({keys_str})(?:\\s+|;|$)', flags=re.I)


def get_handler(sql: Union[str, bytes]) -> Optional[Type[SQLHandler]]:
    """
    Return a fusion handler for the given query.

    Parameters
    ----------
    sql : str or bytes
        The SQL query

    Returns
    -------
    SQLHandler - if a matching one exists
    None - if no matching handler could be found

    """
    if not get_option('fusion.enabled'):
        return None

    if isinstance(sql, (bytes, bytearray)):
        sql = sql.decode('utf-8')

    if _handlers_re is None:
        return None

    m = _handlers_re.match(sql)
    if m:
        return _handlers[re.sub(r'\s+', r' ', m.group(1).strip().upper())]

    return None


def execute(
    connection: connection.Connection,
    sql: str,
    handler: Optional[Type[SQLHandler]] = None,
) -> result.FusionSQLResult:
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
    FusionSQLResult

    """
    if not get_option('fusion.enabled'):
        raise RuntimeError('management API queries have not been enabled')

    if handler is None:
        handler = get_handler(sql)
        if handler is None:
            raise RuntimeError(f'could not find handler for query: {sql}')

    return handler(connection).execute(sql)


class ShowFusionCommandsHandler(SQLHandler):
    """
    SHOW FUSION COMMANDS [ like ];

    # LIKE pattern
    like = LIKE '<pattern>'

    Description
    -----------
    Displays a list of all the Fusion commands.

    Arguments
    ---------
    * `<pattern>``: A pattern similar to SQL LIKE clause. Uses ``%`` as
      the wildcard character.

    Remarks
    -------
    * Use the ``LIKE`` clause to specify a pattern and return only the
      commands that match the specified pattern.

    Example
    -------
    The following command returns all the Fusion commands that start
    with 'SHOW'::

        SHOW FUSION COMMANDS LIKE 'SHOW%'

    See Also
    --------
    * ``SHOW FUSION HELP``

    """

    def run(self, params: Dict[str, Any]) -> Optional[result.FusionSQLResult]:
        res = result.FusionSQLResult()
        res.add_field('Command', result.STRING)

        data: List[Tuple[Any, ...]] = []
        for _, v in sorted(_handlers.items()):
            data.append((v.syntax.lstrip(),))

        res.set_rows(data)

        if params['like']:
            res = res.like(Command=params['like'])

        return res


ShowFusionCommandsHandler.register()


class ShowFusionGrammarHandler(SQLHandler):
    """
    SHOW FUSION GRAMMAR for_query;

    # Query to show grammar for
    for_query = FOR '<query>'

    Description
    -----------
    Show the full grammar of a Fusion SQL command for a given query.

    Arguments
    ---------
    * ``<command>``: A Fusion command.

    Example
    -------
    The following command displays the grammar for the
    ``CREATE WORKSPACE`` Fusion command::

        SHOW FUSION GRAMMAR FOR 'CREATE WORKSPACE';

    """

    def run(self, params: Dict[str, Any]) -> Optional[result.FusionSQLResult]:
        res = result.FusionSQLResult()
        res.add_field('Grammar', result.STRING)
        handler = get_handler(params['for_query'])
        data: List[Tuple[Any, ...]] = []
        if handler is not None:
            data.append((handler._grammar,))
        res.set_rows(data)
        return res


ShowFusionGrammarHandler.register()


class ShowFusionHelpHandler(SQLHandler):
    """
    SHOW FUSION HELP for_command;

    # Command to show help for
    for_command = FOR '<command>'

    Description
    -----------
    Displays the documentation for a Fusion command.

    Arguments
    ---------
    * ``<command>``: A Fusion command.

    Example
    -------
    The following command displays the documentation for
    the ``CREATE WORKSPACE`` Fusion command.

        SHOW FUSION HELP FOR 'CREATE WORKSPACE';

    """

    def run(self, params: Dict[str, Any]) -> Optional[result.FusionSQLResult]:
        handler = get_handler(params['for_command'])
        if handler is not None:
            try:
                from IPython.display import display
                from IPython.display import Markdown
                display(Markdown(handler.help))
            except Exception:
                print(handler.help)
        else:
            print(
                f'No handler found for command \'{params["for_command"]}\'',
                file=sys.stderr,
            )
        return None


ShowFusionHelpHandler.register()
