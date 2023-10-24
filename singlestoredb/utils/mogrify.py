#!/usr/bin/env python3
from typing import Any
from typing import Dict
from typing import Optional
from typing import Sequence
from typing import Union

from ..mysql import converters
from ..mysql.constants import SERVER_STATUS


Encoders = converters.Encoders


def escape(
    obj: Any,
    charset: str = 'utf8',
    mapping: Optional[Encoders] = None,
    server_status: int = 0,
    binary_prefix: bool = False,
) -> str:
    """
    Escape whatever value is passed.

    Non-standard, for internal use; do not use this in your applications.

    """
    dtype = type(obj)
    if dtype is str or isinstance(obj, str):
        return "'{}'".format(escape_string(obj, server_status=server_status))
    if dtype is bytes or dtype is bytearray or isinstance(obj, (bytes, bytearray)):
        return _quote_bytes(
            obj,
            server_status=server_status,
            binary_prefix=binary_prefix,
        )
    if mapping is None:
        mapping = converters.encoders
    return converters.escape_item(obj, charset, mapping=mapping)


def literal(
    obj: Any,
    charset: str = 'utf8',
    encoders: Optional[Encoders] = None,
    server_status: int = 0,
    binary_prefix: bool = False,
) -> str:
    """
    Alias for escape().

    Non-standard, for internal use; do not use this in your applications.

    """
    return escape(
        obj, charset=charset, mapping=encoders,
        server_status=server_status, binary_prefix=binary_prefix,
    )


def escape_string(
    s: str,
    server_status: int = 0,
) -> str:
    """Escape a string value."""
    if server_status & SERVER_STATUS.SERVER_STATUS_NO_BACKSLASH_ESCAPES:
        return s.replace("'", "''")
    return converters.escape_string(s)


def _quote_bytes(
    s: bytes,
    server_status: int = 0,
    binary_prefix: bool = False,
) -> str:
    if server_status & SERVER_STATUS.SERVER_STATUS_NO_BACKSLASH_ESCAPES:
        if binary_prefix:
            return "_binary X'{}'".format(s.hex())
        return "X'{}'".format(s.hex())
    return converters.escape_bytes(s)


def _escape_args(
    args: Union[Sequence[Any], Dict[str, Any], None],
    charset: str = 'utf8',
    encoders: Optional[Encoders] = None,
    server_status: int = 0,
    binary_prefix: bool = False,
) -> Any:
    if encoders is None:
        encoders = converters.encoders

    if isinstance(args, (tuple, list)):
        return tuple(
            literal(
                arg, charset=charset, encoders=encoders,
                server_status=server_status,
                binary_prefix=binary_prefix,
            ) for arg in args
        )

    elif isinstance(args, dict):
        return {
            key: literal(
                val, charset=charset, encoders=encoders,
                server_status=server_status,
                binary_prefix=binary_prefix,
            ) for (key, val) in args.items()
        }

    # If it's not a dictionary let's try escaping it anyways.
    # Worst case it will throw a Value error
    return escape(
        args, charset=charset, mapping=encoders,
        server_status=server_status, binary_prefix=binary_prefix,
    )


def mogrify(
    query: Union[str, bytes],
    args: Union[Sequence[Any], Dict[str, Any], None] = None,
    charset: str = 'utf8',
    encoders: Optional[Encoders] = None,
    server_status: int = 0,
    binary_prefix: bool = False,
) -> Union[str, bytes]:
    """
    Returns the exact string sent to the database by calling the execute() method.

    This method follows the extension to the DB API 2.0 followed by Psycopg.

    Parameters
    ----------
    query : str
        Query to mogrify.
    args : Sequence[Any] or Dict[str, Any] or Any, optional
        Parameters used with query. (optional)

    Returns
    -------
    str : The query with argument binding applied.

    """
    if args:
        query = query % _escape_args(
            args, charset=charset,
            encoders=encoders,
            server_status=server_status,
            binary_prefix=binary_prefix,
        )
    return query
