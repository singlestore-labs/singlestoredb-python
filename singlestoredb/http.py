#!/usr/bin/env python
"""SingleStoreDB HTTP API interface."""
from __future__ import annotations

import functools
import json
import re
from base64 import b64decode
from collections.abc import Mapping
from collections.abc import Sequence
from typing import Any
from typing import Callable
from typing import Dict
from typing import Iterable
from typing import Optional
from typing import Tuple
from typing import Union
from urllib.parse import urljoin

import requests

from . import types
from .config import get_option
from .converters import converters
from .exceptions import DatabaseError  # noqa: F401
from .exceptions import DataError
from .exceptions import Error
from .exceptions import IntegrityError
from .exceptions import InterfaceError
from .exceptions import InternalError
from .exceptions import NotSupportedError
from .exceptions import OperationalError
from .exceptions import ProgrammingError
from .exceptions import Warning  # noqa: F401
from .utils.convert_rows import convert_rows
from .utils.results import Result


# DB-API settings
apilevel = '2.0'
paramstyle = 'qmark'
threadsafety = 1


Description = Tuple[
    str, int, Optional[int], Optional[int], Optional[int],
    Optional[int], bool, Optional[int], Optional[int],
]


_interface_errors = set([
    0,
    2013,  # CR_SERVER_LOST
    2006,  # CR_SERVER_GONE_ERROR
    2012,  # CR_HANDSHAKE_ERR
    2004,  # CR_IPSOCK_ERROR
    2014,  # CR_COMMANDS_OUT_OF_SYNC
])
_data_errors = set([
    1406,  # ER_DATA_TOO_LONG
    1441,  # ER_DATETIME_FUNCTION_OVERFLOW
    1365,  # ER_DIVISION_BY_ZERO
    1230,  # ER_NO_DEFAULT
    1171,  # ER_PRIMARY_CANT_HAVE_NULL
    1264,  # ER_WARN_DATA_OUT_OF_RANGE
    1265,  # ER_WARN_DATA_TRUNCATED
])
_programming_errors = set([
    1065,  # ER_EMPTY_QUERY
    1179,  # ER_CANT_DO_THIS_DURING_AN_TRANSACTION
    1007,  # ER_DB_CREATE_EXISTS
    1110,  # ER_FIELD_SPECIFIED_TWICE
    1111,  # ER_INVALID_GROUP_FUNC_USE
    1082,  # ER_NO_SUCH_INDEX
    1741,  # ER_NO_SUCH_KEY_VALUE
    1146,  # ER_NO_SUCH_TABLE
    1449,  # ER_NO_SUCH_USER
    1064,  # ER_PARSE_ERROR
    1149,  # ER_SYNTAX_ERROR
    1113,  # ER_TABLE_MUST_HAVE_COLUMNS
    1112,  # ER_UNSUPPORTED_EXTENSION
    1102,  # ER_WRONG_DB_NAME
    1103,  # ER_WRONG_TABLE_NAME
    1049,  # ER_BAD_DB_ERROR
    1582,  # ER_??? Wrong number of args
])
_integrity_errors = set([
    1215,  # ER_CANNOT_ADD_FOREIGN
    1062,  # ER_DUP_ENTRY
    1169,  # ER_DUP_UNIQUE
    1364,  # ER_NO_DEFAULT_FOR_FIELD
    1216,  # ER_NO_REFERENCED_ROW
    1452,  # ER_NO_REFERENCED_ROW_2
    1217,  # ER_ROW_IS_REFERENCED
    1451,  # ER_ROW_IS_REFERENCED_2
    1460,  # ER_XAER_OUTSIDE
    1401,  # ER_XAER_RMERR
    1048,  # ER_BAD_NULL_ERROR
    1264,  # ER_DATA_OUT_OF_RANGE
    4025,  # ER_CONSTRAINT_FAILED
    1826,  # ER_DUP_CONSTRAINT_NAME
])


def get_exc_type(code: int) -> type:
    """Map error code to DB-API error type."""
    if code in _interface_errors:
        return InterfaceError
    if code in _data_errors:
        return DataError
    if code in _programming_errors:
        return ProgrammingError
    if code in _integrity_errors:
        return IntegrityError
    if code >= 1000:
        return OperationalError
    return InternalError


def identity(x: Any) -> Any:
    """Return input value."""
    return x


def b64decode_converter(
    converter: Callable[..., Any],
    x: Optional[str],
    encoding: str = 'utf-8',
) -> Optional[bytes]:
    """Decode value before applying converter."""
    if x is None:
        return None
    if converter is None:
        return b64decode(x)
    return converter(b64decode(x))


class Cursor(object):
    """
    SingleStoreDB HTTP database cursor.

    Cursor objects should not be created directly. They should come from
    the `cursor` method on the `Connection` object.

    Parameters
    ----------
    connection : Connection
        The HTTP Connection object the cursor belongs to

    """

    def __init__(self, connection: Connection):
        self.connection: Optional[Connection] = connection
        self._results: list[list[tuple[Any, ...]]] = [[]]
        self._row_idx: int = -1
        self._result_idx: int = -1
        self._descriptions: list[list[Description]] = []
        self.arraysize: int = 1000
        self.rowcount: int = 0
        self.messages: list[tuple[int, str]] = []
        self.lastrowid: Optional[int] = None

    @property
    def description(self) -> Optional[list[Description]]:
        """Return description for current result set."""
        if not self._descriptions:
            return None
        if self._result_idx >= 0 and self._result_idx < len(self._descriptions):
            return self._descriptions[self._result_idx]
        return None

    def _post(self, path: str, *args: Any, **kwargs: Any) -> requests.Response:
        """
        Invoke a POST request on the HTTP connection.

        Parameters
        ----------
        path : str
            The path of the resource
        *args : positional parameters, optional
            Extra parameters to the POST request
        **kwargs : keyword parameters, optional
            Extra keyword parameters to the POST request

        Returns
        -------
        requests.Response

        """
        if self.connection is None:
            raise InterfaceError(errno=2048, msg='Connection is closed.')
        return self.connection._post(path, *args, **kwargs)

    def callproc(
        self, name: str,
        params: Union[Sequence[Any], Mapping[str, Any]],
    ) -> None:
        """
        Call a stored procedure.

        Parameters
        ----------
        name : str
            Name of the stored procedure
        params : iterable or dict, optional
            Parameters to the stored procedure

        """
        if self.connection is None:
            raise InterfaceError(errno=2048, msg='Connection is closed.')
        raise NotImplementedError

    def close(self) -> None:
        """Close the cursor."""
        if self.connection is not None:
            self.connection = None

    def execute(
        self, query: str,
        params: Optional[Union[Sequence[Any], Mapping[str, Any]]] = None,
    ) -> None:
        """
        Execute a SQL statement.

        Parameters
        ----------
        oper : str
            The SQL statement to execute
        params : iterable or dict, optional
            Parameters to substitute into the SQL code

        """
        if self.connection is None:
            raise InterfaceError(errno=2048, msg='Connection is closed.')

        data: Dict[str, Any] = dict(sql=query)
        if params is not None:
            data['args'] = params
        if self.connection._database:
            data['database'] = self.connection._database

        sql_type = 'exec'
        if re.match(r'^\s*(select|show|call|echo)\s+', query, flags=re.I):
            sql_type = 'query'

        if sql_type == 'query':
            res = self._post('query/tuples', json=data)
        else:
            res = self._post('exec', json=data)

        if res.status_code >= 400:
            if res.text:
                if re.match(r'^Error\s+\d+:', res.text):
                    code, msg = res.text.split(':', 1)
                    icode = int(code.split()[-1])
                else:
                    icode = res.status_code
                    msg = res.text
                raise get_exc_type(icode)(icode, msg.strip())
            raise InterfaceError(errno=res.status_code, msg='HTTP Error')

        out = json.loads(res.text)

        self._descriptions = []
        self._results = []
        self._row_idx = -1
        self._result_idx = -1
        self.rowcount = 0

        if sql_type == 'query':
            # description: (name, type_code, display_size, internal_size,
            #               precision, scale, null_ok, column_flags, charset)

            # Remove converters for things the JSON parser already converted
            http_converters = dict(converters)
            http_converters.pop(4, None)
            http_converters.pop(5, None)
            http_converters.pop(6, None)
            http_converters.pop(15, None)
            http_converters.pop(245, None)
            http_converters.pop(247, None)
            http_converters.pop(249, None)
            http_converters.pop(250, None)
            http_converters.pop(251, None)
            http_converters.pop(252, None)
            http_converters.pop(253, None)
            http_converters.pop(254, None)

            results = out['results']

            # Convert data to Python types
            if results and results[0]:
                self._row_idx = 0
                self._result_idx = 0

                for result in results:

                    convs = []

                    description: list[Description] = []
                    for i, col in enumerate(result.get('columns', [])):
                        data_type = col['dataType'].split('(')[0]
                        type_code = types.ColumnType.get_code(data_type)
                        converter = http_converters.get(type_code, None)
                        if data_type.endswith('BLOB') or data_type.endswith('BINARY'):
                            converter = functools.partial(b64decode_converter, converter)
                        if type_code == 0:  # DECIMAL
                            type_code = types.ColumnType.get_code('NEWDECIMAL')
                        elif type_code == 15:  # VARCHAR / VARBINARY
                            type_code = types.ColumnType.get_code('VARSTRING')
                        if converter is not None:
                            convs.append((i, None, converter))
                        description.append((
                            col['name'], type_code,
                            None, None, None, None,
                            col.get('nullable', False), 0, 0,
                        ))
                    self._descriptions.append(description)

                    rows = convert_rows(result.get('rows', []), convs)

                    self._results.append(rows)

            self.rowcount = len(self._results[0])
        else:
            self.rowcount = out['rowsAffected']

    def executemany(
        self, query: str,
        param_seq: Optional[
            Sequence[
                Union[
                    Sequence[Any],
                    Mapping[str, Any],
                ]
            ]
        ] = None,
    ) -> None:
        """
        Execute SQL code against multiple sets of parameters.

        Parameters
        ----------
        query : str
            The SQL statement to execute
        params_seq : iterable of iterables or dicts, optional
            Sets of parameters to substitute into the SQL code

        """
        if self.connection is None:
            raise InterfaceError(errno=2048, msg='Connection is closed.')

        results = []
        if param_seq:
            description = []
            for params in param_seq:
                self.execute(query, params)
                if self._descriptions:
                    description = self._descriptions[-1]
                if self._rows is not None:
                    results.append(self._rows)
            self._results = results
            self._descriptions = [description for _ in range(len(results))]
            if self._results:
                self.rowcount = len(self._results[0])
        else:
            self.execute(query)

    @property
    def _has_row(self) -> bool:
        """Determine if a row is available."""
        if self._result_idx < 0 or self._result_idx >= len(self._results):
            return False
        if self._row_idx < 0 or self._row_idx >= len(self._results[self._result_idx]):
            return False
        return True

    @property
    def _rows(self) -> list[tuple[Any, ...]]:
        """Return current set of rows."""
        if not self._has_row:
            return []
        return self._results[self._result_idx]

    def fetchone(self) -> Optional[Result]:
        """
        Fetch a single row from the result set.

        Returns
        -------
        tuple
            Values of the returned row if there are rows remaining
        None
            If there are no rows left to return

        """
        if not self._has_row:
            return None
        out = self._rows[self._row_idx]
        self._row_idx += 1
        return out

    def fetchmany(
        self,
        size: Optional[int] = None,
    ) -> Result:
        """
        Fetch `size` rows from the result.

        If `size` is not specified, the `arraysize` attribute is used.

        Returns
        -------
        list of tuples
            Values of the returned rows if there are rows remaining

        """
        if not self._has_row:
            return []
        if not size:
            size = max(int(self.arraysize), 1)
        else:
            size = max(int(size), 1)
        out = self._rows[self._row_idx:self._row_idx+size]
        self._row_idx += size
        return out

    def fetchall(self) -> Result:
        """
        Fetch all rows in the result set.

        Returns
        -------
        list of tuples
            Values of the returned rows if there are rows remaining

        """
        if not self._has_row:
            return []
        out = list(self._rows)
        self._row_idx = -1
        return out

    def nextset(self) -> Optional[bool]:
        """Skip to the next available result set."""
        if self._result_idx < 0:
            self._row_idx = -1
            return False

        self._result_idx += 1
        self._row_idx = 0

        if self._result_idx >= len(self._results):
            self._result_idx = -1
            self._row_idx = -1
            return False

        self.rowcount = len(self._results[self._result_idx])

        return True

    def setinputsizes(self, sizes: Sequence[int]) -> None:
        """Predefine memory areas for parameters."""
        pass

    def setoutputsize(self, size: int, column: Optional[str] = None) -> None:
        """Set a column buffer size for fetches of large columns."""
        pass

    @ property
    def rownumber(self) -> Optional[int]:
        """
        Return the zero-based index of the cursor in the result set.

        Returns
        -------
        int

        """
        if self._row_idx < 0:
            return None
        return self._row_idx

    def scroll(self, value: int, mode: str = 'relative') -> None:
        """
        Scroll the cursor to the position in the result set.

        Parameters
        ----------
        value : int
            Value of the positional move
        mode : str
            Type of move that should be made: 'relative' or 'absolute'

        """
        if mode == 'relative':
            self._row_idx += value
        elif mode == 'absolute':
            self._row_idx = value
        else:
            raise ValueError(
                f'{mode} is not a valid mode, '
                'expecting "relative" or "absolute"',
            )

    def next(self) -> Optional[Result]:
        """
        Return the next row from the result set for use in iterators.

        Returns
        -------
        tuple
            Values from the next result row
        None
            If no more rows exist

        """
        out = self.fetchone()
        if out is None:
            raise StopIteration
        return out

    __next__ = next

    def __iter__(self) -> Iterable[tuple[Any, ...]]:
        """Return result iterator."""
        return iter(self._rows)

    def __enter__(self) -> Cursor:
        """Enter a context."""
        return self

    def __exit__(
        self, exc_type: Optional[object],
        exc_value: Optional[Exception], exc_traceback: Optional[str],
    ) -> None:
        """Exit a context."""
        self.close()

    def is_connected(self) -> bool:
        """
        Check if the cursor is still connected.

        Returns
        -------
        bool

        """
        if self.connection is None:
            return False
        return self.connection.is_connected()


class Connection(object):
    """
    SingleStoreDB HTTP database connection.

    Instances of this object are typically created through the
    `connection` function rather than creating them directly.

    See Also
    --------
    `connect`

    """

    Warning = Warning
    Error = Error
    InterfaceError = InterfaceError
    DatabaseError = DatabaseError
    DataError = DataError
    OperationalError = OperationalError
    IntegrityError = IntegrityError
    InternalError = InternalError
    ProgrammingError = ProgrammingError
    NotSupportedError = NotSupportedError

    def __init__(self, **kwargs: Any):
        host = kwargs.get('host', get_option('host'))
        port = kwargs.get('port', get_option('http_port'))

        self._sess: Optional[requests.Session] = requests.Session()

        user = kwargs.get('user', get_option('user'))
        password = kwargs.get('password', get_option('password'))
        if user is not None and password is not None:
            self._sess.auth = (user, password)
        elif user is not None:
            self._sess.auth = (user, '')
        self._sess.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Accept-Encoding': 'compress,identity',
        })

        if kwargs.get('ssl_disabled', get_option('ssl_disabled')):
            self._sess.verify = False
        else:
            ssl_key = kwargs.get('ssl_key', get_option('ssl_key'))
            ssl_cert = kwargs.get('ssl_cert', get_option('ssl_cert'))
            if ssl_key and ssl_cert:
                self._sess.cert = (ssl_key, ssl_cert)
            elif ssl_cert:
                self._sess.cert = ssl_cert

            ssl_ca = kwargs.get('ssl_ca', get_option('ssl_ca'))
            if ssl_ca:
                self._sess.verify = ssl_ca

        version = kwargs.get('version', 'v1')
        protocol = kwargs.get('protocol', 'https')

        self._database = kwargs.get('database', get_option('database'))
        self._url = f'{protocol}://{host}:{port}/api/{version}/'
        self.messages: list[list[Any]] = []
        self._autocommit: bool = True

    def _post(self, path: str, *args: Any, **kwargs: Any) -> requests.Response:
        """
        Invoke a POST request on the HTTP connection.

        Parameters
        ----------
        path : str
            The path of the resource
        *args : positional parameters, optional
            Extra parameters to the POST request
        **kwargs : keyword parameters, optional
            Extra keyword parameters to the POST request

        Returns
        -------
        requests.Response

        """
        if self._sess is None:
            raise InterfaceError(errno=2048, msg='Connection is closed.')
        return self._sess.post(urljoin(self._url, path), *args, **kwargs)

    def close(self) -> None:
        """Close the connection."""
        self._sess = None

    def autocommit(self, value: bool) -> None:
        """Set autocommit mode."""
        self._autocommit = value

    def commit(self) -> None:
        """Commit the pending transaction."""
        if self._autocommit:
            return
        raise NotSupportedError(msg='operation not supported')

    def rollback(self) -> None:
        """Rollback the pending transaction."""
        if self._autocommit:
            return
        raise NotSupportedError(msg='operation not supported')

    def cursor(self) -> Cursor:
        """
        Create a new cursor object.

        Returns
        -------
        Cursor

        """
        return Cursor(self)

    def __enter__(self) -> Connection:
        """Enter a context."""
        return self

    def __exit__(
        self, exc_type: Optional[object],
        exc_value: Optional[Exception], exc_traceback: Optional[str],
    ) -> None:
        """Exit a context."""
        self.close()

    def is_connected(self) -> bool:
        """
        Check if the database is still connected.

        Returns
        -------
        bool

        """
        if self._sess is None:
            return False
        url = '/'.join(self._url.split('/')[:3]) + '/ping'
        res = self._sess.get(url)
        if res.status_code <= 400 and res.text == 'pong':
            return True
        return False


def connect(
    host: Optional[str] = None, port: Optional[int] = None,
    user: Optional[str] = None, password: Optional[str] = None,
    database: Optional[str] = None, protocol: str = 'https', version: str = 'v1',
    ssl_key: Optional[str] = None, ssl_cert: Optional[str] = None,
    ssl_ca: Optional[str] = None, ssl_disabled: Optional[bool] = None,
) -> Connection:
    """
    Connect to a SingleStoreDB using HTTP.

    Parameters
    ----------
    user : str, optional
        Database user name
    password : str, optional
        Database user password
    host : str, optional
        Database host name or IP address
    port : int, optional
        Database port. This defaults to 3306 for non-HTTP connections, 80
        for HTTP connections, and 443 for HTTPS connections.
    database : str, optional
        Database name
    protocol : str, optional
        HTTP protocol: `http` or `https`
    version : str, optional
        Version of the HTTP API
    ssl_key : str, optional
        File containing SSL key
    ssl_cert : str, optional
        File containing SSL certificate
    ssl_ca : str, optional
        File containing SSL certificate authority
    ssl_disabled : bool, optional
        Disable SSL usage

    Returns
    -------
    Connection

    """
    return Connection(**dict(locals()))
