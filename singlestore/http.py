#!/usr/bin/env python
"""SingleStore HTTP API interface."""
from __future__ import annotations

import base64
import functools
import re
from collections.abc import Mapping
from collections.abc import Sequence
from typing import Any
from typing import Dict
from typing import Iterable
from typing import Optional
from typing import Union
from urllib.parse import urljoin

import requests

from . import types
from .converters import converters
from .exceptions import Error  # noqa: F401
from .exceptions import InterfaceError
from .exceptions import NotSupportedError
from .exceptions import OperationalError  # noqa: F401
from .utils.results import Result


# DB-API parameter style
paramstyle = 'qmark'


def b64decode_converter(converter: Any, x: Any, encoding: str = 'utf-8') -> Any:
    """Decode value before applying converter."""
    if x is None:
        return None
    if type(x) is str:
        return converter(base64.b64decode(x))
    return converter(base64.b64decode(str(x, encoding)))


class Cursor(object):
    """
    SingleStore HTTP database cursor.

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
        self.description: Optional[list[tuple[Any, ...]]] = None
        self.arraysize: int = 1000
        self.rowcount: int = 0
        self.messages: list[tuple[int, str]] = []
        self.lastrowid: Optional[int] = None

    def _get(self, path: str, *args: Any, **kwargs: Any) -> requests.Response:
        """
        Invoke a GET request on the HTTP connection.

        Parameters
        ----------
        path : str
            The path of the resource
        *args : positional parameters, optional
            Extra parameters to the GET request
        **kwargs : keyword parameters, optional
            Extra keyword parameters to the GET request

        Returns
        -------
        requests.Response

        """
        if self.connection is None:
            raise InterfaceError(0, 'connection is closed')
        return self.connection._get(path, *args, **kwargs)

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
            raise InterfaceError(0, 'connection is closed')
        return self.connection._post(path, *args, **kwargs)

    def _delete(self, path: str, *args: Any, **kwargs: Any) -> requests.Response:
        """
        Invoke a DELETE request on the HTTP connection.

        Parameters
        ----------
        path : str
            The path of the resource
        *args : positional parameters, optional
            Extra parameters to the DELETE request
        **kwargs : keyword parameters, optional
            Extra keyword parameters to the DELETE request

        Returns
        -------
        requests.Response

        """
        if self.connection is None:
            raise InterfaceError(0, 'connection is closed')
        return self.connection._delete(path, *args, **kwargs)

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
            raise InterfaceError(0, 'connection is closed')

        data: Dict[str, Any] = dict(sql=query)
        if params is not None:
            data['args'] = params
        if self.connection._database:
            data['database'] = self.connection._database

        sql_type = 'exec'
        if re.match(r'^\s*(select|show)\s+', query, flags=re.I):
            sql_type = 'query'

        if sql_type == 'query':
            res = self._post('query/tuples', json=data)
        else:
            res = self._post('exec', json=data)

        if res.status_code >= 400:
            if res.text:
                if ':' in res.text:
                    code, msg = res.text.split(':', 1)
                    icode = int(code.split()[-1])
                else:
                    icode = res.status_code
                    msg = res.text
                raise InterfaceError(icode, msg.strip())
            raise InterfaceError(res.status_code, 'HTTP Error')

        out = res.json()

        self.description = None
        self._results = [[]]
        self._row_idx = -1
        self._result_idx = -1
        self.rowcount = 0

        if sql_type == 'query':
            # description: (name, type_code, display_size, internal_size,
            #               precision, scale, null_ok, column_flags, ?)
            self.description = []
            convs = []
            for item in out['results'][0].get('columns', []):
                data_type = item['dataType'].split('(')[0]
                type_code = types.ColumnType.get_code(data_type)
                converter = converters[type_code]
                if 'BLOB' in data_type or 'BINARY' in data_type:
                    converter = functools.partial(b64decode_converter, converter)
                convs.append(converter)
                self.description.append((
                    item['name'], type_code,
                    None, None, None, None,
                    item.get('nullable', False), 0, 0,
                ))

            # Convert data to Python types
            self._results = [x['rows'] for x in out['results']]
            if self._results and self._results[0]:
                self._row_idx = 0
                self._result_idx = 0
            for result in self._results:
                for i, row in enumerate(result):
                    try:
                        result[i] = tuple(x(y) for x, y in zip(convs, row))
                    except ValueError:
                        print(self.description[i])
                        print(row)
                        raise

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
        results = []
        if param_seq:
            for params in param_seq:
                self.execute(query, params)
                if self._rows is not None:
                    results.append(self._rows)
            self._results = results
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
    ) -> Optional[Result]:
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
        if not size or int(size) <= 0:
            size = self.arraysize
        out = self._rows[self._row_idx:self._row_idx+size]
        self._row_idx += size
        return out

    def fetchall(self) -> Optional[Result]:
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
        if self._result_idx >= len(self._results):
            self._result_idx = -1
            return False
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
    SingleStore HTTP database connection.

    Instances of this object are typically created through the
    `connection` function rather than creating them directly.

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

    See Also
    --------
    `connect`

    """

    def __init__(
            self, host: Optional[str] = None, port: Optional[int] = None,
            user: Optional[str] = None, password: Optional[str] = None,
            database: Optional[str] = None, protocol: str = 'http', version: str = 'v1',
    ):
        host = host or 'localhost'
        port = port or 3306

        self._sess: Optional[requests.Session] = requests.Session()
        if user is not None and password is not None:
            self._sess.auth = (user, password)
        elif user is not None:
            self._sess.auth = (user, '')
        self._sess.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        })

        self._database = database
        self._url = f'{protocol}://{host}:{port}/api/{version}/'
        self.messages: list[list[Any]] = []
        self.autocommit: bool = True

    def _get(self, path: str, *args: Any, **kwargs: Any) -> requests.Response:
        """
        Invoke a GET request on the HTTP connection.

        Parameters
        ----------
        path : str
            The path of the resource
        *args : positional parameters, optional
            Extra parameters to the GET request
        **kwargs : keyword parameters, optional
            Extra keyword parameters to the GET request

        Returns
        -------
        requests.Response

        """
        if self._sess is None:
            raise InterfaceError(0, 'connection is closed')
        return self._sess.get(urljoin(self._url, path), *args, **kwargs)

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
            raise InterfaceError(0, 'connection is closed')
        return self._sess.post(urljoin(self._url, path), *args, **kwargs)

    def _delete(self, path: str, *args: Any, **kwargs: Any) -> requests.Response:
        """
        Invoke a DELETE request on the HTTP connection.

        Parameters
        ----------
        path : str
            The path of the resource
        *args : positional parameters, optional
            Extra parameters to the DELETE request
        **kwargs : keyword parameters, optional
            Extra keyword parameters to the DELETE request

        Returns
        -------
        requests.Response

        """
        if self._sess is None:
            raise InterfaceError(0, 'connection is closed')
        return self._sess.delete(urljoin(self._url, path), *args, **kwargs)

    def close(self) -> None:
        """Close the connection."""
        self._sess = None

    def commit(self) -> None:
        """Commit the pending transaction."""
        if self.autocommit:
            return
        raise NotSupportedError(0, 'operation not supported')

    def rollback(self) -> None:
        """Rollback the pending transaction."""
        if self.autocommit:
            return
        raise NotSupportedError(0, 'operation not supported')

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

    def ping(self, reconnect: bool = False) -> None:
        """
        Check if the database server is still available.

        Parameters
        ----------
        reconnect : bool
            Should the server be reconnected?

        """
        if not self.is_connected():
            raise InterfaceError(2006, 'Could not connect to SingleStore database')


def connect(
    host: Optional[str] = None, port: Optional[int] = None,
    user: Optional[str] = None, password: Optional[str] = None,
    database: Optional[str] = None, protocol: str = 'http', version: str = 'v1',
) -> Connection:
    """
    Connect to a SingleStore database using HTTP.

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

    Returns
    -------
    Connection

    """
    return Connection(
        host=host, port=port, user=user, password=password,
        database=database, protocol=protocol, version=version,
    )
