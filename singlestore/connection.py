#!/usr/bin/env python
"""SingleStore database connections and cursors."""
from __future__ import annotations

import inspect
import re
from collections.abc import Mapping
from collections.abc import Sequence
from typing import Any
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional
from typing import Union
from urllib.parse import parse_qs
from urllib.parse import urlparse

import sqlparams

from . import drivers
from . import exceptions
from . import types
from .config import get_option
from .drivers.base import Driver
from .utils.results import Description
from .utils.results import format_results
from .utils.results import Result


# DB-API settings
apilevel = '2.0'
threadsafety = 1
paramstyle = map_paramstyle = 'named'
positional_paramstyle = 'numeric'


def cast_bool_param(val: Any) -> bool:
    """Cast value to a bool."""
    if val is None or val is False:
        return False

    if val is True:
        return True

    # Test ints
    try:
        return int(val) != 0
    except Exception:
        pass

    # Lowercase strings
    if hasattr(val, 'lower'):
        return val.lower() in ['on', 't', 'true', 'y', 'yes', 'enabled', 'enable']

    raise TypeError('Unrecognized type for bool: {}'.format(val))


def build_params(**kwargs: Any) -> Dict[str, Any]:
    """
    Construct connection parameters from given URL and arbitrary parameters.

    Parameters
    ----------
    **kwargs : keyword-parameters, optional
        Arbitrary keyword parameters corresponding to connection parameters

    Returns
    -------
    dict

    """
    out: Dict[str, Any] = {}

    kwargs = {k: v for k, v in kwargs.items() if v is not None}

    # Set known parameters
    out['host'] = kwargs.get('host', get_option('host'))
    out['port'] = kwargs.get('port', None)
    out['database'] = kwargs.get('database', get_option('database'))
    out['user'] = kwargs.get('user', get_option('user'))
    out['password'] = kwargs.get('password', get_option('password'))
    out['driver'] = kwargs.get('driver', get_option('driver'))
    out['pure_python'] = kwargs.get('pure_python', get_option('pure_python'))
    out['local_infile'] = kwargs.get('local_infile', get_option('local_infile'))
    out['charset'] = kwargs.get('charset', get_option('charset'))
    out['odbc_driver'] = kwargs.get('odbc_driver', get_option('odbc_driver'))

    # See if host actually contains a URL; definitely not a perfect test.
    host = out['host']
    if host and (':' in host or '/' in host or '@' in host or '?' in host):
        out.update(_parse_url(host))

    out = _cast_params(out)

    # Set default port based on driver.
    if 'port' not in out or not out['port']:
        if out['driver'] == 'http':
            out['port'] = int(get_option('http_port') or 80)
        elif out['driver'] == 'https':
            out['port'] = int(get_option('http_port') or 443)
        else:
            out['port'] = int(get_option('port') or 3306)

    # If there is no user and the password is empty, remove the password key.
    if 'user' not in out and not out.get('password', None):
        out.pop('password', None)

    return out


def _get_param_types(func: Any) -> Dict[str, Any]:
    """
    Retrieve the types for the parameters to the given function.

    Note that if a parameter has multiple possible types, only the
    first one is returned.

    Parameters
    ----------
    func : callable
        Callable object to inspect the parameters of

    Returns
    -------
    dict

    """
    out = {}
    args = inspect.getfullargspec(func)
    for name in args.args:
        ann = args.annotations[name]
        if isinstance(ann, str):
            ann = eval(ann)
        if hasattr(ann, '__args__'):
            out[name] = ann.__args__[0]
        else:
            out[name] = ann
    return out


def _cast_params(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Cast known keys to appropriate values.

    Parameters
    ----------
    params : dict
        Dictionary of connection parameters

    Returns
    -------
    dict

    """
    param_types = _get_param_types(connect)
    out = {}
    for key, val in params.items():
        key = key.lower()
        if val is None:
            continue
        if key not in param_types:
            raise ValueError('Unrecognized connection parameter: {}'.format(key))
        dtype = param_types[key]
        if dtype is bool:
            val = cast_bool_param(val)
        else:
            val = dtype(val)
        out[key] = val
    return out


def _parse_url(url: str) -> Dict[str, Any]:
    """
    Parse a connection URL and return only the defined parts.


    Parameters
    ----------
    url : str
        The URL passed in can be a full URL or a partial URL. At a minimum,
        a host name must be specified. All other parts are optional.

    Returns
    -------
    dict

    """
    out: Dict[str, Any] = {}

    if '//' not in url:
        url = '//' + url

    parts = urlparse(url, scheme='singlestore', allow_fragments=True)

    url_db = parts.path
    if url_db.startswith('/'):
        url_db = url_db.split('/')[1].strip()
    url_db = url_db.split('/')[0].strip() or ''

    # Retrieve basic connection parameters
    out['host'] = parts.hostname or None
    out['port'] = parts.port or None
    out['database'] = url_db or None
    out['user'] = parts.username or None

    # Allow an empty string for password
    if out['user'] and parts.password is not None:
        out['password'] = parts.password

    if parts.scheme != 'singlestore':
        out['driver'] = parts.scheme.lower()

    # Convert query string to parameters
    out.update({k.lower(): v[-1] for k, v in parse_qs(parts.query).items()})

    return {k: v for k, v in out.items() if v is not None}


def wrap_exc(driver: Driver, exc: Exception) -> Exception:
    """
    Wrap a database exception with the SingleStore implementation

    Parameters
    ----------
    driver : Driver
        Driver for the connection
    exc : Exception
        Database exception to wrap

    Returns
    -------
    Exception

    """
    new_exc: Optional[type] = None
    if isinstance(exc, driver.dbapi.NotSupportedError):
        new_exc = exceptions.NotSupportedError
    elif isinstance(exc, driver.dbapi.ProgrammingError):
        new_exc = exceptions.ProgrammingError
    elif isinstance(exc, driver.dbapi.InternalError):
        new_exc = exceptions.InternalError
    elif isinstance(exc, driver.dbapi.IntegrityError):
        new_exc = exceptions.IntegrityError
    elif isinstance(exc, driver.dbapi.OperationalError):
        new_exc = exceptions.OperationalError
    elif isinstance(exc, driver.dbapi.DataError):
        new_exc = exceptions.DataError
    elif isinstance(exc, driver.dbapi.DatabaseError):
        new_exc = exceptions.DatabaseError
    elif isinstance(exc, driver.dbapi.InterfaceError):
        new_exc = exceptions.InterfaceError
    elif isinstance(exc, driver.dbapi.Error):
        new_exc = exceptions.Error
    elif isinstance(exc, driver.dbapi.Warning):
        new_exc = exceptions.Warning
    if new_exc is None:
        return exc
    errno: int = -1
    errmsg: str = str(exc)
    if hasattr(exc, 'args'):
        args = exc.args
        if len(args) > 1:
            errno = int(args[0])
            errmsg = args[1]
    return new_exc(getattr(exc, 'errno', errno), getattr(exc, 'errmsg', errmsg))


def _name_check(name: str) -> str:
    """
    Make sure the given name is a legal variable name.

    Parameters
    ----------
    name : str
        Name to check

    Returns
    -------
    str

    """
    name = name.strip()
    if not re.match(r'^[A-Za-z][\w+_]*$', name):
        raise ValueError('Name contains invalid characters')
    return name


class Cursor(object):
    """
    Database cursor for submitting commands and queries.

    This object should not be instantiated directly.
    The `Connection.cursor` method should be used.

    Parameters
    ----------
    connection : Connection
        The connection the cursor belongs to
    cursor : Cursor
        The Cursor object from the underlying MySQL package
    driver : Driver
        Driver of the current database connector

    Returns
    -------
    Cursor

    """

    arraysize: int = 100

    def __init__(
            self, connection: Connection, cursor: Any, driver: Driver,
    ):
        self.errorhandler = connection.errorhandler
        self._conn: Optional[Connection] = connection
        self._cursor = cursor
        self._driver = driver
        self._rownumber = None
        self.description: Optional[List[Description]] = None
        self.arraysize = type(self).arraysize

    @property
    def connection(self) -> Optional[Connection]:
        """
        Return the connection that the cursor belongs to.

        Returns
        -------
        Connection or None

        """
        return self._conn

    def _set_description(self) -> None:
        """
        Return column descriptions for the current result set.

        Returns
        -------
        list of Description

        """
        if self._cursor.description:
            out = []
            for item in self._cursor.description:
                item = list(item)
                item[1] = types.ColumnType.get_code(item[1])
                item[6] = not(not(item[6]))
                out.append(Description(*item[:7]))
            self.description = out

    @property
    def rowcount(self) -> int:
        """
        Return the number of rows the last execute produced or affected.

        Returns
        -------
        int

        """
        return getattr(self._cursor, 'rowcount', getattr(self._cursor, '_rowcount', -1))

    def callproc(
        self, name: str,
        params: Optional[Union[Sequence[Any], Mapping[str, Any]]] = None,
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
        try:
            self._cursor.callproc(name, params)
        except Exception as exc:
            raise wrap_exc(self._driver, exc)

    def close(self) -> None:
        """Close the cursor."""
        try:
            self._cursor.close()
        # Ignore weak reference errors. It just means the connection
        # was closed underneath us.
        except ReferenceError:
            pass
        except Exception as exc:
            raise wrap_exc(self._driver, exc)
        self._conn = None

    def execute(
        self, oper: str,
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
        self.description = None
        self.rownumber = None
        self._format = get_option('results.format')

        default: Union[Sequence[Any], Mapping[str, Any]] = []
        if params is not None:
            default = type(params)()

        param_converter = sqlparams.SQLParams(
            isinstance(params, Mapping) and map_paramstyle or positional_paramstyle,
            self._driver.dbapi.paramstyle,
        )

        try:
            self._cursor.execute(*param_converter.format(oper, params or default))
        except Exception as exc:
            raise wrap_exc(self._driver, exc)

        self._set_description()

        self.rownumber = 0

    def executemany(
        self, oper: str,
        param_seq: Optional[Sequence[Union[Sequence[Any], Mapping[str, Any]]]] = None,
    ) -> None:
        """
        Execute SQL code against multiple sets of parameters.

        Parameters
        ----------
        oper : str
            The SQL statement to execute
        params_seq : iterable of iterables or dicts, optional
            Sets of parameters to substitute into the SQL code

        """
        self.description = None
        self.rownumber = None
        self._format = get_option('results.format')

        param_seq = param_seq or [[]]

        param_converter = sqlparams.SQLParams(
            isinstance(
                param_seq[0], Mapping,
            ) and map_paramstyle or positional_paramstyle,
            self._driver.dbapi.paramstyle,
        )

        try:
            self._cursor.executemany(*param_converter.formatmany(oper, param_seq))
        except Exception as exc:
            raise wrap_exc(self._driver, exc)

        self._set_description()

        self.rownumber = 0

    def fetchone(self) -> Optional[Result]:
        """
        Fetch a single row from the result set.

        Returns
        -------
        tuple
            Values of the returned row if there are rows remaining

        """
        try:
            out = self._cursor.fetchone()
        except Exception as exc:
            raise wrap_exc(self._driver, exc)

        if out is not None and self.rownumber is not None:
            self.rownumber += 1

        return format_results(self._format, self.description or [], out, single=True)

    def fetchmany(self, size: Optional[int] = None) -> Result:
        """
        Fetch `size` rows from the result.

        If `size` is not specified, the `arraysize` attribute is used.

        Returns
        -------
        list of tuples
            Values of the returned rows if there are rows remaining

        """
        if size is not None:
            size = max(int(size), 1)
        else:
            size = max(int(self.arraysize), 1)
        try:
            if size == 1:
                out = [self._cursor.fetchone()]
            else:
                out = self._cursor.fetchmany(size=size)
        except Exception as exc:
            raise wrap_exc(self._driver, exc)

        formatted: Result = format_results(
            self._format, self.description or [], out,
        ) or []
        if self.rownumber is not None:
            self.rownumber += len(formatted)
        return formatted

    def fetchall(self) -> Result:
        """
        Fetch all rows in the result set.

        Returns
        -------
        list of tuples
            Values of the returned rows if there are rows remaining
        None
            If there are no rows to return

        """
        try:
            out = self._cursor.fetchall()
        except Exception as exc:
            raise wrap_exc(self._driver, exc)

        formatted: Result = format_results(
            self._format, self.description or [], out,
        ) or []
        if self.rownumber is not None:
            self.rownumber += len(formatted)
        return formatted

    def nextset(self) -> Optional[bool]:
        """Skip to the next available result set."""
        self.rownumber = None
        try:
            out = self._cursor.nextset()
            self.rownumber = 0
            return out
        except Exception as exc:
            raise wrap_exc(self._driver, exc)

    def setinputsizes(self, sizes: Sequence[int]) -> None:
        """Predefine memory areas for parameters."""
        try:
            self._cursor.setinputsizes(sizes)
        except Exception as exc:
            raise wrap_exc(self._driver, exc)

    def setoutputsize(self, size: int, column: Optional[str] = None) -> None:
        """Set a column buffer size for fetches of large columns."""
        try:
            self._cursor.setoutputsize(size, column)
        except Exception as exc:
            raise wrap_exc(self._driver, exc)

    def scroll(self, value: int, mode: str = 'relative') -> None:
        """
        Scroll the cursor to the position in the result set.

        Parameters
        ----------
        value : int
            Value of the positional move
        mode : str
            Where to move the cursor from: 'relative' or 'absolute'

        """
        assert mode in ('relative', 'absolute')
        value = int(value)
        try:
            self._cursor.scroll(value, mode=mode)
            if self.rownumber is not None:
                if mode == 'relative':
                    self.rownumber += value
                else:
                    self.rownumber = value
        except Exception as exc:
            raise wrap_exc(self._driver, exc)

    @property
    def messages(self) -> Sequence[tuple[int, str]]:
        """
        List of received messages.

        Returns
        -------
        list of tuples
            Tuples contain a numeric code and a message

        """
        return self._cursor.messages

    def next(self) -> Optional[Sequence[Any]]:
        """
        Return the next row from the result set for use in iterators.

        Returns
        -------
        tuple of values
            If a row of results exists
        None
            If there are no more results

        """
        try:
            return self._cursor.next()
        except Exception as exc:
            raise wrap_exc(self._driver, exc)

    __next__ = next

    def __iter__(self) -> Iterable[Sequence[Any]]:
        """Return result iterator."""
        return self._cursor.__iter__()

    @property
    def lastrowid(self) -> Optional[int]:
        """Return the rowid of the last modified row."""
        try:
            return self._cursor.lastrowid or 0
        except Exception as exc:
            raise wrap_exc(self._driver, exc)

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
        Check if the cursor is connected.

        Returns
        -------
        bool

        """
        if self._conn is None:
            return False
        return self._conn.is_connected()


class Connection(object):
    """
    SingleStore database connection.

    Instances of this object are typically created through the
    `connect` function rather than creating them directly.
    See the `connect` function for parameter definitions.


    Examples
    --------
    # Standard database connection
    >>> conn = s2.connect('me:p455w0rd@s2-host.com/my_db')

    # Connect to HTTP API on port 8080
    >>> conn = s2.connect('http://me:p455w0rd@s2-host.com:8080/my_db')

    See Also
    --------
    `connect`

    """

    Warning = exceptions.Warning
    Error = exceptions.Error
    InterfaceError = exceptions.InterfaceError
    DatabaseError = exceptions.DatabaseError
    OperationalError = exceptions.OperationalError
    IntegrityError = exceptions.IntegrityError
    InternalError = exceptions.InternalError
    ProgrammingError = exceptions.ProgrammingError
    NotSupportedError = exceptions.NotSupportedError

    def __init__(self, **kwargs: Any):
        kwargs = build_params(**kwargs)

        self._conn: Optional[Any] = None
        self.errorhandler = None
        self._autocommit: bool = False
        self._format: str = get_option('results.format')
        self.connection_params: Dict[str, Any] = build_params(**kwargs)

        drv_name = re.sub(r'^\w+\+', r'', self.connection_params['driver']).lower()
        self._driver = drivers.get_driver(drv_name, self.connection_params)

        try:
            self._conn = self._driver.connect()
        except Exception as exc:
            raise wrap_exc(self._driver, exc)

    def autocommit(self, value: bool = True) -> None:
        """Set autocommit mode."""
        self._autocommit = value

    def close(self) -> None:
        """Close the database connection."""
        if self._conn is None:
            return None
        try:
            self._conn.close()
        except Exception as exc:
            raise wrap_exc(self._driver, exc)
        finally:
            self._conn = None

    def commit(self) -> None:
        """Commit the pending transaction."""
        if self._conn is None:
            raise exceptions.InterfaceError(2048, 'Connection is closed.')
        try:
            self._conn.commit()
        except Exception as exc:
            raise wrap_exc(self._driver, exc)

    def rollback(self) -> None:
        """Rollback the pending transaction."""
        if self._conn is None:
            raise exceptions.InterfaceError(2048, 'Connection is closed.')
        try:
            self._conn.rollback()
        except Exception as exc:
            raise wrap_exc(self._driver, exc)

    def cursor(self) -> Cursor:
        """
        Create a new cursor object.

        Returns
        -------
        Cursor

        """
        if self._conn is None:
            raise exceptions.InterfaceError(2048, 'Connection is closed.')
        try:
            cur = self._conn.cursor()
        except Exception as exc:
            raise wrap_exc(self._driver, exc)
        return Cursor(self, cur, self._driver)

    @property
    def messages(self) -> Sequence[tuple[int, str]]:
        """
        Return messages generated by the connection.

        Returns
        -------
        list of tuples
            Each tuple contains an int code and a message

        """
        if self._conn is None:
            raise exceptions.InterfaceError(2048, 'Connection is closed.')
        return self._conn.messages

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
        Determine if the database is still connected.

        Returns
        -------
        bool

        """
        if self._conn is None:
            return False
        try:
            if hasattr(self._conn, 'is_connected'):
                return bool(self._conn.is_connected())
            if hasattr(self._conn, 'ping'):
                return not bool(self._conn.ping())
            return bool(self._conn.open)
        except Exception as exc:
            raise wrap_exc(self._driver, exc)
        return False

    def ping(self, reconnect: bool = False) -> bool:
        """
        Check to see if server is still available.

        Parameters
        ----------
        reconnect : bool
            Should a reconnection be attempted?

        Returns
        -------
        bool

        """
        try:
            return self.is_connected()
        except Exception:
            pass
        return False

    def set_global_var(self, **kwargs: Any) -> None:
        """
        Set one or more global variables in the database.

        Parameters
        ----------
        **kwargs : key-value pairs
            Keyword parameters specify the variable names and values to set

        """
        cur = self.cursor()
        for name, value in kwargs.items():
            if value is True:
                value = 'on'
            elif value is False:
                value = 'off'
            cur.execute('set global {}=?'.format(_name_check(name)), [value])

    def set_session_var(self, **kwargs: Any) -> None:
        """
        Set one or more session variables in the database.

        Parameters
        ----------
        **kwargs : key-value pairs
            Keyword parameters specify the variable names and values to set

        """
        cur = self.cursor()
        for name, value in kwargs.items():
            if value is True:
                value = 'on'
            elif value is False:
                value = 'off'
            cur.execute('set session {}=?'.format(_name_check(name)), [value])

    def get_global_var(self, name: str) -> Any:
        """
        Retrieve the value of a global variable.

        Returns
        -------
        Any

        """
        cur = self.cursor()
        cur.execute('select @@global.{}'.format(_name_check(name)))
        return list(cur)[0][0]  # type: ignore

    def get_session_var(self, name: str) -> Any:
        """
        Retrieve the value of a session variable.

        Returns
        -------
        Any

        """
        cur = self.cursor()
        cur.execute('select @@session.{}'.format(_name_check(name)))
        return list(cur)[0][0]  # type: ignore

    def enable_http_api(self, port: Optional[int] = None) -> int:
        """
        Enable the HTTP API in the server.

        Use of this method requires privileges that allow setting global
        variables and starting the HTTP proxy.

        Parameters
        ----------
        port : int, optional
            The port number that the HTTP server should run on. If this
            value is not specified, the current value of the
            `http_proxy_port` is used.

        Returns
        -------
        int
            port number of the HTTP server

        """
        cur = self.cursor()
        if port is not None:
            self.set_global_var(http_proxy_port=int(port))
        self.set_global_var(http_api=True)
        cur.execute('restart proxy')
        return self.get_global_var('http_proxy_port')

    def disable_http_api(self) -> None:
        """Disable the HTTP API."""
        cur = self.cursor()
        self.set_global_var(http_api=False)
        cur.execute('restart proxy')


#
# NOTE: When adding parameters to this function, you should always
#       make the value optional with a default of None. The options
#       processing framework will fill in the default value based
#       on environment variables or other configuration sources.
#
def connect(
    host: Optional[str] = None, user: Optional[str] = None,
    password: Optional[str] = None, port: Optional[int] = None,
    database: Optional[str] = None, driver: Optional[str] = None,
    pure_python: Optional[bool] = None, local_infile: Optional[bool] = None,
    odbc_driver: Optional[str] = None, charset: Optional[str] = None,
) -> Connection:
    """
    Return a SingleStore database connection.

    Parameters
    ----------
    host : str, optional
        Hostname, IP address, or URL that describes the connection.
        The scheme or protocol defines which database connector to use.
        By default, the `mysql-connector` scheme is used. To connect to the
        HTTP API, the scheme can be set to `http` or `https`. The username,
        password, host, and port are specified as in a standard URL. The path
        indicates the database name. The overall form of the URL is:
        `scheme://user:password@host:port/db_name`.  The scheme can
        typically be left off (unless you are using the HTTP API):
        `user:password@host:port/db_name`.
    user : str, optional
        Database user name
    password : str, optional
        Database user password
    port : int, optional
        Database port. This defaults to 3306 for non-HTTP connections, 80
        for HTTP connections, and 443 for HTTPS connections.
    database : str, optional
        Database name
    pure_python : bool, optional
        Use the connector in pure Python mode
    local_infile : bool, optional
        Allow local file uploads
    odbc_driver : str, optional
        Name of the ODBC driver to use for ODBC connections

    Examples
    --------
    # Standard database connection
    >>> conn = s2.connect('me:p455w0rd@s2-host.com/my_db')

    # Connect to HTTP API on port 8080
    >>> conn = s2.connect('http://me:p455w0rd@s2-host.com:8080/my_db')

    Returns
    -------
    Connection

    """
    return Connection(**dict(locals()))
