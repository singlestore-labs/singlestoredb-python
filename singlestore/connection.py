#!/usr/bin/env python
'''
SingleStore database connections and cursors

'''
import os
import re
from collections.abc import Mapping
from collections.abc import Sequence
from typing import NamedTuple
from typing import Optional
from typing import Union
from urllib.parse import urlparse

import sqlparams

from . import exceptions
from . import types


# DB-API settings
apilevel = '2.0'
threadsafety = 1
paramstyle = 'qmark'


def _name_check(name):
    '''
    Make sure the given name is a legal variable name

    Parameters
    ----------
    name : str
        Name to check

    Returns
    -------
    str

    '''
    name = name.strip()
    if not re.match(r'^[A-Za-z][\w+_]*$', name):
        raise ValueError('Name contains invalid characters')
    return name


class Description(NamedTuple):
    ''' Column definition '''
    name: str
    type_code: str
    display_size: Optional[int]
    internal_size: Optional[int]
    precision: Optional[int]
    scale: Optional[int]
    null_ok: Optional[bool]


class Cursor(object):
    '''
    Database cursor for submitting commands and queries

    This object should not be instantiated directly.
    The `Connection.cursor` method should be used.

    Parameters
    ----------
    connection : Connection
        The connection the cursor belongs to
    cursor : Cursor
        The Cursor object from the underlying MySQL package
    param_converter : sqlparams.SQLParams
        The sqlparams converter used to convert parameter replacement
        indicators in queries to the common type for this package

    Returns
    -------
    Cursor

    '''

    def __init__(self, connection, cursor, param_converter) -> 'Cursor':
        self.errorhandler = connection.errorhandler
        self._conn = connection
        self._cursor = cursor
        self._param_converter = param_converter

    @property
    def connection(self) -> 'Connection':
        ''' The Connection that the cursor belongs to '''
        return self._conn

    @property
    def arraysize(self) -> int:
        ''' The batch size used by `fetchmany` '''
        return self._cursor.arraysize

    @arraysize.setter
    def arraysize(self, val: int):
        ''' Set the batch size used by `fetchmany` '''
        self._cursor.arraysize = val

    @property
    def description(self) -> Sequence:
        ''' Column descriptions for the current result set '''
        out = []
        for item in self._cursor.description:
            item = list(item)
            item[1] = types.ColumnType.get_name(item[1])
            item[6] = not(not(item[6]))
            out.append(Description(*item[:7]))
        return out

    @property
    def rowcount(self) -> int:
        ''' Number of rows the last execute produced or affected '''
        return self._cursor.rowcount

    def callproc(self, name: str, params: Union[Sequence, Mapping, None] = None):
        '''
        Call a stored procedure

        Parameters
        ----------
        name : str
            Name of the stored procedure
        params : iterable or dict, optional
            Parameters to the stored procedure

        '''
        self._cursor.callproc(name, params)

    def close(self):
        ''' Close the cursor '''
        self._cursor.close()
        self._conn = None

    def execute(self, oper: str, params: Union[Sequence, Mapping] = None):
        '''
        Execute a SQL statement

        Parameters
        ----------
        oper : str
            The SQL statement to execute
        params : iterable or dict, optional
            Parameters to substitute into the SQL code

        '''
        self._cursor.execute(*self._param_converter.format(oper, params or []))

    def executemany(
        self, oper: str,
        param_seq: Sequence[Union[Sequence, Mapping]] = None,
    ):
        '''
        Execute SQL code against multiple sets of parameters

        Parameters
        ----------
        oper : str
            The SQL statement to execute
        params_seq : iterable of iterables or dicts, optional
            Sets of parameters to substitute into the SQL code

        '''
        self._cursor.executemany(*self._param_converter.formatmany(oper, param_seq or []))

    def fetchone(self) -> Optional[tuple]:
        '''
        Fetch a single row from the result set

        Returns
        -------
        tuple
            Values of the returned row if there are rows remaining
        None
            If there are no rows left to return

        '''
        return self._cursor.fetchone()

    def fetchmany(self, size: Optional[int] = None) -> Optional[Sequence[tuple]]:
        '''
        Fetch `size` rows from the result

        If `size` is not specified, the `arraysize` attribute is used.

        Returns
        -------
        list of tuples
            Values of the returned rows if there are rows remaining
        None
            If there are no rows left to return

        '''
        return self._cursor.fetchmany(size=size or self.arraysize)

    def fetchall(self) -> Optional[Sequence[tuple]]:
        '''
        Fetch all rows in the result set

        Returns
        -------
        list of tuples
            Values of the returned rows if there are rows remaining
        None
            If there are no rows to return

        '''
        return self._cursor.fetchall()

    def fetchframe(self) -> Optional['DataFrame']:  # noqa: F821
        '''
        Fetch all rows in the result set as a `DataFrame`

        Returns
        -------
        DataFrame
            Values of the returned rows if there are rows remaining
        None
            If there are no rows to return

        '''
        from pandas import DataFrame
        columns = [x[0] for x in self.description]
        return DataFrame(data=self.fetchall(), columns=columns)

    def nextset(self) -> Optional[bool]:
        ''' Skip to the next available result set '''
        raise NotImplementedError

    def setinputsizes(self, sizes: Sequence):
        ''' Predefine memory areas for parameters '''
        self._cursor.setinputsizes(sizes)

    def setoutputsize(self, size: int, column=None):
        ''' Set a column buffer size for fetches of large columns '''
        self._cursor.setoutputsize(size, column)

    @property
    def rownumber(self) -> Optional[int]:
        ''' Current zero-based index of the cursor in the result set '''
        return self._cursor.rownumber

    def scroll(self, value, mode='relative'):
        ''' Scroll the cursor to the position in the result set '''
        self._cursor.scroll(mode=mode)

    @property
    def messages(self) -> Sequence[tuple]:
        ''' List of received messages '''
        return self._cursor.messages

    def next(self):
        ''' Return the next row from the result set for use in iterators '''
        return self._cursor.next()

    __next__ = next

    def __iter__(self):
        ''' Return result iterator '''
        return self._cursor.__iter__()

    @property
    def lastrowid(self):
        ''' The rowid of the last modified row '''
        return self._cursor.lastrowid()

    def __enter__(self):
        ''' Enter a context '''
        pass

    def __exit__(self):
        ''' Exit a context '''
        self.close()

    def is_connected(self) -> bool:
        '''
        Is this cursor connected?

        Returns
        -------
        bool

        '''
        return self._conn.is_connected()


class Connection(object):
    '''
    SingleStore database connection

    Instances of this object are typically created through the
    `connection` function rather than creating them directly.

    Parameters
    ----------
    dsn : str, optional
        URL that describes the connection. The scheme or protocol defines
        which database connector to use. By default, the `mysql.connector`
        is used. To connect to the HTTP API, the scheme can be set to `http`
        or `https`. The username, password, host, and port are specified as
        in a standard URL. The path indicates the database name. The overall
        form of the URL is: `scheme://user:password@host:port/db_name`.
        The scheme can typically be left off (unless you are using the HTTP
        API): `user:password@host:port/db_name`.
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
    pure_python : bool, optional
        Use the connector in pure Python mode

    Examples
    --------
    # Standard database connection
    >>> conn = s2.connect('me:p455w0rd@s2-host.com/my_db')

    # Connect to HTTP API on port 8080
    >>> conn = s2.connect('http://me:p455w0rd@s2-host.com:8080/my_db')

    See Also
    --------
    `connect`

    Returns
    -------
    Connection

    '''

    arraysize = 1000
    default_driver = 'mysql.connector'

    Warning = exceptions.Warning
    Error = exceptions.Error
    InterfaceError = exceptions.InterfaceError
    DatabaseError = exceptions.DatabaseError
    OperationalError = exceptions.OperationalError
    IntegrityError = exceptions.IntegrityError
    InternalError = exceptions.InternalError
    ProgrammingError = exceptions.ProgrammingError
    NotSupportedError = exceptions.NotSupportedError

    def __init__(
        self, dsn=None, user=None, password=None, host=None,
        port=None, database=None, driver=None,
        pure_python=False,
    ) -> 'Connection':
        self._conn = None
        self.arraysize = type(self).arraysize
        self.errorhandler = None

        # Setup connection parameters
        params = {}
        params['host'] = host or os.environ.get('SINGLESTORE_HOST', '127.0.0.1')
        params['port'] = port or os.environ.get('SINGLESTORE_PORT', None)
        params['database'] = database or os.environ.get('SINGLESTORE_DATABASE', None)
        params['user'] = user or os.environ.get('SINGLESTORE_USER', None)
        params['password'] = password or os.environ.get('SINGLESTORE_PASSWORD', None)

        # Check environment for dsn
        if not dsn:
            dsn = os.environ.get('SINGLESTORE_DSN', None)

        # If a dsn url is supplied, it takes precedence
        if dsn:
            if '//' not in dsn:
                dsn = '//' + dsn

            dsn = urlparse(dsn, scheme='auto', allow_fragments=True)

            dsn_db = dsn.path
            if dsn_db.startswith('/'):
                dsn_db = dsn_db.split('/')[1].strip()
            dsn_db = dsn_db.split('/')[0].strip() or None

            params['host'] = dsn.hostname or params['host']
            params['port'] = dsn.port or params['port']
            params['database'] = dsn_db or params['database']
            params['user'] = dsn.username or params['user']
            if dsn.password is not None:
                params['password'] = dsn.password

            if dsn.scheme != 'auto':
                driver = dsn.scheme.lower()

        # Load requested driver
        driver = (
            driver or
            os.environ.get('SINGLESTORE_DRIVER', type(self).default_driver)
        ).lower()

        if driver in ['mysqlconnector', 'mysql-connector', 'mysql.connector']:
            import mysql.connector as connector
            params['use_pure'] = pure_python
        elif driver == 'mysqldb':
            import MySQLdb as connector
        elif driver == 'cymysql':
            import cymysql as connector
        elif driver == 'pymysql':
            import pymysql as connector
        elif driver.startswith('pyodbc'):
            import pyodbc as connector
            if '+' in driver:
                params['driver'] = driver.split('+', 1)[1]
            else:
                params['driver'] = 'MySQL'
        elif driver in ['http', 'https']:
            from . import http as connector
            params['protocol'] = driver
        else:
            raise exceptions.Error(0, f'Unrecognized SingleStore driver: {driver}')

        # Fill in port based on driver, if it wasn't specified
        if not params['port']:
            if driver == 'http':
                params['port'] = 80
            elif driver == 'https':
                params['port'] = 443
            else:
                params['port'] = 3306

        params['port'] = int(params['port'])

        params = {k: v for k, v in params.items() if v is not None}

        self._conn = connector.connect(**params)
        self._param_converter = sqlparams.SQLParams(
            paramstyle,
            connector.paramstyle,
        )

    def close(self):
        ''' Close the database connection '''
        self._conn.close()
        self._conn = None

    def commit(self):
        ''' Commit the pending transaction '''
        self._conn.commit()

    def rollback(self):
        ''' Rollback the pending transaction '''
        self._conn.rollback()

    def cursor(self) -> Cursor:
        '''
        Create a new cursor object

        Returns
        -------
        Cursor

        '''
        return Cursor(self, self._conn.cursor(), self._param_converter)

    @property
    def messages(self) -> Sequence[tuple]:
        ''' Messages generated by the connection '''
        return self._cursor.messages

    def __enter__(self):
        ''' Enter a context '''
        pass

    def __exit__(self):
        ''' Exit a context '''
        self.close()

    def is_connected(self) -> bool:
        '''
        Is the database still connected?

        Returns
        -------
        bool

        '''
        if self._conn is None:
            return False
        is_connected = getattr(self._conn, 'is_connected', None)
        if is_connected is not None and is_connected():
            return True
        return False

    def set_global_var(self, **kwargs):
        '''
        Set one or more global variables in the database

        Parameters
        ----------
        **kwargs : key-value pairs
            Keyword parameters specify the variable names and values to set

        '''
        cur = self.cursor()
        for name, value in kwargs.items():
            if value is True:
                value = 'on'
            elif value is False:
                value = 'off'
            cur.execute('set global {}=?'.format(_name_check(name)), [value])

    def set_session_var(self, **kwargs):
        '''
        Set one or more session variables in the database

        Parameters
        ----------
        **kwargs : key-value pairs
            Keyword parameters specify the variable names and values to set

        '''
        cur = self.cursor()
        for name, value in kwargs.items():
            if value is True:
                value = 'on'
            elif value is False:
                value = 'off'
            cur.execute('set session {}=?'.format(_name_check(name)), [value])

    def get_global_var(self, name):
        '''
        Retrieve the value of a global variable

        Returns
        -------
        Any

        '''
        cur = self.cursor()
        cur.execute('select @@global.{}'.format(_name_check(name)))
        return list(cur)[0][0]

    def get_session_var(self, name):
        '''
        Retrieve the value of a session variable

        Returns
        -------
        Any

        '''
        cur = self.cursor()
        cur.execute('select @@session.{}'.format(_name_check(name)))
        return list(cur)[0][0]

    def enable_http_api(self, port=None):
        '''
        Enable the HTTP API in the server

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
        int : port number of the HTTP server

        '''
        cur = self.cursor()
        if port is not None:
            self.set_global_var(http_proxy_port=int(port))
        self.set_global_var(http_api=True)
        cur.execute('restart proxy')
        return self.get_global_var('http_proxy_port')

    def disable_http_api(self):
        ''' Disable the HTTP API '''
        cur = self.cursor()
        self.set_global_var(http_api=False)
        cur.execute('restart proxy')


def connect(
    dsn=None, user=None, password=None, host=None,
    port=None, database=None, driver=None, pure_python=False,
) -> 'Connection':
    '''
    Return a SingleStore database connection

    Parameters
    ----------
    dsn : str, optional
        URL that describes the connection. The scheme or protocol defines
        which database connector to use. By default, the `mysql.connector`
        is used. To connect to the HTTP API, the scheme can be set to `http`
        or `https`. The username, password, host, and port are specified as
        in a standard URL. The path indicates the database name. The overall
        form of the URL is: `scheme://user:password@host:port/db_name`.
        The scheme can typically be left off (unless you are using the HTTP
        API): `user:password@host:port/db_name`.
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
    pure_python : bool, optional
        Use the connector in pure Python mode

    Examples
    --------
    # Standard database connection
    >>> conn = s2.connect('me:p455w0rd@s2-host.com/my_db')

    # Connect to HTTP API on port 8080
    >>> conn = s2.connect('http://me:p455w0rd@s2-host.com:8080/my_db')

    Returns
    -------
    Connection

    '''
    return Connection(
        dsn=dsn, user=user, password=password, host=host,
        port=port, database=database, driver=driver,
        pure_python=pure_python,
    )
