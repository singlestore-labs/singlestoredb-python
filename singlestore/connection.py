
import importlib
import requests
import sqlparams
from collections.abc import Mapping, Sequence
from typing import Union, Optional
from urllib.parse import urlparse
from . import exceptions, dbapi


class Cursor(object):

    def __init__(self, connection, cursor, param_converter) -> 'Cursor':
        self.errorhandler = connection.errorhandler
        self._conn = connection
        self._cursor = cursor
        self._param_converter = param_converter

    @property
    def connection(self) -> 'Connection':
        return self._conn

    @property
    def arraysize(self) -> int:
        return self._cursor.arraysize

    @arraysize.setter
    def arraysize(self, val: int):
        self._cursor.arraysize = val

    @property
    def description(self) -> Sequence:
        return self._cursor.description

    @property
    def rowcount(self) -> int:
        return self._cursor.rowcount

    def callproc(self, name: str, params: Union[Sequence, Mapping]):
        return self._cursor.callproc(name, params)

    def close(self):
        out = self._cursor.close()
        self._conn = None
        return out

    def execute(self, oper: str, params: Union[Sequence, Mapping]=None):
        return self._cursor.execute(*self._param_converter.format(oper, params or []))

    def executemany(self, oper: str, param_seq: Sequence[Union[Sequence, Mapping]]=None):
        return self._cursor.executemany(*self._param_converter.formatmany(oper, param_seq or []))

    def fetchone(self) -> Optional[Sequence]:
        return self._cursor.fetchone()

    def fetchmany(self, size: Optional[int]=None) -> Optional[Sequence]:
        return self._cursor.fetchmany(size=size)

    def nextset(self) -> Optional[bool]:
        return self._cursor.nextset()

    def setinputsizes(self, sizes: Sequence):
        self._cursor.setinputsizes(sizes)

    def setoutputsize(self, size: int, column=None):
        self._cursor.setoutputsize(size, column)

    @property
    def rownumber(self) -> Optional[int]:
        return self._cursor.rownumber

    def scroll(self, value, mode='relative'):
        return self._cursor.scroll(mode=mode)

    @property
    def messages(self) -> Sequence[tuple]:
        return self._cursor.messages

    def next(self):
        return self._cursor.next()

    def __iter__(self):
        return self._cursor.__iter__()

    @property
    def lastrowid(self):
        return self._cursor.lastrowid()

    def __enter__(self):
        pass

    def __exit__(self):
        self.close()

    def is_connected(self) -> bool:
        return self._conn.is_connected()


class Connection(object):

    arraysize = 1000

    Warning = exceptions.Warning
    Error = exceptions.Error
    InterfaceError = exceptions.InterfaceError
    DatabaseError = exceptions.DatabaseError
    OperationalError = exceptions.OperationalError
    IntegrityError = exceptions.IntegrityError
    InternalError = exceptions.InternalError
    ProgrammingError = exceptions.ProgrammingError
    NotSupportedError = exceptions.NotSupportedError

    def __init__(self, url: str) -> 'Connection':
        global DEFAULT_DRIVER

        self._conn = None
        self.url = url
        self.arraysize = type(self).arraysize
        self.errorhandler = None

        if '//' not in url:
            url = '//' + url

        url = urlparse(url, scheme='auto', allow_fragments=True)

        params = {}
        params['host'] = url.hostname or '127.0.0.1'
        params['port'] = url.port or 3306
        database = url.path
        if database.startswith('/'):
            database = database.split('/')[1].strip()
        params['database'] = database.split('/')[0].strip() or None
        params['user'] = url.username 
        params['password'] = url.password 

        driver = url.scheme.lower()

        if driver == 'auto':
            driver = DEFAULT_DRIVER

        if driver == 'mysqldb':
            import MySQLdb as connector
        elif driver == 'mysqlconnector':
            import mysql.connector as connector
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
            if url.port is None:
                params['port'] = 80
            params['protocol'] = driver
        else:
            raise exceptions.Error(0, f'Unrecognized SingleStore driver: {driver}')

        params = {k: v for k, v in params.items() if v is not None}

        self._conn = connector.connect(**params)
        self._param_converter = sqlparams.SQLParams(dbapi.paramstyle,
                                                    connector.paramstyle)

    def close(self):
        self._conn.close()
        self._conn = None

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def cursor(self) -> Cursor:
        return Cursor(self, self._conn.cursor(), self._param_converter)

    @property
    def messages(self) -> Sequence[tuple]:
        return self._cursor.messages

    def __enter__(self):
        pass

    def __exit__(self):
        self.close()

    def is_connected(self) -> bool:
        if self._conn is None:
            return False
        is_connected = getattr(self._conn, 'is_connected', None)
        if is_connected is not None and is_connected():
            return True
        return False


def connect(url: str) -> Connection:
    return Connection(url)


DEFAULT_DRIVER = 'pymysql'

for drv in ['MySQLdb', 'mysql.connector', 'cymysql', 'pyodbc', 'pymysql']:
    try:
        importlib.import_module(drv)
        DEFAULT_DRIVER = drv.lower()
        break
    except ImportError:
        pass
