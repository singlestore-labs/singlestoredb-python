
import importlib
import os
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
        desc = self._cursor.description
        out = []
        for item in self._cursor.description:
            item = list(item)
            item[6] = not(not(item[6]))
            out.append(tuple(item))
        return out

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

    def __init__(self, dsn=None, user=None, password=None, host=None,
                 port=None, database=None, driver='mysql.connector') -> 'Connection':
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
        driver = driver.lower() or os.environ.get('SINGLESTORE_DRIVER',
                                                  'mysql.connector').lower()
        if driver in ['mysqlconnector', 'mysql-connector', 'mysql.connector']:
            import mysql.connector as connector
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


def connect(dsn=None, user=None, password=None, host=None,
            port=None, database=None, driver='mysql.connector') -> 'Connection':
    return Connection(dsn=dsn, user=user, password=password, host=host,
                      port=port, database=database, driver=driver)


