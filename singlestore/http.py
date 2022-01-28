#!/usr/bin/env python

''' SingleStore HTTP API interface '''

import re
import requests
from collections.abc import Mapping, Sequence
from typing import Union, Optional
from urllib.parse import urljoin
from . import exceptions
from . import types
from .converters import converters


# DB-API parameter style
paramstyle = 'qmark'


class Cursor(object):
    '''
    SingleStore HTTP database cursor

    Cursor objects should not be created directly. They should come from
    the `cursor` method on the `Connection` object.

    Parameters
    ----------
    connection : Connection
        The HTTP Connection object the cursor belongs to

    Returns
    -------
    Cursor

    '''

    def __init__(self, connection) -> 'Cursor':
        self.connection = connection
        self._rows = []
        self.description = None
        self.arraysize = 1000
        self.rowcount = 0
        self.messages = []
        self.lastrowid = None

    def _get(self, path, *args, **kwargs):
        '''
        Invoke a GET request on the HTTP connection

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

        '''
        return self.connection._get(path, *args, **kwargs)

    def _post(self, path, *args, **kwargs):
        '''
        Invoke a POST request on the HTTP connection

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

        '''
        return self.connection._post(path, *args, **kwargs)

    def _delete(self, path, *args, **kwargs):
        '''
        Invoke a DELETE request on the HTTP connection

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

        '''
        return self.connection._delete(path, *args, **kwargs)

    def callproc(self, name: str, params: Union[Sequence, Mapping]):
        '''
        Call a stored procedure

        Parameters
        ----------
        name : str
            Name of the stored procedure
        params : iterable or dict, optional
            Parameters to the stored procedure

        '''
        raise NotImplementedError

    def close(self):
        ''' Close the cursor '''
        self.connection.close()
        self.connection = None

    def execute(self, query: str, params=None):
        '''
        Execute a SQL statement

        Parameters
        ----------
        oper : str
            The SQL statement to execute
        params : iterable or dict, optional
            Parameters to substitute into the SQL code

        '''
        data = dict(sql=query)
        if params is not None:
            data['args'] = params
        if self.connection._database:
            data['database'] = self.connection._database

        sql_type = 'exec'
        if re.match(r'^\s*select', query, flags=re.I):
            sql_type = 'query'

        if sql_type == 'query':
            res = self._post('query/tuples', json=data)
        else:
            res = self._post('exec', json=data)

        if res.status_code >= 400:
            if res.text:
                if ':' in res.text:
                    code, msg = res.text.split(':', 1)
                    code = int(code.split()[-1])
                else:
                    code = res.status_code
                    msg = res.text
                raise exceptions.InterfaceError(code, msg.strip())
            raise exceptions.InterfaceError(res.status_code, 'HTTP Error')

        out = res.json()

        self.description = None
        self._rows = []
        self.rowcount = 0

        if sql_type == 'query':
            # description: (name, type_code, display_size, internal_size,
            #               precision, scale, null_ok, column_flags, ?)
            self.description = []
            convs = []
            for item in out['results'][0].get('columns', []):
                col_type = types.ColumnType.get_name(item['dataType'])
                convs.append(converters[col_type])
                self.description.append((item['name'], col_type,
                                         None, None, None, None,
                                         item.get('nullable', False), 0, 0))

            # Convert data to Python types
            self._rows = out['results'][0]['rows']
            for i, row in enumerate(self._rows):
                self._rows[i] = tuple(x(y) for x, y in zip(convs, row))

            self.rowcount = len(self._rows)
        else:
            self.rowcount = out['rowsAffected']

    def executemany(self, query: str,
                    param_seq: Sequence[Union[Sequence, Mapping]] = None):
        '''
        Execute SQL code against multiple sets of parameters

        Parameters
        ----------
        query : str
            The SQL statement to execute
        params_seq : iterable of iterables or dicts, optional
            Sets of parameters to substitute into the SQL code

        '''
        # TODO: What to do with the results?
        for params in param_seq:
            self.execute(query, params)

    def fetchone(self) -> Optional[Sequence]:
        '''
        Fetch a single row from the result set

        Returns
        -------
        tuple
            Values of the returned row if there are rows remaining
        None
            If there are no rows left to return

        '''
        if self._rows:
            return self._rows.pop(0)
        self.description = None
        return None

    def fetchmany(self, size: Optional[int] = None) -> Optional[Sequence]:
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
        if not size or int(size) <= 0:
            size = self.arraysize
        out = []
        while size > 0:
            row = self.fetchone()
            if row is None:
                break
            out.append(row)
        return out or None

    def fetchall(self):
        '''
        Fetch all rows in the result set

        Returns
        -------
        list of tuples
            Values of the returned rows if there are rows remaining
        None
            If there are no rows to return

        '''
        out = []
        while True:
            row = self.fetchone()
            if row is None:
                break
            out.append(row)
        return out or None

    def nextset(self) -> Optional[bool]:
        ''' Skip to the next available result set '''
        raise NotImplementedError

    def setinputsizes(self, sizes: Sequence):
        ''' Predefine memory areas for parameters '''
        pass

    def setoutputsize(self, size: int, column=None):
        ''' Set a column buffer size for fetches of large columns '''
        pass

    @property
    def rownumber(self) -> Optional[int]:
        ''' Current zero-based index of the cursor in the result set '''
        return self.rowcount - len(self._rows)

    def scroll(self, value, mode='relative'):
        ''' Scroll the cursor to the position in the result set '''
        raise exceptions.NotSupportedError

    def next(self):
        ''' Return the next row from the result set for use in iterators '''
        out = self.fetchone()
        if out is None:
            raise StopIteration
        return out

    __next__ = next

    def __iter__(self):
        ''' Return result iterator '''
        return iter(self._rows)

    def __enter__(self):
        ''' Enter a context '''
        pass

    def __exit__(self):
        ''' Exit a context '''
        self.close()

    def is_connected(self):
        '''
        Is this cursor connected?

        Returns
        -------
        bool

        '''
        return self._conn.is_connected()


class Connection(object):
    '''
    SingleStore HTTP database connection

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

    Returns
    -------
    Connection

    '''

    def __init__(self, host=None, port=None, user=None, password=None,
                 database=None, protocol='http', version='v1'):
        host = host or 'localhost'
        port = port or 3306

        self._sess = requests.Session()
        if user is not None and password is not None:
            self._sess.auth = (user, password)
        self._sess.headers.update({
            'Content-Type': 'application/json',
            'Accepts': 'application/json',
        })

        self._database = database
        self._url = f'{protocol}://{host}:{port}/api/{version}/'
        self.messages = []

    def _get(self, path, *args, **kwargs):
        '''
        Invoke a GET request on the HTTP connection

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

        '''
        return self._sess.get(urljoin(self._url, path), *args, **kwargs)

    def _post(self, path, *args, **kwargs):
        '''
        Invoke a POST request on the HTTP connection

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

        '''
        return self._sess.post(urljoin(self._url, path), *args, **kwargs)

    def _delete(self, path, *args, **kwargs):
        '''
        Invoke a DELETE request on the HTTP connection

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

        '''
        return self._sess.delete(urljoin(self._url, path), *args, **kwargs)

    def close(self):
        ''' Close the connection '''
        self._sess = None

    def commit(self):
        ''' Commit the pending transaction '''
        raise exceptions.NotSupportedError

    def rollback(self):
        ''' Rollback the pending transaction '''
        raise exceptions.NotSupportedError

    def cursor(self) -> Cursor:
        '''
        Create a new cursor object

        Returns
        -------
        Cursor

        '''
        return Cursor(self)

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
        if self._sess is None:
            return False
        url = '/'.join(self._url.split('/')[:3]) + '/ping'
        res = self._sess.get(url)
        if res.status_code <= 400 and res.text == 'pong':
            return True
        return False


def connect(host=None, port=None, user=None, password=None,
            database=None, protocol='http', version='v1') -> Connection:
    '''
    SingleStore HTTP database connection

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

    '''
    return Connection(host=host, port=port, user=user, password=password,
                      database=database, protocol=protocol, version=version)
