
import re
import requests
from collections.abc import Mapping, Sequence
from typing import Union, Optional
from urllib.parse import urljoin
from . import exceptions
from . import types

paramstyle = 'qmark'


class Cursor(object):

    def __init__(self, connection) -> 'Cursor':
        self.connection = connection
        self._rows = []
        self.description = []
        self.arraysize = 1000
        self.rowcount = 0
        self.messages = []
        self.lastrowid = None

    def _get(self, path, *args, **kwargs):
        return self.connection._get(path, *args, **kwargs)

    def _post(self, path, *args, **kwargs):
        return self.connection._post(path, *args, **kwargs)

    def _delete(self, path, *args, **kwargs):
        return self.connection._delete(path, *args, **kwargs)

    def callproc(self, name: str, params: Union[Sequence, Mapping]):
        raise NotImplementedError

    def close(self):
        self.connection.close()
        self.connection = None

    def execute(self, query: str, params=None):
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
            print(res.request.body)
            print(res.text)
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

        self.description = []
        self._rows = []
        self.rowcount = 0

        if sql_type == 'query':
            self._rows = out['results'][0]['rows']
            self.rowcount = len(self._rows)
            # description: (name, type_code, display_size, internal_size,
            #               precision, scale, null_ok, column_flags, ?)
            for item in out['results'][0].get('columns', []):
                self.description.append((item['name'], types.MAP[item['dataType']],
                                         None, None, None, None,
                                         item.get('nullable', False, 0, 0)))
        else:
            self.rowcount = out['rowsAffected']

        return self.rowcount

    def executemany(self, query: str, param_seq: Sequence[Union[Sequence, Mapping]]=None):
        # TODO: What to do with the results?
        for params in param_seq:
            self.execute(query, params)

    def fetchone(self) -> Optional[Sequence]:
        if self._rows:
            return self._rows.pop(0)
        return None

    def fetchmany(self, size: Optional[int]=None) -> Optional[Sequence]:
        if self._rows:
            if size and int(size) > 0:
                out = []
                while size > 0:
                    out.append(self._rows.pop(0))
                return out
            out = self.fetchone()
            if out is not None:
                return [out]
        return None

    def nextset(self) -> Optional[bool]:
        raise NotImplementedError

    def setinputsizes(self, sizes: Sequence):
        pass

    def setoutputsize(self, size: int, column=None):
        pass

    @property
    def rownumber(self) -> Optional[int]:
        return self.rowcount - len(self._rows)

    def scroll(self, value, mode='relative'):
        raise exceptions.NotSupportedError

    def next(self):
        out = fetchone()
        if out is None:
            raise StopIteration
        return out

    def __iter__(self):
        return iter(self._rows)

    def __enter__(self):
        pass

    def __exit__(self):
        self.close()

    def is_connected(self):
        return self._conn.is_connected()


class Connection(object):

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
        self._is_closed = False
        self.messages = []

    def _get(self, path, *args, **kwargs):
        return self._sess.get(urljoin(self._url, path), *args, **kwargs)

    def _post(self, path, *args, **kwargs):
        return self._sess.post(urljoin(self._url, path), *args, **kwargs)

    def _delete(self, path, *args, **kwargs):
        return self._sess.delete(urljoin(self._url, path), *args, **kwargs)

    def close(self):
        self._is_closed = True

    def commit(self):
        raise exceptions.NotSupportedError

    def rollback(self):
        raise exceptions.NotSupportedError

    def cursor(self) -> Cursor:
        return Cursor(self)

    def __enter__(self):
        pass

    def __exit__(self):
        self.close()

    def is_connected(self):
        if self._is_closed:
            return False
        return True


def connect(host=None, port=None, user=None, password=None,
             database=None, protocol='http', version='v1') -> Connection:
    return Connection(host=host, port=port, user=user, password=password,
                      database=database, protocol=protocol, version=version)
