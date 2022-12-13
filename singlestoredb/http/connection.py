#!/usr/bin/env python
"""SingleStoreDB HTTP API interface."""
import functools
import json
import re
from base64 import b64decode
from typing import Any
from typing import Callable
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional
from typing import Sequence
from typing import Tuple
from typing import Union
from urllib.parse import urljoin

import requests

from .. import connection
from .. import types
from ..config import get_option
from ..converters import converters
from ..exceptions import DatabaseError  # noqa: F401
from ..exceptions import DataError
from ..exceptions import Error  # noqa: F401
from ..exceptions import IntegrityError
from ..exceptions import InterfaceError
from ..exceptions import InternalError
from ..exceptions import NotSupportedError
from ..exceptions import OperationalError
from ..exceptions import ProgrammingError
from ..exceptions import Warning  # noqa: F401
from ..utils.convert_rows import convert_rows
from ..utils.debug import log_query
from ..utils.results import Description
from ..utils.results import format_results
from ..utils.results import Result


# DB-API settings
apilevel = '2.0'
paramstyle = 'named'
threadsafety = 1


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


def get_precision_scale(type_code: str) -> Tuple[Optional[int], Optional[int]]:
    """Parse the precision and scale from a data type."""
    if '(' not in type_code:
        return (None, None)
    m = re.search(r'\(\s*(\d+)\s*,\s*(\d+)\s*\)', type_code)
    if m:
        return int(m.group(1)), int(m.group(2))
    m = re.search(r'\(\s*(\d+)\s*\)', type_code)
    if m:
        return (int(m.group(1)), None)
    raise ValueError(f'Unrecognized type code: {type_code}')


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


class PyMyField(object):
    """Field for PyMySQL compatibility."""

    def __init__(self, name: str, flags: int, charset: int) -> None:
        self.name = name
        self.flags = flags
        self.charsetnr = charset


class PyMyResult(object):
    """Result for PyMySQL compatibility."""

    def __init__(self) -> None:
        self.fields: List[PyMyField] = []
        self.unbuffered_active = False

    def append(self, item: PyMyField) -> None:
        self.fields.append(item)


class Cursor(connection.Cursor):
    """
    SingleStoreDB HTTP database cursor.

    Cursor objects should not be created directly. They should come from
    the `cursor` method on the `Connection` object.

    Parameters
    ----------
    connection : Connection
        The HTTP Connection object the cursor belongs to

    """

    def __init__(self, conn: 'Connection'):
        connection.Cursor.__init__(self, conn)
        self._connection: Optional[Connection] = conn
        self._results: List[List[Tuple[Any, ...]]] = [[]]
        self._results_type: str = self._connection._results_type \
            if self._connection is not None else 'tuples'
        self._row_idx: int = -1
        self._result_idx: int = -1
        self._descriptions: List[List[Description]] = []
        self.arraysize: int = get_option('results.arraysize')
        self.rowcount: int = 0
        self.lastrowid: Optional[int] = None
        self._pymy_results: List[PyMyResult] = []
        self._expect_results: bool = False

    @property
    def _result(self) -> Optional[PyMyResult]:
        """Return Result object for PyMySQL compatibility."""
        if self._result_idx < 0:
            return None
        return self._pymy_results[self._result_idx]

    @property
    def description(self) -> Optional[List[Description]]:
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
        if self._connection is None:
            raise ProgrammingError(errno=2048, msg='Connection is closed.')
        return self._connection._post(path, *args, **kwargs)

    def callproc(
        self, name: str,
        params: Optional[Sequence[Any]] = None,
    ) -> None:
        """
        Call a stored procedure.

        Parameters
        ----------
        name : str
            Name of the stored procedure
        params : sequence, optional
            Parameters to the stored procedure

        """
        if self._connection is None:
            raise ProgrammingError(errno=2048, msg='Connection is closed.')

        name = connection._name_check(name)

        if not params:
            self._execute(f'CALL {name}();', is_callproc=True)
        else:
            keys = ', '.join(['%s' for i in range(len(params))])
            self._execute(f'CALL {name}({keys});', params, is_callproc=True)

    def close(self) -> None:
        """Close the cursor."""
        self._connection = None

    def execute(
        self, query: str,
        args: Optional[Union[Sequence[Any], Dict[str, Any]]] = None,
    ) -> int:
        """
        Execute a SQL statement.

        Parameters
        ----------
        query : str
            The SQL statement to execute
        args : iterable or dict, optional
            Parameters to substitute into the SQL code

        """
        return self._execute(query, args)

    def _validate_param_subs(
        self, query: str,
        args: Optional[Union[Sequence[Any], Dict[str, Any]]] = None,
    ) -> None:
        """Make sure the parameter substitions are valid."""
        if args is not None:
            query = query % args

    def _execute(
        self, oper: str,
        params: Optional[Union[Sequence[Any], Dict[str, Any]]] = None,
        is_callproc: bool = False,
    ) -> int:
        if self._connection is None:
            raise ProgrammingError(errno=2048, msg='Connection is closed.')

        self._validate_param_subs(oper, params)

        oper, params = self._connection._convert_params(oper, params)

        log_query(oper, params)

        data: Dict[str, Any] = dict(sql=oper)
        if params is not None:
            data['args'] = params
        if self._connection._database:
            data['database'] = self._connection._database

        self._expect_results = False
        sql_type = 'exec'
        if re.match(r'^\s*(select|show|call|echo)\s+', oper, flags=re.I):
            self._expect_results = True
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

            # Merge passed in converters
            if self._connection._conv:
                for k, v in self._connection._conv.items():
                    if isinstance(k, int):
                        http_converters[k] = v

            results = out['results']

            # Convert data to Python types
            if results and results[0]:
                self._row_idx = 0
                self._result_idx = 0

                for result in results:

                    pymy_res = PyMyResult()
                    convs = []

                    description: List[Description] = []
                    for i, col in enumerate(result.get('columns', [])):
                        charset = 0
                        flags = 0
                        data_type = col['dataType'].split('(')[0]
                        type_code = types.ColumnType.get_code(data_type)
                        prec, scale = get_precision_scale(col['dataType'])
                        converter = http_converters.get(type_code, None)
                        if 'UNSIGNED' in data_type:
                            flags = 32
                        if data_type.endswith('BLOB') or data_type.endswith('BINARY'):
                            converter = functools.partial(b64decode_converter, converter)
                            charset = 63  # BINARY
                        if type_code == 0:  # DECIMAL
                            type_code = types.ColumnType.get_code('NEWDECIMAL')
                        elif type_code == 15:  # VARCHAR / VARBINARY
                            type_code = types.ColumnType.get_code('VARSTRING')
                        if type_code == 246 and prec is not None:  # NEWDECIMAL
                            prec += 1  # for sign
                            if scale is not None and scale > 0:
                                prec += 1  # for decimal
                        if converter is not None:
                            convs.append((i, None, converter))
                        description.append(
                            Description(
                                str(col['name']), type_code,
                                None, None, prec, scale,
                                col.get('nullable', False),
                                flags, charset,
                            ),
                        )
                        pymy_res.append(PyMyField(col['name'], flags, charset))
                    self._descriptions.append(description)

                    rows = convert_rows(result.get('rows', []), convs)

                    self._results.append(rows)
                    self._pymy_results.append(pymy_res)

            # For compatibility with PyMySQL/MySQLdb
            if is_callproc:
                self._results.append([])

            self.rowcount = len(self._results[0])

        else:
            # For compatibility with PyMySQL/MySQLdb
            if is_callproc:
                self._results.append([])

            self.rowcount = out['rowsAffected']

        return self.rowcount

    def executemany(
        self, query: str,
        args: Optional[Sequence[Union[Sequence[Any], Dict[str, Any]]]] = None,
    ) -> int:
        """
        Execute SQL code against multiple sets of parameters.

        Parameters
        ----------
        query : str
            The SQL statement to execute
        args : iterable of iterables or dicts, optional
            Sets of parameters to substitute into the SQL code

        """
        if self._connection is None:
            raise ProgrammingError(errno=2048, msg='Connection is closed.')

        results = []
        rowcount = 0
        if args is not None and len(args) > 0:
            description = []
            # Detect dataframes
            if hasattr(args, 'itertuples'):
                argiter = args.itertuples(index=False)  # type: ignore
            else:
                argiter = iter(args)
            for params in argiter:
                self.execute(query, params)
                if self._descriptions:
                    description = self._descriptions[-1]
                if self._rows is not None:
                    results.append(self._rows)
                rowcount += self.rowcount
            self._results = results
            self._descriptions = [description for _ in range(len(results))]
        else:
            self.execute(query)
            rowcount += self.rowcount

        self.rowcount = rowcount

        return self.rowcount

    @property
    def _has_row(self) -> bool:
        """Determine if a row is available."""
        if self._result_idx < 0 or self._result_idx >= len(self._results):
            return False
        if self._row_idx < 0 or self._row_idx >= len(self._results[self._result_idx]):
            return False
        return True

    @property
    def _rows(self) -> List[Tuple[Any, ...]]:
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
        if self._connection is None:
            raise ProgrammingError(errno=2048, msg='Connection is closed')
        if not self._expect_results:
            raise self._connection.ProgrammingError(msg='No query has been submitted')
        if not self._has_row:
            return None
        out = self._rows[self._row_idx]
        self._row_idx += 1
        return format_results(
            self._results_type,
            self.description or [],
            out, single=True,
        )

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
        if self._connection is None:
            raise ProgrammingError(errno=2048, msg='Connection is closed')
        if not self._expect_results:
            raise self._connection.ProgrammingError(msg='No query has been submitted')
        if not self._has_row:
            if 'dict' in self._results_type:
                return {}
            return tuple()
        if not size:
            size = max(int(self.arraysize), 1)
        else:
            size = max(int(size), 1)
        out = self._rows[self._row_idx:self._row_idx+size]
        self._row_idx += len(out)
        return format_results(self._results_type, self.description or [], out)

    def fetchall(self) -> Result:
        """
        Fetch all rows in the result set.

        Returns
        -------
        list of tuples
            Values of the returned rows if there are rows remaining

        """
        if self._connection is None:
            raise ProgrammingError(errno=2048, msg='Connection is closed')
        if not self._expect_results:
            raise self._connection.ProgrammingError(msg='No query has been submitted')
        if not self._has_row:
            if 'dict' in self._results_type:
                return {}
            return tuple()
        out = list(self._rows[self._row_idx:])
        self._row_idx = len(out)
        return format_results(self._results_type, self.description or [], out)

    def nextset(self) -> Optional[bool]:
        """Skip to the next available result set."""
        if self._connection is None:
            raise ProgrammingError(errno=2048, msg='Connection is closed')

        if self._result_idx < 0:
            self._row_idx = -1
            return None

        self._result_idx += 1
        self._row_idx = 0

        if self._result_idx >= len(self._results):
            self._result_idx = -1
            self._row_idx = -1
            return None

        self.rowcount = len(self._results[self._result_idx])

        return True

    def setinputsizes(self, sizes: Sequence[int]) -> None:
        """Predefine memory areas for parameters."""
        pass

    def setoutputsize(self, size: int, column: Optional[str] = None) -> None:
        """Set a column buffer size for fetches of large columns."""
        pass

    @property
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
        if self._connection is None:
            raise ProgrammingError(errno=2048, msg='Connection is closed')
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
        if self._connection is None:
            raise InterfaceError(errno=2048, msg='Connection is closed')
        out = self.fetchone()
        if out is None:
            raise StopIteration
        return out

    __next__ = next

    def __iter__(self) -> Iterable[Tuple[Any, ...]]:
        """Return result iterator."""
        return iter(self._rows)

    def __enter__(self) -> 'Cursor':
        """Enter a context."""
        return self

    def __exit__(
        self, exc_type: Optional[object],
        exc_value: Optional[Exception], exc_traceback: Optional[str],
    ) -> None:
        """Exit a context."""
        self.close()

    @property
    def open(self) -> bool:
        """Check if the cursor is still connected."""
        if self._connection is None:
            return False
        return self._connection.is_connected()

    def is_connected(self) -> bool:
        """
        Check if the cursor is still connected.

        Returns
        -------
        bool

        """
        return self.open


class Connection(connection.Connection):
    """
    SingleStoreDB HTTP database connection.

    Instances of this object are typically created through the
    `connection` function rather than creating them directly.

    See Also
    --------
    `connect`

    """
    driver = 'https'
    paramstyle = 'qmark'

    def __init__(self, **kwargs: Any):
        connection.Connection.__init__(self, **kwargs)

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

            ssl_verify_cert = kwargs.get('ssl_verify_cert', True)
            if not ssl_verify_cert:
                self._sess.verify = False

        version = kwargs.get('version', 'v2')
        self.driver = kwargs.get('driver', 'https')

        self._database = kwargs.get('database', get_option('database'))
        self._url = f'{self.driver}://{host}:{port}/api/{version}/'
        self._messages: List[Tuple[int, str]] = []
        self._autocommit: bool = True
        self._conv = kwargs.get('conv', None)

    @property
    def messages(self) -> List[Tuple[int, str]]:
        return self._messages

    def connect(self) -> 'Connection':
        """Connect to the server."""
        pass

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
        if self._sess is None:
            raise Error(errno=2048, msg='Connection is closed')
        self._sess = None

    def autocommit(self, value: bool = True) -> None:
        """Set autocommit mode."""
        if self._sess is None:
            raise InterfaceError(errno=2048, msg='Connection is closed')
        self._autocommit = value

    def commit(self) -> None:
        """Commit the pending transaction."""
        if self._sess is None:
            raise InterfaceError(errno=2048, msg='Connection is closed')
        if self._autocommit:
            return
        raise NotSupportedError(msg='operation not supported')

    def rollback(self) -> None:
        """Rollback the pending transaction."""
        if self._sess is None:
            raise InterfaceError(errno=2048, msg='Connection is closed')
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

    def __enter__(self) -> 'Connection':
        """Enter a context."""
        return self

    def __exit__(
        self, exc_type: Optional[object],
        exc_value: Optional[Exception], exc_traceback: Optional[str],
    ) -> None:
        """Exit a context."""
        self.close()

    @property
    def open(self) -> bool:
        """Check if the database is still connected."""
        if self._sess is None:
            return False
        url = '/'.join(self._url.split('/')[:3]) + '/ping'
        res = self._sess.get(url)
        if res.status_code <= 400 and res.text == 'pong':
            return True
        return False

    def is_connected(self) -> bool:
        """
        Check if the database is still connected.

        Returns
        -------
        bool

        """
        return self.open


def connect(
    host: Optional[str] = None, user: Optional[str] = None,
    password: Optional[str] = None, port: Optional[int] = None,
    database: Optional[str] = None, driver: Optional[str] = None,
    pure_python: Optional[bool] = None, local_infile: Optional[bool] = None,
    charset: Optional[str] = None,
    ssl_key: Optional[str] = None, ssl_cert: Optional[str] = None,
    ssl_ca: Optional[str] = None, ssl_disabled: Optional[bool] = None,
    ssl_cipher: Optional[str] = None, ssl_verify_cert: Optional[bool] = None,
    ssl_verify_identity: Optional[bool] = None,
    conv: Optional[Dict[int, Callable[..., Any]]] = None,
    credential_type: Optional[str] = None,
    results_type: Optional[str] = None,
    autocommit: Optional[bool] = None,
) -> Connection:
    return Connection(**dict(locals()))
