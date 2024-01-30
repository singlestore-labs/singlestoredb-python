#!/usr/bin/env python
"""SingleStoreDB connections and cursors."""
import abc
import inspect
import re
import warnings
import weakref
from collections.abc import Mapping
from collections.abc import MutableMapping
from typing import Any
from typing import Callable
from typing import Dict
from typing import Iterator
from typing import List
from typing import Optional
from typing import Sequence
from typing import Tuple
from typing import Union
from urllib.parse import parse_qs
from urllib.parse import urlparse

import sqlparams
try:
    from pandas import DataFrame
except ImportError:
    class DataFrame(object):  # type: ignore
        def itertuples(self, *args: Any, **kwargs: Any) -> None:
            pass

from . import auth
from . import exceptions
from .config import get_option
from .utils.results import Description
from .utils.results import Result


# DB-API settings
apilevel = '2.0'
threadsafety = 1
paramstyle = map_paramstyle = 'pyformat'
positional_paramstyle = 'format'


# Type codes for character-based columns
CHAR_COLUMNS = set(list(range(247, 256)) + [245])


def under2camel(s: str) -> str:
    """Format underscore-delimited strings to camel-case."""

    def upper_mid(m: Any) -> str:
        """Uppercase middle group of matches."""
        return m.group(1) + m.group(2).upper() + m.group(3)

    def upper(m: Any) -> str:
        """Uppercase match."""
        return m.group(1).upper()

    s = re.sub(r'(\b|_)(xml|sql|json)(\b|_)', upper_mid, s, flags=re.I)
    s = re.sub(r'(?:^|_+)(\w)', upper, s)
    s = re.sub(r'_+$', r'', s)

    return s


def nested_converter(
    conv: Callable[[Any], Any],
    inner: Callable[[Any], Any],
) -> Callable[[Any], Any]:
    """Create a pipeline of two functions."""
    def converter(value: Any) -> Any:
        return conv(inner(value))
    return converter


def cast_bool_param(val: Any) -> bool:
    """Cast value to a bool."""
    if val is None or val is False:
        return False

    if val is True:
        return True

    # Test ints
    try:
        ival = int(val)
        if ival == 1:
            return True
        if ival == 0:
            return False
    except Exception:
        pass

    # Lowercase strings
    if hasattr(val, 'lower'):
        if val.lower() in ['on', 't', 'true', 'y', 'yes', 'enabled', 'enable']:
            return True
        elif val.lower() in ['off', 'f', 'false', 'n', 'no', 'disabled', 'disable']:
            return False

    raise ValueError('Unrecognized value for bool: {}'.format(val))


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
    for name in inspect.getfullargspec(connect).args:
        if name == 'conv':
            out[name] = kwargs.get(name, None)
        elif name == 'results_format':  # deprecated
            if kwargs.get(name, None) is not None:
                warnings.warn(
                    'The `results_format=` parameter has been '
                    'renamed to `results_type=`.',
                    DeprecationWarning,
                )
                out['results_type'] = kwargs.get(name, get_option('results.type'))
        elif name == 'results_type':
            out[name] = kwargs.get(name, get_option('results.type'))
        else:
            out[name] = kwargs.get(name, get_option(name))

    # See if host actually contains a URL; definitely not a perfect test.
    host = out['host']
    if host and (':' in host or '/' in host or '@' in host or '?' in host):
        urlp = _parse_url(host)
        if 'driver' not in urlp:
            urlp['driver'] = get_option('driver')
        out.update(urlp)

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

    if out.get('ssl_ca', '') and not out.get('ssl_verify_cert', None):
        out['ssl_verify_cert'] = True

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
        elif getattr(dtype, '_name', '') in ['Dict', 'Mapping'] or \
                str(dtype).startswith('typing.Dict'):
            val = dict(val)
        elif getattr(dtype, '_name', '') == 'List':
            val = list(val)
        elif getattr(dtype, '_name', '') == 'Tuple':
            val = tuple(val)
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

    if url.startswith('singlestoredb+'):
        url = re.sub(r'^singlestoredb\+', r'', url)

    parts = urlparse(url, scheme='singlestoredb', allow_fragments=True)

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

    if parts.scheme != 'singlestoredb':
        out['driver'] = parts.scheme.lower()

    # Convert query string to parameters
    out.update({k.lower(): v[-1] for k, v in parse_qs(parts.query).items()})

    return {k: v for k, v in out.items() if v is not None}


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
    if not re.match(r'^[A-Za-z_][\w+_]*$', name):
        raise ValueError('Name contains invalid characters')
    return name


def quote_identifier(name: str) -> str:
    """Escape identifier value."""
    return f'`{name}`'


class Driver(object):
    """Compatibility class for driver name."""

    def __init__(self, name: str):
        self.name = name


class VariableAccessor(MutableMapping):  # type: ignore
    """Variable accessor class."""

    def __init__(self, conn: 'Connection', vtype: str):
        object.__setattr__(self, 'connection', weakref.proxy(conn))
        object.__setattr__(self, 'vtype', vtype.lower())
        if self.vtype not in [
            'global', 'local', '',
            'cluster', 'cluster global', 'cluster local',
        ]:
            raise ValueError(
                'Variable type must be global, local, cluster, '
                'cluster global, cluster local, or empty',
            )

    def _cast_value(self, value: Any) -> Any:
        if isinstance(value, str):
            if value.lower() in ['on', 'true']:
                return True
            if value.lower() in ['off', 'false']:
                return False
        return value

    def __getitem__(self, name: str) -> Any:
        name = _name_check(name)
        out = self.connection._iquery(
            'show {} variables like %s;'.format(self.vtype),
            [name],
        )
        if not out:
            raise KeyError(f"No variable found with the name '{name}'.")
        if len(out) > 1:
            raise KeyError(f"Multiple variables found with the name '{name}'.")
        return self._cast_value(out[0]['Value'])

    def __setitem__(self, name: str, value: Any) -> None:
        name = _name_check(name)
        if value is True:
            value = 'ON'
        elif value is False:
            value = 'OFF'
        if 'local' in self.vtype:
            self.connection._iquery(
                'set {} {}=%s;'.format(
                    self.vtype.replace('local', 'session'), name,
                ), [value],
            )
        else:
            self.connection._iquery('set {} {}=%s;'.format(self.vtype, name), [value])

    def __delitem__(self, name: str) -> None:
        raise TypeError('Variables can not be deleted.')

    def __getattr__(self, name: str) -> Any:
        return self[name]

    def __setattr__(self, name: str, value: Any) -> None:
        self[name] = value

    def __delattr__(self, name: str) -> None:
        del self[name]

    def __len__(self) -> int:
        out = self.connection._iquery('show {} variables;'.format(self.vtype))
        return len(list(out))

    def __iter__(self) -> Iterator[str]:
        out = self.connection._iquery('show {} variables;'.format(self.vtype))
        return iter(list(x.values())[0] for x in out)


class Cursor(metaclass=abc.ABCMeta):
    """
    Database cursor for submitting commands and queries.

    This object should not be instantiated directly.
    The ``Connection.cursor`` method should be used.

    """

    def __init__(self, connection: 'Connection'):
        """Call ``Connection.cursor`` instead."""
        self.errorhandler = connection.errorhandler
        self._connection: Optional[Connection] = weakref.proxy(connection)

        self._rownumber: Optional[int] = None

        self._description: Optional[List[Description]] = None

        #: Default batch size of ``fetchmany`` calls.
        self.arraysize = get_option('results.arraysize')

        self._converters: List[
            Tuple[
                int, Optional[str],
                Optional[Callable[..., Any]],
            ]
        ] = []

        #: Number of rows affected by the last query.
        self.rowcount: int = -1

        self._messages: List[Tuple[int, str]] = []

        #: Row ID of the last modified row.
        self.lastrowid: Optional[int] = None

    @property
    def messages(self) -> List[Tuple[int, str]]:
        """Messages created by the server."""
        return self._messages

    @abc.abstractproperty
    def description(self) -> Optional[List[Description]]:
        """The field descriptions of the last query."""
        return self._description

    @abc.abstractproperty
    def rownumber(self) -> Optional[int]:
        """The last modified row number."""
        return self._rownumber

    @property
    def connection(self) -> Optional['Connection']:
        """the connection that the cursor belongs to."""
        return self._connection

    @abc.abstractmethod
    def callproc(
        self, name: str,
        params: Optional[Sequence[Any]] = None,
    ) -> None:
        """
        Call a stored procedure.

        The result sets generated by a store procedure can be retrieved
        like the results of any other query using :meth:`fetchone`,
        :meth:`fetchmany`, or :meth:`fetchall`. If the procedure generates
        multiple result sets, subsequent result sets can be accessed
        using :meth:`nextset`.

        Examples
        --------
        >>> cur.callproc('myprocedure', ['arg1', 'arg2'])
        >>> print(cur.fetchall())

        Parameters
        ----------
        name : str
            Name of the stored procedure
        params : iterable, optional
            Parameters to the stored procedure

        """
        # NOTE: The `callproc` interface varies quite a bit between drivers
        #       so it is implemented using `execute` here.

        if not self.is_connected():
            raise exceptions.InterfaceError(2048, 'Cursor is closed.')

        name = _name_check(name)

        if not params:
            self.execute(f'CALL {name}();')
        else:
            keys = ', '.join([f':{i+1}' for i in range(len(params))])
            self.execute(f'CALL {name}({keys});', params)

    @abc.abstractmethod
    def is_connected(self) -> bool:
        """Is the cursor still connected?"""
        raise NotImplementedError

    @abc.abstractmethod
    def close(self) -> None:
        """Close the cursor."""
        raise NotImplementedError

    @abc.abstractmethod
    def execute(
        self, query: str,
        args: Optional[Union[Sequence[Any], Dict[str, Any], Any]] = None,
    ) -> int:
        """
        Execute a SQL statement.

        Queries can use the ``format``-style parameters (``%s``) when using a
        list of paramters or ``pyformat``-style parameters (``%(key)s``)
        when using a dictionary of parameters.

        Parameters
        ----------
        query : str
            The SQL statement to execute
        args : Sequence or dict, optional
            Parameters to substitute into the SQL code

        Examples
        --------
        >>> cur.execute('select * from mytable')

        >>> cur.execute('select * from mytable where id < %s', [100])

        >>> cur.execute('select * from mytable where id < %(max)s', dict(max=100))

        Returns
        -------
        Number of rows affected

        """
        raise NotImplementedError

    def executemany(
        self, query: str,
        args: Optional[Sequence[Union[Sequence[Any], Dict[str, Any], Any]]] = None,
    ) -> int:
        """
        Execute SQL code against multiple sets of parameters.

        Queries can use the ``format``-style parameters (``%s``) when using
        lists of paramters or ``pyformat``-style parameters (``%(key)s``)
        when using dictionaries of parameters.

        Parameters
        ----------
        query : str
            The SQL statement to execute
        args : iterable of iterables or dicts, optional
            Sets of parameters to substitute into the SQL code

        Examples
        --------
        >>> cur.executemany('select * from mytable where id < %s',
        ...                 [[100], [200], [300]])

        >>> cur.executemany('select * from mytable where id < %(max)s',
        ...                 [dict(max=100), dict(max=100), dict(max=300)])

        Returns
        -------
        Number of rows affected

        """
        # NOTE: Just implement using `execute` to cover driver inconsistencies
        if not args:
            self.execute(query)
        else:
            for params in args:
                self.execute(query, params)
        return self.rowcount

    @abc.abstractmethod
    def fetchone(self) -> Optional[Result]:
        """
        Fetch a single row from the result set.

        Examples
        --------
        >>> while True:
        ...    row = cur.fetchone()
        ...    if row is None:
        ...       break
        ...    print(row)

        Returns
        -------
        tuple
            Values of the returned row if there are rows remaining

        """
        raise NotImplementedError

    @abc.abstractmethod
    def fetchmany(self, size: Optional[int] = None) -> Result:
        """
        Fetch `size` rows from the result.

        If `size` is not specified, the `arraysize` attribute is used.

        Examples
        --------
        >>> while True:
        ...    out = cur.fetchmany(100)
        ...    if not len(out):
        ...        break
        ...    for row in out:
        ...        print(row)

        Returns
        -------
        list of tuples
            Values of the returned rows if there are rows remaining

        """
        raise NotImplementedError

    @abc.abstractmethod
    def fetchall(self) -> Result:
        """
        Fetch all rows in the result set.

        Examples
        --------
        >>> for row in cur.fetchall():
        ...     print(row)

        Returns
        -------
        list of tuples
            Values of the returned rows if there are rows remaining
        None
            If there are no rows to return

        """
        raise NotImplementedError

    @abc.abstractmethod
    def nextset(self) -> Optional[bool]:
        """
        Skip to the next available result set.

        This is used when calling a procedure that returns multiple
        results sets.

        Note
        ----
        The ``nextset`` method must be called until it returns an empty
        set (i.e., once more than the number of expected result sets).
        This is to retain compatibility with PyMySQL and MySOLdb.

        Returns
        -------
        ``True``
            If another result set is available
        ``False``
            If no other result set is available

        """
        raise NotImplementedError

    @abc.abstractmethod
    def setinputsizes(self, sizes: Sequence[int]) -> None:
        """Predefine memory areas for parameters."""
        raise NotImplementedError

    @abc.abstractmethod
    def setoutputsize(self, size: int, column: Optional[str] = None) -> None:
        """Set a column buffer size for fetches of large columns."""
        raise NotImplementedError

    @abc.abstractmethod
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
        raise NotImplementedError

    def next(self) -> Optional[Result]:
        """
        Return the next row from the result set for use in iterators.

        Raises
        ------
        StopIteration
            If no more results exist

        Returns
        -------
        tuple of values

        """
        if not self.is_connected():
            raise exceptions.InterfaceError(2048, 'Cursor is closed.')
        out = self.fetchone()
        if out is None:
            raise StopIteration
        return out

    __next__ = next

    def __iter__(self) -> Any:
        """Return result iterator."""
        return self

    def __enter__(self) -> 'Cursor':
        """Enter a context."""
        return self

    def __exit__(
        self, exc_type: Optional[object],
        exc_value: Optional[Exception], exc_traceback: Optional[str],
    ) -> None:
        """Exit a context."""
        self.close()


class ShowResult(Sequence[Any]):
    """
    Simple result object.

    This object is primarily used for displaying results to a
    terminal or web browser, but it can also be treated like a
    simple data frame where columns are accessible using either
    dictionary key-like syntax or attribute syntax.

    Examples
    --------
    >>> conn.show.status().Value[10]

    >>> conn.show.status()[10]['Value']

    Parameters
    ----------
    *args : Any
        Parameters to send to underlying list constructor
    **kwargs : Any
        Keyword parameters to send to underlying list constructor

    See Also
    --------
    :attr:`Connection.show`

    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._data: List[Dict[str, Any]] = []
        item: Any = None
        for item in list(*args, **kwargs):
            self._data.append(item)

    def __getitem__(self, item: Union[int, slice]) -> Any:
        return self._data[item]

    def __getattr__(self, name: str) -> List[Any]:
        if name.startswith('_ipython'):
            raise AttributeError(name)
        out = []
        for item in self._data:
            out.append(item[name])
        return out

    def __len__(self) -> int:
        return len(self._data)

    def __repr__(self) -> str:
        if not self._data:
            return ''
        return '\n{}\n'.format(self._format_table(self._data))

    @property
    def columns(self) -> List[str]:
        """The columns in the result."""
        if not self._data:
            return []
        return list(self._data[0].keys())

    def _format_table(self, rows: Sequence[Dict[str, Any]]) -> str:
        if not self._data:
            return ''

        keys = rows[0].keys()
        lens = [len(x) for x in keys]

        for row in self._data:
            align = ['<'] * len(keys)
            for i, k in enumerate(keys):
                lens[i] = max(lens[i], len(str(row[k])))
                align[i] = '<' if isinstance(row[k], (bytes, bytearray, str)) else '>'

        fmt = '| %s |' % '|'.join([' {:%s%d} ' % (x, y) for x, y in zip(align, lens)])

        out = []
        out.append(fmt.format(*keys))
        out.append('-' * len(out[0]))
        for row in rows:
            out.append(fmt.format(*[str(x) for x in row.values()]))
        return '\n'.join(out)

    def __str__(self) -> str:
        return self.__repr__()

    def _repr_html_(self) -> str:
        if not self._data:
            return ''
        cell_style = 'style="text-align: left; vertical-align: top"'
        out = []
        out.append('<table border="1" class="dataframe">')
        out.append('<thead>')
        out.append('<tr>')
        for name in self._data[0].keys():
            out.append(f'<th {cell_style}>{name}</th>')
        out.append('</tr>')
        out.append('</thead>')
        out.append('<tbody>')
        for row in self._data:
            out.append('<tr>')
            for item in row.values():
                out.append(f'<td {cell_style}>{item}</td>')
            out.append('</tr>')
        out.append('</tbody>')
        out.append('</table>')
        return ''.join(out)


class ShowAccessor(object):
    """
    Accessor for ``SHOW`` commands.

    See Also
    --------
    :attr:`Connection.show`

    """

    def __init__(self, conn: 'Connection'):
        self._conn = conn

    def columns(self, table: str, full: bool = False) -> ShowResult:
        """Show the column information for the given table."""
        table = quote_identifier(table)
        if full:
            return self._iquery(f'full columns in {table}')
        return self._iquery(f'columns in {table}')

    def tables(self, extended: bool = False) -> ShowResult:
        """Show tables in the current database."""
        if extended:
            return self._iquery('tables extended')
        return self._iquery('tables')

    def warnings(self) -> ShowResult:
        """Show warnings."""
        return self._iquery('warnings')

    def errors(self) -> ShowResult:
        """Show errors."""
        return self._iquery('errors')

    def databases(self, extended: bool = False) -> ShowResult:
        """Show all databases in the server."""
        if extended:
            return self._iquery('databases extended')
        return self._iquery('databases')

    def database_status(self) -> ShowResult:
        """Show status of the current database."""
        return self._iquery('database status')

    def global_status(self) -> ShowResult:
        """Show global status of the current server."""
        return self._iquery('global status')

    def indexes(self, table: str) -> ShowResult:
        """Show all indexes in the given table."""
        table = quote_identifier(table)
        return self._iquery(f'indexes in {table}')

    def functions(self) -> ShowResult:
        """Show all functions in the current database."""
        return self._iquery('functions')

    def partitions(self, extended: bool = False) -> ShowResult:
        """Show partitions in the current database."""
        if extended:
            return self._iquery('partitions extended')
        return self._iquery('partitions')

    def pipelines(self) -> ShowResult:
        """Show all pipelines in the current database."""
        return self._iquery('pipelines')

    def plan(self, plan_id: int, json: bool = False) -> ShowResult:
        """Show the plan for the given plan ID."""
        plan_id = int(plan_id)
        if json:
            return self._iquery(f'plan json {plan_id}')
        return self._iquery(f'plan {plan_id}')

    def plancache(self) -> ShowResult:
        """Show all query statements compiled and executed."""
        return self._iquery('plancache')

    def processlist(self) -> ShowResult:
        """Show details about currently running threads."""
        return self._iquery('processlist')

    def reproduction(self, outfile: Optional[str] = None) -> ShowResult:
        """Show troubleshooting data for query optimizer and code generation."""
        if outfile:
            outfile = outfile.replace('"', r'\"')
            return self._iquery('reproduction into outfile "{outfile}"')
        return self._iquery('reproduction')

    def schemas(self) -> ShowResult:
        """Show schemas in the server."""
        return self._iquery('schemas')

    def session_status(self) -> ShowResult:
        """Show server status information for a session."""
        return self._iquery('session status')

    def status(self, extended: bool = False) -> ShowResult:
        """Show server status information."""
        if extended:
            return self._iquery('status extended')
        return self._iquery('status')

    def table_status(self) -> ShowResult:
        """Show table status information for the current database."""
        return self._iquery('table status')

    def procedures(self) -> ShowResult:
        """Show all procedures in the current database."""
        return self._iquery('procedures')

    def aggregates(self) -> ShowResult:
        """Show all aggregate functions in the current database."""
        return self._iquery('aggregates')

    def create_aggregate(self, name: str) -> ShowResult:
        """Show the function creation code for the given aggregate function."""
        name = quote_identifier(name)
        return self._iquery(f'create aggregate {name}')

    def create_function(self, name: str) -> ShowResult:
        """Show the function creation code for the given function."""
        name = quote_identifier(name)
        return self._iquery(f'create function {name}')

    def create_pipeline(self, name: str, extended: bool = False) -> ShowResult:
        """Show the pipeline creation code for the given pipeline."""
        name = quote_identifier(name)
        if extended:
            return self._iquery(f'create pipeline {name} extended')
        return self._iquery(f'create pipeline {name}')

    def create_table(self, name: str) -> ShowResult:
        """Show the table creation code for the given table."""
        name = quote_identifier(name)
        return self._iquery(f'create table {name}')

    def create_view(self, name: str) -> ShowResult:
        """Show the view creation code for the given view."""
        name = quote_identifier(name)
        return self._iquery(f'create view {name}')

#   def grants(
#       self,
#       user: Optional[str] = None,
#       hostname: Optional[str] = None,
#       role: Optional[str] = None
#   ) -> ShowResult:
#       """Show the privileges for the given user or role."""
#       if user:
#           if not re.match(r'^[\w+-_]+$', user):
#               raise ValueError(f'User name is not valid: {user}')
#           if hostname and not re.match(r'^[\w+-_\.]+$', hostname):
#               raise ValueError(f'Hostname is not valid: {hostname}')
#           if hostname:
#               return self._iquery(f"grants for '{user}@{hostname}'")
#           return self._iquery(f"grants for '{user}'")
#       if role:
#           if not re.match(r'^[\w+-_]+$', role):
#               raise ValueError(f'Role is not valid: {role}')
#           return self._iquery(f"grants for role '{role}'")
#       return self._iquery('grants')

    def _iquery(self, qtype: str) -> ShowResult:
        """Query the given object type."""
        out = self._conn._iquery(f'show {qtype}')
        for i, row in enumerate(out):
            new_row = {}
            for j, (k, v) in enumerate(row.items()):
                if j == 0:
                    k = 'Name'
                new_row[under2camel(k)] = v
            out[i] = new_row
        return ShowResult(out)


class Connection(metaclass=abc.ABCMeta):
    """
    SingleStoreDB connection.

    Instances of this object are typically created through the
    :func:`singlestoredb.connect` function rather than creating them directly.
    See the :func:`singlestoredb.connect` function for parameter definitions.

    See Also
    --------
    :func:`singlestoredb.connect`

    """

    Warning = exceptions.Warning
    Error = exceptions.Error
    InterfaceError = exceptions.InterfaceError
    DataError = exceptions.DataError
    DatabaseError = exceptions.DatabaseError
    OperationalError = exceptions.OperationalError
    IntegrityError = exceptions.IntegrityError
    InternalError = exceptions.InternalError
    ProgrammingError = exceptions.ProgrammingError
    NotSupportedError = exceptions.NotSupportedError

    #: Read-only DB-API parameter style
    paramstyle = 'pyformat'

    # Must be set by subclass
    driver = ''

    # Populated when first needed
    _map_param_converter: Optional[sqlparams.SQLParams] = None
    _positional_param_converter: Optional[sqlparams.SQLParams] = None

    def __init__(self, **kwargs: Any):
        """Call :func:`singlestoredb.connect` instead."""
        self.connection_params: Dict[str, Any] = kwargs
        self.errorhandler = None
        self._results_type: str = kwargs.get('results_type', None) or 'tuples'

        #: Session encoding
        self.encoding = self.connection_params.get('charset', None) or 'utf-8'
        self.encoding = self.encoding.replace('mb4', '')

        # Handle various authentication types
        credential_type = self.connection_params.get('credential_type', None)
        if credential_type == auth.BROWSER_SSO:
            # TODO: Cache info for token refreshes
            info = auth.get_jwt(self.connection_params['user'])
            self.connection_params['password'] = str(info)
            self.connection_params['credential_type'] = auth.JWT

        #: Attribute-like access to global server variables
        self.globals = VariableAccessor(self, 'global')

        #: Attribute-like access to local / session server variables
        self.locals = VariableAccessor(self, 'local')

        #: Attribute-like access to cluster global server variables
        self.cluster_globals = VariableAccessor(self, 'cluster global')

        #: Attribute-like access to cluster local / session server variables
        self.cluster_locals = VariableAccessor(self, 'cluster local')

        #: Attribute-like access to all server variables
        self.vars = VariableAccessor(self, '')

        #: Attribute-like access to all cluster server variables
        self.cluster_vars = VariableAccessor(self, 'cluster')

        # For backwards compatibility with SQLAlchemy package
        self._driver = Driver(self.driver)

        # Output decoders
        self.decoders: Dict[int, Callable[[Any], Any]] = {}

    @classmethod
    def _convert_params(
        cls, oper: str,
        params: Optional[Union[Sequence[Any], Dict[str, Any], Any]],
    ) -> Tuple[Any, ...]:
        """Convert query to correct parameter format."""
        if params:

            if cls._map_param_converter is None:
                cls._map_param_converter = sqlparams.SQLParams(
                    map_paramstyle, cls.paramstyle, escape_char=True,
                )

            if cls._positional_param_converter is None:
                cls._positional_param_converter = sqlparams.SQLParams(
                    positional_paramstyle, cls.paramstyle, escape_char=True,
                )

            is_sequence = isinstance(params, Sequence) \
                and not isinstance(params, str) \
                and not isinstance(params, bytes)
            is_mapping = isinstance(params, Mapping)

            param_converter = cls._map_param_converter \
                if is_mapping else cls._positional_param_converter

            if not is_sequence and not is_mapping:
                params = [params]

            return param_converter.format(oper, params)

        return (oper, None)

    def autocommit(self, value: bool = True) -> None:
        """Set autocommit mode."""
        self.locals.autocommit = bool(value)

    @abc.abstractmethod
    def connect(self) -> 'Connection':
        """Connect to the server."""
        raise NotImplementedError

    def _iquery(
        self, oper: str,
        params: Optional[Union[Sequence[Any], Dict[str, Any]]] = None,
        fix_names: bool = True,
    ) -> List[Dict[str, Any]]:
        """Return the results of a query as a list of dicts (for internal use)."""
        with self.cursor() as cur:
            cur.execute(oper, params)
            if not re.match(r'^\s*(select|show|call|echo)\s+', oper, flags=re.I):
                return []
            out = list(cur.fetchall())
            if not out:
                return []
            if isinstance(out, DataFrame):
                out = out.to_dict(orient='records')
            elif isinstance(out[0], (tuple, list)):
                if cur.description:
                    names = [x[0] for x in cur.description]
                    if fix_names:
                        names = [under2camel(str(x).replace(' ', '')) for x in names]
                    out = [{k: v for k, v in zip(names, row)} for row in out]
            return out

    @abc.abstractmethod
    def close(self) -> None:
        """Close the database connection."""
        raise NotImplementedError

    @abc.abstractmethod
    def commit(self) -> None:
        """Commit the pending transaction."""
        raise NotImplementedError

    @abc.abstractmethod
    def rollback(self) -> None:
        """Rollback the pending transaction."""
        raise NotImplementedError

    @abc.abstractmethod
    def cursor(self) -> Cursor:
        """
        Create a new cursor object.

        See Also
        --------
        :class:`Cursor`

        Returns
        -------
        :class:`Cursor`

        """
        raise NotImplementedError

    @abc.abstractproperty
    def messages(self) -> List[Tuple[int, str]]:
        """Messages generated during the connection."""
        raise NotImplementedError

    def __enter__(self) -> 'Connection':
        """Enter a context."""
        return self

    def __exit__(
        self, exc_type: Optional[object],
        exc_value: Optional[Exception], exc_traceback: Optional[str],
    ) -> None:
        """Exit a context."""
        self.close()

    @abc.abstractmethod
    def is_connected(self) -> bool:
        """
        Determine if the database is still connected.

        Returns
        -------
        bool

        """
        raise NotImplementedError

    def enable_data_api(self, port: Optional[int] = None) -> int:
        """
        Enable the data API in the server.

        Use of this method requires privileges that allow setting global
        variables and starting the HTTP proxy.

        Parameters
        ----------
        port : int, optional
            The port number that the HTTP server should run on. If this
            value is not specified, the current value of the
            ``http_proxy_port`` is used.

        See Also
        --------
        :meth:`disable_data_api`

        Returns
        -------
        int
            port number of the HTTP server

        """
        if port is not None:
            self.globals.http_proxy_port = int(port)
        self.globals.http_api = True
        self._iquery('restart proxy')
        return int(self.globals.http_proxy_port)

    enable_http_api = enable_data_api

    def disable_data_api(self) -> None:
        """
        Disable the data API.

        See Also
        --------
        :meth:`enable_data_api`

        """
        self.globals.http_api = False
        self._iquery('restart proxy')

    disable_http_api = disable_data_api

    @property
    def show(self) -> ShowAccessor:
        """Access server properties managed by the SHOW statement."""
        return ShowAccessor(self)


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
    charset: Optional[str] = None,
    ssl_key: Optional[str] = None, ssl_cert: Optional[str] = None,
    ssl_ca: Optional[str] = None, ssl_disabled: Optional[bool] = None,
    ssl_cipher: Optional[str] = None, ssl_verify_cert: Optional[bool] = None,
    ssl_verify_identity: Optional[bool] = None,
    conv: Optional[Dict[int, Callable[..., Any]]] = None,
    credential_type: Optional[str] = None,
    autocommit: Optional[bool] = None,
    results_type: Optional[str] = None,
    buffered: Optional[bool] = None,
    results_format: Optional[str] = None,
    program_name: Optional[str] = None,
    conn_attrs: Optional[Dict[str, str]] = None,
    multi_statements: Optional[bool] = None,
    connect_timeout: Optional[int] = None,
    nan_as_null: Optional[bool] = None,
    inf_as_null: Optional[bool] = None,
    encoding_errors: Optional[str] = None,
    track_env: Optional[bool] = None,
) -> Connection:
    """
    Return a SingleStoreDB connection.

    Parameters
    ----------
    host : str, optional
        Hostname, IP address, or URL that describes the connection.
        The scheme or protocol defines which database connector to use.
        By default, the ``mysql`` scheme is used. To connect to the
        HTTP API, the scheme can be set to ``http`` or ``https``. The username,
        password, host, and port are specified as in a standard URL. The path
        indicates the database name. The overall form of the URL is:
        ``scheme://user:password@host:port/db_name``.  The scheme can
        typically be left off (unless you are using the HTTP API):
        ``user:password@host:port/db_name``.
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
    charset : str, optional
        Character set for string values
    ssl_key : str, optional
        File containing SSL key
    ssl_cert : str, optional
        File containing SSL certificate
    ssl_ca : str, optional
        File containing SSL certificate authority
    ssl_cipher : str, optional
        Sets the SSL cipher list
    ssl_disabled : bool, optional
        Disable SSL usage
    ssl_verify_cert : bool, optional
        Verify the server's certificate. This is automatically enabled if
        ``ssl_ca`` is also specified.
    ssl_verify_identity : bool, optional
        Verify the server's identity
    conv : dict[int, Callable], optional
        Dictionary of data conversion functions
    credential_type : str, optional
        Type of authentication to use: auth.PASSWORD, auth.JWT, or auth.BROWSER_SSO
    autocommit : bool, optional
        Enable autocommits
    results_type : str, optional
        The form of the query results: tuples, namedtuples, dicts
    results_format : str, optional
        Deprecated. This option has been renamed to results_type.
    program_name : str, optional
        Name of the program
    conn_attrs : dict, optional
        Additional connection attributes for telemetry. Example:
        {'program_version': "1.0.2", "_connector_name": "dbt connector"}
    multi_statements: bool, optional
        Should multiple statements be allowed within a single query?
    connect_timeout : int, optional
        The timeout for connecting to the database in seconds.
        (default: 10, min: 1, max: 31536000)
    nan_as_null : bool, optional
        Should NaN values be treated as NULLs when used in parameter
        substitutions including uploaded data?
    inf_as_null : bool, optional
        Should Inf values be treated as NULLs when used in parameter
        substitutions including uploaded data?
    encoding_errors : str, optional
        The error handler name for value decoding errors
    track_env : bool, optional
        Should the connection track the SINGLESTOREDB_URL environment variable?

    Examples
    --------
    Standard database connection

    >>> conn = s2.connect('me:p455w0rd@s2-host.com/my_db')

    Connect to HTTP API on port 8080

    >>> conn = s2.connect('http://me:p455w0rd@s2-host.com:8080/my_db')

    Using an environment variable for connection string

    >>> os.environ['SINGLESTOREDB_URL'] = 'me:p455w0rd@s2-host.com/my_db'
    >>> conn = s2.connect()

    Specifying credentials using environment variables

    >>> os.environ['SINGLESTOREDB_USER'] = 'me'
    >>> os.environ['SINGLESTOREDB_PASSWORD'] = 'p455w0rd'
    >>> conn = s2.connect('s2-host.com/my_db')

    Specifying options with keyword parameters

    >>> conn = s2.connect('s2-host.com/my_db', user='me', password='p455w0rd',
                          local_infile=True)

    Specifying options with URL parameters

    >>> conn = s2.connect('s2-host.com/my_db?local_infile=True&charset=utf8')

    Connecting within a context manager

    >>> with s2.connect('...') as conn:
    ...     with conn.cursor() as cur:
    ...         cur.execute('...')

    Setting session variables, the code below sets the ``autocommit`` option

    >>> conn.locals.autocommit = True

    Getting session variables

    >>> conn.locals.autocommit
    True

    See Also
    --------
    :class:`Connection`

    Returns
    -------
    :class:`Connection`

    """
    params = build_params(**dict(locals()))
    driver = params.get('driver', 'mysql')

    if not driver or driver == 'mysql':
        from .mysql.connection import Connection  # type: ignore
        return Connection(**params)

    if driver in ['http', 'https']:
        from .http.connection import Connection
        return Connection(**params)

    raise ValueError(f'Unrecognized protocol: {driver}')
