#!/usr/bin/env python3
import datetime
import inspect
import numbers
import os
import re
import string
import textwrap
import typing
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Sequence
from typing import Tuple
from typing import TypeVar
from typing import Union
from urllib.parse import urljoin

try:
    import numpy as np
    has_numpy = True
except ImportError:
    has_numpy = False

from . import dtypes as dt
from ..mysql.converters import escape_item  # type: ignore


array_types: Tuple[Any, ...]

if has_numpy:
    array_types = (Sequence, np.ndarray)
    numpy_type_map = {
        np.integer: 'int64',
        np.int_: 'int64',
        np.int64: 'int64',
        np.int32: 'int32',
        np.int16: 'int16',
        np.int8: 'int8',
        np.uint: 'uint64',
        np.unsignedinteger: 'uint64',
        np.uint64: 'uint64',
        np.uint32: 'uint32',
        np.uint16: 'uint16',
        np.uint8: 'uint8',
        np.longlong: 'uint64',
        np.ulonglong: 'uint64',
        np.unicode_: 'str',
        np.str_: 'str',
        np.bytes_: 'bytes',
        np.float_: 'float64',
        np.float64: 'float64',
        np.float32: 'float32',
        np.float16: 'float16',
        np.double: 'float64',
    }
else:
    array_types = (Sequence,)
    numpy_type_map = {}

float_type_map = {
    'float': 'float64',
    'float_': 'float64',
    'float64': 'float64',
    'f8': 'float64',
    'double': 'float64',
    'float32': 'float32',
    'f4': 'float32',
    'float16': 'float16',
    'f2': 'float16',
    'float8': 'float8',
    'f1': 'float8',
}

int_type_map = {
    'int': 'int64',
    'integer': 'int64',
    'int_': 'int64',
    'int64': 'int64',
    'i8': 'int64',
    'int32': 'int32',
    'i4': 'int32',
    'int16': 'int16',
    'i2': 'int16',
    'int8': 'int8',
    'i1': 'int8',
    'uint': 'uint64',
    'uinteger': 'uint64',
    'uint_': 'uint64',
    'uint64': 'uint64',
    'u8': 'uint64',
    'uint32': 'uint32',
    'u4': 'uint32',
    'uint16': 'uint16',
    'u2': 'uint16',
    'uint8': 'uint8',
    'u1': 'uint8',
}

sql_type_map = {
    'bool': 'BOOL',
    'int8': 'TINYINT',
    'int16': 'SMALLINT',
    'int32': 'INT',
    'int64': 'BIGINT',
    'uint8': 'TINYINT UNSIGNED',
    'uint16': 'SMALLINT UNSIGNED',
    'uint32': 'INT UNSIGNED',
    'uint64': 'BIGINT UNSIGNED',
    'float32': 'FLOAT',
    'float64': 'DOUBLE',
    'str': 'TEXT',
    'bytes': 'BLOB',
    'null': 'NULL',
    'datetime': 'DATETIME',
    'datetime6': 'DATETIME(6)',
    'date': 'DATE',
    'time': 'TIME',
    'time6': 'TIME(6)',
}

sql_to_type_map = {
    'BOOL': 'bool',
    'TINYINT': 'int8',
    'TINYINT UNSIGNED': 'uint8',
    'SMALLINT': 'int16',
    'SMALLINT UNSIGNED': 'int16',
    'MEDIUMINT': 'int32',
    'MEDIUMINT UNSIGNED': 'int32',
    'INT24': 'int32',
    'INT24 UNSIGNED': 'int32',
    'INT': 'int32',
    'INT UNSIGNED': 'int32',
    'INTEGER': 'int32',
    'INTEGER UNSIGNED': 'int32',
    'BIGINT': 'int64',
    'BIGINT UNSIGNED': 'int64',
    'FLOAT': 'float32',
    'DOUBLE': 'float64',
    'REAL': 'float64',
    'DATE': 'date',
    'TIME': 'time',
    'TIME(6)': 'time6',
    'DATETIME': 'datetime',
    'DATETIME(6)': 'datetime',
    'TIMESTAMP': 'datetime',
    'TIMESTAMP(6)': 'datetime',
    'YEAR': 'uint64',
    'CHAR': 'str',
    'VARCHAR': 'str',
    'TEXT': 'str',
    'TINYTEXT': 'str',
    'MEDIUMTEXT': 'str',
    'LONGTEXT': 'str',
    'BINARY': 'bytes',
    'VARBINARY': 'bytes',
    'BLOB': 'bytes',
    'TINYBLOB': 'bytes',
    'MEDIUMBLOB': 'bytes',
    'LONGBLOB': 'bytes',
}


class Collection:
    """Base class for collection data types."""

    def __init__(self, *item_dtypes: Union[List[type], type]):
        self.item_dtypes = item_dtypes


class TupleCollection(Collection):
    pass


class ArrayCollection(Collection):
    pass


def escape_name(name: str) -> str:
    """Escape a function parameter name."""
    if '`' in name:
        name = name.replace('`', '``')
    return f'`{name}`'


def simplify_dtype(dtype: Any) -> List[Any]:
    """
    Expand a type annotation to a flattened list of atomic types.

    Parameters
    ----------
    dtype : Any
        Python type annotation

    Returns
    -------
    List[Any] -- list of dtype strings, TupleCollections, and ArrayCollections

    """
    origin = typing.get_origin(dtype)
    atype = type(dtype)
    args = []

    # Flatten Unions
    if origin is Union:
        for x in typing.get_args(dtype):
            args.extend(simplify_dtype(x))

    # Expand custom types to individual types (does not support bounds)
    elif atype is TypeVar:
        for x in dtype.__constraints__:
            args.extend(simplify_dtype(x))
        if dtype.__bound__:
            args.extend(simplify_dtype(dtype.__bound__))

    # Sequence types
    elif origin is not None and issubclass(origin, Sequence):
        item_args: List[Union[List[type], type]] = []
        for x in typing.get_args(dtype):
            item_dtype = simplify_dtype(x)
            if len(item_dtype) == 1:
                item_args.append(item_dtype[0])
            else:
                item_args.append(item_dtype)
        if origin is tuple or origin is Tuple:
            args.append(TupleCollection(*item_args))
        elif len(item_args) > 1:
            raise TypeError('sequence types may only contain one item data type')
        else:
            args.append(ArrayCollection(*item_args))

    # Not a Union or TypeVar
    else:
        args.append(dtype)

    return args


def classify_dtype(dtype: Any) -> str:
    """Classify the type annotation into a type name."""
    if isinstance(dtype, list):
        return '|'.join(classify_dtype(x) for x in dtype)

    # Specific types
    if dtype is None or dtype is type(None):  # noqa: E721
        return 'null'
    if dtype is int:
        return 'int64'
    if dtype is float:
        return 'float64'
    if dtype is bool:
        return 'bool'

    if not inspect.isclass(dtype):
        # Check for compound types
        origin = typing.get_origin(dtype)
        if origin is not None:
            # Tuple type
            if origin is Tuple:
                args = typing.get_args(dtype)
                item_dtypes = ','.join(classify_dtype(x) for x in args)
                return f'tuple:{item_dtypes}'

            # Array types
            elif issubclass(origin, array_types):
                args = typing.get_args(dtype)
                item_dtype = classify_dtype(args[0])
                return f'array[{item_dtype}]'

            raise TypeError(f'unsupported type annotation: {dtype}')

        if isinstance(dtype, ArrayCollection):
            item_dtypes = ','.join(classify_dtype(x) for x in dtype.item_dtypes)
            return f'array[{item_dtypes}]'

        if isinstance(dtype, TupleCollection):
            item_dtypes = ','.join(classify_dtype(x) for x in dtype.item_dtypes)
            return f'tuple[{item_dtypes}]'

    # Check numpy types if it's available
    if dtype in numpy_type_map:
        return numpy_type_map[dtype]

    # Broad numeric types
    if issubclass(dtype, int):
        return 'int64'
    if issubclass(dtype, float):
        return 'float64'

    # Strings / Bytes
    if issubclass(dtype, str):
        return 'str'
    if issubclass(dtype, (bytes, bytearray)):
        return 'bytes'

    # Date / Times
    if issubclass(dtype, datetime.datetime):
        return 'datetime'
    if issubclass(dtype, datetime.date):
        return 'date'
    if issubclass(dtype, datetime.timedelta):
        return 'time'

    # Last resort, guess it by the name...
    name = dtype.__name__.lower()
    is_float = issubclass(dtype, numbers.Real)
    is_int = issubclass(dtype, numbers.Integral)
    if is_float:
        return float_type_map.get(name, 'float64')
    if is_int:
        return int_type_map.get(name, 'int64')

    raise TypeError(f'unsupported type annotation: {dtype}')


def collapse_dtypes(dtypes: Union[str, List[str]]) -> str:
    """
    Collapse a dtype possibly containing multiple data types to one type.

    Parameters
    ----------
    dtypes : str or list[str]
        The data types to collapse

    Returns
    -------
    str

    """
    if not isinstance(dtypes, list):
        return dtypes

    orig_dtypes = dtypes
    dtypes = list(set(dtypes))

    is_nullable = 'null' in dtypes

    dtypes = [x for x in dtypes if x != 'null']

    if 'uint64' in dtypes:
        dtypes = [x for x in dtypes if x not in ('uint8', 'uint16', 'uint32')]
    if 'uint32' in dtypes:
        dtypes = [x for x in dtypes if x not in ('uint8', 'uint16')]
    if 'uint16' in dtypes:
        dtypes = [x for x in dtypes if x not in ('uint8',)]

    if 'int64' in dtypes:
        dtypes = [
            x for x in dtypes if x not in (
                'int8', 'int16', 'int32',
                'uint8', 'uint16', 'uint32',
            )
        ]
    if 'int32' in dtypes:
        dtypes = [
            x for x in dtypes if x not in (
                'int8', 'int16',
                'uint8', 'uint16',
            )
        ]
    if 'int16' in dtypes:
        dtypes = [x for x in dtypes if x not in ('int8', 'uint8')]

    if 'float64' in dtypes:
        dtypes = [
            x for x in dtypes if x not in (
                'float32',
                'int8', 'int16', 'int32', 'int64',
                'uint8', 'uint16', 'uint32', 'uint64',
            )
        ]
    if 'float32' in dtypes:
        dtypes = [
            x for x in dtypes if x not in (
                'int8', 'int16', 'int32',
                'uint8', 'uint16', 'uint32',
            )
        ]

    for i, item in enumerate(dtypes):

        if item.startswith('array[') and '|' in item:
            _, item_spec = item.split('[', 1)
            item_spec = item_spec[:-1]
            item = collapse_dtypes(item_spec.split('|'))
            dtypes[i] = f'array[{item}]'

        elif item.startswith('tuple[') and '|' in item:
            _, item_spec = item.split('[', 1)
            item_spec = item_spec[:-1]
            sub_dtypes = []
            for subitem_spec in item_spec.split(','):
                item = collapse_dtypes(subitem_spec.split('|'))
                sub_dtypes.append(item)
            dtypes[i] = f'tuple[{",".join(sub_dtypes)}]'

    if len(dtypes) > 1:
        raise TypeError(
            'types can not be collapsed to a single type: '
            f'{", ".join(orig_dtypes)}',
        )

    if not dtypes:
        return 'null'

    return dtypes[0] + ('?' if is_nullable else '')


def get_signature(func: Callable[..., Any], name: Optional[str] = None) -> Dict[str, Any]:
    '''
    Print the UDF signature of the Python callable.

    Parameters
    ----------
    func : Callable
        The function to extract the signature of
    name : str, optional
        Name override for function

    Returns
    -------
    Dict[str, Any]

    '''
    signature = inspect.signature(func)
    args: List[Dict[str, Any]] = []
    attrs = getattr(func, '_singlestoredb_attrs', {})
    name = attrs.get('name', name if name else func.__name__)
    out: Dict[str, Any] = dict(name=name, args=args)

    arg_names = [x for x in signature.parameters]
    defaults = [
        x.default if x.default is not inspect.Parameter.empty else None
        for x in signature.parameters.values()
    ]
    annotations = {
        k: x.annotation for k, x in signature.parameters.items()
        if x.annotation is not inspect.Parameter.empty
    }

    for p in signature.parameters.values():
        if p.kind == inspect.Parameter.VAR_POSITIONAL:
            raise TypeError('variable positional arguments are not supported')
        elif p.kind == inspect.Parameter.VAR_KEYWORD:
            raise TypeError('variable keyword arguments are not supported')

    args_overrides = attrs.get('args', None)
    returns_overrides = attrs.get('returns', None)

    spec_diff = set(arg_names).difference(set(annotations.keys()))

    # Make sure all arguments are annotated
    if spec_diff and args_overrides is None:
        raise TypeError(
            'missing annotations for {} in {}'
            .format(', '.join(spec_diff), name),
        )
    elif isinstance(args_overrides, dict):
        for s in spec_diff:
            if s not in args_overrides:
                raise TypeError(
                    'missing annotations for {} in {}'
                    .format(', '.join(spec_diff), name),
                )
    elif isinstance(args_overrides, list):
        if len(arg_names) != len(args_overrides):
            raise TypeError(
                'number of annotations does not match in {}: {}'
                .format(name, ', '.join(spec_diff)),
            )

    for i, arg in enumerate(arg_names):
        if isinstance(args_overrides, list):
            sql = args_overrides[i]
            arg_type = sql_to_dtype(sql)
        elif isinstance(args_overrides, dict) and arg in args_overrides:
            sql = args_overrides[arg]
            arg_type = sql_to_dtype(sql)
        elif isinstance(args_overrides, str):
            sql = args_overrides
            arg_type = sql_to_dtype(sql)
        elif args_overrides is not None \
                and not isinstance(args_overrides, (list, dict, str)):
            raise TypeError(f'unrecognized type for arguments: {args_overrides}')
        else:
            arg_type = collapse_dtypes([
                classify_dtype(x) for x in simplify_dtype(annotations[arg])
            ])
            sql = dtype_to_sql(arg_type)
        args.append(dict(name=arg, dtype=arg_type, sql=sql, default=defaults[i]))

    if returns_overrides is None \
            and signature.return_annotation is inspect.Signature.empty:
        raise TypeError(f'no return value annotation in function {name}')

    if isinstance(returns_overrides, str):
        sql = returns_overrides
        out_type = sql_to_dtype(sql)
    elif returns_overrides is not None and not isinstance(returns_overrides, str):
        raise TypeError(f'unrecognized type for return value: {returns_overrides}')
    else:
        out_type = collapse_dtypes([
            classify_dtype(x) for x in simplify_dtype(signature.return_annotation)
        ])
        sql = dtype_to_sql(out_type)
    out['returns'] = dict(dtype=out_type, sql=sql, default=None)

    copied_keys = ['database', 'environment', 'packages', 'resources', 'replace']
    for key in copied_keys:
        if attrs.get(key):
            out[key] = attrs[key]

    out['endpoint'] = '/invoke'
    out['doc'] = func.__doc__

    return out


def sql_to_dtype(sql: str) -> str:
    """
    Convert a SQL type into a normalized data type identifier.

    Parameters
    ----------
    sql : str
        SQL data type specification

    Returns
    -------
    str

    """
    sql = re.sub(r'\s+', r' ', sql.upper().strip())

    m = re.match(r'(\w+)(\([^\)]+\))?', sql)
    if not m:
        raise TypeError(f'unrecognized data type: {sql}')

    sql_type = m.group(1)
    type_attrs = re.split(r'\s*,\s*', m.group(2) or '')

    if sql_type in ('DATETIME', 'TIME', 'TIMESTAMP') and \
            type_attrs and type_attrs[0] == '6':
        sql_type += '(6)'

    elif ' UNSIGNED' in sql:
        sql_type += ' UNSIGNED'

    try:
        dtype = sql_to_type_map[sql_type]
    except KeyError:
        raise TypeError(f'unrecognized data type: {sql_type}')

    if ' NOT NULL' not in sql:
        dtype += '?'

    return dtype


def dtype_to_sql(dtype: str, default: Any = None) -> str:
    """
    Convert a collapsed dtype string to a SQL type.

    Parameters
    ----------
    dtype : str
        Simplified data type string
    default : Any, optional
        Default value

    Returns
    -------
    str

    """
    nullable = ' NOT NULL'
    if dtype.endswith('?'):
        nullable = ' NULL'
        dtype = dtype[:-1]

    if dtype == 'null':
        nullable = ''

    default_clause = ''
    if default is not None:
        if default is dt.NULL:
            default = None
        default_clause = f' DEFAULT {escape_item(default, "utf8")}'

    if dtype.startswith('array['):
        _, dtypes = dtype.split('[', 1)
        dtypes = dtypes[:-1]
        item_dtype = dtype_to_sql(dtypes)
        return f'ARRAY({item_dtype}){nullable}{default_clause}'

    if dtype.startswith('tuple['):
        _, dtypes = dtype.split('[', 1)
        dtypes = dtypes[:-1]
        item_dtypes = []
        for i, item in enumerate(dtypes.split(',')):
            name = string.ascii_letters[i]
            if '=' in item:
                name, item = item.split('=', 1)
            item_dtypes.append(name + ' ' + dtype_to_sql(item))
        return f'RECORD({", ".join(item_dtypes)}){nullable}{default_clause}'

    return f'{sql_type_map[dtype]}{nullable}{default_clause}'


def signature_to_sql(
    signature: Dict[str, Any],
    base_url: Optional[str] = None,
    data_format: str = 'rowdat_1',
) -> str:
    '''
    Convert a dictionary function signature into SQL.

    Parameters
    ----------
    signature : Dict[str, Any]
        Function signature in the form of a dictionary as returned by
        the `get_signature` function

    Returns
    -------
    str : SQL formatted function signature

    '''
    args = []
    for arg in signature['args']:
        # Use default value from Python function if SQL doesn't set one
        default = ''
        if not re.search(r'\s+default\s+\S', arg['sql'], flags=re.I):
            default = ''
            if arg.get('default', None) is not None:
                default = f' DEFAULT {escape_item(arg["default"], "utf8")}'
        args.append(escape_name(arg['name']) + ' ' + arg['sql'] + default)

    returns = ''
    if signature.get('returns'):
        res = signature['returns']['sql']
        returns = f' RETURNS {res}'

    host = os.environ.get('SINGLESTOREDB_EXT_HOST', '127.0.0.1')
    port = os.environ.get('SINGLESTOREDB_EXT_PORT', '8000')

    url = urljoin(base_url or f'https://{host}:{port}', signature['endpoint'])

    database = ''
    if signature.get('database'):
        database = escape_name(signature['database']) + '.'

    replace = 'OR REPLACE ' if signature.get('replace') else ''

    return (
        f'CREATE {replace}EXTERNAL FUNCTION {database}{escape_name(signature["name"])}' +
        '(' + ', '.join(args) + ')' + returns +
        f' AS REMOTE SERVICE "{url}" FORMAT {data_format.upper()};'
    )


def func_to_env(func: Callable[..., Any]) -> str:
    # TODO: multiple functions
    signature = get_signature(func)
    env_name = signature['environment']
    replace = 'OR REPLACE ' if signature.get('replace') else ''
    packages = ', '.join(escape_item(x, 'utf8') for x in signature.get('packages', []))
    resources = ', '.join(escape_item(x, 'utf8') for x in signature.get('resources', []))
    code = inspect.getsource(func)

    return (
        f'CREATE {replace}ENVIRONMENT {env_name} LANGUAGE PYTHON ' +
        'USING EXPORTS ' + escape_name(func.__name__) + ' ' +
        (f'\n    PACKAGES ({packages}) ' if packages else '') +
        (f'\n    RESOURCES ({resources}) ' if resources else '') +
        '\n    AS CLOUD SERVICE' +
        '\n    BEGIN\n' +
        textwrap.indent(code, '    ') +
        '    END;'
    )
