#!/usr/bin/env python3
import dataclasses
import datetime
import inspect
import numbers
import os
import re
import string
import sys
import types
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

try:
    import numpy as np
    has_numpy = True
except ImportError:
    has_numpy = False

try:
    import pydantic
    has_pydantic = True
except ImportError:
    has_pydantic = False


from . import dtypes as dt
from ..mysql.converters import escape_item  # type: ignore

if sys.version_info >= (3, 10):
    _UNION_TYPES = {typing.Union, types.UnionType}
else:
    _UNION_TYPES = {typing.Union}


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
        np.str_: 'str',
        np.bytes_: 'bytes',
        np.float64: 'float64',
        np.float32: 'float32',
        np.float16: 'float16',
        np.double: 'float64',
    }
    if hasattr(np, 'unicode_'):
        numpy_type_map[np.unicode_] = 'str'
    if hasattr(np, 'float_'):
        numpy_type_map[np.float_] = 'float64'
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


def get_annotations(obj: Any) -> Dict[str, Any]:
    """Get the annotations of an object."""
    if hasattr(inspect, 'get_annotations'):
        return inspect.get_annotations(obj)
    if isinstance(obj, type):
        return obj.__dict__.get('__annotations__', {})
    return getattr(obj, '__annotations__', {})


def is_dataframe(obj: Any) -> bool:
    """Check if an object is a DataFrame."""
    # Cheating here a bit so we don't have to import pandas / polars / pyarrow
    # unless we absolutely need to
    return getattr(obj, '__name__', '') in ['DataFrame', 'Table']


def is_typeddict(obj: Any) -> bool:
    """Check if an object is a TypedDict."""
    if hasattr(typing, 'is_typeddict'):
        return typing.is_typeddict(obj)  # noqa: TYP006
    return False


def is_namedtuple(obj: Any) -> bool:
    """Check if an object is a named tuple."""
    if inspect.isclass(obj):
        return (
                issubclass(obj, tuple) and
                hasattr(obj, '_asdict') and
                hasattr(obj, '_fields')
        )
    return (
            isinstance(obj, tuple) and
            hasattr(obj, '_asdict') and
            hasattr(obj, '_fields')
    )


def is_pydantic(obj: Any) -> bool:
    """Check if an object is a pydantic model."""
    if not has_pydantic:
        return False

    if inspect.isclass(obj):
        return issubclass(obj, pydantic.BaseModel)

    return isinstance(obj, pydantic.BaseModel)


def escape_name(name: str) -> str:
    """Escape a function parameter name."""
    if '`' in name:
        name = name.replace('`', '``')
    return f'`{name}`'


def simplify_dtype(dtype: Any) -> List[Any]:
    """
    Expand a type annotation to a flattened list of atomic types.

    This function will attempty to find the underlying type of a
    type annotation. For example, a Union of types will be flattened
    to a list of types. A Tuple or Array type will be expanded to
    a list of types. A TypeVar will be expanded to a list of
    constraints and bounds.

    Parameters
    ----------
    dtype : Any
        Python type annotation

    Returns
    -------
    List[Any]
        list of dtype strings, TupleCollections, and ArrayCollections

    """
    origin = typing.get_origin(dtype)
    atype = type(dtype)
    args = []

    # Flatten Unions
    if origin in _UNION_TYPES:
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


def normalize_dtype(dtype: Any) -> str:
    """
    Normalize the type annotation into a type name.

    Parameters
    ----------
    dtype : Any
        Type annotation, list of type annotations, or a string
        containing a SQL type name

    Returns
    -------
    str
        Normalized type name

    """
    if isinstance(dtype, list):
        return '|'.join(normalize_dtype(x) for x in dtype)

    if isinstance(dtype, str):
        return sql_to_dtype(dtype)

    # Specific types
    if dtype is None or dtype is type(None):  # noqa: E721
        return 'null'
    if dtype is int:
        return 'int64'
    if dtype is float:
        return 'float64'
    if dtype is bool:
        return 'bool'

    if dataclasses.is_dataclass(dtype):
        dc_fields = dataclasses.fields(dtype)
        item_dtypes = ','.join(
            f'{normalize_dtype(simplify_dtype(x.type))}' for x in dc_fields
        )
        return f'tuple[{item_dtypes}]'

    if is_typeddict(dtype):
        td_fields = get_annotations(dtype).keys()
        item_dtypes = ','.join(
            f'{normalize_dtype(simplify_dtype(dtype[x]))}' for x in td_fields
        )
        return f'tuple[{item_dtypes}]'

    if is_pydantic(dtype):
        pyd_fields = dtype.model_fields.values()
        item_dtypes = ','.join(
            f'{normalize_dtype(simplify_dtype(x.annotation))}'  # type: ignore
            for x in pyd_fields
        )
        return f'tuple[{item_dtypes}]'

    if is_namedtuple(dtype):
        nt_fields = get_annotations(dtype).values()
        item_dtypes = ','.join(
            f'{normalize_dtype(simplify_dtype(dtype[x]))}' for x in nt_fields
        )
        return f'tuple[{item_dtypes}]'

    if not inspect.isclass(dtype):

        # Check for compound types
        origin = typing.get_origin(dtype)
        if origin is not None:

            # Tuple type
            if origin is Tuple:
                args = typing.get_args(dtype)
                item_dtypes = ','.join(normalize_dtype(x) for x in args)
                return f'tuple[{item_dtypes}]'

            # Array types
            elif issubclass(origin, array_types):
                args = typing.get_args(dtype)
                item_dtype = normalize_dtype(args[0])
                return f'array[{item_dtype}]'

            raise TypeError(f'unsupported type annotation: {dtype}')

        if isinstance(dtype, ArrayCollection):
            item_dtypes = ','.join(normalize_dtype(x) for x in dtype.item_dtypes)
            return f'array[{item_dtypes}]'

        if isinstance(dtype, TupleCollection):
            item_dtypes = ','.join(normalize_dtype(x) for x in dtype.item_dtypes)
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

    raise TypeError(
        f'unsupported type annotation: {dtype}; '
        'use `args`/`returns` on the @udf/@tvf decorator to specify the data type',
    )


def collapse_dtypes(dtypes: Union[str, List[str]]) -> str:
    """
    Collapse a dtype possibly containing multiple data types to one type.

    This function can fail if there is no single type that naturally
    encompasses all of the types in the list.

    Parameters
    ----------
    dtypes : str or list[str]
        The data types to collapse

    Returns
    -------
    str

    """
    if isinstance(dtypes, str) and '|' in dtypes:
        dtypes = dtypes.split('|')

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


def create_type(
    types: List[Any],
    output_fields: List[str],
    function_type: str = 'udf',
) -> Tuple[str, str]:
    """
    Create the normalized type and SQL code for the given type information.

    Parameters
    ----------
    types : List[Any]
        List of types to be used
    output_fields : List[str]
        List of field names for the resulting type
    function_type : str
        Type of function, either 'udf' or 'tvf'

    Returns
    -------
    Tuple[str, str]
        Tuple containing the output type and SQL code

    """
    out_type = 'tuple[' + ','.join([
        collapse_dtypes(normalize_dtype(x))
        for x in [simplify_dtype(y) for y in types]
    ]) + ']'

    sql = dtype_to_sql(
        out_type, function_type=function_type, field_names=output_fields,
    )

    return out_type, sql


def get_dataclass_schema(obj: Any) -> List[Tuple[str, Any]]:
    """
    Get the schema of a dataclass.

    Parameters
    ----------
    obj : dataclass
        The dataclass to get the schema of

    Returns
    -------
    List[Tuple[str, Any]]
        A list of tuples containing the field names and field types

    """
    return list(get_annotations(obj).items())


def get_typeddict_schema(obj: Any) -> List[Tuple[str, Any]]:
    """
    Get the schema of a TypedDict.

    Parameters
    ----------
    obj : TypedDict
        The TypedDict to get the schema of

    Returns
    -------
    List[Tuple[str, Any]]
        A list of tuples containing the field names and field types

    """
    return list(get_annotations(obj).items())


def get_pydantic_schema(obj: pydantic.BaseModel) -> List[Tuple[str, Any]]:
    """
    Get the schema of a pydantic model.

    Parameters
    ----------
    obj : pydantic.BaseModel
        The pydantic model to get the schema of

    Returns
    -------
    List[Tuple[str, Any]]
        A list of tuples containing the field names and field types

    """
    return [(k, v.annotation) for k, v in obj.model_fields.items()]


def get_namedtuple_schema(obj: Any) -> List[Tuple[Any, str]]:
    """
    Get the schema of a named tuple.

    Parameters
    ----------
    obj : NamedTuple
        The named tuple to get the schema of

    Returns
    -------
    List[Tuple[Any, str]]
        A list of tuples containing the field names and field types

    """
    return list(get_annotations(obj).items())


def get_return_schema(
    spec: Any,
    output_fields: Optional[List[str]] = None,
    function_type: str = 'udf',
) -> List[Tuple[str, Any]]:
    """
    Expand a return type annotation into a list of types and field names.

    Parameters
    ----------
    spec : Any
        The return type specification
    output_fields : List[str], optional
        The output field names
    function_type : str
        The type of function, either 'udf' or 'tvf'

    Returns
    -------
    List[Tuple[str, Any]]
        A list of tuples containing the field names and field types

    """
    # Make sure that the result of a TVF is a list or dataframe
    if function_type == 'tvf':

        if typing.get_origin(spec) is list:
            spec = typing.get_args(spec)[0]

        # DataFrames require special handling. You can't get the schema
        # from the annotation, you need a separate structure to specify
        # the types. This should be specified in the output_fields.
        elif is_dataframe(spec):
            if output_fields is None:
                raise TypeError(
                    'output_fields must be specified for DataFrames / Tables',
                )
            spec = output_fields
            output_fields = None

        else:
            raise TypeError(
                'return type for TVF must be a list or DataFrame',
            )

    elif typing.get_origin(spec) in [list, tuple, dict] \
            or is_dataframe(spec) \
            or dataclasses.is_dataclass(spec) \
            or is_typeddict(spec) \
            or is_pydantic(spec) \
            or is_namedtuple(spec):
        raise TypeError('return type for UDF must be a scalar type')

    # Return type is specified by a SQL string
    if isinstance(spec, str):
        return [('', sql_to_dtype(spec))]

    # Return type is specified by a dataclass definition
    if dataclasses.is_dataclass(spec):
        schema = get_dataclass_schema(spec)

    # Return type is specified by a TypedDict definition
    elif is_typeddict(spec):
        schema = get_typeddict_schema(spec)

    # Return type is specified by a pydantic model
    elif is_pydantic(spec):
        schema = get_pydantic_schema(spec)

    # Return type is specified by a named tuple
    elif is_namedtuple(spec):
        schema = get_namedtuple_schema(spec)

    # Unrecognized return type
    elif spec is not None:
        if typing.get_origin(spec) is tuple:
            output_fields = [
                string.ascii_letters[i] for i in range(len(typing.get_args(spec)))
            ]
            schema = [(x, y) for x, y in zip(output_fields, typing.get_args(spec))]
        else:
            schema = [('', spec)]

    # Normalize schema data types
    out = []
    for k, v in schema:
        out.append((
            k, collapse_dtypes([normalize_dtype(x) for x in simplify_dtype(v)]),
        ))
    return out


def get_signature(
    func: Callable[..., Any],
    func_name: Optional[str] = None,
) -> Dict[str, Any]:
    '''
    Print the UDF signature of the Python callable.

    Parameters
    ----------
    func : Callable
        The function to extract the signature of
    func_name : str, optional
        Name override for function

    Returns
    -------
    Dict[str, Any]

    '''
    signature = inspect.signature(func)
    args: List[Dict[str, Any]] = []
    returns: List[Dict[str, Any]] = []
    attrs = getattr(func, '_singlestoredb_attrs', {})
    name = attrs.get('name', func_name if func_name else func.__name__)
    function_type = attrs.get('function_type', 'udf')
    out: Dict[str, Any] = dict(name=name, args=args, returns=returns)

    # Get parameter names, defaults, and annotations
    arg_names = [x for x in signature.parameters]
    args_overrides = attrs.get('args', None)
    defaults = [
        x.default if x.default is not inspect.Parameter.empty else None
        for x in signature.parameters.values()
    ]
    annotations = {
        k: x.annotation for k, x in signature.parameters.items()
        if x.annotation is not inspect.Parameter.empty
    }

    # Do not allow variable positional or keyword arguments
    for p in signature.parameters.values():
        if p.kind == inspect.Parameter.VAR_POSITIONAL:
            raise TypeError('variable positional arguments are not supported')
        elif p.kind == inspect.Parameter.VAR_KEYWORD:
            raise TypeError('variable keyword arguments are not supported')

    spec_diff = set(arg_names).difference(set(annotations.keys()))

    #
    # Make sure all arguments are annotated
    #

    # If there are missing annotations and no overrides, raise an error
    if spec_diff and args_overrides is None:
        raise TypeError(
            'missing annotations for {} in {}'
            .format(', '.join(spec_diff), name),
        )

    # If there are missing annotations and overrides are provided, make sure they match
    elif isinstance(args_overrides, dict):
        for s in spec_diff:
            if s not in args_overrides:
                raise TypeError(
                    'missing annotations for {} in {}'
                    .format(', '.join(spec_diff), name),
                )

    # If there are missing annotations and overrides are provided, make sure they match
    elif isinstance(args_overrides, list):
        if len(arg_names) != len(args_overrides):
            raise TypeError(
                'number of annotations does not match in {}: {}'
                .format(name, ', '.join(spec_diff)),
            )

    #
    # Generate the parameter type and the corresponding SQL code for that parameter
    #

    for i, arg in enumerate(arg_names):

        # If arg_overrides is a list, use corresponding item as SQL
        if isinstance(args_overrides, list):
            sql = args_overrides[i]
            arg_type = sql_to_dtype(sql)

        # If arg_overrides is a dict, use the corresponding key as SQL
        elif isinstance(args_overrides, dict) and arg in args_overrides:
            sql = args_overrides[arg]
            arg_type = sql_to_dtype(sql)

        # If args_overrides is a string, use it as SQL (only one function parameter)
        elif isinstance(args_overrides, str):
            sql = args_overrides
            arg_type = sql_to_dtype(sql)

        # Unrecognized type for args_overrides
        elif args_overrides is not None \
                and not isinstance(args_overrides, (list, dict, str)):
            raise TypeError(f'unrecognized type for arguments: {args_overrides}')

        # No args_overrides, use the Python type annotation
        else:
            arg_type = collapse_dtypes([
                normalize_dtype(x) for x in simplify_dtype(annotations[arg])
            ])
            sql = dtype_to_sql(arg_type, function_type=function_type)

        # Append parameter information to the args list
        args.append(dict(name=arg, dtype=arg_type, sql=sql, default=defaults[i]))

    #
    # Generate the return types and the corresponding SQL code for those values
    #

    ret_schema = get_return_schema(
        attrs.get('returns', signature.return_annotation),
        output_fields=attrs.get('output_fields', None),
        function_type=function_type,
    )

    for i, (name, rtype) in enumerate(ret_schema):
        sql = dtype_to_sql(rtype, function_type=function_type)
        returns.append(dict(name=name, dtype=rtype, sql=sql))

    # Copy keys from decorator to signature
    copied_keys = ['database', 'environment', 'packages', 'resources', 'replace']
    for key in copied_keys:
        if attrs.get(key):
            out[key] = attrs[key]

    # Set the function endpoint
    out['endpoint'] = '/invoke'

    # Set the function doc string
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


def dtype_to_sql(
    dtype: str,
    default: Any = None,
    field_names: Optional[List[str]] = None,
    function_type: str = 'udf',
) -> str:
    """
    Convert a collapsed dtype string to a SQL type.

    Parameters
    ----------
    dtype : str
        Simplified data type string
    default : Any, optional
        Default value
    field_names : List[str], optional
        Field names for tuple types

    Returns
    -------
    str

    """
    nullable = ' NOT NULL'
    if dtype.endswith('?'):
        nullable = ' NULL'
        dtype = dtype[:-1]
    elif '|null' in dtype:
        nullable = ' NULL'
        dtype = dtype.replace('|null', '')

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
        item_dtype = dtype_to_sql(dtypes, function_type=function_type)
        return f'ARRAY({item_dtype}){nullable}{default_clause}'

    if dtype.startswith('tuple['):
        _, dtypes = dtype.split('[', 1)
        dtypes = dtypes[:-1]
        item_dtypes = []
        for i, item in enumerate(dtypes.split(',')):
            if field_names:
                name = field_names[i]
            else:
                name = string.ascii_letters[i]
            if '=' in item:
                name, item = item.split('=', 1)
            item_dtypes.append(
                f'`{name}` ' + dtype_to_sql(item, function_type=function_type),
            )
        if function_type == 'udf':
            return f'RECORD({", ".join(item_dtypes)}){nullable}{default_clause}'
        else:
            return re.sub(
                r' NOT NULL\s*$', r'',
                f'TABLE({", ".join(item_dtypes)}){nullable}{default_clause}',
            )

    return f'{sql_type_map[dtype]}{nullable}{default_clause}'


def signature_to_sql(
    signature: Dict[str, Any],
    url: Optional[str] = None,
    data_format: str = 'rowdat_1',
    app_mode: str = 'remote',
    link: Optional[str] = None,
    replace: bool = False,
    function_type: str = 'udf',
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
        ret = signature['returns']
        if function_type == 'tvf':
            res = 'TABLE(' + ', '.join(
                f'{escape_name(x["name"])} {x["sql"]}' for x in ret
            ) + ')'
        elif ret[0]['name']:
            res = 'RECORD(' + ', '.join(
                f'{escape_name(x["name"])} {x["sql"]}' for x in ret
            ) + ')'
        else:
            res = ret[0]['sql']
        returns = f' RETURNS {res}'

    host = os.environ.get('SINGLESTOREDB_EXT_HOST', '127.0.0.1')
    port = os.environ.get('SINGLESTOREDB_EXT_PORT', '8000')

    if app_mode.lower() == 'remote':
        url = url or f'https://{host}:{port}/invoke'
    elif url is None:
        raise ValueError('url can not be `None`')

    database = ''
    if signature.get('database'):
        database = escape_name(signature['database']) + '.'

    or_replace = 'OR REPLACE ' if (bool(signature.get('replace')) or replace) else ''

    link_str = ''
    if link:
        if not re.match(r'^[\w_]+$', link):
            raise ValueError(f'invalid LINK name: {link}')
        link_str = f' LINK {link}'

    return (
        f'CREATE {or_replace}EXTERNAL FUNCTION ' +
        f'{database}{escape_name(signature["name"])}' +
        '(' + ', '.join(args) + ')' + returns +
        f' AS {app_mode.upper()} SERVICE "{url}" FORMAT {data_format.upper()}'
        f'{link_str};'
    )
