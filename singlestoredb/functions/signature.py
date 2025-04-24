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
from typing import Type
from typing import TypeVar
from typing import Union

try:
    import numpy as np
    has_numpy = True
except ImportError:
    has_numpy = False


from . import dtypes as dt
from . import utils
from .typing import Table
from .typing import Masked
from ..mysql.converters import escape_item  # type: ignore

if sys.version_info >= (3, 10):
    _UNION_TYPES = {typing.Union, types.UnionType}
else:
    _UNION_TYPES = {typing.Union}


def is_union(x: Any) -> bool:
    """Check if the object is a Union."""
    return typing.get_origin(x) in _UNION_TYPES


class NoDefaultType:
    pass


NO_DEFAULT = NoDefaultType()


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


def get_data_format(obj: Any) -> str:
    """Return the data format of the DataFrame / Table / vector."""
    # Cheating here a bit so we don't have to import pandas / polars / pyarrow
    # unless we absolutely need to
    if getattr(obj, '__module__', '').startswith('pandas.'):
        return 'pandas'
    if getattr(obj, '__module__', '').startswith('polars.'):
        return 'polars'
    if getattr(obj, '__module__', '').startswith('pyarrow.'):
        return 'arrow'
    if getattr(obj, '__module__', '').startswith('numpy.'):
        return 'numpy'
    if isinstance(obj, list):
        return 'list'
    return 'scalar'


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
    if is_union(dtype):
        for x in typing.get_args(dtype):
            args.extend(simplify_dtype(x))

    # Expand custom types to individual types (does not support bounds)
    elif atype is TypeVar:
        for x in dtype.__constraints__:
            args.extend(simplify_dtype(x))
        if dtype.__bound__:
            args.extend(simplify_dtype(dtype.__bound__))

    # Sequence types
    elif origin is not None and inspect.isclass(origin) and issubclass(origin, Sequence):
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

    if typing.get_origin(dtype) is np.dtype:
        dtype = typing.get_args(dtype)[0]

    # Specific types
    if dtype is None or dtype is type(None):  # noqa: E721
        return 'null'
    if dtype is int:
        return 'int64'
    if dtype is float:
        return 'float64'
    if dtype is bool:
        return 'bool'

    if utils.is_dataclass(dtype):
        dc_fields = dataclasses.fields(dtype)
        item_dtypes = ','.join(
            f'{normalize_dtype(simplify_dtype(x.type))}' for x in dc_fields
        )
        return f'tuple[{item_dtypes}]'

    if utils.is_typeddict(dtype):
        td_fields = utils.get_annotations(dtype).keys()
        item_dtypes = ','.join(
            f'{normalize_dtype(simplify_dtype(dtype[x]))}' for x in td_fields
        )
        return f'tuple[{item_dtypes}]'

    if utils.is_pydantic(dtype):
        pyd_fields = dtype.model_fields.values()
        item_dtypes = ','.join(
            f'{normalize_dtype(simplify_dtype(x.annotation))}'  # type: ignore
            for x in pyd_fields
        )
        return f'tuple[{item_dtypes}]'

    if utils.is_namedtuple(dtype):
        nt_fields = utils.get_annotations(dtype).values()
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
            elif inspect.isclass(origin) and issubclass(origin, array_types):
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


def collapse_dtypes(dtypes: Union[str, List[str]], include_null: bool = False) -> str:
    """
    Collapse a dtype possibly containing multiple data types to one type.

    This function can fail if there is no single type that naturally
    encompasses all of the types in the list.

    Parameters
    ----------
    dtypes : str or list[str]
        The data types to collapse
    include_null : bool, optional
        Whether to force include null types in the result

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

    is_nullable = include_null or 'null' in dtypes

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


def get_dataclass_schema(
    obj: Any,
    include_default: bool = False,
) -> List[Union[Tuple[str, Any], Tuple[str, Any, Any]]]:
    """
    Get the schema of a dataclass.

    Parameters
    ----------
    obj : dataclass
        The dataclass to get the schema of

    Returns
    -------
    List[Tuple[str, Any]] | List[Tuple[str, Any, Any]]
        A list of tuples containing the field names and field types

    """
    if include_default:
        return [
            (
                f.name, f.type,
                NO_DEFAULT if f.default is dataclasses.MISSING else f.default,
            )
            for f in dataclasses.fields(obj)
        ]
    return [(f.name, f.type) for f in dataclasses.fields(obj)]


def get_typeddict_schema(
    obj: Any,
    include_default: bool = False,
) -> List[Union[Tuple[str, Any], Tuple[str, Any, Any]]]:
    """
    Get the schema of a TypedDict.

    Parameters
    ----------
    obj : TypedDict
        The TypedDict to get the schema of
    include_default : bool, optional
        Whether to include the default value in the column specification

    Returns
    -------
    List[Tuple[str, Any]] | List[Tuple[str, Any, Any]]
        A list of tuples containing the field names and field types

    """
    if include_default:
        return [
            (k, v, getattr(obj, k, NO_DEFAULT))
            for k, v in utils.get_annotations(obj).items()
        ]
    return list(utils.get_annotations(obj).items())


def get_pydantic_schema(
    obj: Any,
    include_default: bool = False,
) -> List[Union[Tuple[str, Any], Tuple[str, Any, Any]]]:
    """
    Get the schema of a pydantic model.

    Parameters
    ----------
    obj : pydantic.BaseModel
        The pydantic model to get the schema of
    include_default : bool, optional
        Whether to include the default value in the column specification

    Returns
    -------
    List[Tuple[str, Any]] | List[Tuple[str, Any, Any]]
        A list of tuples containing the field names and field types

    """
    import pydantic_core
    if include_default:
        return [
            (
                k, v.annotation,
                NO_DEFAULT if v.default is pydantic_core.PydanticUndefined else v.default,
            )
            for k, v in obj.model_fields.items()
        ]
    return [(k, v.annotation) for k, v in obj.model_fields.items()]


def get_namedtuple_schema(
    obj: Any,
    include_default: bool = False,
) -> List[Union[Tuple[Any, str], Tuple[Any, str, Any]]]:
    """
    Get the schema of a named tuple.

    Parameters
    ----------
    obj : NamedTuple
        The named tuple to get the schema of
    include_default : bool, optional
        Whether to include the default value in the column specification

    Returns
    -------
    List[Tuple[Any, str]] | List[Tuple[Any, str, Any]]
        A list of tuples containing the field names and field types

    """
    if include_default:
        return [
            (
                k, v,
                obj._field_defaults.get(k, NO_DEFAULT),
            )
            for k, v in utils.get_annotations(obj).items()
        ]
    return list(utils.get_annotations(obj).items())


def get_table_schema(
    obj: Any,
    include_default: bool = False,
) -> List[Union[Tuple[Any, str], Tuple[Any, str, Any]]]:
    """
    Get the schema of a Table.

    Parameters
    ----------
    obj : Table
        The Table to get the schema of
    include_default : bool, optional
        Whether to include the default value in the column specification

    Returns
    -------
    List[Tuple[Any, str]] | List[Tuple[Any, str, Any]]
        A list of tuples containing the field names and field types

    """
    if include_default:
        return [
            (k, v, getattr(obj, k, NO_DEFAULT))
            for k, v in utils.get_annotations(obj).items()
        ]
    return list(utils.get_annotations(obj).items())


def get_colspec(
    overrides: Any,
    include_default: bool = False,
) -> List[Union[Tuple[str, Any], Tuple[str, Any, Any]]]:
    """
    Get the column specification from the overrides.

    Parameters
    ----------
    overrides : Any
        The overrides to get the column specification from
    include_default : bool, optional
        Whether to include the default value in the column specification

    Returns
    -------
    List[Tuple[str, Any]] | List[Tuple[str, Any, Any]]
        A list of tuples containing the field names and field types

    """
    overrides_colspec = []

    if overrides:

        # Dataclass
        if utils.is_dataclass(overrides):
            overrides_colspec = get_dataclass_schema(
                overrides, include_default=include_default,
            )

        # TypedDict
        elif utils.is_typeddict(overrides):
            overrides_colspec = get_typeddict_schema(
                overrides, include_default=include_default,
            )

        # Named tuple
        elif utils.is_namedtuple(overrides):
            overrides_colspec = get_namedtuple_schema(
                overrides, include_default=include_default,
            )

        # Pydantic model
        elif utils.is_pydantic(overrides):
            overrides_colspec = get_pydantic_schema(
                overrides, include_default=include_default,
            )

        # List of types
        elif isinstance(overrides, list):
            if include_default:
                overrides_colspec = [
                    (getattr(x, 'name', ''), x, NO_DEFAULT) for x in overrides
                ]
            else:
                overrides_colspec = [(getattr(x, 'name', ''), x) for x in overrides]

        # Other
        else:
            if include_default:
                overrides_colspec = [
                    (getattr(overrides, 'name', ''), overrides, NO_DEFAULT),
                ]
            else:
                overrides_colspec = [(getattr(overrides, 'name', ''), overrides)]

    return overrides_colspec


def unpack_masked_type(obj: Any) -> Any:
    """
    Unpack a masked type into a single type.

    Parameters
    ----------
    obj : Any
        The masked type to unpack

    Returns
    -------
    Any
        The unpacked type

    """
    if typing.get_origin(obj) is Masked:
        return typing.get_args(obj)[0]
    return obj


def get_schema(
    spec: Any,
    overrides: Optional[Union[List[str], Type[Any]]] = None,
    mode: str = 'parameter',
) -> Tuple[List[Tuple[str, Any, Optional[str]]], str, str]:
    """
    Expand a return type annotation into a list of types and field names.

    Parameters
    ----------
    spec : Any
        The return type specification
    overrides : List[str], optional
        List of SQL type specifications for the return type
    mode : str
        The mode of the function, either 'parameter' or 'return'

    Returns
    -------
    Tuple[List[Tuple[str, Any, Optional[str]]], str, str]
        A list of tuples containing the field names and field types,
        the normalized data format, optionally the SQL
        definition of the type, and the data format of the type

    """
    colspec = []
    data_format = ''
    function_type = 'udf'

    origin = typing.get_origin(spec)
    args = typing.get_args(spec)
    args_origins = [typing.get_origin(x) if x is not None else None for x in args]

    # Make sure that the result of a TVF is a list or dataframe
    if mode == 'return':

        # See if it's a Table subclass with annotations
        if inspect.isclass(origin) and origin is Table:

            function_type = 'tvf'

            if utils.is_dataframe(args[0]):
                if not overrides:
                    raise TypeError(
                        'column types must be specified by the '
                        '`returns=` parameter of the @udf decorator',
                    )

                if utils.get_module(args[0]) in ['pandas', 'polars', 'pyarrow']:
                    data_format = utils.get_module(args[0])
                    spec = args[0]
                else:
                    raise TypeError(
                        'only pandas.DataFrames, polars.DataFrames, '
                        'and pyarrow.Tables are supported as tables.',
                    )

            elif typing.get_origin(args[0]) is list:
                if len(args) != 1:
                    raise TypeError(
                        'only one list is supported within a table; to '
                        'return multiple columns, use a tuple, NamedTuple, '
                        'dataclass, TypedDict, or pydantic model',
                    )
                spec = typing.get_args(args[0])[0]
                data_format = 'list'

            elif all([utils.is_vector(x, include_masks=True) for x in args]):
                pass

            else:
                raise TypeError(
                    'return type for TVF must be a list, DataFrame / Table, '
                    'or tuple of vectors',
                )

        # Short circuit check for common valid types
        elif utils.is_vector(spec) or spec in [str, float, int, bytes]:
            pass

        # Try to catch some common mistakes
        elif origin in [tuple, dict] or tuple in args_origins or \
                (
                    inspect.isclass(spec) and
                    (
                        utils.is_dataframe(spec)
                        or utils.is_dataclass(spec)
                        or utils.is_typeddict(spec)
                        or utils.is_pydantic(spec)
                        or utils.is_namedtuple(spec)
                    )
                ):
            raise TypeError(
                'invalid return type for a UDF; '
                f'expecting a scalar or vector, but got {spec}',
            )

    # Short circuit check for common valid types
    elif utils.is_vector(spec) or spec in [str, float, int, bytes]:
        pass

    # Error out for incorrect parameter types
    elif origin in [tuple, dict] or tuple in args_origins or \
            (
                inspect.isclass(spec) and
                (
                    utils.is_dataframe(spec)
                    or utils.is_dataclass(spec)
                    or utils.is_typeddict(spec)
                    or utils.is_pydantic(spec)
                    or utils.is_namedtuple(spec)
                )
            ):
        raise TypeError(f'parameter types must be scalar or vector, got {spec}')

    #
    # Process each parameter / return type into a colspec
    #

    # Compute overrides colspec from various formats
    overrides_colspec = get_colspec(overrides)

    # Dataframe type
    if utils.is_dataframe(spec):
        colspec = overrides_colspec

    # Numpy array types
    elif utils.is_numpy(spec):
        data_format = 'numpy'
        if overrides:
            colspec = overrides_colspec
        elif len(typing.get_args(spec)) < 2:
            raise TypeError(
                'numpy array must have a data type specified '
                'in the @udf decorator or with an NDArray type annotation',
            )
        else:
            colspec = [('', typing.get_args(spec)[1])]

    # Pandas Series
    elif utils.is_pandas_series(spec):
        data_format = 'pandas'
        if not overrides:
            raise TypeError(
                'pandas Series must have a data type specified '
                'in the @udf decorator',
            )
        colspec = overrides_colspec

    # Polars Series
    elif utils.is_polars_series(spec):
        data_format = 'polars'
        if not overrides:
            raise TypeError(
                'polars Series must have a data type specified '
                'in the @udf decorator',
            )
        colspec = overrides_colspec

    # PyArrow Array
    elif utils.is_pyarrow_array(spec):
        data_format = 'arrow'
        if not overrides:
            raise TypeError(
                'pyarrow Arrays must have a data type specified '
                'in the @udf decorator',
            )
        colspec = overrides_colspec

    # Return type is specified by a dataclass definition
    elif utils.is_dataclass(spec):
        colspec = overrides_colspec or get_dataclass_schema(spec)

    # Return type is specified by a TypedDict definition
    elif utils.is_typeddict(spec):
        colspec = overrides_colspec or get_typeddict_schema(spec)

    # Return type is specified by a pydantic model
    elif utils.is_pydantic(spec):
        colspec = overrides_colspec or get_pydantic_schema(spec)

    # Return type is specified by a named tuple
    elif utils.is_namedtuple(spec):
        colspec = overrides_colspec or get_namedtuple_schema(spec)

    # Unrecognized return type
    elif spec is not None:

        # Return type is specified by a SQL string
        if isinstance(spec, str):
            data_format = 'scalar'
            colspec = [(getattr(spec, 'name', ''), spec)]

        # Plain list vector
        elif typing.get_origin(spec) is list:
            data_format = 'list'
            colspec = [('', typing.get_args(spec)[0])]

        # Multiple return values
        elif inspect.isclass(typing.get_origin(spec)) \
                and issubclass(typing.get_origin(spec), tuple):  # type: ignore[arg-type]

            out_names, out_overrides = [], []

            # Get the colspec for the overrides
            if overrides:
                out_colspec = [
                    x for x in get_colspec(overrides, include_default=True)
                ]
                out_names = [x[0] for x in out_colspec]
                out_overrides = [x[1] for x in out_colspec]

            # Make sure that the number of overrides matches the number of
            # return types or parameter types
            if out_overrides and len(typing.get_args(spec)) != len(out_overrides):
                raise ValueError(
                    f'number of {mode} types does not match the number of '
                    'overrides specified',
                )

            colspec = []
            out_data_formats = []

            # Get the colspec for each item in the tuple
            for i, x in enumerate(typing.get_args(spec)):
                out_item, out_data_format, _ = get_schema(
                    unpack_masked_type(x),
                    overrides=out_overrides[i] if out_overrides else [],
                    # Always pass UDF mode for individual items
                    mode=mode,
                )

                # Use the name from the overrides if specified
                if out_names and out_names[i] and not out_item[0][0]:
                    out_item = [(out_names[i], *out_item[0][1:])]
                elif not out_item[0][0]:
                    out_item = [(f'{string.ascii_letters[i]}', *out_item[0][1:])]

                colspec += out_item
                out_data_formats.append(out_data_format)

            # Make sure that all the data formats are the same
            if len(set(out_data_formats)) > 1:
                raise TypeError(
                    'data formats must be all be the same vector / scalar type: '
                    f'{", ".join(out_data_formats)}',
                )

            if data_format != 'list' and out_data_formats:
                data_format = out_data_formats[0]

            # Since the colspec was computed by get_schema already, don't go
            # through the process of normalizing the dtypes again
            return colspec, data_format, function_type  # type: ignore

        # Use overrides if specified
        elif overrides:
            if not data_format:
                data_format = get_data_format(spec)
            colspec = overrides_colspec

        # Single value, no override
        else:
            if not data_format:
                data_format = 'scalar'
            colspec = [('', spec)]

    out = []

    # Normalize colspec data types
    for k, v, *_ in colspec:
        out.append((
            k,
            collapse_dtypes(
                [normalize_dtype(x) for x in simplify_dtype(v)],
            ),
            v if isinstance(v, str) else None,
        ))

    return out, data_format, function_type


def vector_check(obj: Any) -> Tuple[Any, str]:
    """
    Check if the object is a vector type.

    Parameters
    ----------
    obj : Any
        The object to check

    Returns
    -------
    Tuple[Any, str]
        The scalar type and the data format:
        'scalar', 'list', 'numpy', 'pandas', or 'polars'

    """
    if utils.is_numpy(obj):
        if len(typing.get_args(obj)) < 2:
            return None, 'numpy'
        return typing.get_args(obj)[1], 'numpy'
    if utils.is_pandas_series(obj):
        if len(typing.get_args(obj)) < 2:
            return None, 'pandas'
        return typing.get_args(obj)[1], 'pandas'
    if utils.is_polars_series(obj):
        return None, 'polars'
    if utils.is_pyarrow_array(obj):
        return None, 'arrow'
    if obj is list or typing.get_origin(obj) is list:
        if len(typing.get_args(obj)) < 1:
            return None, 'list'
        return typing.get_args(obj)[0], 'list'
    return obj, 'scalar'


def get_masks(func: Callable[..., Any]) -> Tuple[List[bool], List[bool]]:
    """
    Get the list of masked parameters and return values for the function.

    Parameters
    ----------
    func : Callable
        The function to call as the endpoint

    Returns
    -------
    Tuple[List[bool], List[bool]]
        A Tuple containing the parameter / return value masks
        as lists of booleans


    """
    params = inspect.signature(func).parameters
    returns = inspect.signature(func).return_annotation

    ret_masks = []
    if typing.get_origin(returns) is Masked:
        ret_masks = [True]
    elif typing.get_origin(returns) is Table:
        for x in typing.get_args(returns):
            if typing.get_origin(x) is Masked:
                ret_masks.append(True)
            else:
                ret_masks.append(False)
        if not any(ret_masks):
            ret_masks = []

    return (
        [typing.get_origin(x.annotation) is Masked for x in params.values()],
        ret_masks,
    )


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

    out: Dict[str, Any] = dict(name=name, args=args, returns=returns)

    # Do not allow variable positional or keyword arguments
    for p in signature.parameters.values():
        if p.kind == inspect.Parameter.VAR_POSITIONAL:
            raise TypeError('variable positional arguments are not supported')
        elif p.kind == inspect.Parameter.VAR_KEYWORD:
            raise TypeError('variable keyword arguments are not supported')

    # TODO: Use typing.get_type_hints() for parameters / return values?

    # Generate the parameter type and the corresponding SQL code for that parameter
    args_schema = []
    args_data_formats = []
    args_colspec = [x for x in get_colspec(attrs.get('args', []), include_default=True)]
    args_overrides = [x[1] for x in args_colspec]
    args_defaults = [x[2] for x in args_colspec]  # type: ignore
    args_masks, ret_masks = get_masks(func)

    if args_overrides and len(args_overrides) != len(signature.parameters):
        raise ValueError(
            'number of args in the decorator does not match '
            'the number of parameters in the function signature',
        )

    params = list(signature.parameters.values())

    # Get the colspec for each parameter
    for i, param in enumerate(params):
        arg_schema, args_data_format, _ = get_schema(
            unpack_masked_type(param.annotation),
            overrides=args_overrides[i] if args_overrides else [],
            mode='parameter',
        )
        args_data_formats.append(args_data_format)

        # Insert parameter names as needed
        if not arg_schema[0][0]:
            args_schema.append((param.name, *arg_schema[0][1:]))

    for i, (name, atype, sql) in enumerate(args_schema):
        default_option = {}

        # Insert default values as needed
        if args_defaults:
            if args_defaults[i] is not NO_DEFAULT:
                default_option['default'] = args_defaults[i]
        else:
            if params[i].default is not param.empty:
                default_option['default'] = params[i].default

        # Generate SQL code for the parameter
        sql = sql or dtype_to_sql(
            atype, force_nullable=args_masks[i], **default_option,
        )

        # Add parameter to args definitions
        args.append(dict(name=name, dtype=atype, sql=sql, **default_option))

    # Check that all the data formats are all the same
    if len(set(args_data_formats)) > 1:
        raise TypeError(
            'input data formats must be all be the same: '
            f'{", ".join(args_data_formats)}',
        )

    out['args_data_format'] = args_data_formats[0] if args_data_formats else 'scalar'

    # Generate the return types and the corresponding SQL code for those values
    ret_schema, out['returns_data_format'], function_type = get_schema(
        unpack_masked_type(signature.return_annotation),
        overrides=attrs.get('returns', None),
        mode='return',
    )

    out['returns_data_format'] = out['returns_data_format'] or 'scalar'
    out['function_type'] = function_type

    # All functions have to return a value, so if none was specified try to
    # insert a reasonable default that includes NULLs.
    if not ret_schema:
        ret_schema = [('', 'int8?', 'TINYINT NULL')]

    # Generate field names for the return values
    if function_type == 'tvf' or len(ret_schema) > 1:
        for i, (name, rtype, sql) in enumerate(ret_schema):
            if not name:
                ret_schema[i] = (string.ascii_letters[i], rtype, sql)

    # Generate SQL code for the return values
    for i, (name, rtype, sql) in enumerate(ret_schema):
        sql = sql or dtype_to_sql(
            rtype,
            force_nullable=ret_masks[i] if ret_masks else False,
            function_type=function_type,
        )
        returns.append(dict(name=name, dtype=rtype, sql=sql))

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
    default: Any = NO_DEFAULT,
    field_names: Optional[List[str]] = None,
    function_type: str = 'udf',
    force_nullable: bool = False,
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
    function_type : str, optional
        Function type, either 'udf' or 'tvf'
    force_nullable : bool, optional
        Whether to force the type to be nullable

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
    elif force_nullable:
        nullable = ' NULL'

    if dtype == 'null':
        nullable = ''

    default_clause = ''
    if default is not NO_DEFAULT:
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
    function_type = signature.get('function_type') or 'udf'

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
        elif ret[0]['name'] and len(ret) > 1:
            res = 'RECORD(' + ', '.join(
                f'{escape_name(x["name"])} {x["sql"]}' for x in ret
            ) + ')'
        else:
            res = ret[0]['sql']
        returns = f' RETURNS {res}'
    else:
        raise ValueError(
            'function signature must have a return type specified',
        )

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
