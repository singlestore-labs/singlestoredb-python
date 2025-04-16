import dataclasses
import inspect
import sys
import types
import typing
from typing import Any
from typing import Dict


if sys.version_info >= (3, 10):
    _UNION_TYPES = {typing.Union, types.UnionType}
else:
    _UNION_TYPES = {typing.Union}


is_dataclass = dataclasses.is_dataclass


def is_union(x: Any) -> bool:
    """Check if the object is a Union."""
    return typing.get_origin(x) in _UNION_TYPES


def get_annotations(obj: Any) -> Dict[str, Any]:
    """Get the annotations of an object."""
    if hasattr(inspect, 'get_annotations'):
        return inspect.get_annotations(obj)
    if isinstance(obj, type):
        return obj.__dict__.get('__annotations__', {})
    return getattr(obj, '__annotations__', {})


def get_module(obj: Any) -> str:
    """Get the module of an object."""
    module = getattr(obj, '__module__', '').split('.')
    if module:
        return module[0]
    return ''


def get_type_name(obj: Any) -> str:
    """Get the type name of an object."""
    if hasattr(obj, '__name__'):
        return obj.__name__
    if hasattr(obj, '__class__'):
        return obj.__class__.__name__
    return ''


def is_numpy(obj: Any) -> bool:
    """Check if an object is a numpy array."""
    if inspect.isclass(obj):
        if get_module(obj) == 'numpy':
            return get_type_name(obj) == 'ndarray'

    origin = typing.get_origin(obj)
    if get_module(origin) == 'numpy':
        if get_type_name(origin) == 'ndarray':
            return True

    dtype = type(obj)
    if get_module(dtype) == 'numpy':
        return get_type_name(dtype) == 'ndarray'

    return False


def is_dataframe(obj: Any) -> bool:
    """Check if an object is a DataFrame."""
    # Cheating here a bit so we don't have to import pandas / polars / pyarrow:
    # unless we absolutely need to
    if get_module(obj) == 'pandas':
        return get_type_name(obj) == 'DataFrame'
    if get_module(obj) == 'polars':
        return get_type_name(obj) == 'DataFrame'
    if get_module(obj) == 'pyarrow':
        return get_type_name(obj) == 'Table'
    return False


def is_vector(obj: Any) -> bool:
    """Check if an object is a vector type."""
    return is_pandas_series(obj) \
        or is_polars_series(obj) \
        or is_pyarrow_array(obj) \
        or is_numpy(obj)


def get_data_format(obj: Any) -> str:
    """Return the data format of the DataFrame / Table / vector."""
    # Cheating here a bit so we don't have to import pandas / polars / pyarrow
    # unless we absolutely need to
    if get_module(obj) == 'pandas':
        return 'pandas'
    if get_module(obj) == 'polars':
        return 'polars'
    if get_module(obj) == 'pyarrow':
        return 'arrow'
    if get_module(obj) == 'numpy':
        return 'numpy'
    if isinstance(obj, list):
        return 'list'
    return 'scalar'


def is_pandas_series(obj: Any) -> bool:
    """Check if an object is a pandas Series."""
    if is_union(obj):
        obj = typing.get_args(obj)[0]
    return (
        get_module(obj) == 'pandas' and
        get_type_name(obj) == 'Series'
    )


def is_polars_series(obj: Any) -> bool:
    """Check if an object is a polars Series."""
    if is_union(obj):
        obj = typing.get_args(obj)[0]
    return (
        get_module(obj) == 'polars' and
        get_type_name(obj) == 'Series'
    )


def is_pyarrow_array(obj: Any) -> bool:
    """Check if an object is a pyarrow Array."""
    if is_union(obj):
        obj = typing.get_args(obj)[0]
    return (
        get_module(obj) == 'pyarrow' and
        get_type_name(obj) == 'Array'
    )


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
    if not inspect.isclass(obj):
        return False
    # We don't want to import pydantic here, so we check if
    # the class is a subclass
    return bool([
        x for x in inspect.getmro(obj)
        if get_module(x) == 'pydantic'
        and get_type_name(x) == 'BaseModel'
    ])
