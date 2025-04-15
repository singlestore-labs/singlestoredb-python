import dataclasses
import inspect
import sys
import types
import typing
from typing import Any
from typing import Dict

try:
    import numpy as np
    has_numpy = True
except ImportError:
    has_numpy = False


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


def is_numpy(obj: Any) -> bool:
    """Check if an object is a numpy array."""
    if is_union(obj):
        obj = typing.get_args(obj)[0]
    if not has_numpy:
        return False
    if inspect.isclass(obj):
        return obj is np.ndarray
    if typing.get_origin(obj) is np.ndarray:
        return True
    return isinstance(obj, np.ndarray)


def is_dataframe(obj: Any) -> bool:
    """Check if an object is a DataFrame."""
    # Cheating here a bit so we don't have to import pandas / polars / pyarrow:
    # unless we absolutely need to
    if getattr(obj, '__module__', '').startswith('pandas.'):
        return getattr(obj, '__name__', '') == 'DataFrame'
    if getattr(obj, '__module__', '').startswith('polars.'):
        return getattr(obj, '__name__', '') == 'DataFrame'
    if getattr(obj, '__module__', '').startswith('pyarrow.'):
        return getattr(obj, '__name__', '') == 'Table'
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


def is_pandas_series(obj: Any) -> bool:
    """Check if an object is a pandas Series."""
    if is_union(obj):
        obj = typing.get_args(obj)[0]
    return (
        getattr(obj, '__module__', '').startswith('pandas.') and
        getattr(obj, '__name__', '') == 'Series'
    )


def is_polars_series(obj: Any) -> bool:
    """Check if an object is a polars Series."""
    if is_union(obj):
        obj = typing.get_args(obj)[0]
    return (
        getattr(obj, '__module__', '').startswith('polars.') and
        getattr(obj, '__name__', '') == 'Series'
    )


def is_pyarrow_array(obj: Any) -> bool:
    """Check if an object is a pyarrow Array."""
    if is_union(obj):
        obj = typing.get_args(obj)[0]
    return (
        getattr(obj, '__module__', '').startswith('pyarrow.') and
        getattr(obj, '__name__', '') == 'Array'
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
        if x.__module__.startswith('pydantic.')
        and x.__name__ == 'BaseModel'
    ])
