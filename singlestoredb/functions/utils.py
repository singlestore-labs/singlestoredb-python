import dataclasses
import inspect
import struct
import sys
import types
import typing
from enum import Enum
from typing import Any
from typing import Dict
from typing import Iterable

from .typing import Masked

if sys.version_info >= (3, 10):
    _UNION_TYPES = {typing.Union, types.UnionType}
else:
    _UNION_TYPES = {typing.Union}


is_dataclass = dataclasses.is_dataclass


def is_masked(obj: Any) -> bool:
    """Check if an object is a Masked type."""
    origin = typing.get_origin(obj)
    if origin is not None:
        return origin is Masked or \
            (inspect.isclass(origin) and issubclass(origin, Masked))
    return False


def is_union(x: Any) -> bool:
    """Check if the object is a Union."""
    return typing.get_origin(x) in _UNION_TYPES


def get_annotations(obj: Any) -> Dict[str, Any]:
    """Get the annotations of an object."""
    return typing.get_type_hints(obj)


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
    if str(obj).startswith('numpy.ndarray['):
        return True

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


def is_vector(obj: Any, include_masks: bool = False) -> bool:
    """Check if an object is a vector type."""
    return is_pandas_series(obj) \
        or is_polars_series(obj) \
        or is_pyarrow_array(obj) \
        or is_numpy(obj) \
        or is_masked(obj)


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


class VectorTypes(str, Enum):
    """Enum for vector types."""
    F16 = 'f16'
    F32 = 'f32'
    F64 = 'f64'
    I8 = 'i8'
    I16 = 'i16'
    I32 = 'i32'
    I64 = 'i64'


def unpack_vector(
    obj: Any,
    element_type: VectorTypes = VectorTypes.F32,
) -> Iterable[Any]:
    """
    Unpack a vector from bytes.

    Parameters
    ----------
    obj : Any
        The object to unpack.
    element_type : VectorTypes
        The type of the elements in the vector.
        Can be one of 'f32', 'f64', 'i8', 'i16', 'i32', or 'i64'.
        Default is 'f32'.

    Returns
    -------
    Iterable[Any]
        The unpacked vector.

    """
    if isinstance(obj, (bytes, bytearray, list, tuple)):
        if element_type == 'f32':
            n = len(obj) // 4
            fmt = 'f'
        elif element_type == 'f64':
            n = len(obj) // 8
            fmt = 'd'
        elif element_type == 'i8':
            n = len(obj)
            fmt = 'b'
        elif element_type == 'i16':
            n = len(obj) // 2
            fmt = 'h'
        elif element_type == 'i32':
            n = len(obj) // 4
            fmt = 'i'
        elif element_type == 'i64':
            n = len(obj) // 8
            fmt = 'q'
        else:
            raise ValueError(f'unsupported element type: {element_type}')

        if isinstance(obj, (bytes, bytearray)):
            return struct.unpack(f'<{n}{fmt}', obj)
        return tuple([struct.unpack(f'<{n}{fmt}', x) for x in obj])

    if element_type == 'f32':
        np_type = 'f4'
    elif element_type == 'f64':
        np_type = 'f8'
    elif element_type == 'i8':
        np_type = 'i1'
    elif element_type == 'i16':
        np_type = 'i2'
    elif element_type == 'i32':
        np_type = 'i4'
    elif element_type == 'i64':
        np_type = 'i8'
    else:
        raise ValueError(f'unsupported element type: {element_type}')

    if is_numpy(obj):
        import numpy as np
        return np.array([np.frombuffer(x, dtype=np_type) for x in obj])

    if is_pandas_series(obj):
        import numpy as np
        import pandas as pd
        return pd.Series([np.frombuffer(x, dtype=np_type) for x in obj])

    if is_polars_series(obj):
        import numpy as np
        import polars as pl
        return pl.Series([np.frombuffer(x, dtype=np_type) for x in obj])

    if is_pyarrow_array(obj):
        import numpy as np
        import pyarrow as pa
        return pa.array([np.frombuffer(x, dtype=np_type) for x in obj])

    raise ValueError(
        f'unsupported object type: {type(obj)}',
    )


def pack_vector(
    obj: Any,
    element_type: VectorTypes = VectorTypes.F32,
) -> bytes:
    """
    Pack a vector into bytes.

    Parameters
    ----------
    obj : Any
        The object to pack.
    element_type : VectorTypes
        The type of the elements in the vector.
        Can be one of 'f32', 'f64', 'i8', 'i16', 'i32', or 'i64'.
        Default is 'f32'.

    Returns
    -------
    bytes
        The packed vector.

    """
    if element_type == 'f32':
        fmt = 'f'
    elif element_type == 'f64':
        fmt = 'd'
    elif element_type == 'i8':
        fmt = 'b'
    elif element_type == 'i16':
        fmt = 'h'
    elif element_type == 'i32':
        fmt = 'i'
    elif element_type == 'i64':
        fmt = 'q'
    else:
        raise ValueError(f'unsupported element type: {element_type}')

    if isinstance(obj, (list, tuple)):
        return struct.pack(f'<{len(obj)}{fmt}', *obj)

    elif is_numpy(obj):
        return obj.tobytes()

    elif is_pandas_series(obj):
        # TODO: Nested vectors
        import pandas as pd
        return pd.Series(obj).to_numpy().tobytes()

    elif is_polars_series(obj):
        # TODO: Nested vectors
        import polars as pl
        return pl.Series(obj).to_numpy().tobytes()

    elif is_pyarrow_array(obj):
        # TODO: Nested vectors
        import pyarrow as pa
        return pa.array(obj).to_numpy().tobytes()

    raise ValueError(
        f'unsupported object type: {type(obj)}',
    )
