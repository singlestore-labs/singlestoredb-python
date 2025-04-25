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
from typing import Tuple
from typing import Union

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


def _vector_type_to_numpy_type(
    vector_type: VectorTypes,
) -> str:
    """Convert a vector type to a numpy type."""
    if vector_type == VectorTypes.F32:
        return 'f4'
    elif vector_type == VectorTypes.F64:
        return 'f8'
    elif vector_type == VectorTypes.I8:
        return 'i1'
    elif vector_type == VectorTypes.I16:
        return 'i2'
    elif vector_type == VectorTypes.I32:
        return 'i4'
    elif vector_type == VectorTypes.I64:
        return 'i8'
    raise ValueError(f'unsupported element type: {vector_type}')


def _vector_type_to_struct_format(
    vec: Any,
    vector_type: VectorTypes,
) -> str:
    """Convert a vector type to a struct format string."""
    n = len(vec)
    if vector_type == VectorTypes.F32:
        if isinstance(vec, (bytes, bytearray)):
            n = n // 4
        return f'<{n}f'
    elif vector_type == VectorTypes.F64:
        if isinstance(vec, (bytes, bytearray)):
            n = n // 8
        return f'<{n}d'
    elif vector_type == VectorTypes.I8:
        return f'<{n}b'
    elif vector_type == VectorTypes.I16:
        if isinstance(vec, (bytes, bytearray)):
            n = n // 2
        return f'<{n}h'
    elif vector_type == VectorTypes.I32:
        if isinstance(vec, (bytes, bytearray)):
            n = n // 4
        return f'<{n}i'
    elif vector_type == VectorTypes.I64:
        if isinstance(vec, (bytes, bytearray)):
            n = n // 8
        return f'<{n}q'
    raise ValueError(f'unsupported element type: {vector_type}')


def unpack_vector(
    obj: Union[bytes, bytearray],
    vec_type: VectorTypes = VectorTypes.F32,
) -> Tuple[Any]:
    """
    Unpack a vector from bytes.

    Parameters
    ----------
    obj : bytes or bytearray
        The object to unpack.
    vec_type : VectorTypes
        The type of the elements in the vector.
        Can be one of 'f32', 'f64', 'i8', 'i16', 'i32', or 'i64'.
        Default is 'f32'.

    Returns
    -------
    Tuple[Any]
        The unpacked vector.

    """
    return struct.unpack(_vector_type_to_struct_format(obj, vec_type), obj)


def pack_vector(
    obj: Any,
    vec_type: VectorTypes = VectorTypes.F32,
) -> bytes:
    """
    Pack a vector into bytes.

    Parameters
    ----------
    obj : Any
        The object to pack.
    vec_type : VectorTypes
        The type of the elements in the vector.
        Can be one of 'f32', 'f64', 'i8', 'i16', 'i32', or 'i64'.
        Default is 'f32'.

    Returns
    -------
    bytes
        The packed vector.

    """
    if isinstance(obj, (list, tuple)):
        return struct.pack(_vector_type_to_struct_format(obj, vec_type), *obj)

    if is_numpy(obj):
        return obj.tobytes()

    if is_pandas_series(obj):
        import pandas as pd
        return pd.Series(obj).to_numpy().tobytes()

    if is_polars_series(obj):
        import polars as pl
        return pl.Series(obj).to_numpy().tobytes()

    if is_pyarrow_array(obj):
        import pyarrow as pa
        return pa.array(obj).to_numpy().tobytes()

    raise ValueError(
        f'unsupported object type: {type(obj)}',
    )


def unpack_vectors(
    arr_of_vec: Any,
    vec_type: VectorTypes = VectorTypes.F32,
) -> Iterable[Any]:
    """
    Unpack a vector from an array of bytes.

    Parameters
    ----------
    arr_of_vec : Iterable[Any]
        The array of bytes to unpack.
    vec_type : VectorTypes
        The type of the elements in the vector.
        Can be one of 'f32', 'f64', 'i8', 'i16', 'i32', or 'i64'.
        Default is 'f32'.

    Returns
    -------
    Iterable[Any]
        The unpacked vector.

    """
    if isinstance(arr_of_vec, (list, tuple)):
        return [unpack_vector(x, vec_type) for x in arr_of_vec]

    import numpy as np

    dtype = _vector_type_to_numpy_type(vec_type)

    np_arr = np.array(
        [np.frombuffer(x, dtype=dtype) for x in arr_of_vec],
        dtype=dtype,
    )

    if is_numpy(arr_of_vec):
        return np_arr

    if is_pandas_series(arr_of_vec):
        import pandas as pd
        return pd.Series(np_arr)

    if is_polars_series(arr_of_vec):
        import polars as pl
        return pl.Series(np_arr)

    if is_pyarrow_array(arr_of_vec):
        import pyarrow as pa
        return pa.array(np_arr)

    raise ValueError(
        f'unsupported object type: {type(arr_of_vec)}',
    )


def pack_vectors(
    arr_of_arr: Iterable[Any],
    vec_type: VectorTypes = VectorTypes.F32,
) -> Iterable[Any]:
    """
    Pack a vector into an array of bytes.

    Parameters
    ----------
    arr_of_arr : Iterable[Any]
        The array of bytes to pack.
    vec_type : VectorTypes
        The type of the elements in the vector.
        Can be one of 'f32', 'f64', 'i8', 'i16', 'i32', or 'i64'.
        Default is 'f32'.

    Returns
    -------
    Iterable[Any]
        The array of packed vectors.

    """
    if isinstance(arr_of_arr, (list, tuple)):
        if not arr_of_arr:
            return []
        fmt = _vector_type_to_struct_format(arr_of_arr[0], vec_type)
        return [struct.pack(fmt, x) for x in arr_of_arr]

    import numpy as np

    # Use object type because numpy truncates nulls at the end of fixed binary
    np_arr = np.array([x.tobytes() for x in arr_of_arr], dtype=np.object_)

    if is_numpy(arr_of_arr):
        return np_arr

    if is_pandas_series(arr_of_arr):
        import pandas as pd
        return pd.Series(np_arr)

    if is_polars_series(arr_of_arr):
        import polars as pl
        return pl.Series(np_arr)

    if is_pyarrow_array(arr_of_arr):
        import pyarrow as pa
        return pa.array(np_arr)

    raise ValueError(
        f'unsupported object type: {type(arr_of_arr)}',
    )
