import dataclasses
import json
from collections.abc import Sequence
from typing import Annotated
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Protocol
from typing import Tuple
from typing import TypeVar
from typing import Union

try:
    from typing import TypeVarTuple  # type: ignore
    from typing import Unpack  # type: ignore
    from typing import TypeAlias  # type: ignore
except ImportError:
    # Python 3.8 and earlier do not have TypeVarTuple
    from typing_extensions import TypeVarTuple  # type: ignore
    from typing_extensions import Unpack  # type: ignore
    from typing_extensions import TypeAlias  # type: ignore

from .. import sql_types

T = TypeVar('T', bound=Sequence[Any])  # Generic type for iterable types

#
# Masked types are used for pairs of vectors where the first element is the
# vector and the second element is a boolean mask indicating which elements
# are NULL. The boolean mask is a vector of the same length as the first
# element, where True indicates that the corresponding element in the first
# element is NULL.
#
# This is needed for vector types that do not support NULL values, such as
# numpy arrays and pandas Series.
#


class Masked(Tuple[T, T]):
    def __new__(cls, *args: T) -> 'Masked[Tuple[T, T]]':  # type: ignore
        return tuple.__new__(cls, (args[0], args[1]))  # type: ignore


Ts = TypeVarTuple('Ts')


class Table(Tuple[Unpack[Ts]]):
    """Return type for a table valued function."""

    def __new__(cls, *args: Unpack[Ts]) -> 'Table[Tuple[Unpack[Ts]]]':  # type: ignore
        return tuple.__new__(cls, args)  # type: ignore


class TypedTransformer(Protocol):

    output_type: Optional[Any] = None

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        ...


Transformer = Union[Callable[..., Any], TypedTransformer]


def output_type(dtype: Any) -> Callable[..., Any]:
    """
    Decorator that sets the output_type attribute on a function.

    Parameters
    ----------
    dtype : Any
        The data type to set as the function's output_type attribute

    Returns
    -------
    Callable
        The decorated function with output_type attribute set
    """

    def decorator(func: Callable[..., Any]) -> Transformer:
        func.output_type = dtype  # type: ignore
        return func  # type: ignore

    return decorator


@dataclasses.dataclass
class UDFAttrs:
    sql_type: Optional[sql_types.SQLString] = None
    args_transformer: Optional[Transformer] = None
    returns_transformer: Optional[Transformer] = None


def json_or_null_dumps(v: Optional[Any], **kwargs: Any) -> Optional[str]:
    """
    Serialize a Python object to a JSON string or None

    Parameters
    ----------
    v : Optional[Any]
        The Python object to serialize. If None or empty, the function returns None.
    **kwargs : Any
        Additional keyword arguments to pass to `json.dumps`.

    Returns
    -------
    Optional[str]
        The JSON string representation of the input object,
        or None if the input is None or empty

    """
    if not v:
        return None
    return json.dumps(v, **kwargs)


# Force numpy dtype to 'object' to avoid issues with
# numpy trying to infer the dtype and creating multidimensional arrays
# instead of an array of Python objects.
@output_type('object')
def json_or_null_loads(v: Optional[str], **kwargs: Any) -> Optional[Any]:
    """
    Deserialize a JSON string to a Python object or None

    Parameters
    ----------
    v : Optional[str]
        The JSON string to deserialize. If None or empty, the function returns None.
    **kwargs : Any
        Additional keyword arguments to pass to `json.loads`.

    Returns
    -------
    Optional[Any]
        The Python object represented by the JSON string,
        or None if the input is None or empty

    """
    if not v:
        return None
    return json.loads(v, **kwargs)


JSON: TypeAlias = Annotated[
    Union[Dict[str, Any], List[Any], int, float, str, bool, None],
    UDFAttrs(
        sql_type=sql_types.JSON(nullable=False),
        args_transformer=json_or_null_loads,
        returns_transformer=json_or_null_dumps,
    ),
]


__all__ = [
    'Table',
    'Masked',
    'JSON',
    'UDFAttrs',
    'Transformer',
]
