from collections.abc import Iterable
import dataclasses
import json
from typing import Annotated
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import TypeVar
from typing import Union

try:
    from typing import TypeVarTuple  # type: ignore
    from typing import Unpack  # type: ignore
except ImportError:
    # Python 3.8 and earlier do not have TypeVarTuple
    from typing_extensions import TypeVarTuple  # type: ignore
    from typing_extensions import Unpack  # type: ignore

from .. import dtypes

T = TypeVar('T', bound=Iterable[Any])  # Generic type for iterable types

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


@dataclasses.dataclass
class UDFAttrs:
    sql_type: Optional[dtypes.SQLString] = None
    input_transformer: Optional[Callable[..., Any]] = None
    output_transformer: Optional[Callable[..., Any]] = None


JSON = Annotated[
    Union[Dict[str, Any], List[Any], int, float, str, bool, None],
    UDFAttrs(
        sql_type=dtypes.JSON(nullable=False),
        input_transformer=json.loads,
        output_transformer=json.dumps,
    ),
]


__all__ = [
    'Table',
    'Masked',
    'JSON',
    'UDFAttrs',
]
