from typing import Any
from typing import Iterable
from typing import Tuple
from typing import TypeVar

try:
    from typing import TypeVarTuple  # type: ignore
    from typing import Unpack  # type: ignore
except ImportError:
    # Python 3.8 and earlier do not have TypeVarTuple
    from typing_extensions import TypeVarTuple  # type: ignore
    from typing_extensions import Unpack  # type: ignore


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
