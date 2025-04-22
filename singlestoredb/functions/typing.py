from typing import Tuple
from typing import TypeVar

try:
    import numpy as np
    import numpy.typing as npt
    has_numpy = True
except ImportError:
    has_numpy = False


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
T = TypeVar('T')
Masked = Tuple[T, T]


#
# The MaskedNDArray type is used for pairs of numpy arrays where the first
# element is the numpy array and the second element is a boolean mask
# indicating which elements are NULL. The boolean mask is a numpy array of
# the same shape as the first element, where True indicates that the
# corresponding element in the first element is NULL.
#
# This is needed bebause numpy arrays do not support NULL values, so we need to
# use a boolean mask to indicate which elements are NULL.
#
if has_numpy:
    TT = TypeVar('TT', bound=np.generic)  # Generic type for NumPy data types
    MaskedNDArray = Tuple[npt.NDArray[TT], npt.NDArray[np.bool_]]
