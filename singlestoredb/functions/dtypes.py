from warnings import warn

from .sql_types import *  # noqa: F403, F401

warn(
    'The dtypes module has been renamed to sql_types. '
    'Please update your imports to remove this warning.',
    DeprecationWarning, stacklevel=2,
)
