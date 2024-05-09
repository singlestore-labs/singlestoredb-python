#!/usr/bin/env python
import os as _os
import warnings as _warnings

from ._objects import organization  # noqa: F401
from ._objects import secrets  # noqa: F401
from ._objects import stage  # noqa: F401
from ._objects import workspace  # noqa: F401
from ._objects import workspace_group  # noqa: F401
from ._portal import portal  # noqa: F401

if 'SINGLESTOREDB_ORGANIZATION' not in _os.environ:
    _warnings.warn(
        'This package is intended for use in the SingleStoreDB notebook environment',
        RuntimeWarning,
    )
