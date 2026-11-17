#!/usr/bin/env python
"""Check that the C extension loads, and leaves the GIL off where there is none.

On a free-threaded interpreter, importing an extension that does not declare
itself safe without the GIL turns the GIL back on for the whole process, with
only a RuntimeWarning to show for it. Run this against an installed wheel; it
needs no database.
"""
import sys
import sysconfig

import _singlestoredb_accel

import singlestoredb  # noqa: F401

free_threaded = bool(sysconfig.get_config_var('Py_GIL_DISABLED'))
gil_enabled = getattr(sys, '_is_gil_enabled', lambda: True)()

print(f'{_singlestoredb_accel.__file__}: free-threaded={free_threaded} gil={gil_enabled}')

if free_threaded and gil_enabled:
    sys.exit('importing the C extension re-enabled the GIL')
