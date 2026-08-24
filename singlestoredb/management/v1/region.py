#!/usr/bin/env python
"""
SingleStoreDB Region Management API v1.

Both ``GET /v1/regions`` and ``GET /v1/regions/sharedtier`` behave exactly as
the shared :mod:`singlestoredb.management.region` module implements them --
``regions/sharedtier`` answers identically at v1 and v2 -- so this module only
re-exports it. ``regionID`` is present at v1 and absent from v2, but
:meth:`Region.from_dict` already treats it as optional.
"""
from ..region import Region as Region
from ..region import RegionManager as RegionManager
