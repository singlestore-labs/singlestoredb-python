#!/usr/bin/env python
"""
SingleStoreDB Region Management API v2.

``GET /v2/regions`` returns entries containing ``provider``, ``region``, and
``regionName`` only -- no ``regionID``. :class:`Region` instances therefore
have ``id is None`` and ``region_name`` set; v2 identifies a region by
``(provider, region_name)``. That is what the shared
:mod:`singlestoredb.management.region` module implements, so this module only
re-exports it.
"""
from ..region import Region as Region
from ..region import RegionManager as RegionManager
