#!/usr/bin/env python
"""
SingleStoreDB Region Management API v1.

``GET /v1/regions`` and ``GET /v1/regions/sharedtier`` are implemented by the
shared :mod:`singlestoredb.management.region` module, so this module only
re-exports those classes under the names ``VersionedMixin`` looks up when
resolving ``obj.v1``.
"""
from ..region import Region as Region
from ..region import RegionManager as RegionManager
