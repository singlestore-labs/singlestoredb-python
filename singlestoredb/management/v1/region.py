#!/usr/bin/env python
"""
SingleStoreDB Region Management API v1 -- **deprecated**.

.. deprecated::
   Only the module path is deprecated; the names re-exported below are the
   shared implementations. Import them from
   :mod:`singlestoredb.management.region`, and call ``manage_regions()``
   without ``version='v1'``.

``GET /v1/regions`` and ``GET /v1/regions/sharedtier`` behave exactly as
:mod:`singlestoredb.management.region` implements them, so this module only
re-exports it. These routes report a ``regionID``, which
:meth:`Region.from_dict` treats as optional.
"""
from ..region import Region as Region
from ..region import RegionManager as RegionManager
