#!/usr/bin/env python
"""
SingleStoreDB Management API v1 -- **deprecated**.

.. deprecated::
   Use :mod:`singlestoredb.management.v2`, which the ``management.version``
   option (the ``SINGLESTOREDB_MANAGEMENT_VERSION`` environment variable) names
   by default. This whole package is scheduled for removal; the public entry
   points warn when they resolve to it. Drop ``version='v1'`` and leave the
   option unset.

   The one exception is :mod:`.inference_api`, which has no replacement yet --
   see that module.
"""
# The version-neutral helpers in singlestoredb.management look these up here by
# name. A deployment is a workspace group, so they live in the workspace
# module.
from .workspace import get_organization as get_organization
from .workspace import get_secret as get_secret
from .workspace import get_stage as get_stage
