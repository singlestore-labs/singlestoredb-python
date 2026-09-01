#!/usr/bin/env python
"""
SingleStoreDB Management API v1 -- **deprecated**.

.. deprecated::
   v1 has been replaced by :mod:`singlestoredb.management.v2`, which is what
   the ``management.version`` option (the ``SINGLESTOREDB_MANAGEMENT_VERSION``
   environment variable) now names by default. This whole package is scheduled
   for removal; the public entry points warn when they resolve to it. Reach v2
   by dropping ``version='v1'`` and leaving the option unset.

   The one exception is :mod:`.inference_api`, which has no v2 counterpart --
   see that module.
"""
# The version-neutral helpers in singlestoredb.management look these up here by
# name, so each version can keep them wherever they belong. At v1 a deployment
# is a workspace group, so they live in the workspace module.
from .workspace import get_organization as get_organization
from .workspace import get_secret as get_secret
from .workspace import get_stage as get_stage
