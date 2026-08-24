#!/usr/bin/env python
"""SingleStoreDB Management API v2."""
# The version-neutral helpers in singlestoredb.management look these up here by
# name, so each version can keep them wherever they belong. At v2 a deployment
# is a cluster, so they live in the cluster module.
from .cluster import get_organization as get_organization
from .cluster import get_secret as get_secret
from .cluster import get_stage as get_stage
