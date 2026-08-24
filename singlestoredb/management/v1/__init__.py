#!/usr/bin/env python
"""SingleStoreDB Management API v1."""
# The version-neutral helpers in singlestoredb.management look these up here by
# name, so each version can keep them wherever they belong. At v1 a deployment
# is a workspace group, so they live in the workspace module.
from .workspace import get_organization as get_organization
from .workspace import get_secret as get_secret
from .workspace import get_stage as get_stage
