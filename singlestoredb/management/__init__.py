#!/usr/bin/env python
# Everything exported here is version-neutral: an explicit ``version=`` wins,
# otherwise the ``management.version`` option (the
# SINGLESTOREDB_MANAGEMENT_VERSION environment variable) decides which version
# package answers the call. Import from .v1/.v2 -- or from the version-locked
# shims .workspace and .cluster -- to pin a version instead.
from .cluster import manage_clusters
from .files import manage_files
from .manager import get_token
from .organization import get_organization
from .organization import get_secret
from .region import manage_regions
from .stage import get_stage
from .workspace import manage_workspaces
