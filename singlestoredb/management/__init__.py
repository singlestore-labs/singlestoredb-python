#!/usr/bin/env python
# manage_workspaces() and the get_* helpers below are the deprecated v1
# workspace-group grammar; manage_clusters() is the front door. They disappear
# with the v1 package.
from .cluster import manage_clusters
from .files import manage_files
from .manager import get_token
from .region import manage_regions
from .workspace import get_organization
from .workspace import get_secret
from .workspace import get_stage
from .workspace import manage_workspaces
