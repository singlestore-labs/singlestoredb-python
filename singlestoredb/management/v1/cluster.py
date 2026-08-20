#!/usr/bin/env python
"""
v1 landing point for v2 cluster objects.

``VersionedMixin`` resolves ``cluster.v1`` by importing the module named after
the source class's module -- ``cluster`` -- from the ``v1`` package and looking
up the source class's name in it. This module supplies those names, adapting a
v2 cluster response body onto the v1 workspace classes.

It lives here rather than in ``v2/`` so that the v2 modules never have to name a
v1 resource, and so the adapters disappear together with the rest of the v1
package once the v1 endpoints are retired. See ``TestV1IsDeletable``.
"""
from typing import Any
from typing import Dict

from ._translate import cluster_to_workspace
from ._translate import starter_cluster_to_starter_workspace
from .workspace import StarterWorkspace as _StarterWorkspace
from .workspace import Workspace as _Workspace
from .workspace import WorkspaceManager as ClusterManager  # noqa: F401


class Cluster:
    """Adapter that rebuilds a v1 :class:`Workspace` from a v2 cluster body."""

    @classmethod
    def from_dict(
        cls, obj: Dict[str, Any], manager: 'ClusterManager',
    ) -> _Workspace:
        return _Workspace.from_dict(cluster_to_workspace(obj) or {}, manager)


class StarterCluster:
    """Adapter that rebuilds a v1 :class:`StarterWorkspace` from a v2 body."""

    @classmethod
    def from_dict(
        cls, obj: Dict[str, Any], manager: 'ClusterManager',
    ) -> _StarterWorkspace:
        return _StarterWorkspace.from_dict(
            starter_cluster_to_starter_workspace(obj) or {}, manager,
        )
