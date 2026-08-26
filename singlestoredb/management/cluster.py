#!/usr/bin/env python
"""
SingleStoreDB Cluster Management.

Clusters are the flat deployment resource introduced by management API v2, so
the names below come from :mod:`singlestoredb.management.v2.cluster`. There is
no v1 cluster resource; :func:`manage_clusters` defaults to v2 accordingly.
"""
from typing import Optional

from ._version_import import _import_versioned_module
from .v2.cluster import Cluster as Cluster
from .v2.cluster import ClusterManager as ClusterManager
from .v2.cluster import get_cluster as get_cluster
from .v2.cluster import get_organization as get_organization
from .v2.cluster import get_secret as get_secret
from .v2.cluster import get_stage as get_stage
from .v2.cluster import Project as Project
from .v2.cluster import PROJECT_ID_RE as PROJECT_ID_RE
from .v2.cluster import SHAREDTIER_PATH as SHAREDTIER_PATH
from .v2.cluster import Stage as Stage
from .v2.cluster import StageObject as StageObject
from .v2.cluster import StarterCluster as StarterCluster

#: API version used by :func:`manage_clusters` when neither the caller nor the
#: ``management.version`` option names one.
DEFAULT_CLUSTER_VERSION = 'v2'


def manage_clusters(
    access_token: Optional[str] = None,
    version: Optional[str] = None,
    base_url: Optional[str] = None,
    *,
    organization_id: Optional[str] = None,
) -> ClusterManager:
    """
    Retrieve a SingleStoreDB cluster manager.

    Parameters
    ----------
    access_token : str, optional
        The API key or other access token for the cluster management API
    version : str, optional
        Version of the API to use. Defaults to the ``management.version``
        option (the ``SINGLESTOREDB_MANAGEMENT_VERSION`` environment
        variable), or to :data:`DEFAULT_CLUSTER_VERSION` when that is unset.
    base_url : str, optional
        Base URL of the cluster management API
    organization_id : str, optional
        ID of organization, if using a JWT for authentication

    Returns
    -------
    :class:`ClusterManager`

    Raises
    ------
    :class:`ManagementError`
        If ``v1`` is the resolved version, whether requested by the caller or
        by the ``management.version`` option. Clusters were introduced in v2;
        the v1 equivalents are workspaces, reached with
        :func:`singlestoredb.manage_workspaces`.

    """
    from ..exceptions import ManagementError
    from ._version_import import _resolve_version
    # Follows the management.version option like the other public entry points
    # rather than pinning the front door to one version, so a future version is
    # picked up from the environment. That option still defaults to 'v1', which
    # has no clusters, so a bare call raises until that default is flipped.
    ver = _resolve_version(version, default=DEFAULT_CLUSTER_VERSION)
    if ver == 'v1':
        raise ManagementError(
            msg='clusters do not exist in management API v1; they replaced '
                'workspaces in v2. Use manage_workspaces() instead, or ask '
                'for v2, either with version="v2" here or by setting the '
                'management.version option.',
        )
    mod = _import_versioned_module(ver, 'cluster')
    return mod.ClusterManager(
        access_token=access_token, base_url=base_url,
        version=ver, organization_id=organization_id,
    )
