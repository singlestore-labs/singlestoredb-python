#!/usr/bin/env python
"""SingleStoreDB Cluster Management."""
from typing import Optional

from .v1.cluster import Cluster as Cluster
from .v1.cluster import ClusterManager as ClusterManager
from .versioned import _import_versioned_module
# Re-export from default version for backward compatibility


def manage_cluster(
    access_token: Optional[str] = None,
    version: Optional[str] = None,
    base_url: Optional[str] = None,
    *,
    organization_id: Optional[str] = None,
) -> 'ClusterManager':
    """
    Retrieve a SingleStoreDB cluster manager.

    Parameters
    ----------
    access_token : str, optional
        The API key or other access token for the workspace management API
    version : str, optional
        Version of the API to use
    base_url : str, optional
        Base URL of the workspace management API
    organization_id : str, optional
        ID of organization, if using a JWT for authentication

    Returns
    -------
    :class:`ClusterManager`

    """
    from .. import config
    ver = version or config.get_option('management.version') or 'v1'
    mod = _import_versioned_module(ver, 'cluster')
    return mod.ClusterManager(
        access_token=access_token, base_url=base_url,
        version=ver, organization_id=organization_id,
    )
