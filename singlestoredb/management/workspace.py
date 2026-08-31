#!/usr/bin/env python
"""SingleStoreDB Workspace Management."""
import warnings
from typing import Optional

from ._version_import import _import_versioned_module
from ._version_import import _resolve_version
from .v1.organization import Organization as Organization
from .v1.workspace import Billing as Billing
from .v1.workspace import get_organization as get_organization
from .v1.workspace import get_secret as get_secret
from .v1.workspace import get_stage as get_stage
from .v1.workspace import get_workspace as get_workspace
from .v1.workspace import get_workspace_group as get_workspace_group
from .v1.workspace import Organizations as Organizations
from .v1.workspace import Stage as Stage
from .v1.workspace import StarterWorkspace as StarterWorkspace
from .v1.workspace import Workspace as Workspace
from .v1.workspace import WorkspaceGroup as WorkspaceGroup
from .v1.workspace import WorkspaceManager as WorkspaceManager
# Re-export from default version for backward compatibility


def _manage_workspaces_v1(
    access_token: Optional[str] = None,
    version: Optional[str] = None,
    base_url: Optional[str] = None,
    *,
    organization_id: Optional[str] = None,
) -> 'WorkspaceManager':
    """
    Retrieve a SingleStoreDB workspace manager without warning.

    This is the body of :func:`manage_workspaces` minus the deprecation
    warning and the version resolution. Internal callers that are v1-only by
    design -- Fusion, the UDF ``stage://`` handling, the AI inference helpers --
    go through here so they neither emit a warning the caller can do nothing
    about nor break when the ``management.version`` option names another
    version. They are asking for a workspace manager specifically, not for
    whatever the environment prefers.
    """
    from ..exceptions import ManagementError
    ver = version or 'v1'
    if ver != 'v1':
        raise ManagementError(
            msg=f'workspaces do not exist in management API {ver}; they were '
                'replaced by clusters. Use manage_clusters() instead, or ask '
                'for v1, either with version="v1" here or by setting the '
                'management.version option.',
        )
    mod = _import_versioned_module(ver, 'workspace')
    return mod.WorkspaceManager(
        access_token=access_token, base_url=base_url,
        version=ver, organization_id=organization_id,
    )


def manage_workspaces(
    access_token: Optional[str] = None,
    version: Optional[str] = None,
    base_url: Optional[str] = None,
    *,
    organization_id: Optional[str] = None,
) -> 'WorkspaceManager':
    """
    Retrieve a SingleStoreDB workspace manager.

    .. deprecated::
       Workspaces and workspace groups were replaced by the flat ``Cluster``
       resource in management API v2. Use
       :func:`singlestoredb.manage_clusters` instead.

    Parameters
    ----------
    access_token : str, optional
        The API key or other access token for the workspace management API
    version : str, optional
        Version of the API to use. Defaults to the ``management.version``
        option (the ``SINGLESTOREDB_MANAGEMENT_VERSION`` environment
        variable), or to ``DEFAULT_VERSION`` when that is unset. Both now name
        ``v2``, and v2 has no workspaces, so a caller who wants a workspace
        manager has to ask for ``v1`` outright.
    base_url : str, optional
        Base URL of the workspace management API
    organization_id : str, optional
        ID of organization, if using a JWT for authentication

    Returns
    -------
    :class:`WorkspaceManager`

    Raises
    ------
    :class:`ManagementError`
        If the resolved version is not ``v1``, whether it was requested by the
        caller or by the ``management.version`` option. Workspaces and
        workspace groups were replaced by clusters in v2; use
        :func:`singlestoredb.manage_clusters` instead.

    """
    warnings.warn(
        'manage_workspaces() is deprecated: workspaces and workspace groups '
        'were replaced by the flat Cluster resource in management API v2. '
        'Use manage_clusters() instead.',
        DeprecationWarning,
        stacklevel=2,
    )
    # Follows the management.version option like the other public entry
    # points rather than pinning to v1, so that once the option names a version
    # without workspaces the caller is told to move rather than quietly handed
    # a v1 manager for an org that has outgrown it.
    return _manage_workspaces_v1(
        access_token, _resolve_version(version), base_url,
        organization_id=organization_id,
    )
