#!/usr/bin/env python
"""SingleStoreDB Workspace Management."""
from typing import Optional

from ._version_import import _import_versioned_module
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


def manage_workspaces(
    access_token: Optional[str] = None,
    version: Optional[str] = None,
    base_url: Optional[str] = None,
    *,
    organization_id: Optional[str] = None,
) -> 'WorkspaceManager':
    """
    Retrieve a SingleStoreDB workspace manager.

    Parameters
    ----------
    access_token : str, optional
        The API key or other access token for the workspace management API
    version : str, optional
        Version of the API to use. Workspaces only exist at ``v1``, so this
        defaults to ``v1`` regardless of the ``management.version`` option.
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
        If a version other than ``v1`` is explicitly requested. Workspaces and
        workspace groups were replaced by clusters in v2; use
        :func:`singlestoredb.manage_clusters` instead.

    """
    from ..exceptions import ManagementError
    # Deliberately not routed through the ``management.version`` option:
    # workspaces are a v1-only resource, so a global preference for another
    # version has nothing to say about them. Only an explicit ``version``
    # argument is an error, because only that is a caller asking for a
    # workspace manager that cannot exist.
    ver = version or 'v1'
    if ver != 'v1':
        raise ManagementError(
            msg=f'workspaces do not exist in management API {ver}; '
                'they were replaced by clusters. Use manage_clusters() '
                'instead, or request version="v1".',
        )
    mod = _import_versioned_module(ver, 'workspace')
    return mod.WorkspaceManager(
        access_token=access_token, base_url=base_url,
        version=ver, organization_id=organization_id,
    )
