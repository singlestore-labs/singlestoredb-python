#!/usr/bin/env python
"""
SingleStoreDB Workspace Management (management API v1) -- **deprecated**.

.. deprecated::
   Every name below comes from :mod:`singlestoredb.management.v1.workspace`.
   Workspaces and workspace groups are the management API v1 deployment
   vocabulary; v2 replaced both with the flat
   :class:`~singlestoredb.management.cluster.Cluster`. Use
   :mod:`singlestoredb.management.cluster` and
   :func:`singlestoredb.manage_clusters` instead.

   Deprecated, not removed: every name here still works against the live v1
   endpoints, and :func:`manage_workspaces` still hands back a working manager
   without being asked for a version. Only the eventual removal of
   :mod:`singlestoredb.management.v1` takes it away, and that has not happened.
"""
import warnings
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


def _manage_workspaces_v1(
    access_token: Optional[str] = None,
    version: Optional[str] = None,
    base_url: Optional[str] = None,
    *,
    organization_id: Optional[str] = None,
) -> 'WorkspaceManager':
    """
    Retrieve a SingleStoreDB workspace manager without warning.

    This is the body of :func:`manage_workspaces` minus the deprecation warning.
    Internal callers that are v1-only by design -- Fusion, the UDF ``stage://``
    handling, the AI inference helpers -- go through here so they do not emit a
    warning the caller can do nothing about. They are asking for a workspace
    manager specifically, not for whatever the environment prefers.

    Neither function consults the ``management.version`` option: workspaces
    exist only at v1. See :func:`manage_workspaces` for why.
    """
    from ..exceptions import ManagementError
    ver = version or 'v1'
    if ver != 'v1':
        raise ManagementError(
            msg=f'workspaces do not exist in management API {ver}; they were '
                'replaced by clusters. Use manage_clusters() instead, or pass '
                'version="v1" here. Note that the management.version option '
                'does not reach this function: workspaces are v1-only, so it '
                'has nothing to select.',
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
        Version of the API to use. Defaults to ``'v1'``, **not** to the
        ``management.version`` option: workspaces exist only at v1, so there is
        no version for this function to dispatch on. Passing anything else
        raises. This is the one public entry point the option does not steer.
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
        If the caller explicitly asks for a version other than ``v1``.
        Workspaces and workspace groups were replaced by clusters in v2; use
        :func:`singlestoredb.manage_clusters` instead.

    """
    warnings.warn(
        'manage_workspaces() is deprecated: workspaces and workspace groups '
        'were replaced by the flat Cluster resource in management API v2. '
        'Use manage_clusters() instead. This still returns a working v1 '
        'manager.',
        DeprecationWarning,
        stacklevel=2,
    )
    # Pinned to v1 rather than resolved through the management.version option.
    # The option selects between implementations of a resource that exists at
    # more than one version; workspaces exist only at v1, so there is nothing
    # here for it to select, and resolving it would make a bare
    # manage_workspaces() raise as soon as the default moved past v1 -- v1
    # ceasing to work rather than v1 being deprecated. Callers are steered to
    # clusters by the deprecation warning above, not by an exception.
    #
    # Deliberately *not* symmetrical with manage_clusters(), which does consult
    # the option and raises when it names v1: an option reading v1 is a
    # deliberate request that manage_clusters() cannot satisfy, whereas an
    # option sitting at its default says nothing about workspaces.
    return _manage_workspaces_v1(
        access_token, version, base_url,
        organization_id=organization_id,
    )
