#!/usr/bin/env python
"""SingleStoreDB Workspace Management API v2."""
from typing import Optional

from ...exceptions import ManagementError
from ..v1.workspace import Billing as Billing
from ..v1.workspace import get_organization as get_organization
from ..v1.workspace import get_secret as get_secret
from ..v1.workspace import get_stage as get_stage
from ..v1.workspace import get_workspace as get_workspace
from ..v1.workspace import get_workspace_group as get_workspace_group
from ..v1.workspace import Organizations as Organizations
from ..v1.workspace import Stage as Stage
from ..v1.workspace import StarterWorkspace as StarterWorkspace
from ..v1.workspace import Workspace as Workspace
from ..v1.workspace import WorkspaceGroup as V1WorkspaceGroup
from ..v1.workspace import WorkspaceManager as WorkspaceManager


class WorkspaceGroup(V1WorkspaceGroup):
    """
    Workspace group (API v2).

    Adds methods that hit ``/v2/`` paths. Field/parsing behavior is
    identical to v1 — v2 inherits all v1 attributes and parsers.

    Access via ``wg.v2`` on a v1 :class:`WorkspaceGroup` instance.
    """

    def get_metrics(self) -> str:
        """
        Return OpenMetrics-formatted metrics for this workspace group.

        Calls ``GET /v2/organizations/{organizationID}/workspaceGroups/
        {workspaceGroupID}/metrics``. The organization ID is taken from the
        manager's configured ID, falling back to
        ``self._manager.v1.organization.id`` if not set.

        The fallback intentionally drops to v1 because the v2 API has no
        ``organizations/current`` endpoint; the OpenAPI spec for this v2
        metrics endpoint explicitly directs callers to
        ``/v1/organizations/current`` to resolve the organization ID.

        Returns
        -------
        str
            Raw OpenMetrics text body.

        Raises
        ------
        ManagementError
            If no manager is associated with this object, or the
            organization ID cannot be resolved.
        """
        if self._manager is None:
            raise ManagementError(
                msg='No workspace manager is associated with this object.',
            )

        org_id: Optional[str] = (
            self._manager._organization_id
            or self._manager._params.get('organizationID')
        )
        if not org_id:
            org_id = self._manager.v1.organization.id
        if not org_id:
            raise ManagementError(
                msg='Could not resolve organization ID for metrics request.',
            )

        res = self._manager._get(
            f'organizations/{org_id}/workspaceGroups/{self.id}/metrics',
            headers={'Accept': 'text/plain'},
        )
        return res.text
