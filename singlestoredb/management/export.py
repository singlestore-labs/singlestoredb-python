#!/usr/bin/env python
"""SingleStoreDB export service."""
from __future__ import annotations

import copy
import json
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

from .. import ManagementError
from .utils import vars_to_str
from .workspace import WorkspaceGroup
from .workspace import WorkspaceManager


class ExportService(object):
    """Export service."""

    database: str
    table: str
    catalog_info: Dict[str, Any]
    storage_info: Dict[str, Any]
    columns: Optional[List[str]]
    partition_by: Optional[List[Dict[str, str]]]
    order_by: Optional[List[Dict[str, Dict[str, str]]]]
    properties: Optional[Dict[str, Any]]
    incremental: bool
    refresh_interval: Optional[int]
    export_id: Optional[str]

    def __init__(
        self,
        workspace_group: WorkspaceGroup,
        database: str,
        table: str,
        catalog_info: Union[str, Dict[str, Any]],
        storage_info: Union[str, Dict[str, Any]],
        columns: Optional[List[str]] = None,
        partition_by: Optional[List[Dict[str, str]]] = None,
        order_by: Optional[List[Dict[str, Dict[str, str]]]] = None,
        incremental: bool = False,
        refresh_interval: Optional[int] = None,
        properties: Optional[Dict[str, Any]] = None,
    ):
        #: Workspace group
        self.workspace_group = workspace_group

        #: Name of SingleStoreDB database
        self.database = database

        #: Name of SingleStoreDB table
        self.table = table

        #: List of columns to export
        self.columns = columns

        #: Catalog
        if isinstance(catalog_info, str):
            self.catalog_info = json.loads(catalog_info)
        else:
            self.catalog_info = copy.copy(catalog_info)

        #: Storage
        if isinstance(storage_info, str):
            self.storage_info = json.loads(storage_info)
        else:
            self.storage_info = copy.copy(storage_info)

        self.partition_by = partition_by or None
        self.order_by = order_by or None
        self.properties = properties or None

        self.incremental = incremental
        self.refresh_interval = refresh_interval

        self.export_id = None

        self._manager: Optional[WorkspaceManager] = workspace_group._manager

    @classmethod
    def from_export_id(
        self,
        workspace_group: WorkspaceGroup,
        export_id: str,
    ) -> ExportService:
        """Create export service from export ID."""
        out = ExportService(
            workspace_group=workspace_group,
            database='',
            table='',
            catalog_info={},
            storage_info={},
        )
        out.export_id = export_id
        return out

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)

    def create_cluster_identity(self) -> Dict[str, Any]:
        """Create a cluster identity."""
        if self._manager is None:
            raise ManagementError(
                msg='No workspace manager is associated with this object.',
            )

        out = self._manager._post(
            f'workspaceGroups/{self.workspace_group.id}/'
            'egress/createEgressClusterIdentity',
            json=dict(
                catalogInfo=self.catalog_info,
                storageInfo=self.storage_info,
            ),
        )

        return out.json()

    def start(self, tags: Optional[List[str]] = None) -> 'ExportStatus':
        """Start the export process."""
        if not self.table or not self.database:
            raise ManagementError(
                msg='Database and table must be set before starting the export.',
            )

        if self._manager is None:
            raise ManagementError(
                msg='No workspace manager is associated with this object.',
            )

        partition_spec = None
        if self.partition_by:
            partition_spec = dict(partitions=self.partition_by)

        sort_order_spec = None
        if self.order_by:
            sort_order_spec = dict(keys=self.order_by)

        out = self._manager._post(
            f'workspaceGroups/{self.workspace_group.id}/egress/startTableEgress',
            json={
                k: v for k, v in dict(
                    databaseName=self.database,
                    tableName=self.table,
                    storageInfo=self.storage_info,
                    catalogInfo=self.catalog_info,
                    partitionSpec=partition_spec,
                    sortOrderSpec=sort_order_spec,
                    properties=self.properties,
                    incremental=self.incremental or None,
                    refreshInterval=self.refresh_interval
                    if self.refresh_interval is not None else None,
                ).items() if v is not None
            },
        )

        self.export_id = str(out.json()['egressID'])

        return ExportStatus(self.export_id, self.workspace_group)

    def suspend(self) -> 'ExportStatus':
        """Suspend the export process."""
        if self._manager is None:
            raise ManagementError(
                msg='No workspace manager is associated with this object.',
            )

        if self.export_id is None:
            raise ManagementError(
                msg='Export ID is not set. You must start the export first.',
            )

        self._manager._post(
            f'workspaceGroups/{self.workspace_group.id}/egress/suspendTableEgress',
            json=dict(egressID=self.export_id),
        )

        return ExportStatus(self.export_id, self.workspace_group)

    def resume(self) -> 'ExportStatus':
        """Resume the export process."""
        if self._manager is None:
            raise ManagementError(
                msg='No workspace manager is associated with this object.',
            )

        if self.export_id is None:
            raise ManagementError(
                msg='Export ID is not set. You must start the export first.',
            )

        self._manager._post(
            f'workspaceGroups/{self.workspace_group.id}/egress/resumeTableEgress',
            json=dict(egressID=self.export_id),
        )

        return ExportStatus(self.export_id, self.workspace_group)

    def drop(self) -> None:
        """Drop the export process."""
        if self._manager is None:
            raise ManagementError(
                msg='No workspace manager is associated with this object.',
            )

        if self.export_id is None:
            raise ManagementError(
                msg='Export ID is not set. You must start the export first.',
            )

        self._manager._post(
            f'workspaceGroups/{self.workspace_group.id}/egress/dropTableEgress',
            json=dict(egressID=self.export_id),
        )

        return None

    def status(self) -> ExportStatus:
        """Get the status of the export process."""
        if self._manager is None:
            raise ManagementError(
                msg='No workspace manager is associated with this object.',
            )

        if self.export_id is None:
            raise ManagementError(
                msg='Export ID is not set. You must start the export first.',
            )

        return ExportStatus(self.export_id, self.workspace_group)


class ExportStatus(object):

    export_id: str

    def __init__(self, export_id: str, workspace_group: WorkspaceGroup):
        self.export_id = export_id
        self.workspace_group = workspace_group
        self._manager: Optional[WorkspaceManager] = workspace_group._manager

    def _info(self) -> Dict[str, Any]:
        """Return export status."""
        if self._manager is None:
            raise ManagementError(
                msg='No workspace manager is associated with this object.',
            )

        out = self._manager._get(
            f'workspaceGroups/{self.workspace_group.id}/egress/tableEgressStatus',
            json=dict(egressID=self.export_id),
        )

        return out.json()

    @property
    def status(self) -> str:
        """Return export status."""
        return self._info().get('status', 'Unknown')

    @property
    def message(self) -> str:
        """Return export status message."""
        return self._info().get('statusMsg', '')

    def __str__(self) -> str:
        return self.status

    def __repr__(self) -> str:
        return self.status


def _get_exports(
    workspace_group: WorkspaceGroup,
    scope: str = 'all',
) -> List[ExportStatus]:
    """Get all exports in the workspace group."""
    if workspace_group._manager is None:
        raise ManagementError(
            msg='No workspace manager is associated with this object.',
        )

    out = workspace_group._manager._get(
        f'workspaceGroups/{workspace_group.id}/egress/tableEgressStatus',
        json=dict(scope=scope),
    )

    return out.json()
