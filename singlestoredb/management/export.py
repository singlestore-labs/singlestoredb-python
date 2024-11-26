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

    def __init__(
        self,
        workspace_group: WorkspaceGroup,
        database: str,
        table: str,
        catalog_info: Union[str, Dict[str, Any]],
        storage_info: Union[str, Dict[str, Any]],
        columns: Optional[List[str]],
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

        self._manager: Optional[WorkspaceManager] = workspace_group._manager

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
        if self._manager is None:
            raise ManagementError(
                msg='No workspace manager is associated with this object.',
            )

        out = self._manager._post(
            f'workspaceGroups/{self.workspace_group.id}/egress/startTableEgress',
            json=dict(
                databaseName=self.database,
                tableName=self.table,
                storageInfo=self.storage_info,
                catalogInfo=self.catalog_info,
            ),
        )

        return ExportStatus(out.json()['egressID'], self.workspace_group)


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
