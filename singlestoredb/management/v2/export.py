#!/usr/bin/env python
"""
SingleStoreDB export service (API v2).

Table egress is driven through ``clusters/{id}/egress/...``, so an export is
owned by a :class:`~singlestoredb.management.v2.cluster.Cluster`. Nothing here
imports from :mod:`singlestoredb.management.v1`; see
``TestVersionPackagesAreIndependent``.
"""
from __future__ import annotations

import copy
import json
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

from ...exceptions import ManagementError
from ..utils import vars_to_str
from .cluster import Cluster
from .cluster import ClusterManager


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
        cluster: Cluster,
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
        #: Cluster the export runs against
        self.cluster = cluster

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

        self._manager: Optional[ClusterManager] = cluster._manager

    @classmethod
    def from_export_id(
        cls,
        cluster: Cluster,
        export_id: str,
    ) -> ExportService:
        """Create export service from export ID."""
        out = cls(
            cluster=cluster,
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

    def _require_manager(self) -> ClusterManager:
        if self._manager is None:
            raise ManagementError(
                msg='No cluster manager is associated with this object.',
            )
        return self._manager

    def _require_export_id(self) -> str:
        if self.export_id is None:
            raise ManagementError(
                msg='Export ID is not set. You must start the export first.',
            )
        return self.export_id

    def _egress_path(self, verb: str) -> str:
        return f'clusters/{self.cluster.id}/egress/{verb}'

    def create_cluster_identity(self) -> Dict[str, Any]:
        """Create a cluster identity."""
        out = self._require_manager()._post(
            self._egress_path('createEgressClusterIdentity'),
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

        manager = self._require_manager()

        partition_spec = None
        if self.partition_by:
            partition_spec = dict(partitions=self.partition_by)

        sort_order_spec = None
        if self.order_by:
            sort_order_spec = dict(keys=self.order_by)

        out = manager._post(
            self._egress_path('startTableEgress'),
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

        return ExportStatus(self.export_id, self.cluster)

    def suspend(self) -> 'ExportStatus':
        """Suspend the export process."""
        manager = self._require_manager()
        export_id = self._require_export_id()
        manager._post(
            self._egress_path('suspendTableEgress'),
            json=dict(egressID=export_id),
        )
        return ExportStatus(export_id, self.cluster)

    def resume(self) -> 'ExportStatus':
        """Resume the export process."""
        manager = self._require_manager()
        export_id = self._require_export_id()
        manager._post(
            self._egress_path('resumeTableEgress'),
            json=dict(egressID=export_id),
        )
        return ExportStatus(export_id, self.cluster)

    def drop(self) -> None:
        """Drop the export process."""
        manager = self._require_manager()
        export_id = self._require_export_id()
        manager._delete(
            self._egress_path('dropTableEgress'),
            json=dict(egressID=export_id),
        )
        return None

    def status(self) -> ExportStatus:
        """Get the status of the export process."""
        self._require_manager()
        return ExportStatus(self._require_export_id(), self.cluster)


class ExportStatus(object):
    """Status of a table egress process."""

    export_id: str

    def __init__(self, export_id: str, cluster: Cluster):
        self.export_id = export_id
        self.cluster = cluster
        self._manager: Optional[ClusterManager] = cluster._manager

    def _info(self) -> Dict[str, Any]:
        """Return export status."""
        if self._manager is None:
            raise ManagementError(
                msg='No cluster manager is associated with this object.',
            )

        out = self._manager._get(
            f'clusters/{self.cluster.id}/egress/tableEgressStatus',
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
    cluster: Cluster,
    scope: str = 'all',
) -> List[ExportStatus]:
    """Get all exports in the cluster."""
    if cluster._manager is None:
        raise ManagementError(
            msg='No cluster manager is associated with this object.',
        )

    out = cluster._manager._get(
        f'clusters/{cluster.id}/egress/tableEgressStatus',
        json=dict(scope=scope),
    )

    return [
        ExportStatus(item['egressID'], cluster)
        for item in out.json()
    ]
