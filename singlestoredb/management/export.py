#!/usr/bin/env python
"""SingleStoreDB export service."""
from __future__ import annotations

import abc
import re
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

from .. import ManagementError
from .utils import vars_to_str
from .workspace import WorkspaceGroup
from .workspace import WorkspaceManager


class Link(object):
    """Generic storage base class."""
    scheme: str = 'unknown'

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)

    @abc.abstractmethod
    def to_storage_location(self) -> Dict[str, Any]:
        raise NotImplementedError

    @classmethod
    def from_config_and_creds(
        cls,
        scheme: str,
        config: Dict[str, Any],
        credentials: Dict[str, Any],
        manager: 'WorkspaceManager',
    ) -> 'Link':
        out_cls = None
        for c in cls.__subclasses__():
            if c.scheme == scheme.upper():
                out_cls = c
                break

        if out_cls is None:
            raise TypeError(f'No link class found for given information: {scheme}')

        return out_cls.from_config_and_creds(scheme, config, credentials, manager)


class S3Link(Link):
    """S3 link."""

    scheme: str = 'S3'
    region: str
    storage_base_url: str

    def __init__(self, region: str, storage_base_url: str):
        self.region = region
        self.storage_base_url = storage_base_url
        self._manager: Optional[WorkspaceManager] = None

    def to_storage_location(self) -> Dict[str, Any]:
        return dict(
            storageBaseURL=self.storage_base_url,
            storageRegion=self.region,
        )

    @classmethod
    def from_config_and_creds(
        cls,
        scheme: str,
        config: Dict[str, Any],
        credentials: Dict[str, Any],
        manager: 'WorkspaceManager',
    ) -> 'S3Link':
        assert scheme.upper() == cls.scheme

        params: Dict[str, Any] = {}
        params.update(config)
        params.update(credentials)

        assert params.get('region'), 'region is required'
        assert params.get('endpoint_url'), 'endpoint_url is required'

        out = cls(params['region'], params['endpoint_url'])
        out._manager = manager
        return out


class Catalog(object):
    """Generic catalog base class."""

    catalog_type: str = 'UNKNOWN'
    table_format: str = 'UNKNOWN'

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)

    @classmethod
    def from_config_and_creds(
        cls,
        config: Dict[str, Any],
        credentials: Dict[str, Any],
        manager: 'WorkspaceManager',
    ) -> 'Catalog':
        catalog_type = config['type'].upper()
        table_format = config['table_format'].upper()

        out_cls = None
        for c in cls.__subclasses__():
            if c.catalog_type == catalog_type and c.table_format == table_format:
                out_cls = c
                break

        if out_cls is None:
            raise TypeError(f'No catalog class found for given information: {config}')

        return out_cls.from_config_and_creds(config, credentials, manager)

    @abc.abstractmethod
    def to_catalog_info(self) -> Dict[str, Any]:
        """Return a catalog info dictionary."""
        raise NotImplementedError


class IcebergGlueCatalog(Catalog):
    """Iceberg glue catalog."""

    table_format = 'ICEBERG'
    catalog_type = 'GLUE'

    region: str
    catalog_id: str

    def __init__(self, region: str, catalog_id: str):
        self.region = region
        self.catalog_id = catalog_id
        self._manager: Optional[WorkspaceManager] = None

    @classmethod
    def from_config_and_creds(
        cls,
        config: Dict[str, Any],
        credentials: Dict[str, Any],
        manager: 'WorkspaceManager',
    ) -> 'IcebergGlueCatalog':
        params = {}
        params.update(config)
        params.update(credentials)

        out = cls(
            region=params['region'],
            catalog_id=params['id'],
        )
        out._manager = manager
        return out

    def to_catalog_info(self) -> Dict[str, Any]:
        """Return a catalog info dictionary."""
        return dict(
            catalogSource=self.catalog_type,
            tableFormat=self.table_format,
            glueRegion=self.region,
            glueCatalogID=self.catalog_id,
        )


class ExportService(object):
    """Export service."""

    database: str
    table: str
    catalog: Catalog
    storage_link: Link
    columns: Optional[List[str]]

    def __init__(
        self,
        workspace_group: WorkspaceGroup,
        database: str,
        table: str,
        catalog: Catalog,
        storage_link: Link,
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
        self.catalog = catalog

        #: Storage
        self.storage_link = storage_link

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

        if not isinstance(self.catalog, IcebergGlueCatalog):
            raise TypeError('Only Iceberg Glue catalog is supported at this time.')

        if not isinstance(self.storage_link, S3Link):
            raise TypeError('Only S3 links are supported at this time.')

        out = self._manager._post(
            f'workspaceGroups/{self.workspace_group.id}/'
            'egress/createEgressClusterIdentity',
            json=dict(
                storageBucketName=re.split(
                    r'/+', self.storage_link.storage_base_url,
                )[1],
                glueRegion=self.catalog.region,
                glueCatalogID=self.catalog.catalog_id,
            ),
        )

        return out.json()

    def start(self, tags: Optional[List[str]] = None) -> 'ExportStatus':
        """Start the export process."""
        if self._manager is None:
            raise ManagementError(
                msg='No workspace manager is associated with this object.',
            )

        if not isinstance(self.storage_link, S3Link):
            raise TypeError('Only S3 links are supported at this time.')

        out = self._manager._post(
            f'workspaceGroups/{self.workspace_group.id}/egress/startTableEgress',
            json=dict(
                databaseName=self.database,
                tableName=self.table,
                storageLocation=self.storage_link.to_storage_location(),
                catalogInfo=self.catalog.to_catalog_info(),
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

        out = self._manager._post(
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
