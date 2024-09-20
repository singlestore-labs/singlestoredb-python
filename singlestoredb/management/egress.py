#!/usr/bin/env python
"""SingleStoreDB Egress service."""
from __future__ import annotations

import abc
import re
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

from .. import ManagementError
from .region import Region
from .utils import vars_to_str
from .workspace import get_workspace_group
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
    def to_storage_location(self, base_url: str) -> Dict[str, Any]:
        raise NotImplementedError


class S3Link(Link):
    """S3 link."""

    scheme: str = 's3'
    region: Region

    def __init__(self, region: Region):
        self.region = region
        self._manager: Optional[WorkspaceManager] = None

    def to_storage_location(self, base_url: str) -> Dict[str, Any]:
        return dict(
            storageBaseURL=base_url,
            storageRegion=self.region,
        )


class Catalog(object):
    """Generic catalog base class."""

    catalog_type: str = 'unknown'
    table_format: str = 'unknown'

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)

    @classmethod
    def from_dict(
        cls,
        obj: Dict[str, Any],
        manager: 'WorkspaceManager',
    ) -> 'Catalog':
        """Convert response to profile."""
        catalog_type = obj['catalogSource'].lower()
        table_format = obj['tableFormat'].lower()

        out_cls = None
        for c in cls.__subclasses__():
            if c.catalog_type == catalog_type and c.table_format == table_format:
                out_cls = c
                break

        if out_cls is None:
            raise TypeError(f'No catalog class found for given information: {obj}')

        return out_cls.from_dict(obj, manager)

    @abc.abstractmethod
    def to_catalog_info(self) -> Dict[str, Any]:
        """Return a catalog info dictionary."""
        raise NotImplementedError


class IcebergGlueCatalog(Catalog):
    """Iceberg glue catalog."""

    table_format = 'iceberg'
    catalog_type = 'glue'

    def __init__(self, region: Region, catalog_id: str):
        self.region = region
        self.catalog_id = catalog_id
        self._manager: Optional[WorkspaceManager] = None

    @classmethod
    def from_dict(
        cls,
        obj: Dict[str, Any],
        manager: 'WorkspaceManager',
    ) -> 'IcebergGlueCatalog':
        """Convert response to profile."""
        out = cls(
            region=obj['glueRegion'],
            catalog_id=obj['glueCatalogId'],
        )
        out._manager = manager
        return out

    def to_catalog_info(self) -> Dict[str, Any]:
        """Return a catalog info dictionary."""
        return dict(
            catalogSource=self.catalog_type,
            tableFormat=self.table_format,
            glueRegion=self.region.name,
            glueCatalogID=self.catalog_id,
        )


class EgressService(object):
    """Egress service."""

    database: str
    table: str
    catalog: Catalog
    storage_link: Link
    storage_base_url: str
    columns: Optional[List[str]]

    def __init__(
        self,
        workspace_group: WorkspaceGroup,
        database: str,
        table: str,
        catalog: Catalog,
        storage_link: Link,
        storage_base_url: str,
        columns: Optional[List[str]],
    ):
        #: Workspace group
        self.workspace_group = workspace_group

        #: Name of SingleStoreDB database
        self.database = database

        #: Name of SingleStoreDB table
        self.table = table

        #: List of columns to egress
        self.columns = columns

        #: Catalog
        self.catalog = catalog

        #: Storage
        self.storage_link = storage_link
        self.storage_base_url = storage_base_url

        self._manager: Optional[WorkspaceManager] = None

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)

    @classmethod
    def from_dict(
        cls,
        obj: Dict[str, Any],
        manager: 'WorkspaceManager',
    ) -> 'EgressService':
        """
        Construct an EgressService from a dictionary of values.

        Parameters
        ----------
        obj : dict
            Dictionary of values
        manager : WorkspaceManager, optional
            The WorkspaceManager the Workspace belongs to

        Returns
        -------
        :class:`EgressService`

        """
        storage = obj['storageLocation']
        storage_link = S3Link(storage['region'])
        storage_base_url = storage['storageBaseURL']

        out = cls(
            get_workspace_group(),
            database=obj['databaseName'],
            table=obj['tableName'],
            storage_link=storage_link,
            storage_base_url=storage_base_url,
            catalog=Catalog.from_dict(obj['catalogInfo'], manager),
            columns=obj.get('columnNames'),
        )
        out._manager = manager
        return out

    def create_cluster_identity(self) -> Dict[str, Any]:
        """Create a cluster identity."""
        if self._manager is None:
            raise ManagementError(
                msg='No workspace manager is associated with this object.',
            )

        if not isinstance(self.catalog, IcebergGlueCatalog):
            raise TypeError('Only Iceberg Glue catalog is supported at this time.')

        out = self._manager._post(
            'workspaceGroups/{self.workspace_group.id}/egress/createClusterIdentity',
            json=dict(
                storageBucketName=re.split(r'/+', self.storage_base_url)[1],
                glueRegion=self.catalog.region,
                glueCatalog=self.catalog.catalog_id,
            ),
        )

        return out.json()

    def start(self, tags: Optional[List[str]] = None) -> Dict[str, Any]:
        """Start the egress process."""
        if self._manager is None:
            raise ManagementError(
                msg='No workspace manager is associated with this object.',
            )

        if not isinstance(self.storage_link, S3Link):
            raise TypeError('Only S3 links are supported at this time.')

        out = self._manager._post(
            'workspaceGroups/{self.workspace_group.id}/egress/startTableEgress',
            json=dict(
                databaseName=self.database,
                tableName=self.table,
                storageLocation=self.storage_link.to_storage_location(
                    self.storage_base_url,
                ),
                catalogInfo=self.catalog.to_catalog_info(),
            ),
        )

        return out.json()

    def stop(self) -> None:
        """Stop a running egress process."""
        raise NotImplementedError
