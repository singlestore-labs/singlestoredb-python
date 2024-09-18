#!/usr/bin/env python
"""SingleStoreDB Egress service."""
from __future__ import annotations

import abc
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

from .. import ManagementError
from .region import Region
from .utils import vars_to_str
from .workspace import WorkspaceManager


class StorageProfile(object):
    """Generic storage profile base class."""
    scheme: str = 'unknown'

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
    ) -> 'StorageProfile':
        """Convert response to profile."""
        scheme = obj['storageBaseURL'].split(':', 1)[0].lower()

        out_cls = None
        for c in cls.__subclasses__():
            if c.scheme == scheme:
                out_cls = c
                break

        if out_cls is None:
            raise TypeError(f'No storage class found for given information: {obj}')

        return out_cls.from_dict(obj, manager)

    @abc.abstractmethod
    def to_storage_location(self) -> Dict[str, Any]:
        """Return a storage location dictionary."""
        raise NotImplementedError


class S3StorageProfile(StorageProfile):
    """S3 storage profile."""

    scheme: str = 's3'
    base_url: str
    region: Region

    def __init__(self, base_url: str, region: Region):
        self.base_url = base_url
        self.region = region
        self._manager: Optional[WorkspaceManager] = None

    @classmethod
    def from_dict(
        cls,
        obj: Dict[str, Any],
        manager: 'WorkspaceManager',
    ) -> 'S3StorageProfile':
        """Convert response to profile."""
        out = cls(
            base_url=obj['storageBaseURL'],
            region=obj['storageRegion'],
        )
        out._manager = manager
        return out

    def to_storage_location(self) -> Dict[str, Any]:
        """Return a storage location dictionary."""
        return dict(
            storageBaseURL=self.base_url,
            storageRegion=self.region.name,
        )


class CatalogProfile(object):
    """Generic catalog profile base class."""

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
    ) -> 'CatalogProfile':
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


class IcebergGlueCatalogProfile(CatalogProfile):
    """Iceberg glue catalog profile."""

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
    ) -> 'IcebergGlueCatalogProfile':
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
    catalog: CatalogProfile
    storage: StorageProfile
    columns: Optional[List[str]]

    def __init__(
        self,
        database: str,
        table: str,
        catalog: CatalogProfile,
        storage: StorageProfile,
        columns: Optional[List[str]],
    ):
        #: Name of SingleStoreDB database
        self.database = database

        #: Name of SingleStoreDB table
        self.table = table

        #: List of columns to egress
        self.columns = columns

        #: Catalog definition
        self.catalog = catalog

        #: Storage definition
        self.storage = storage

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
        out = cls(
            database=obj['databaseName'],
            table=obj['tableName'],
            storage=StorageProfile.from_dict(obj['storageLocation'], manager),
            catalog=CatalogProfile.from_dict(obj['catalogInfo'], manager),
            columns=obj.get('columnNames'),
        )
        out._manager = manager
        return out

    def start(self) -> None:
        """Start the egress process."""
        if self._manager is None:
            raise ManagementError(
                msg='No workspace manager is associated with this object.',
            )

        self._manager._post(
            'workspaceGroups/{self.id}/egress/startTableEgress',
            json=dict(
                databaseName=self.database,
                tableName=self.table,
                storageLocation=self.storage.to_storage_location(),
                catalogInfo=self.catalog.to_catalog_info(),
            ),
        )

    def stop(self) -> None:
        """Stop a running egress process."""
        raise NotImplementedError
