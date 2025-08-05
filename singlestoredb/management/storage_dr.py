#!/usr/bin/env python
"""SingleStoreDB Storage Disaster Recovery Management."""
import datetime
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

from .utils import to_datetime
from .utils import vars_to_str


class ReplicatedDatabase(object):
    """Replicated database configuration for Storage DR."""

    def __init__(
        self,
        database_name: str,
        replication_enabled: bool = True,
    ):
        #: Name of the database to replicate
        self.database_name = database_name

        #: Whether replication is enabled for this database
        self.replication_enabled = replication_enabled

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)

    @classmethod
    def from_dict(cls, obj: Dict[str, Any]) -> 'ReplicatedDatabase':
        """
        Construct a ReplicatedDatabase from a dictionary of values.

        Parameters
        ----------
        obj : dict
            Dictionary of values

        Returns
        -------
        :class:`ReplicatedDatabase`

        """
        return cls(
            database_name=obj['databaseName'],
            replication_enabled=obj.get('replicationEnabled', True),
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format for API calls."""
        return {
            'databaseName': self.database_name,
            'replicationEnabled': self.replication_enabled,
        }


class StorageDRStatus(object):
    """Storage disaster recovery status information."""

    def __init__(
        self,
        workspace_group_id: str,
        dr_enabled: bool,
        primary_region: Optional[str] = None,
        backup_region: Optional[str] = None,
        status: Optional[str] = None,
        last_backup_time: Optional[Union[str, datetime.datetime]] = None,
        replicated_databases: Optional[List[ReplicatedDatabase]] = None,
        failover_status: Optional[str] = None,
        pre_provision_status: Optional[str] = None,
    ):
        #: Workspace group ID
        self.workspace_group_id = workspace_group_id

        #: Whether DR is enabled
        self.dr_enabled = dr_enabled

        #: Primary region
        self.primary_region = primary_region

        #: Backup region
        self.backup_region = backup_region

        #: Overall DR status
        self.status = status

        #: Last backup timestamp
        self.last_backup_time = to_datetime(last_backup_time)

        #: List of databases being replicated
        self.replicated_databases = replicated_databases or []

        #: Failover status
        self.failover_status = failover_status

        #: Pre-provisioning status
        self.pre_provision_status = pre_provision_status

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)

    @classmethod
    def from_dict(cls, obj: Dict[str, Any]) -> 'StorageDRStatus':
        """
        Construct a StorageDRStatus from a dictionary of values.

        Parameters
        ----------
        obj : dict
            Dictionary of values

        Returns
        -------
        :class:`StorageDRStatus`

        """
        replicated_dbs = []
        if 'replicatedDatabases' in obj:
            replicated_dbs = [
                ReplicatedDatabase.from_dict(db)
                for db in obj['replicatedDatabases']
            ]

        return cls(
            workspace_group_id=obj['workspaceGroupID'],
            dr_enabled=obj.get('drEnabled', False),
            primary_region=obj.get('primaryRegion'),
            backup_region=obj.get('backupRegion'),
            status=obj.get('status'),
            last_backup_time=obj.get('lastBackupTime'),
            replicated_databases=replicated_dbs,
            failover_status=obj.get('failoverStatus'),
            pre_provision_status=obj.get('preProvisionStatus'),
        )


class StorageDRRegion(object):
    """Available region for Storage DR setup."""

    def __init__(
        self,
        region_id: str,
        region_name: str,
        provider: str,
        available: bool = True,
    ):
        #: Region ID
        self.region_id = region_id

        #: Region name
        self.region_name = region_name

        #: Cloud provider
        self.provider = provider

        #: Whether this region is available for DR
        self.available = available

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)

    @classmethod
    def from_dict(cls, obj: Dict[str, Any]) -> 'StorageDRRegion':
        """
        Construct a StorageDRRegion from a dictionary of values.

        Parameters
        ----------
        obj : dict
            Dictionary of values

        Returns
        -------
        :class:`StorageDRRegion`

        """
        return cls(
            region_id=obj['regionID'],
            region_name=obj['regionName'],
            provider=obj['provider'],
            available=obj.get('available', True),
        )
