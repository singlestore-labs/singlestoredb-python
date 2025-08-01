#!/usr/bin/env python
"""SingleStoreDB Storage Disaster Recovery Management."""
import datetime
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

from ..exceptions import ManagementError
from .manager import Manager
from .utils import to_datetime
from .utils import vars_to_str


class ReplicatedDatabase(object):
    """
    Replicated database configuration for Storage DR.
    """

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
    """
    Storage disaster recovery status information.
    """

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
    """
    Available region for Storage DR setup.
    """

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


class StorageDRManager(Manager):
    """
    SingleStoreDB Storage Disaster Recovery manager.

    This class should be instantiated using :func:`singlestoredb.manage_storage_dr`
    or accessed via :attr:`WorkspaceManager.storage_dr`.

    Parameters
    ----------
    access_token : str, optional
        The API key or other access token for the management API
    version : str, optional
        Version of the API to use
    base_url : str, optional
        Base URL of the management API
    """

    #: Object type
    obj_type = 'storage_dr'

    def get_storage_dr_status(self, workspace_group_id: str) -> StorageDRStatus:
        """
        Get Storage DR status for a workspace group.

        Parameters
        ----------
        workspace_group_id : str
            ID of the workspace group

        Returns
        -------
        :class:`StorageDRStatus`
            Storage DR status information

        Examples
        --------
        >>> dr_mgr = singlestoredb.manage_storage_dr()
        >>> status = dr_mgr.get_storage_dr_status("wg-123")
        >>> print(f"DR enabled: {status.dr_enabled}")
        >>> print(f"Primary region: {status.primary_region}")
        >>> print(f"Backup region: {status.backup_region}")
        """
        path = f'workspaceGroups/{workspace_group_id}/storage/DR/status'
        res = self._get(path)
        return StorageDRStatus.from_dict(res.json())

    def get_available_dr_regions(self, workspace_group_id: str) -> List[StorageDRRegion]:
        """
        Get available regions for Storage DR setup.

        Parameters
        ----------
        workspace_group_id : str
            ID of the workspace group

        Returns
        -------
        List[StorageDRRegion]
            List of available DR regions

        Examples
        --------
        >>> dr_mgr = singlestoredb.manage_storage_dr()
        >>> regions = dr_mgr.get_available_dr_regions("wg-123")
        >>> for region in regions:
        ...     print(f"{region.region_name} ({region.provider})")
        """
        path = f'workspaceGroups/{workspace_group_id}/storage/DR/regions'
        res = self._get(path)
        return [StorageDRRegion.from_dict(item) for item in res.json()]

    def setup_storage_dr(
        self,
        workspace_group_id: str,
        backup_region: str,
        replicated_databases: List[Union[str, ReplicatedDatabase]],
    ) -> StorageDRStatus:
        """
        Set up Storage DR for a workspace group.

        Parameters
        ----------
        workspace_group_id : str
            ID of the workspace group
        backup_region : str
            ID of the backup region
        replicated_databases : List[str or ReplicatedDatabase]
            List of database names or ReplicatedDatabase objects to replicate

        Returns
        -------
        :class:`StorageDRStatus`
            Updated Storage DR status

        Examples
        --------
        >>> dr_mgr = singlestoredb.manage_storage_dr()
        >>> status = dr_mgr.setup_storage_dr(
        ...     workspace_group_id="wg-123",
        ...     backup_region="us-west-2",
        ...     replicated_databases=["production_db", "analytics_db"]
        ... )
        >>> print(f"DR setup status: {status.status}")
        """
        # Convert string database names to ReplicatedDatabase objects
        db_configs = []
        for db in replicated_databases:
            if isinstance(db, str):
                db_configs.append(ReplicatedDatabase(db).to_dict())
            else:
                db_configs.append(db.to_dict())

        data = {
            'backupRegion': backup_region,
            'replicatedDatabases': db_configs,
        }

        path = f'workspaceGroups/{workspace_group_id}/storage/DR/setup'
        self._post(path, json=data)

        # Return updated status
        return self.get_storage_dr_status(workspace_group_id)

    def start_failover(self, workspace_group_id: str) -> StorageDRStatus:
        """
        Start failover to the secondary region.

        Parameters
        ----------
        workspace_group_id : str
            ID of the workspace group

        Returns
        -------
        :class:`StorageDRStatus`
            Updated Storage DR status

        Examples
        --------
        >>> dr_mgr = singlestoredb.manage_storage_dr()
        >>> status = dr_mgr.start_failover("wg-123")
        >>> print(f"Failover status: {status.failover_status}")
        """
        path = f'workspaceGroups/{workspace_group_id}/storage/DR/failover'
        self._patch(path)
        return self.get_storage_dr_status(workspace_group_id)

    def start_failback(self, workspace_group_id: str) -> StorageDRStatus:
        """
        Start failback to the primary region.

        Parameters
        ----------
        workspace_group_id : str
            ID of the workspace group

        Returns
        -------
        :class:`StorageDRStatus`
            Updated Storage DR status

        Examples
        --------
        >>> dr_mgr = singlestoredb.manage_storage_dr()
        >>> status = dr_mgr.start_failback("wg-123")
        >>> print(f"Failback status: {status.status}")
        """
        path = f'workspaceGroups/{workspace_group_id}/storage/DR/failback'
        self._patch(path)
        return self.get_storage_dr_status(workspace_group_id)

    def start_pre_provision(self, workspace_group_id: str) -> StorageDRStatus:
        """
        Start pre-provisioning from primary region.

        Parameters
        ----------
        workspace_group_id : str
            ID of the workspace group

        Returns
        -------
        :class:`StorageDRStatus`
            Updated Storage DR status

        Examples
        --------
        >>> dr_mgr = singlestoredb.manage_storage_dr()
        >>> status = dr_mgr.start_pre_provision("wg-123")
        >>> print(f"Pre-provision status: {status.pre_provision_status}")
        """
        path = f'workspaceGroups/{workspace_group_id}/storage/DR/startPreProvision'
        self._patch(path)
        return self.get_storage_dr_status(workspace_group_id)

    def stop_pre_provision(self, workspace_group_id: str) -> StorageDRStatus:
        """
        Stop pre-provisioning from primary region.

        Parameters
        ----------
        workspace_group_id : str
            ID of the workspace group

        Returns
        -------
        :class:`StorageDRStatus`
            Updated Storage DR status

        Examples
        --------
        >>> dr_mgr = singlestoredb.manage_storage_dr()
        >>> status = dr_mgr.stop_pre_provision("wg-123")
        >>> print(f"Pre-provision status: {status.pre_provision_status}")
        """
        path = f'workspaceGroups/{workspace_group_id}/storage/DR/stopPreProvision'
        self._patch(path)
        return self.get_storage_dr_status(workspace_group_id)

    def update_retention_period(
        self,
        workspace_group_id: str,
        retention_days: int,
    ) -> None:
        """
        Update the retention period for continuous backups.

        Parameters
        ----------
        workspace_group_id : str
            ID of the workspace group
        retention_days : int
            Number of days to retain backups

        Examples
        --------
        >>> dr_mgr = singlestoredb.manage_storage_dr()
        >>> dr_mgr.update_retention_period("wg-123", retention_days=30)
        """
        data = {
            'retentionDays': retention_days,
        }

        path = f'workspaceGroups/{workspace_group_id}/storage/retentionPeriod'
        self._patch(path, json=data)

    def wait_for_dr_operation(
        self,
        workspace_group_id: str,
        operation_type: str,
        target_status: str,
        interval: int = 30,
        timeout: int = 3600,
    ) -> StorageDRStatus:
        """
        Wait for a DR operation to complete.

        Parameters
        ----------
        workspace_group_id : str
            ID of the workspace group
        operation_type : str
            Type of operation ('failover', 'failback', 'pre_provision')
        target_status : str
            Target status to wait for
        interval : int, optional
            Polling interval in seconds
        timeout : int, optional
            Maximum time to wait in seconds

        Returns
        -------
        :class:`StorageDRStatus`
            Final Storage DR status

        Raises
        ------
        ManagementError
            If timeout is reached

        Examples
        --------
        >>> dr_mgr = singlestoredb.manage_storage_dr()
        >>> dr_mgr.start_failover("wg-123")
        >>> final_status = dr_mgr.wait_for_dr_operation(
        ...     "wg-123", "failover", "completed"
        ... )
        """
        import time

        elapsed = 0
        while elapsed < timeout:
            status = self.get_storage_dr_status(workspace_group_id)

            if operation_type == 'failover' and status.failover_status == target_status:
                return status
            elif operation_type == 'failback' and status.status == target_status:
                return status
            elif (
                operation_type == 'pre_provision' and
                status.pre_provision_status == target_status
            ):
                return status

            time.sleep(interval)
            elapsed += interval

        raise ManagementError(
            msg=(
                f'Timeout waiting for {operation_type} operation to '
                f'reach {target_status}'
            ),
        )


def manage_storage_dr(
    access_token: Optional[str] = None,
    version: Optional[str] = None,
    base_url: Optional[str] = None,
    *,
    organization_id: Optional[str] = None,
) -> StorageDRManager:
    """
    Retrieve a SingleStoreDB Storage DR manager.

    Parameters
    ----------
    access_token : str, optional
        The API key or other access token for the management API
    version : str, optional
        Version of the API to use
    base_url : str, optional
        Base URL of the management API
    organization_id : str, optional
        ID of organization, if using a JWT for authentication

    Returns
    -------
    :class:`StorageDRManager`

    Examples
    --------
    >>> import singlestoredb as s2
    >>> dr_mgr = s2.manage_storage_dr()
    >>> status = dr_mgr.get_storage_dr_status("wg-123")
    >>> print(f"DR enabled: {status.dr_enabled}")
    """
    return StorageDRManager(
        access_token=access_token,
        base_url=base_url,
        version=version,
        organization_id=organization_id,
    )
