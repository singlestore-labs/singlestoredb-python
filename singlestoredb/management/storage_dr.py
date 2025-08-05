#!/usr/bin/env python
"""SingleStoreDB Storage Disaster Recovery Management."""
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

from .manager import Manager
from .utils import vars_to_str


class ReplicatedDatabase(object):
    """
    Replicated database configuration for Storage DR.

    Represents information related to a database's replication status.
    """

    def __init__(
        self,
        database_name: str,
        region: str,
        duplication_state: str,
    ):
        #: Name of the database
        self.database_name = database_name

        #: Name of the region
        self.region = region

        #: Duplication state of the database (Pending, Active, Inactive, Error)
        self.duplication_state = duplication_state

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            'databaseName': self.database_name,
            'region': self.region,
            'duplicationState': self.duplication_state,
        }

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
            region=obj['region'],
            duplication_state=obj['duplicationState'],
        )


class StorageDRCompute(object):
    """
    Storage DR compute operation information.

    Represents information related to a workspace group's latest storage DR operation.
    """

    def __init__(
        self,
        storage_dr_type: str,
        storage_dr_state: str,
        total_workspaces: int,
        total_attachments: int,
        completed_workspaces: int,
        completed_attachments: int,
    ):
        #: Name of Storage DR operation (Failover, Failback,
        #: PreProvisionStart, PreProvisionStop)
        self.storage_dr_type = storage_dr_type

        #: Status of Storage DR operation (Active, Completed, Failed, Expired, Canceled)
        self.storage_dr_state = storage_dr_state

        #: The total number of workspaces to setup
        self.total_workspaces = total_workspaces

        #: The total number of database attachments to setup
        self.total_attachments = total_attachments

        #: The number of workspaces that have been setup
        self.completed_workspaces = completed_workspaces

        #: The number of database attachments that have been setup
        self.completed_attachments = completed_attachments

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)

    @classmethod
    def from_dict(cls, obj: Dict[str, Any]) -> 'StorageDRCompute':
        """
        Construct a StorageDRCompute from a dictionary of values.

        Parameters
        ----------
        obj : dict
            Dictionary of values

        Returns
        -------
        :class:`StorageDRCompute`

        """
        return cls(
            storage_dr_type=obj['storageDRType'],
            storage_dr_state=obj['storageDRState'],
            total_workspaces=obj['totalWorkspaces'],
            total_attachments=obj['totalAttachments'],
            completed_workspaces=obj['completedWorkspaces'],
            completed_attachments=obj['completedAttachments'],
        )


class StorageDRStatus(object):
    """
    Storage disaster recovery status information.

    Represents Storage DR status information for a workspace group.
    """

    def __init__(
        self,
        compute: StorageDRCompute,
        storage: List[ReplicatedDatabase],
    ):
        #: Compute operation information
        self.compute = compute

        #: List of replicated databases
        self.storage = storage

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
        compute = StorageDRCompute.from_dict(obj['compute'])
        storage = [ReplicatedDatabase.from_dict(db) for db in obj['storage']]

        return cls(
            compute=compute,
            storage=storage,
        )


class StorageDRRegion(object):
    """Available region for Storage DR setup."""

    def __init__(
        self,
        region_id: str,
        region_name: str,
        provider: str,
    ):
        #: Region ID
        self.region_id = region_id

        #: Region name
        self.region_name = region_name

        #: Cloud provider
        self.provider = provider

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
        )


class StorageDRManager(Manager):
    """
    SingleStoreDB Storage DR manager.

    This class should be instantiated using
    :func:`singlestoredb.manage_storage_dr` or accessed via
    :attr:`WorkspaceGroupManager.storage_dr`.

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

    def get_status(self, workspace_group_id: str) -> StorageDRStatus:
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
        >>> status = dr_mgr.get_status("wg-123")
        >>> print(f"DR State: {status.compute.storage_dr_state}")

        """
        res = self._get(f'workspaceGroups/{workspace_group_id}/storage/DR/status')
        return StorageDRStatus.from_dict(res.json())

    def get_available_regions(self, workspace_group_id: str) -> List[StorageDRRegion]:
        """
        Get available regions for Storage DR setup.

        Parameters
        ----------
        workspace_group_id : str
            ID of the workspace group

        Returns
        -------
        List[StorageDRRegion]
            List of available regions for DR

        Examples
        --------
        >>> dr_mgr = singlestoredb.manage_storage_dr()
        >>> regions = dr_mgr.get_available_regions("wg-123")
        >>> for region in regions:
        ...     print(f"{region.provider}: {region.region_name}")

        """
        res = self._get(f'workspaceGroups/{workspace_group_id}/storage/DR/regions')
        return [StorageDRRegion.from_dict(region) for region in res.json()]

    def setup_storage_dr(
        self,
        workspace_group_id: str,
        region_id: str,
        database_names: List[str],
        auto_replication: Optional[bool] = None,
        backup_bucket_kms_key_id: Optional[str] = None,
        data_bucket_kms_key_id: Optional[str] = None,
    ) -> None:
        """
        Setup Storage DR for a workspace group.

        Parameters
        ----------
        workspace_group_id : str
            ID of the workspace group
        region_id : str
            Region ID of the secondary region
        database_names : List[str]
            List of database names (can be empty if setting up auto-replication)
        auto_replication : bool, optional
            If true, all existing and future databases will be automatically replicated
        backup_bucket_kms_key_id : str, optional
            KMS key ID for backup bucket encryption (AWS only)
        data_bucket_kms_key_id : str, optional
            KMS key ID for data bucket encryption (AWS only)

        Examples
        --------
        >>> dr_mgr = singlestoredb.manage_storage_dr()
        >>> dr_mgr.setup_storage_dr(
        ...     "wg-123",
        ...     "region-456",
        ...     ["db1", "db2"],
        ...     auto_replication=True
        ... )

        """
        data: Dict[str, Any] = {
            'regionID': region_id,
            'databaseNames': database_names,
        }

        if auto_replication is not None:
            data['autoReplication'] = auto_replication
        if backup_bucket_kms_key_id is not None:
            data['backupBucketKMSKeyID'] = backup_bucket_kms_key_id
        if data_bucket_kms_key_id is not None:
            data['dataBucketKMSKeyID'] = data_bucket_kms_key_id

        self._post(f'workspaceGroups/{workspace_group_id}/storage/DR/setup', json=data)

    def start_failover(self, workspace_group_id: str) -> None:
        """
        Start failover operation for Storage DR.

        Parameters
        ----------
        workspace_group_id : str
            ID of the workspace group

        Examples
        --------
        >>> dr_mgr = singlestoredb.manage_storage_dr()
        >>> dr_mgr.start_failover("wg-123")

        """
        self._post(f'workspaceGroups/{workspace_group_id}/storage/DR/failover')

    def start_failback(self, workspace_group_id: str) -> None:
        """
        Start failback operation for Storage DR.

        Parameters
        ----------
        workspace_group_id : str
            ID of the workspace group

        Examples
        --------
        >>> dr_mgr = singlestoredb.manage_storage_dr()
        >>> dr_mgr.start_failback("wg-123")

        """
        self._post(f'workspaceGroups/{workspace_group_id}/storage/DR/failback')

    def start_pre_provision(self, workspace_group_id: str) -> None:
        """
        Start pre-provisioning for Storage DR.

        Parameters
        ----------
        workspace_group_id : str
            ID of the workspace group

        Examples
        --------
        >>> dr_mgr = singlestoredb.manage_storage_dr()
        >>> dr_mgr.start_pre_provision("wg-123")

        """
        self._post(f'workspaceGroups/{workspace_group_id}/storage/DR/startPreProvision')

    def stop_pre_provision(self, workspace_group_id: str) -> None:
        """
        Stop pre-provisioning for Storage DR.

        Parameters
        ----------
        workspace_group_id : str
            ID of the workspace group

        Examples
        --------
        >>> dr_mgr = singlestoredb.manage_storage_dr()
        >>> dr_mgr.stop_pre_provision("wg-123")

        """
        self._post(f'workspaceGroups/{workspace_group_id}/storage/DR/stopPreProvision')


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
    >>> status = dr_mgr.get_status("wg-123")
    >>> print(f"DR State: {status.compute.storage_dr_state}")

    """
    return StorageDRManager(
        access_token=access_token,
        base_url=base_url,
        version=version,
        organization_id=organization_id,
    )
