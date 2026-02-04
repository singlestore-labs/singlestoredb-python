#!/usr/bin/env python
"""SingleStoreDB Storage and Disaster Recovery Management."""
from __future__ import annotations

from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import TYPE_CHECKING

from .utils import normalize_cloud_provider
from .utils import vars_to_str

if TYPE_CHECKING:
    from .manager import Manager


class DRRegion:
    """
    Available region for disaster recovery.

    """

    region_id: str
    region: str
    provider: str

    def __init__(
        self,
        region_id: str,
        region: str,
        provider: str,
    ):
        #: Unique ID of the region
        self.region_id = region_id

        #: Region name/code
        self.region = region

        #: Cloud provider (e.g., 'AWS', 'GCP', 'AZURE')
        self.provider = normalize_cloud_provider(provider) or provider

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)

    @classmethod
    def from_dict(cls, obj: Dict[str, Any]) -> 'DRRegion':
        """Construct from a dictionary."""
        return cls(
            region_id=obj.get('regionID', ''),
            region=obj.get('region', ''),
            provider=obj.get('provider', ''),
        )


class DRStatus:
    """
    Disaster recovery status information.

    """

    state: str
    primary_region_id: Optional[str]
    secondary_region_id: Optional[str]
    databases: List[str]

    def __init__(
        self,
        state: str,
        primary_region_id: Optional[str] = None,
        secondary_region_id: Optional[str] = None,
        databases: Optional[List[str]] = None,
    ):
        #: Current DR state (e.g., 'ENABLED', 'DISABLED', 'FAILOVER_IN_PROGRESS')
        self.state = state

        #: ID of the primary region
        self.primary_region_id = primary_region_id

        #: ID of the secondary (DR) region
        self.secondary_region_id = secondary_region_id

        #: List of databases included in DR
        self.databases = databases or []

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)

    @classmethod
    def from_dict(cls, obj: Dict[str, Any]) -> 'DRStatus':
        """Construct from a dictionary."""
        return cls(
            state=obj.get('state', ''),
            primary_region_id=obj.get('primaryRegionID'),
            secondary_region_id=obj.get('secondaryRegionID'),
            databases=obj.get('databases', []),
        )


class DisasterRecovery:
    """
    Disaster recovery operations for a workspace group.

    This object is not instantiated directly. It is accessed via
    :attr:`Storage.dr`.

    """

    def __init__(self, workspace_group_id: str, manager: 'Manager'):
        self._workspace_group_id = workspace_group_id
        self._manager = manager

    def __str__(self) -> str:
        """Return string representation."""
        return f'DisasterRecovery(workspace_group_id={self._workspace_group_id!r})'

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)

    def get_status(self) -> DRStatus:
        """
        Get the current disaster recovery status.

        Returns
        -------
        :class:`DRStatus`

        """
        res = self._manager._get(
            f'workspaceGroups/{self._workspace_group_id}/storage/DR/status',
        )
        return DRStatus.from_dict(res.json())

    def get_regions(self) -> List[DRRegion]:
        """
        Get available regions for disaster recovery.

        Returns
        -------
        List[:class:`DRRegion`]

        """
        res = self._manager._get(
            f'workspaceGroups/{self._workspace_group_id}/storage/DR/regions',
        )
        return [
            DRRegion.from_dict(item)
            for item in res.json().get('regions', [])
        ]

    def setup(
        self,
        region_id: str,
        databases: Optional[List[str]] = None,
    ) -> None:
        """
        Set up disaster recovery.

        Parameters
        ----------
        region_id : str
            ID of the secondary region for DR
        databases : List[str], optional
            List of database names to include in DR

        """
        data: Dict[str, Any] = {'regionID': region_id}
        if databases is not None:
            data['databases'] = databases

        self._manager._post(
            f'workspaceGroups/{self._workspace_group_id}/storage/DR/setup',
            json=data,
        )

    def failover(self) -> None:
        """
        Trigger a failover to the secondary region.

        """
        self._manager._patch(
            f'workspaceGroups/{self._workspace_group_id}/storage/DR/failover',
        )

    def failback(self) -> None:
        """
        Trigger a failback to the primary region.

        """
        self._manager._patch(
            f'workspaceGroups/{self._workspace_group_id}/storage/DR/failback',
        )

    def start_pre_provision(self) -> None:
        """
        Start pre-provisioning for faster failover.

        """
        self._manager._patch(
            f'workspaceGroups/{self._workspace_group_id}/storage/DR/startPreProvision',
        )

    def stop_pre_provision(self) -> None:
        """
        Stop pre-provisioning.

        """
        self._manager._patch(
            f'workspaceGroups/{self._workspace_group_id}/storage/DR/stopPreProvision',
        )


class Storage:
    """
    Storage operations for a workspace group.

    This object is not instantiated directly. It is accessed via
    :attr:`WorkspaceGroup.storage`.

    """

    def __init__(self, workspace_group_id: str, manager: 'Manager'):
        self._workspace_group_id = workspace_group_id
        self._manager = manager
        self._dr: Optional[DisasterRecovery] = None

    def __str__(self) -> str:
        """Return string representation."""
        return f'Storage(workspace_group_id={self._workspace_group_id!r})'

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)

    @property
    def dr(self) -> DisasterRecovery:
        """
        Access disaster recovery operations.

        Returns
        -------
        :class:`DisasterRecovery`

        """
        if self._dr is None:
            self._dr = DisasterRecovery(self._workspace_group_id, self._manager)
        return self._dr

    def update_retention_period(self, days: int) -> None:
        """
        Update the storage retention period.

        Parameters
        ----------
        days : int
            Number of days to retain storage data

        """
        self._manager._patch(
            f'workspaceGroups/{self._workspace_group_id}/storage/retentionPeriod',
            json={'retentionPeriod': days},
        )
