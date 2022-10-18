#!/usr/bin/env python
"""SingleStoreDB Cluster Management."""
import datetime
import warnings
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

from .. import connection
from ..exceptions import ManagementError
from .manager import Manager
from .region import Region
from .utils import to_datetime
from .utils import vars_to_str


class Cluster(object):
    """
    SingleStoreDB cluster definition.

    This object is not instantiated directly. It is used in the results
    of API calls on the :class:`ClusterManager`. Clusters are created using
    :meth:`ClusterManager.create_cluster`, or existing clusters are accessed by either
    :attr:`ClusterManager.clusters` or by calling :meth:`ClusterManager.get_cluster`.

    See Also
    --------
    :meth:`ClusterManager.create_cluster`
    :meth:`ClusterManager.get_cluster`
    :attr:`ClusterManager.clusters`

    """

    def __init__(
        self, name: str, id: str, region: Region, size: str,
        units: float, state: str, version: str,
        created_at: Union[str, datetime.datetime],
        expires_at: Optional[Union[str, datetime.datetime]] = None,
        firewall_ranges: Optional[List[str]] = None,
        terminated_at: Optional[Union[str, datetime.datetime]] = None,
        endpoint: Optional[str] = None,
    ):
        """Use :attr:`ClusterManager.clusters` or :meth:`ClusterManager.get_cluster`."""
        #: Name of the cluster
        self.name = name.strip()

        #: Unique ID of the cluster
        self.id = id

        #: Region of the cluster (see :class:`Region`)
        self.region = region

        #: Size of the cluster in cluster size notation (S-00, S-1, etc.)
        self.size = size

        #: Size of the cluster in units such as 0.25, 1.0, etc.
        self.units = units

        #: State of the cluster: PendingCreation, Transitioning, Active,
        #: Terminated, Suspended, Resuming, Failed
        self.state = state.strip()

        #: Version of the SingleStoreDB server
        self.version = version.strip()

        #: Timestamp of when the cluster was created
        self.created_at = to_datetime(created_at)

        #: Timestamp of when the cluster expires
        self.expires_at = to_datetime(expires_at)

        #: List of allowed incoming IP addresses / ranges
        self.firewall_ranges = firewall_ranges

        #: Timestamp of when the cluster was terminated
        self.terminated_at = to_datetime(terminated_at)

        #: Hostname (or IP address) of the cluster database server
        self.endpoint = endpoint

        self._manager: Optional[ClusterManager] = None

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)

    @classmethod
    def from_dict(cls, obj: Dict[str, Any], manager: 'ClusterManager') -> 'Cluster':
        """
        Construct a Cluster from a dictionary of values.

        Parameters
        ----------
        obj : dict
            Dictionary of values
        manager : ClusterManager, optional
            The ClusterManager the Cluster belongs to

        Returns
        -------
        :class:`Cluster`

        """
        out = cls(
            name=obj['name'], id=obj['clusterID'],
            region=Region.from_dict(obj['region'], manager),
            size=obj.get('size', 'Unknown'), units=obj.get('units', float('nan')),
            state=obj['state'], version=obj['version'],
            created_at=obj['createdAt'], expires_at=obj.get('expiresAt'),
            firewall_ranges=obj.get('firewallRanges'),
            terminated_at=obj.get('terminatedAt'),
            endpoint=obj.get('endpoint'),
        )
        out._manager = manager
        return out

    def refresh(self) -> 'Cluster':
        """Update the object to the current state."""
        if self._manager is None:
            raise ManagementError(
                msg='No cluster manager is associated with this object.',
            )
        new_obj = self._manager.get_cluster(self.id)
        for name, value in vars(new_obj).items():
            setattr(self, name, value)
        return self

    def update(
        self, name: Optional[str] = None,
        admin_password: Optional[str] = None,
        expires_at: Optional[str] = None,
        size: Optional[str] = None, firewall_ranges: Optional[List[str]] = None,
    ) -> None:
        """
        Update the cluster definition.

        Parameters
        ----------
        name : str, optional
            Cluster name
        admim_password : str, optional
            Admin password for the cluster
        expires_at : str, optional
            Timestamp when the cluster expires
        size : str, optional
            Cluster size in cluster size notation (S-00, S-1, etc.)
        firewall_ranges : Sequence[str], optional
            List of allowed incoming IP addresses

        """
        if self._manager is None:
            raise ManagementError(
                msg='No cluster manager is associated with this object.',
            )
        data = {
            k: v for k, v in dict(
                name=name, adminPassword=admin_password,
                expiresAt=expires_at, size=size,
                firewallRanges=firewall_ranges,
            ).items() if v is not None
        }
        self._manager._patch(f'clusters/{self.id}', json=data)
        self.refresh()

    def suspend(
        self,
        wait_on_suspended: bool = False,
        wait_interval: int = 20,
        wait_timeout: int = 600,
    ) -> None:
        """
        Suspend the cluster.

        Parameters
        ----------
        wait_on_suspended : bool, optional
            Wait for the cluster to go into 'Suspended' mode before returning
        wait_interval : int, optional
            Number of seconds between each server check
        wait_timeout : int, optional
            Total number of seconds to check server before giving up

        Raises
        ------
        ManagementError
            If timeout is reached

        """
        if self._manager is None:
            raise ManagementError(
                msg='No cluster manager is associated with this object.',
            )
        self._manager._post(
            f'clusters/{self.id}/suspend',
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
        )
        if wait_on_suspended:
            self._manager._wait_on_state(
                self._manager.get_cluster(self.id),
                'Suspended', interval=wait_interval, timeout=wait_timeout,
            )
            self.refresh()

    def resume(
        self,
        wait_on_resumed: bool = False,
        wait_interval: int = 20,
        wait_timeout: int = 600,
    ) -> None:
        """
        Resume the cluster.

        Parameters
        ----------
        wait_on_resumed : bool, optional
            Wait for the cluster to go into 'Resumed' or 'Active' mode before returning
        wait_interval : int, optional
            Number of seconds between each server check
        wait_timeout : int, optional
            Total number of seconds to check server before giving up

        Raises
        ------
        ManagementError
            If timeout is reached

        """
        if self._manager is None:
            raise ManagementError(
                msg='No cluster manager is associated with this object.',
            )
        self._manager._post(
            f'clusters/{self.id}/resume',
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
        )
        if wait_on_resumed:
            self._manager._wait_on_state(
                self._manager.get_cluster(self.id),
                ['Resumed', 'Active'], interval=wait_interval, timeout=wait_timeout,
            )
            self.refresh()

    def terminate(
        self,
        wait_on_terminated: bool = False,
        wait_interval: int = 10,
        wait_timeout: int = 600,
    ) -> None:
        """
        Terminate the cluster.

        Parameters
        ----------
        wait_on_terminated : bool, optional
            Wait for the cluster to go into 'Terminated' mode before returning
        wait_interval : int, optional
            Number of seconds between each server check
        wait_timeout : int, optional
            Total number of seconds to check server before giving up

        Raises
        ------
        ManagementError
            If timeout is reached

        """
        if self._manager is None:
            raise ManagementError(
                msg='No cluster manager is associated with this object.',
            )
        self._manager._delete(f'clusters/{self.id}')
        if wait_on_terminated:
            self._manager._wait_on_state(
                self._manager.get_cluster(self.id),
                'Terminated', interval=wait_interval, timeout=wait_timeout,
            )
            self.refresh()

    def connect(self, **kwargs: Any) -> connection.Connection:
        """
        Create a connection to the database server for this cluster.

        Parameters
        ----------
        **kwargs : keyword-arguments, optional
            Parameters to the SingleStoreDB `connect` function except host
            and port which are supplied by the cluster object

        Returns
        -------
        :class:`Connection`

        """
        if not self.endpoint:
            raise ManagementError(
                msg='An endpoint has not been set in '
                'this cluster configuration',
            )
        kwargs['host'] = self.endpoint
        return connection.connect(**kwargs)


class ClusterManager(Manager):
    """
    SingleStoreDB cluster manager.

    This class should be instantiated using :func:`singlestoredb.manage_cluster`.

    Parameters
    ----------
    access_token : str, optional
        The API key or other access token for the cluster management API
    version : str, optional
        Version of the API to use
    base_url : str, optional
        Base URL of the cluster management API

    See Also
    --------
    :func:`singlestoredb.manage_cluster`

    """

    #: Cluster management API version if none is specified.
    default_version = 'v0beta'

    #: Base URL if none is specified.
    default_base_url = 'https://api.singlestore.com'

    #: Object type
    obj_type = 'cluster'

    @property
    def clusters(self) -> List[Cluster]:
        """Return a list of available clusters."""
        res = self._get('clusters')
        return [Cluster.from_dict(item, self) for item in res.json()]

    @property
    def regions(self) -> List[Region]:
        """Return a list of available regions."""
        res = self._get('regions')
        return [Region.from_dict(item, self) for item in res.json()]

    def create_cluster(
        self, name: str, region: Union[str, Region], admin_password: str,
        firewall_ranges: List[str], expires_at: Optional[str] = None,
        size: Optional[str] = None, plan: Optional[str] = None,
        wait_on_active: bool = False, wait_timeout: int = 600,
        wait_interval: int = 20,
    ) -> Cluster:
        """
        Create a new cluster.

        Parameters
        ----------
        name : str
            Name of the cluster
        region : str or Region
            The region ID of the cluster
        admin_password : str
            Admin password for the cluster
        firewall_ranges : Sequence[str], optional
            List of allowed incoming IP addresses
        expires_at : str, optional
            Timestamp of when the cluster expires
        size : str, optional
            Cluster size in cluster size notation (S-00, S-1, etc.)
        plan : str, optional
            Internal use only
        wait_on_active : bool, optional
            Wait for the cluster to be active before returning
        wait_timeout : int, optional
            Maximum number of seconds to wait before raising an exception
            if wait=True
        wait_interval : int, optional
            Number of seconds between each polling interval

        Returns
        -------
        :class:`Cluster`

        """
        if isinstance(region, Region):
            region = region.id
        res = self._post(
            'clusters', json=dict(
                name=name, regionID=region, adminPassword=admin_password,
                expiresAt=expires_at, size=size, firewallRanges=firewall_ranges,
                plan=plan,
            ),
        )
        out = self.get_cluster(res.json()['clusterID'])
        if wait_on_active:
            out = self._wait_on_state(
                out, 'Active', interval=wait_interval,
                timeout=wait_timeout,
            )
        return out

    def get_cluster(self, id: str) -> Cluster:
        """
        Retrieve a cluster definition.

        Parameters
        ----------
        id : str
            ID of the cluster

        Returns
        -------
        :class:`Cluster`

        """
        res = self._get(f'clusters/{id}')
        return Cluster.from_dict(res.json(), manager=self)


def manage_cluster(
    access_token: Optional[str] = None,
    version: str = ClusterManager.default_version,
    base_url: str = ClusterManager.default_base_url,
) -> ClusterManager:
    """
    Retrieve a SingleStoreDB cluster manager.

    Parameters
    ----------
    access_token : str, optional
        The API key or other access token for the cluster management API
    version : str, optional
        Version of the API to use
    base_url : str, optional
        Base URL of the cluster management API

    Returns
    -------
    :class:`ClusterManager`

    """
    warnings.warn(
        'The cluster management API is deprecated; '
        'use manage_workspace instead.',
        category=DeprecationWarning,
    )
    return ClusterManager(access_token=access_token, base_url=base_url, version=version)
