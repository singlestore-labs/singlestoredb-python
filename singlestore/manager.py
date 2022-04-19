#!/usr/bin/env python
"""SingleStore Cluster Management."""
from __future__ import annotations

import datetime
import time
from collections.abc import Sequence
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union
from urllib.parse import urljoin

import requests

from . import config
from . import connection
from .exceptions import ClusterManagerError


def to_datetime(
    obj: Optional[Union[str, datetime.datetime]],
) -> Optional[datetime.datetime]:
    """Convert string to datetime."""
    if not obj:
        return None
    if isinstance(obj, datetime.datetime):
        return obj
    obj = obj.replace('Z', '')
    # Fix datetimes with truncated zeros
    if '.' in obj:
        obj, micros = obj.split('.', 1)
        micros = micros + '0' * (6 - len(micros))
        obj = obj + '.' + micros
    return datetime.datetime.fromisoformat(obj)


def vars_to_str(obj: Any) -> str:
    """Render a string representation of vars(obj)."""
    attrs = []
    for name, value in sorted(vars(obj).items()):
        if not value or name.startswith('_'):
            continue
        attrs.append('{}={}'.format(name, repr(value)))
    return '{}({})'.format(type(obj).__name__, ', '.join(attrs))


class Region(object):
    """
    Cluster region information.

    This object is not directly instantiated. It is used in results
    of `ClusterManager` API calls.

    See Also
    --------
    :attr:`ClusterManager.regions`

    """

    def __init__(self, region_id: str, region: str, provider: str):
        """Use :attr:`ClusterManager.regions` instead."""
        #: Unique ID of the region
        self.id = region_id

        #: Name of the region
        self.region = region

        #: Name of the cloud provider
        self.provider = provider

        self._manager: Optional[ClusterManager] = None

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)

    @classmethod
    def from_dict(cls, obj: Dict[str, str], manager: 'ClusterManager') -> Region:
        """
        Convert dictionary to a ``Region`` object.

        Parameters
        ----------
        obj : dict
            Key-value pairs to retrieve region information from
        manager : ClusterManager, optional
            The ClusterManager the Region belongs to

        Returns
        -------
        :class:`Region`

        """
        out = cls(
            region_id=obj['regionID'],
            region=obj['region'],
            provider=obj['provider'],
        )
        out._manager = manager
        return out


class Cluster(object):
    """
    SingleStore cluster definition.

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
        self, name: str, cluster_id: str, region: Region, size: str,
        units: float, state: str, version: str,
        created_at: Union[str, datetime.datetime],
        expires_at: Optional[Union[str, datetime.datetime]] = None,
        firewall_ranges: Optional[Sequence[str]] = None,
        terminated_at: Optional[Union[str, datetime.datetime]] = None,
        endpoint: Optional[str] = None,
    ):
        """Use :attr:`ClusterManager.clusters` or :meth:`ClusterManager.get_cluster`."""
        #: Name of the cluster
        self.name = name.strip()

        #: Unique ID of the cluster
        self.id = cluster_id.strip()

        #: Region of the cluster (see :class:`Region`)
        self.region = region

        #: Size of the cluster in cluster size notation (S-00, S-1, etc.)
        self.size = size

        #: Size of the cluster in units such as 0.25, 1.0, etc.
        self.units = units

        #: State of the cluster: PendingCreation, Transitioning, Active,
        #: Terminated, Suspended, Resuming, Failed
        self.state = state.strip()

        #: Version of the SingleStore server
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
    def from_dict(cls, obj: Dict[str, Any], manager: 'ClusterManager') -> Cluster:
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
            name=obj['name'], cluster_id=obj['clusterID'],
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

    def refresh(self) -> Cluster:
        """Update the object to the current state."""
        if self._manager is None:
            raise ClusterManagerError(
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
        size: Optional[str] = None, firewall_ranges: Optional[Sequence[str]] = None,
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
            raise ClusterManagerError(
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
        ClusterManagerError
            If timeout is reached

        """
        if self._manager is None:
            raise ClusterManagerError(
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
        ClusterManagerError
            If timeout is reached

        """
        if self._manager is None:
            raise ClusterManagerError(
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
        ClusterManagerError
            If timeout is reached

        """
        if self._manager is None:
            raise ClusterManagerError(
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
            Parameters to the SingleStore `connect` function except host
            and port which are supplied by the cluster object

        Returns
        -------
        :class:`Connection`

        """
        if not self.endpoint:
            raise ClusterManagerError(
                msg='An endpoint has not been set in '
                'this cluster configuration',
            )
        kwargs['host'] = self.endpoint
        return connection.connect(**kwargs)


class ClusterManager(object):
    """
    SingleStore cluster manager.

    This class should be instantiated using :func:`singlestore.manage_cluster`.

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
    :func:`singlestore.manage_cluster`

    """

    #: Cluster management API version if none is specified.
    default_version = 'v0beta'

    #: Base URL if none is specified.
    default_base_url = 'https://api.singlestore.com'

    def __init__(
        self, access_token: Optional[str] = None, version: Optional[str] = None,
        base_url: Optional[str] = None,
    ):
        access_token = (
            access_token or
            config.get_option('cluster_manager.token')
        )
        if not access_token:
            raise ClusterManagerError(msg='No cluster management token was configured.')
        self._sess = requests.Session()
        self._sess.headers.update({
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        })
        self._base_url = urljoin(
            base_url or type(self).default_base_url,
            version or type(self).default_version,
        ) + '/'

    def _check(self, res: requests.Response) -> requests.Response:
        """
        Check the HTTP response status code and raise an exception as needed.

        Parameters
        ----------
        res : requests.Response
            HTTP response to check

        Returns
        -------
        requests.Response

        """
        if res.status_code >= 400:
            raise ClusterManagerError(errno=res.status_code, msg=res.text)
        return res

    def _get(self, path: str, *args: Any, **kwargs: Any) -> requests.Response:
        """
        Invoke a GET request.

        Parameters
        ----------
        path : str
            Path of the resource
        *args : positional arguments, optional
            Arguments to add to the GET request
        **kwargs : keyword arguments, optional
            Keyword arguments to add to the GET request

        Returns
        -------
        requests.Response

        """
        return self._check(
            self._sess.get(
                urljoin(self._base_url, path),
                *args, **kwargs,
            ),
        )

    def _post(self, path: str, *args: Any, **kwargs: Any) -> requests.Response:
        """
        Invoke a POST request.

        Parameters
        ----------
        path : str
            Path of the resource
        *args : positional arguments, optional
            Arguments to add to the POST request
        **kwargs : keyword arguments, optional
            Keyword arguments to add to the POST request

        Returns
        -------
        requests.Response

        """
        return self._check(
            self._sess.post(
                urljoin(self._base_url, path),
                *args, **kwargs,
            ),
        )

    def _delete(self, path: str, *args: Any, **kwargs: Any) -> requests.Response:
        """
        Invoke a DELETE request.

        Parameters
        ----------
        path : str
            Path of the resource
        *args : positional arguments, optional
            Arguments to add to the DELETE request
        **kwargs : keyword arguments, optional
            Keyword arguments to add to the DELETE request

        Returns
        -------
        requests.Response

        """
        return self._check(
            self._sess.delete(
                urljoin(self._base_url, path),
                *args, **kwargs,
            ),
        )

    def _patch(self, path: str, *args: Any, **kwargs: Any) -> requests.Response:
        """
        Invoke a PATCH request.

        Parameters
        ----------
        path : str
            Path of the resource
        *args : positional arguments, optional
            Arguments to add to the PATCH request
        **kwargs : keyword arguments, optional
            Keyword arguments to add to the PATCH request

        Returns
        -------
        requests.Response

        """
        return self._check(
            self._sess.patch(
                urljoin(self._base_url, path),
                *args, **kwargs,
            ),
        )

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
        self, name: str, region_id: str, admin_password: str,
        firewall_ranges: Sequence[str], expires_at: Optional[str] = None,
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
        region_id : str
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
        res = self._post(
            'clusters', json=dict(
                name=name, regionID=region_id, adminPassword=admin_password,
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

    def _wait_on_state(
        self,
        out: Cluster,
        state: Union[str, List[str]],
        interval: int = 20,
        timeout: int = 600,
    ) -> Cluster:
        """
        Wait on server state before continuing.


        Parameters
        ----------
        out : Cluster
            Current cluster object
        state : str or List[str]
            State(s) to wait for
        interval : int, optional
            Interval between each server poll
        timeout : int, optional
            Maximum time to wait before raising an exception

        Raises
        ------
        ClusterManagerError
            If timeout is reached

        Returns
        -------
        :class:`Cluster`

        """
        states = [
            x.lower().strip()
            for x in (isinstance(state, str) and [state] or state)
        ]
        while True:
            if out.state.lower() in states:
                break
            if timeout <= 0:
                raise ClusterManagerError(
                    msg='Exceeded waiting time for cluster to become '
                        '{}.'.format(', '.join(states)),
                )
            time.sleep(interval)
            timeout -= interval
            out = self.get_cluster(out.id)
        return out

    def get_cluster(self, cluster_id: str) -> Cluster:
        """
        Retrieve a cluster definition.

        Parameters
        ----------
        cluster_id : str
            ID of the cluster

        Returns
        -------
        :class:`Cluster`

        """
        res = self._get(f'clusters/{cluster_id}')
        return Cluster.from_dict(res.json(), manager=self)


def manage_cluster(
    access_token: Optional[str] = None,
    version: str = ClusterManager.default_version,
    base_url: str = ClusterManager.default_base_url,
) -> ClusterManager:
    """
    Retrieve a SingleStore cluster manager.

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
    return ClusterManager(access_token=access_token, base_url=base_url, version=version)
