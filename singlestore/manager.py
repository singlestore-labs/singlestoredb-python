#!/usr/bin/env python
'''
SingleStore Cluster Management

'''
from __future__ import annotations

import os
from collections.abc import Sequence
from typing import Any
from typing import Dict
from typing import Optional
from urllib.parse import urljoin

import requests

from . import exceptions


class Region(object):
    '''
    Cluster region information

    This object is not directly instantiated. It is used in results
    of `ClusterManager` API calls.

    Parameters
    ----------
    region_id : str
        Unique ID of the region
    region : str
        Name of the region
    provider : str
        Name of the cloud provider

    Returns
    -------
    Region

    '''

    def __init__(self, region_id: str, region: str, provider: str):
        self.id = region_id
        self.region = region
        self.provider = provider
        self._manager: Optional[ClusterManager] = None

    @classmethod
    def from_dict(cls, obj: Dict[str, str], manager: 'ClusterManager') -> Region:
        '''
        Convert dictionary to a `Region` object

        Parameters
        ----------
        obj : dict
            Key-value pairs to retrieve region information from
        manager : ClusterManager, optional
            The ClusterManager the Region belongs to

        Returns
        -------
        Region

        '''
        out = cls(
            region_id=obj['regionId'],
            region=obj['region'],
            provider=obj['provider'],
        )
        out._manager = manager
        return out


class Cluster(object):
    '''
    SingleStore cluster definition

    This object is not instantiated directly. It is used in the results
    of API calls on the `ClusterManager`.

    Parameters
    ----------
    name : str
        Name of the cluster
    cluster_id : str
        Unique ID of the cluster
    region : Region
        The region of the cluster
    size : str
        Cluster size in cluster size notation (S-00, S-1, etc.)
    units : float
        Size of the cluster in units such as 0.25, 1.0, etc.
    state : str
        State of the cluster: PendingCreation, Transitioning, Active,
        Terminated, Suspended, Resuming, Failed
    version : str
        The SingleStore version
    created_at : str
        Timestamp of when the cluster was created
    expires_at : str, optional
        Timestamp of when the cluster expires
    firewall_ranges : Sequence[str], optional
        List of allowed incoming IP addresses
    terminated_at : str, optional
        Timestamp of when the cluster was terminated

    See Also
    --------
    `ClusterManager.create_cluster`

    Returns
    -------
    Cluster

    '''

    def __init__(
        self, name: str, cluster_id: str, region: Region, size: str,
        units: float, state: str, version: str,
        created_at: str, expires_at: Optional[str] = None,
        firewall_ranges: Optional[Sequence[str]] = None,
        terminated_at: Optional[str] = None,
    ):
        self.name = name
        self.id = cluster_id
        self.region = region
        self.size = size
        self.units = units
        self.state = state
        self.version = version
        self.created_at = created_at
        self.expires_at = expires_at
        self.firewall_ranges = firewall_ranges
        self.terminated_at = terminated_at
        self._manager: Optional[ClusterManager] = None

    @classmethod
    def from_dict(cls, obj: Dict[str, Any], manager: 'ClusterManager') -> Cluster:
        '''
        Construct a Cluster from a dictionary of values

        Parameters
        ----------
        obj : dict
            Dictionary of values
        manager : ClusterManager, optional
            The ClusterManager the Cluster belongs to

        Returns
        -------
        Cluster

        '''
        out = cls(
            name=obj['name'], cluster_id=obj['clusterId'],
            region=Region.from_dict(obj['region'], manager),
            size=obj['size'], units=obj['units'],
            state=obj['state'], version=obj['version'],
            created_at=obj['createdAt'], expires_at=obj['expiresAt'],
            firewall_ranges=obj['firewallRanges'],
            terminated_at=obj['terminatedAt'],
        )
        out._manager = manager
        return out

    def update(
        self, name: Optional[str] = None,
        admin_password: Optional[str] = None,
        expires_at: Optional[str] = None,
        size: Optional[str] = None, firewall_ranges: Optional[Sequence[str]] = None,
    ):
        '''
        Update the cluster definition

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

        '''
        if self._manager is None:
            raise ValueError('ClusterManager value has not been set')
        data = {
            k: v for k, v in dict(
                name=name, adminPassword=admin_password,
                expiresAt=expires_at, size=size,
                firewallRanges=firewall_ranges,
            ).items() if v is not None
        }
        self._manager._patch(f'clusters/{self.id}', json=data)

    def suspend(self):
        ''' Suspend the cluster '''
        if self._manager is None:
            raise ValueError('ClusterManager value has not been set')
        self._manager._post(
            f'clusters/{self.cluster_id}/suspend',
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
        )

    def resume(self):
        ''' Resume the cluster '''
        if self._manager is None:
            raise ValueError('ClusterManager value has not been set')
        self._manager._post(
            f'clusters/{self.cluster_id}/resume',
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
        )

    def terminate(self):
        ''' Terminate the cluster '''
        if self._manager is None:
            raise ValueError('ClusterManager value has not been set')
        self._manager._delete(f'clusters/{self.cluster_id}')


class ClusterManager(object):
    '''
    SingleStore cluster manager

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
    ClusterManager

    '''

    default_version = 'v0beta'
    default_base_url = 'https://api.singlestore.com'

    def __init__(
        self, access_token: str = None, version: str = None,
        base_url: str = None,
    ):
        access_token = (
            access_token or
            os.environ.get('SINGLESTORE_MANAGEMENT_TOKEN', None)
        )
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

    def _check(self, res: requests.Response):
        '''
        Check the HTTP response status code and raise an exception as needed

        Parameters
        ----------
        res : requests.Response
            HTTP response to check

        Returns
        -------
        requests.Response

        '''
        if res.status_code >= 400:
            raise exceptions.ClusterManagerError(res.status_code, res.text)
        return res

    def _get(self, path: str, *args, **kwargs):
        '''
        Invoke a GET request

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

        '''
        return self._check(
            self._sess.get(
                urljoin(self._base_url, path),
                *args, **kwargs,
            ),
        )

    def _post(self, path: str, *args, **kwargs):
        '''
        Invoke a POST request

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

        '''
        return self._check(
            self._sess.post(
                urljoin(self._base_url, path),
                *args, **kwargs,
            ),
        )

    def _put(self, path: str, *args, **kwargs):
        '''
        Invoke a PUT request

        Parameters
        ----------
        path : str
            Path of the resource
        *args : positional arguments, optional
            Arguments to add to the PUT request
        **kwargs : keyword arguments, optional
            Keyword arguments to add to the PUT request

        Returns
        -------
        requests.Response

        '''
        return self._check(
            self._sess.put(
                urljoin(self._base_url, path),
                *args, **kwargs,
            ),
        )

    def _delete(self, path: str, *args, **kwargs):
        '''
        Invoke a DELETE request

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

        '''
        return self._check(
            self._sess.delete(
                urljoin(self._base_url, path),
                *args, **kwargs,
            ),
        )

    def _patch(self, path: str, *args, **kwargs):
        '''
        Invoke a PATCH request

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

        '''
        return self._check(
            self._sess.patch(
                urljoin(self._base_url, path),
                *args, **kwargs,
            ),
        )

    @property
    def clusters(self) -> Sequence[Cluster]:
        ''' List of available clusters '''
        res = self._get('clusters')
        return [Cluster.from_dict(item, self) for item in res.json()]

    @property
    def regions(self) -> Sequence[Region]:
        ''' List of available regions '''
        res = self._get('regions')
        return [Region.from_dict(item, self) for item in res.json()]

    def create_cluster(
        self, name: str, region_id: str, admin_password: str,
        firewall_ranges: Sequence[str], expires_at: Optional[str] = None,
        size: Optional[str] = None, plan: Optional[str] = None,
    ) -> Cluster:
        '''
        Create a new cluster

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

        Returns
        -------
        Cluster

        '''
        res = self._post(
            'clusters', json=dict(
                name=name, regionID=region_id, adminPassword=admin_password,
                expiresAt=expires_at, size=size, firewallRanges=firewall_ranges,
                plan=plan,
            ),
        )
        return Cluster.from_dict(res.json(), manager=self)

    def get_cluster(self, cluster_id: str):
        '''
        Retrieve a cluster definition

        Parameters
        ----------
        cluster_id : str
            ID of the cluster

        Returns
        -------
        Cluster

        '''
        res = self._get(f'clusters/{cluster_id}')
        return Cluster.from_dict(res.json(), manager=self)


def manage_cluster(
    access_token: Optional[str] = None,
    version: str = ClusterManager.default_version,
    base_url: str = ClusterManager.default_base_url,
) -> ClusterManager:
    '''
    Retrieve a SingleStore cluster manager

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
    ClusterManager

    '''
    return ClusterManager(access_token=access_token, base_url=base_url, version=version)
