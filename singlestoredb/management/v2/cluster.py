#!/usr/bin/env python
"""
SingleStoreDB Cluster Management API v2.

At v2 the two-level v1 hierarchy of workspace groups containing workspaces
collapses into a single flat ``clusters`` resource: a v2 cluster carries the
union of the fields v1 split between ``Workspace`` and ``WorkspaceGroup``.
There is no ``/v2/workspaceGroups`` and no ``/v2/workspaces`` -- both return
``404 page not found``.

This module deliberately shares no code and no vocabulary with
:mod:`singlestoredb.management.v1`. The v1 package is intended to be deletable
in one step once the v1 endpoints are retired (see
``TestVersionPackagesAreIndependent``), so everything here either is written
fresh or is imported from the version-neutral modules directly under
:mod:`singlestoredb.management`. The v1 names live
entirely in :mod:`singlestoredb.management.v1`, so nothing in this module has to
know what a workspace was.
"""
from __future__ import annotations

import datetime
import os
import time
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

from ... import config
from ... import connection
from ...exceptions import ManagementError
from ..billing import Billing as Billing
from ..manager import Manager
from ..organization import Organization
from ..organization import Organizations as Organizations
from ..region import Region
from ..stage import Stage as Stage
from ..stage import StageObject as StageObject
from ..utils import camel_to_snake_dict
from ..utils import NamedList
from ..utils import PathLike
from ..utils import snake_to_camel_dict
from ..utils import to_datetime
from ..utils import ttl_property
from ..utils import vars_to_str

#: Base management API path for the shared-tier resource.
SHAREDTIER_PATH = 'sharedtier/virtualClusters'

#: Environment variables that name the deployment the current process is
#: running against, in priority order. These are set by the SingleStore
#: notebook environment and are part of its external contract, so they keep
#: their published names regardless of API version.
CLUSTER_ENV_VARS = ('SINGLESTOREDB_CLUSTER', 'SINGLESTOREDB_WORKSPACE')


def get_organization() -> Organization:
    """Get the organization."""
    from ..cluster import manage_clusters
    return manage_clusters().organization


def get_secret(name: str) -> Optional[str]:
    """Get a secret from the organization."""
    return get_organization().get_secret(name).value


def get_cluster(
    cluster: Optional[Union['Cluster', str]] = None,
) -> 'Cluster':
    """
    Get a cluster.

    Parameters
    ----------
    cluster : Cluster or str, optional
        A cluster object, or the name or ID of a cluster. If not given, the
        cluster named by one of the deployment environment variables listed in
        :data:`CLUSTER_ENV_VARS` is used.

    Returns
    -------
    :class:`Cluster`

    """
    if isinstance(cluster, Cluster):
        return cluster
    from ..cluster import manage_clusters
    mgr = manage_clusters()
    if cluster:
        return mgr.clusters[cluster]
    for envvar in CLUSTER_ENV_VARS:
        if envvar in os.environ:
            return mgr.clusters[os.environ[envvar]]
    raise RuntimeError('no cluster specified')


def get_stage(
    cluster: Optional[Union['Cluster', str]] = None,
) -> Stage:
    """Get the stage for a cluster."""
    return get_cluster(cluster).stage


class Cluster:
    """
    SingleStoreDB cluster definition.

    This object is not instantiated directly. It is used in the results of API
    calls on the :class:`ClusterManager`. Clusters are created using
    :meth:`ClusterManager.create_cluster`, or existing clusters are accessed by
    either :attr:`ClusterManager.clusters` or by calling
    :meth:`ClusterManager.get_cluster`.

    A cluster is a single flat resource: the compute settings (size,
    auto-suspend, cache) and the deployment-wide settings (firewall, update
    window, expiration) all live on this object.

    See Also
    --------
    :meth:`ClusterManager.create_cluster`
    :meth:`ClusterManager.get_cluster`
    :attr:`ClusterManager.clusters`

    """

    name: str
    id: str
    group_id: Optional[str]
    size: Optional[str]
    scale_factor: Optional[float]
    state: str
    created_at: Optional[datetime.datetime]
    terminated_at: Optional[datetime.datetime]
    expires_at: Optional[datetime.datetime]
    last_resumed_at: Optional[datetime.datetime]
    endpoint: Optional[str]
    provider: Optional[str]
    region_name: Optional[str]
    project_id: Optional[str]
    deployment_type: Optional[str]
    kai: Optional[bool]
    multi_az: Optional[bool]
    allow_all_traffic: bool
    firewall_ranges: List[str]
    outbound_allow_list: Optional[str]
    opt_in_preview_feature: Optional[bool]
    update_window: Optional[Dict[str, Any]]
    auto_suspend: Optional[Dict[str, Any]]
    auto_scale: Optional[Dict[str, Any]]
    cache_config: Optional[float]
    resume_attachments: List[Dict[str, Any]]
    scaling_progress: Optional[int]
    smart_dr_status: Optional[str]

    def __init__(
        self,
        name: str,
        id: str,
        state: str,
        group_id: Optional[str] = None,
        size: Optional[str] = None,
        scale_factor: Optional[float] = None,
        created_at: Optional[Union[str, datetime.datetime]] = None,
        terminated_at: Optional[Union[str, datetime.datetime]] = None,
        expires_at: Optional[Union[str, datetime.datetime]] = None,
        last_resumed_at: Optional[Union[str, datetime.datetime]] = None,
        endpoint: Optional[str] = None,
        provider: Optional[str] = None,
        region_name: Optional[str] = None,
        project_id: Optional[str] = None,
        deployment_type: Optional[str] = None,
        kai: Optional[bool] = None,
        multi_az: Optional[bool] = None,
        allow_all_traffic: Optional[bool] = None,
        firewall_ranges: Optional[List[str]] = None,
        outbound_allow_list: Optional[str] = None,
        opt_in_preview_feature: Optional[bool] = None,
        update_window: Optional[Dict[str, Any]] = None,
        auto_suspend: Optional[Dict[str, Any]] = None,
        auto_scale: Optional[Dict[str, Any]] = None,
        cache_config: Optional[float] = None,
        resume_attachments: Optional[List[Dict[str, Any]]] = None,
        scaling_progress: Optional[int] = None,
        smart_dr_status: Optional[str] = None,
    ):
        #: Name of the cluster
        self.name = name

        #: Unique ID of the cluster
        self.id = id

        #: State of the cluster: PENDING, ACTIVE, SUSPENDED, TERMINATED,
        #: TRANSITIONING, RESUMING, FAILED
        self.state = state.strip()

        #: Unique ID of the group the cluster belongs to
        self.group_id = group_id

        #: Size of the cluster in cluster size notation (S-00, S-1, etc.)
        self.size = size

        #: Current scale factor for the cluster
        self.scale_factor = scale_factor

        #: Timestamp of when the cluster was created
        self.created_at = to_datetime(created_at)

        #: Timestamp of when the cluster was terminated
        self.terminated_at = to_datetime(terminated_at)

        #: Timestamp of when the cluster will expire
        self.expires_at = to_datetime(expires_at)

        #: Timestamp of when the cluster was last resumed
        self.last_resumed_at = to_datetime(last_resumed_at)

        #: Hostname (or IP address) of the cluster database server
        self.endpoint = endpoint

        #: Cloud provider hosting the cluster (AWS | GCP | Azure)
        self.provider = provider

        #: Cloud provider region name, e.g., ``us-east-1``. Unlike v1, v2 does
        #: not report a region ID; a region is identified by the
        #: ``(provider, region_name)`` pair.
        self.region_name = region_name

        #: Project ID associated with the cluster
        self.project_id = project_id

        #: Deployment type of the cluster (PRODUCTION | NON-PRODUCTION)
        self.deployment_type = deployment_type

        #: Whether SingleStore Kai is enabled on this cluster. v1 spelled this
        #: field ``kaiEnabled``.
        self.kai = kai

        #: Whether the cluster is deployed across multiple availability zones.
        #: v1 spelled this ``highAvailabilityTwoZones``.
        self.multi_az = multi_az

        #: Should all inbound traffic be allowed?
        self.allow_all_traffic = allow_all_traffic or False

        #: List of allowed incoming IP addresses / ranges
        self.firewall_ranges = firewall_ranges or []

        #: Account ID for outbound connections
        self.outbound_allow_list = outbound_allow_list

        #: Whether preview features are opted in
        self.opt_in_preview_feature = opt_in_preview_feature

        #: Update window settings: dict(day=0-6, hour=0-23)
        self.update_window = update_window

        #: Current auto-suspend settings
        self.auto_suspend = camel_to_snake_dict(auto_suspend)

        #: Auto-scale settings for the cluster
        self.auto_scale = camel_to_snake_dict(auto_scale)

        #: Multiplier for the persistent cache
        self.cache_config = cache_config

        #: Database attachments
        self.resume_attachments = [
            camel_to_snake_dict(x)  # type: ignore
            for x in resume_attachments or []
            if x is not None
        ]

        #: Current progress percentage for scaling the cluster
        self.scaling_progress = scaling_progress

        #: SmartDR status of the cluster (ACTIVE | STANDBY)
        self.smart_dr_status = smart_dr_status

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

        Every field other than the name and ID is optional: the v2 API omits
        null fields entirely rather than returning them as ``null``.

        Parameters
        ----------
        obj : dict
            Dictionary of values
        manager : ClusterManager
            The ClusterManager the Cluster belongs to

        Returns
        -------
        :class:`Cluster`

        """
        # Size is reported as an object: dict(size='S-00', scaleFactor=1)
        size_spec = obj.get('size') or {}

        out = cls(
            name=obj['name'],
            id=obj['clusterID'],
            state=obj.get('state', 'Unknown'),
            group_id=obj.get('groupID'),
            size=size_spec.get('size'),
            scale_factor=size_spec.get('scaleFactor'),
            created_at=obj.get('createdAt'),
            terminated_at=obj.get('terminatedAt'),
            expires_at=obj.get('expiresAt'),
            last_resumed_at=obj.get('lastResumedAt'),
            endpoint=obj.get('endpoint'),
            provider=obj.get('provider'),
            region_name=obj.get('region'),
            project_id=obj.get('projectID'),
            deployment_type=obj.get('deploymentType'),
            kai=obj.get('kai'),
            multi_az=obj.get('multiAZ'),
            allow_all_traffic=obj.get('allowAllTraffic'),
            firewall_ranges=obj.get('firewallRanges'),
            outbound_allow_list=obj.get('outboundAllowList'),
            opt_in_preview_feature=obj.get('optInPreviewFeature'),
            update_window=obj.get('updateWindow'),
            auto_suspend=obj.get('autoSuspend'),
            auto_scale=obj.get('autoScale'),
            cache_config=obj.get('cacheConfig'),
            resume_attachments=obj.get('resumeAttachments'),
            scaling_progress=obj.get('scalingProgress'),
            smart_dr_status=obj.get('smartDRStatus'),
        )
        out._manager = manager
        return out

    def _require_manager(self) -> 'ClusterManager':
        if self._manager is None:
            raise ManagementError(
                msg='No cluster manager is associated with this object.',
            )
        return self._manager

    @property
    def organization(self) -> Organization:
        """Return the organization the cluster belongs to."""
        return self._require_manager().organization

    @property
    def stage(self) -> Stage:
        """Stage manager."""
        return Stage(self.id, self._require_manager())

    stages = stage

    def refresh(self) -> 'Cluster':
        """Update the object to the current state."""
        manager = self._require_manager()
        new_obj = manager.get_cluster(self.id)
        for name, value in vars(new_obj).items():
            setattr(self, name, value)
        return self

    def update(
        self,
        name: Optional[str] = None,
        size: Optional[str] = None,
        scale_factor: Optional[float] = None,
        auto_suspend: Optional[Dict[str, Any]] = None,
        auto_scale: Optional[Dict[str, Any]] = None,
        cache_config: Optional[float] = None,
        deployment_type: Optional[str] = None,
        firewall_ranges: Optional[List[str]] = None,
        allow_all_traffic: Optional[bool] = None,
        admin_password: Optional[str] = None,
        expires_at: Optional[str] = None,
        update_window: Optional[Dict[str, int]] = None,
        kai: Optional[bool] = None,
    ) -> None:
        """
        Update the cluster definition.

        Both the compute settings (size, auto-suspend, cache) and the
        deployment-wide settings (firewall, update window, expiration) are
        changed through this one call.

        Parameters
        ----------
        name : str, optional
            Name of the cluster
        size : str, optional
            Size of the cluster in cluster size notation, such as "S-1".
            Resizing is done through this field; v2 has no ``resize`` route.
        scale_factor : float, optional
            Scale factor for the cluster
        auto_suspend : Dict[str, Any], optional
            Auto-suspend mode for the cluster: IDLE, SCHEDULED, DISABLED
        auto_scale : Dict[str, Any], optional
            Auto-scale settings for the cluster
        cache_config : float, optional
            Multiplier for the persistent cache associated with the cluster.
            It can have one of the following values: 1, 2, or 4.
        deployment_type : str, optional
            Deployment type of the cluster (PRODUCTION | NON-PRODUCTION)
        firewall_ranges : List[str], optional
            List of allowed CIDR ranges. An empty list indicates that all
            inbound requests are allowed.
        allow_all_traffic : bool, optional
            Allow all traffic to the cluster
        admin_password : str, optional
            Admin password for the cluster
        expires_at : str, optional
            Timestamp of when the cluster will expire. Expiration time can be
            specified as a timestamp or a duration.
            Example: "2021-01-02T15:04:05Z07:00", "2021-01-02", "3h30m"
        update_window : Dict[str, int], optional
            Day and hour of an update window: dict(day=0-6, hour=0-23)
        kai : bool, optional
            Whether SingleStore Kai is enabled on this cluster

        """
        manager = self._require_manager()
        size_spec: Optional[Dict[str, Any]] = None
        if size is not None or scale_factor is not None:
            size_spec = {
                k: v for k, v in dict(
                    size=size, scaleFactor=scale_factor,
                ).items() if v is not None
            }
        data = {
            k: v for k, v in dict(
                name=name,
                size=size_spec,
                autoSuspend=snake_to_camel_dict(auto_suspend),
                autoScale=snake_to_camel_dict(auto_scale),
                cacheConfig=cache_config,
                deploymentType=deployment_type,
                firewallRanges=firewall_ranges,
                allowAllTraffic=allow_all_traffic,
                adminPassword=admin_password,
                expiresAt=expires_at,
                updateWindow=snake_to_camel_dict(update_window),
                kai=kai,
            ).items() if v is not None
        }
        manager._patch(f'clusters/{self.id}', json=data)
        self.refresh()

    def terminate(
        self,
        wait_on_terminated: bool = False,
        wait_interval: int = 10,
        wait_timeout: int = 600,
        force: bool = False,
    ) -> None:
        """
        Terminate the cluster.

        Parameters
        ----------
        wait_on_terminated : bool, optional
            Wait for the cluster to be terminated before returning
        wait_interval : int, optional
            Number of seconds between each server check
        wait_timeout : int, optional
            Total number of seconds to check server before giving up
        force : bool, optional
            Should the cluster be terminated even if it is in use?

        Raises
        ------
        ManagementError
            If timeout is reached

        """
        manager = self._require_manager()
        manager._delete(f'clusters/{self.id}', params=dict(force=force))
        if wait_on_terminated:
            while True:
                self.refresh()
                if self.terminated_at is not None:
                    break
                if wait_timeout <= 0:
                    raise ManagementError(
                        msg='Exceeded waiting time for Cluster to terminate',
                    )
                time.sleep(wait_interval)
                wait_timeout -= wait_interval

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
                msg='An endpoint has not been set in this cluster configuration',
            )
        kwargs['host'] = self.endpoint
        return connection.connect(**kwargs)

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
            Wait for the cluster to be suspended before returning
        wait_interval : int, optional
            Number of seconds between each server check
        wait_timeout : int, optional
            Total number of seconds to check server before giving up

        Raises
        ------
        ManagementError
            If timeout is reached

        """
        manager = self._require_manager()
        manager._post(f'clusters/{self.id}/suspend')
        if wait_on_suspended:
            manager._wait_on_state(
                manager.get_cluster(self.id),
                'SUSPENDED', interval=wait_interval, timeout=wait_timeout,
            )
            self.refresh()

    def resume(
        self,
        disable_auto_suspend: bool = False,
        wait_on_resumed: bool = False,
        wait_interval: int = 20,
        wait_timeout: int = 600,
    ) -> None:
        """
        Resume the cluster.

        Parameters
        ----------
        disable_auto_suspend : bool, optional
            Should auto-suspend be disabled?
        wait_on_resumed : bool, optional
            Wait for the cluster to be resumed or active before returning
        wait_interval : int, optional
            Number of seconds between each server check
        wait_timeout : int, optional
            Total number of seconds to check server before giving up

        Raises
        ------
        ManagementError
            If timeout is reached

        """
        manager = self._require_manager()
        manager._post(
            f'clusters/{self.id}/resume',
            json=dict(disableAutoSuspend=disable_auto_suspend),
        )
        if wait_on_resumed:
            manager._wait_on_state(
                manager.get_cluster(self.id),
                ['RESUMED', 'ACTIVE'], interval=wait_interval, timeout=wait_timeout,
            )
            self.refresh()


class StarterCluster:
    """
    SingleStoreDB starter (shared tier) cluster definition.

    This object is not instantiated directly. Existing starter clusters are
    accessed by either :attr:`ClusterManager.starter_clusters` or by calling
    :meth:`ClusterManager.get_starter_cluster`.

    See Also
    --------
    :meth:`ClusterManager.get_starter_cluster`
    :meth:`ClusterManager.create_starter_cluster`
    :attr:`ClusterManager.starter_clusters`

    """

    name: str
    id: str
    database_name: str
    endpoint: Optional[str]
    mysql_dml_port: Optional[int]
    websocket_port: Optional[int]
    project_id: Optional[str]

    def __init__(
        self,
        name: str,
        id: str,
        database_name: str,
        endpoint: Optional[str] = None,
        mysql_dml_port: Optional[int] = None,
        websocket_port: Optional[int] = None,
        project_id: Optional[str] = None,
    ):
        #: Name of the starter cluster
        self.name = name

        #: Unique ID of the starter cluster
        self.id = id

        #: Name of the database associated with the starter cluster
        self.database_name = database_name

        #: Endpoint to connect to the starter cluster, in the form
        #: ``hostname:port``
        self.endpoint = endpoint

        #: MySQL DML port for the starter cluster
        self.mysql_dml_port = mysql_dml_port

        #: WebSocket port for the starter cluster
        self.websocket_port = websocket_port

        #: Project ID associated with the starter cluster
        self.project_id = project_id

        self._manager: Optional[ClusterManager] = None

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)

    @classmethod
    def from_dict(
        cls, obj: Dict[str, Any], manager: 'ClusterManager',
    ) -> 'StarterCluster':
        """
        Construct a StarterCluster from a dictionary of values.

        Parameters
        ----------
        obj : dict
            Dictionary of values
        manager : ClusterManager
            The ClusterManager the StarterCluster belongs to

        Returns
        -------
        :class:`StarterCluster`

        """
        out = cls(
            name=obj['name'],
            id=obj['virtualClusterID'],
            database_name=obj['databaseName'],
            endpoint=obj.get('endpoint'),
            mysql_dml_port=obj.get('mysqlDmlPort'),
            websocket_port=obj.get('websocketPort'),
            project_id=obj.get('projectID'),
        )
        out._manager = manager
        return out

    def _require_manager(self) -> 'ClusterManager':
        if self._manager is None:
            raise ManagementError(
                msg='No cluster manager is associated with this object.',
            )
        return self._manager

    def connect(self, **kwargs: Any) -> connection.Connection:
        """
        Create a connection to the database server for this starter cluster.

        Parameters
        ----------
        **kwargs : keyword-arguments, optional
            Parameters to the SingleStoreDB `connect` function except host
            and port which are supplied by the starter cluster object

        Returns
        -------
        :class:`Connection`

        """
        if not self.endpoint:
            raise ManagementError(
                msg='An endpoint has not been set in this '
                    'starter cluster configuration',
            )
        kwargs['host'] = self.endpoint
        kwargs['database'] = self.database_name
        return connection.connect(**kwargs)

    def terminate(self) -> None:
        """Terminate the starter cluster."""
        self._require_manager()._delete(f'{SHAREDTIER_PATH}/{self.id}')

    def refresh(self) -> 'StarterCluster':
        """Update the object to the current state."""
        manager = self._require_manager()
        new_obj = manager.get_starter_cluster(self.id)
        for name, value in vars(new_obj).items():
            setattr(self, name, value)
        return self

    @property
    def organization(self) -> Organization:
        """Return the organization the starter cluster belongs to."""
        return self._require_manager().organization

    @property
    def stage(self) -> Stage:
        """
        Stage manager.

        .. warning:: There is no Stage route for a starter (shared tier)
           deployment at either API version -- ``clusters/{id}/stage/fs/``
           only resolves for a full cluster ID. This property is kept for
           parity with :class:`Cluster`, but requests made through it will
           fail.

        """
        return Stage(self.id, self._require_manager())

    stages = stage

    @property
    def starter_clusters(self) -> NamedList['StarterCluster']:
        """Return a list of available starter clusters."""
        manager = self._require_manager()
        res = manager._get(SHAREDTIER_PATH)
        return NamedList(
            [type(self).from_dict(item, manager) for item in res.json()],
        )

    def create_user(
        self,
        username: str,
        password: Optional[str] = None,
    ) -> Dict[str, str]:
        """
        Create a new user for this starter cluster.

        Parameters
        ----------
        username : str
            The user name to connect the new user to the database
        password : str, optional
            Password for the new user. If not provided, a password will be
            auto-generated by the system.

        Returns
        -------
        Dict[str, str]
            Dictionary containing 'user_id' and 'password' of the created user

        Raises
        ------
        ManagementError
            If no cluster manager is associated with this object

        """
        manager = self._require_manager()

        payload = {'userName': username}
        if password is not None:
            payload['password'] = password

        res = manager._post(
            f'{SHAREDTIER_PATH}/{self.id}/users',
            json=payload,
        )

        response_data = res.json()
        user_id = response_data.get('userID')
        if not user_id:
            raise ManagementError(msg='No userID returned from API')

        # Return the password provided by user or generated by API
        returned_password = password if password is not None \
            else response_data.get('password')
        if not returned_password:
            raise ManagementError(msg='No password available from API response')

        return {
            'user_id': user_id,
            'password': returned_password,
        }


class ClusterManager(Manager):
    """
    SingleStoreDB cluster manager.

    This class should be instantiated using
    :func:`singlestoredb.manage_clusters`.

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
    :func:`singlestoredb.manage_clusters`

    """

    #: Cluster management API version if none is specified.
    default_version = 'v2'

    #: Base URL if none is specified.
    default_base_url = config.get_option('management.base_url') \
        or 'https://api.singlestore.com'

    #: Object type
    obj_type = 'cluster'

    @property
    def clusters(self) -> NamedList[Cluster]:
        """Return a list of available clusters."""
        res = self._get('clusters')
        return NamedList([Cluster.from_dict(item, self) for item in res.json()])

    @property
    def starter_clusters(self) -> NamedList[StarterCluster]:
        """Return a list of available starter clusters."""
        res = self._get(SHAREDTIER_PATH)
        return NamedList(
            [StarterCluster.from_dict(item, self) for item in res.json()],
        )

    @property
    def organizations(self) -> Organizations:
        """Return the organizations."""
        return Organizations(self)

    @property
    def organization(self) -> Organization:
        """Return the current organization."""
        return self.organizations.current

    @property
    def billing(self) -> Billing:
        """Return the current billing information."""
        return Billing(self)

    @ttl_property(datetime.timedelta(hours=1))
    def regions(self) -> NamedList[Region]:
        """Return a list of available regions."""
        res = self._get('regions')
        return NamedList([Region.from_dict(item, self) for item in res.json()])

    def create_cluster(
        self,
        name: str,
        region: Union[str, Region, None] = None,
        provider: Optional[str] = None,
        region_name: Optional[str] = None,
        size: Optional[str] = None,
        scale_factor: Optional[float] = None,
        firewall_ranges: Optional[List[str]] = None,
        allow_all_traffic: Optional[bool] = None,
        admin_password: Optional[str] = None,
        auto_suspend: Optional[Dict[str, Any]] = None,
        auto_scale: Optional[Dict[str, Any]] = None,
        cache_config: Optional[float] = None,
        deployment_type: Optional[str] = None,
        expires_at: Optional[str] = None,
        update_window: Optional[Dict[str, int]] = None,
        kai: Optional[bool] = None,
        multi_az: Optional[bool] = None,
        opt_in_preview_feature: Optional[bool] = None,
        project_id: Optional[str] = None,
        wait_on_active: bool = False,
        wait_interval: int = 10,
        wait_timeout: int = 600,
    ) -> Cluster:
        """
        Create a new cluster.

        A cluster is created in one call: the firewall, update window and
        expiration settings are passed here alongside the compute settings.

        Parameters
        ----------
        name : str
            Name of the cluster
        region : str or Region, optional
            Region to create the cluster in. A :class:`Region` is reduced to
            its ``(provider, region_name)`` pair; a string is taken as the
            provider region name. v2 has no region IDs.
        provider : str, optional
            Cloud provider for the cluster (AWS | GCP | Azure). Used together
            with ``region_name`` as an alternative to ``region``.
        region_name : str, optional
            Cloud provider region name, e.g., ``us-east-1``
        size : str, optional
            Cluster size in cluster size notation (S-00, S-1, etc.)
        scale_factor : float, optional
            Scale factor for the cluster
        firewall_ranges : List[str], optional
            List of allowed CIDR ranges. An empty list indicates that all
            inbound requests are allowed.
        allow_all_traffic : bool, optional
            Allow all traffic to the cluster
        admin_password : str, optional
            Admin password for the cluster. If no password is supplied, a
            password will be generated and returned in the response.
        auto_suspend : Dict[str, Any], optional
            Auto-suspend settings for the cluster
        auto_scale : Dict[str, Any], optional
            Auto-scale settings for the cluster
        cache_config : float, optional
            Multiplier for the persistent cache: 1, 2, or 4
        deployment_type : str, optional
            Deployment type of the cluster (PRODUCTION | NON-PRODUCTION)
        expires_at : str, optional
            Timestamp of when the cluster will expire
        update_window : Dict[str, int], optional
            Day and hour of an update window: dict(day=0-6, hour=0-23)
        kai : bool, optional
            Whether to enable SingleStore Kai on this cluster
        multi_az : bool, optional
            Whether to deploy across multiple availability zones
        opt_in_preview_feature : bool, optional
            Whether to opt in to preview features
        project_id : str, optional
            Project ID to associate the cluster with
        wait_on_active : bool, optional
            Wait for the cluster to be active before returning
        wait_interval : int, optional
            Number of seconds between each polling interval
        wait_timeout : int, optional
            Maximum number of seconds to wait before raising an exception

        Returns
        -------
        :class:`Cluster`

        """
        if isinstance(region, Region):
            provider = provider or region.provider
            region_name = region_name or region.region_name or region.name
        elif region is not None:
            region_name = region_name or region

        size_spec: Optional[Dict[str, Any]] = None
        if size is not None or scale_factor is not None:
            size_spec = {
                k: v for k, v in dict(
                    size=size, scaleFactor=scale_factor,
                ).items() if v is not None
            }

        res = self._post(
            'clusters', json={
                k: v for k, v in dict(
                    name=name,
                    provider=provider,
                    region=region_name,
                    size=size_spec,
                    firewallRanges=firewall_ranges,
                    allowAllTraffic=allow_all_traffic,
                    adminPassword=admin_password,
                    autoSuspend=snake_to_camel_dict(auto_suspend),
                    autoScale=snake_to_camel_dict(auto_scale),
                    cacheConfig=cache_config,
                    deploymentType=deployment_type,
                    expiresAt=expires_at,
                    updateWindow=snake_to_camel_dict(update_window),
                    kai=kai,
                    multiAZ=multi_az,
                    optInPreviewFeature=opt_in_preview_feature,
                    projectID=project_id,
                ).items() if v is not None
            },
        )
        out = self.get_cluster(res.json()['clusterID'])
        if wait_on_active:
            out = self._wait_on_state(
                out, 'ACTIVE', interval=wait_interval, timeout=wait_timeout,
            )
            # After the cluster is active, wait for the endpoint to be ready
            out = self._wait_on_endpoint(
                out, interval=wait_interval, timeout=wait_timeout,
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

    def get_starter_cluster(self, id: str) -> StarterCluster:
        """
        Retrieve a starter cluster definition.

        Parameters
        ----------
        id : str
            ID of the starter cluster

        Returns
        -------
        :class:`StarterCluster`

        """
        res = self._get(f'{SHAREDTIER_PATH}/{id}')
        return StarterCluster.from_dict(res.json(), manager=self)

    def create_starter_cluster(
        self,
        name: str,
        database_name: str,
        provider: str,
        region_name: str,
        project_id: Optional[str] = None,
    ) -> StarterCluster:
        """
        Create a new starter (shared tier) cluster.

        Parameters
        ----------
        name : str
            Name of the starter cluster
        database_name : str
            Name of the database for the starter cluster
        provider : str
            Cloud provider for the starter cluster (e.g., 'aws', 'gcp', 'azure')
        region_name : str
            Cloud provider region for the starter cluster (e.g., 'us-east-1')
        project_id : str, optional
            Project ID to associate the starter cluster with

        Returns
        -------
        :class:`StarterCluster`

        """
        payload: Dict[str, Any] = {
            'name': name,
            'databaseName': database_name,
            'provider': provider,
            'regionName': region_name,
        }
        if project_id is not None:
            payload['projectID'] = project_id

        res = self._post(SHAREDTIER_PATH, json=payload)
        cluster_id = res.json().get('virtualClusterID')
        if not cluster_id:
            raise ManagementError(msg='No virtualClusterID returned from API')

        return self.get_starter_cluster(cluster_id)

    @property
    def shared_tier_regions(self) -> NamedList[Region]:
        """
        Return a list of regions that support starter clusters.

        .. warning:: Not available at v2. ``GET /v2/regions/sharedtier``
           returns ``404 page not found`` and no alternate spelling responds.

        """
        raise ManagementError(
            msg='Listing shared tier regions is not supported by management '
                'API v2; there is no v2 equivalent of '
                'GET /v1/regions/sharedtier.',
        )
