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
from .project import Project as Project

#: Base management API path for the shared-tier resource.
SHAREDTIER_PATH = 'sharedtier/virtualClusters'

#: Environment variable naming the project new deployments belong to. Set by
#: the SingleStore notebook environment; also read by the v1 inference API
#: wrapper, so the name is shared rather than v2-specific.
PROJECT_ENV_VAR = 'SINGLESTOREDB_PROJECT'

#: Environment variables that name the deployment the current process is
#: running against, in priority order. These are set by the SingleStore
#: notebook environment and are part of its external contract, so they keep
#: their published names regardless of API version.
CLUSTER_ENV_VARS = ('SINGLESTOREDB_CLUSTER', 'SINGLESTOREDB_WORKSPACE')


def get_organization() -> Organization:
    """Get the organization."""
    from ..cluster import manage_clusters
    # Pinned: these helpers are the v2 module's own, so they must not follow
    # the management.version option out of v2.
    return manage_clusters(version='v2').organization


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
    mgr = manage_clusters(version='v2')
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

        # Set by ClusterManager.create_cluster only; see the admin_password
        # property. Private so it stays out of str() / repr().
        self._admin_password: Optional[str] = None

    @property
    def admin_password(self) -> Optional[str]:
        """
        Generated password for the ``admin`` database user.

        ``POST /v2/clusters`` generates the admin password itself and returns it
        in the create response -- the ``admin_password`` passed to
        :meth:`ClusterManager.create_cluster` is ignored -- and no other route
        reports it. So this is set on the cluster returned by ``create_cluster``
        and is ``None`` everywhere else, including after :meth:`refresh`. Record
        it when the cluster is created or it cannot be recovered.

        """
        return self._admin_password

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
        wait_on_active: bool = False,
        wait_interval: int = 10,
        wait_timeout: int = 600,
    ) -> None:
        """
        Update the cluster definition.

        Both the compute settings (size, auto-suspend, cache) and the
        deployment-wide settings (firewall, update window, expiration) are
        changed through this one call.

        The API applies the ``PATCH`` asynchronously: the cluster cycles back
        through PENDING and the trailing :meth:`refresh` still reports the
        pre-PATCH values. Pass ``wait_on_active=True`` to wait the change out
        so the object reflects it on return.

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
        wait_on_active : bool, optional
            Wait for the cluster to be ACTIVE again -- and, if a firewall was
            requested, for the new ranges to be reported -- before returning.
            Defaults to ``False``, which returns as soon as the ``PATCH`` is
            accepted and therefore reports pre-PATCH values.
        wait_interval : int, optional
            Number of seconds between each server check
        wait_timeout : int, optional
            Maximum number of seconds to wait before raising an exception

        Raises
        ------
        ManagementError
            If ``wait_on_active`` is given and the timeout is reached

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

        if wait_on_active:
            out = manager._wait_on_state(
                manager.get_cluster(self.id), 'ACTIVE',
                interval=wait_interval, timeout=wait_timeout,
            )
            if firewall_ranges or allow_all_traffic:
                manager._wait_on_firewall(
                    out, interval=wait_interval, timeout=wait_timeout,
                    expected=firewall_ranges,
                )

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

    @property
    def projects(self) -> NamedList[Project]:
        """Return a list of projects in the current organization."""
        res = self._get('projects')
        return NamedList([Project.from_dict(item, self) for item in res.json()])

    def get_project(self, id: str) -> Project:
        """
        Retrieve a project definition.

        Parameters
        ----------
        id : str
            ID of the project

        Returns
        -------
        :class:`Project`

        """
        res = self._get(f'projects/{id}')
        return Project.from_dict(res.json(), manager=self)

    def _wait_on_firewall(
        self,
        out: Cluster,
        interval: int = 10,
        timeout: int = 600,
        expected: Optional[List[str]] = None,
    ) -> Cluster:
        """
        Wait until the cluster reports the firewall that was asked for.

        ``POST /v2/clusters`` and ``PATCH /v2/clusters/{id}`` apply
        ``firewallRanges`` asynchronously and outside the state machine: the
        cluster reaches ACTIVE with a resolvable endpoint while
        ``GET /v2/clusters/{id}`` still reports ``firewallRanges: []`` and
        ``allowAllTraffic: null``. That combination denies all inbound traffic,
        so a connection attempt in that window times out at the TCP level
        rather than failing authentication.

        By default the wait is for the cluster to admit *anything* -- either
        non-empty ``firewall_ranges`` or ``allow_all_traffic`` -- rather than
        for set-equality with the ranges that were requested, because the
        server normalizes: verified live, ``firewallRanges: ['0.0.0.0/0']``
        comes back as ``allowAllTraffic: True`` with ``firewallRanges: []``,
        and that cluster accepts connections. Admitting something is the
        property that actually matters on a fresh cluster -- it is the
        difference between deny-all and reachable.

        On an *existing* cluster that already admits traffic, that says
        nothing. Pass ``expected`` there to wait for the specific ranges
        instead; a requested ``0.0.0.0/0`` is also satisfied by
        ``allow_all_traffic``, which is how the server stores it.

        This lives in the v2 package rather than in
        :class:`~singlestoredb.management.manager.Manager` because it is a v2
        API quirk; the v1 workspace path must not be affected.

        Parameters
        ----------
        out : Cluster
            Cluster to poll
        interval : int, optional
            Number of seconds between each server poll
        timeout : int, optional
            Maximum number of seconds to wait before raising an exception
        expected : List[str], optional
            Wait for exactly these ranges (compared as a set) rather than for
            the firewall to admit anything at all

        Raises
        ------
        ManagementError
            If timeout is reached

        Returns
        -------
        :class:`Cluster`

        """
        def done(cluster: Cluster) -> bool:
            if expected is not None:
                if set(cluster.firewall_ranges or []) == set(expected):
                    return True
                # The server stores a requested 0.0.0.0/0 as allowAllTraffic
                # and leaves firewallRanges empty.
                return bool(cluster.allow_all_traffic) \
                    and set(expected) == {'0.0.0.0/0'}
            return bool(cluster.firewall_ranges) \
                or bool(cluster.allow_all_traffic)

        waited = 0
        while not done(out):
            if timeout <= 0:
                wanted = 'to become {}'.format(expected) \
                    if expected is not None else 'to be applied'
                raise ManagementError(
                    msg=f'Exceeded waiting time for the firewall of cluster '
                        f'{out.id} {wanted} ({waited}s); it reports '
                        f'firewall_ranges={out.firewall_ranges!r}, '
                        f'allow_all_traffic={out.allow_all_traffic!r}. While '
                        'the firewall admits nothing the endpoint refuses all '
                        'inbound connections.',
                )
            time.sleep(interval)
            timeout -= interval
            waited += interval
            out = self.get_cluster(out.id)

        return out

    def _resolve_project_id(self, project_id: Optional[str] = None) -> str:
        """
        Return the project ID a new deployment should be created in.

        ``POST /v2/clusters`` requires ``projectID``, where the v1 workspace
        group route assigned one implicitly. In priority order: the ID passed
        by the caller, the :data:`PROJECT_ENV_VAR` environment variable, or the
        organization's only project. An organization with more than one project
        has no default -- naming the candidates is more useful than picking one.

        Parameters
        ----------
        project_id : str, optional
            Project ID supplied by the caller

        Returns
        -------
        str

        Raises
        ------
        ManagementError
            If no project ID can be determined

        """
        if project_id:
            return project_id

        from_env = os.environ.get(PROJECT_ENV_VAR)
        if from_env:
            return from_env

        projects = self.projects
        if len(projects) == 1:
            return projects[0].id

        if not projects:
            raise ManagementError(
                msg='A project ID is required to create a cluster, but the '
                    'current organization reports no projects.',
            )

        raise ManagementError(
            msg='A project ID is required to create a cluster and the current '
                'organization has more than one project. Pass project_id= or '
                f'set the {PROJECT_ENV_VAR} environment variable to one of: ' +
                ', '.join(f'{x.name} ({x.id})' for x in projects) + '.',
        )

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
            Admin password for the cluster.

            .. warning:: v2 ignores this. ``POST /v2/clusters`` generates the
               admin password regardless of what is sent and returns the
               generated value, so read
               :attr:`Cluster.admin_password` off the returned cluster instead
               -- it is reported there and nowhere else. The field is still sent
               in case the API starts honoring it.
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
            Project ID to create the cluster in. Required by the API; if it is
            not given it is resolved by :meth:`_resolve_project_id` from the
            ``SINGLESTOREDB_PROJECT`` environment variable or from the
            organization's only project.
        wait_on_active : bool, optional
            Wait for the cluster to be usable before returning: first for the
            state to become ACTIVE, then for the endpoint, then -- if a
            firewall was requested -- for the firewall to be applied. The
            firewall is included because the API applies it asynchronously and
            outside the state machine, so an ACTIVE cluster with a resolvable
            endpoint still refuses every inbound connection until the ranges
            land. See :meth:`_wait_on_firewall`.
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

        project_id = self._resolve_project_id(project_id)

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
        body = res.json()
        out = self.get_cluster(body['clusterID'])
        if wait_on_active:
            out = self._wait_on_state(
                out, 'ACTIVE', interval=wait_interval, timeout=wait_timeout,
            )
            # After the cluster is active, wait for the endpoint to be ready
            out = self._wait_on_endpoint(
                out, interval=wait_interval, timeout=wait_timeout,
            )
            # ...and the endpoint refuses everything until the firewall lands.
            # Only when a firewall was actually asked for: firewall_ranges=[]
            # is a legitimate deny-all request and must not hang waiting for a
            # non-empty value that is never coming.
            if firewall_ranges or allow_all_traffic:
                out = self._wait_on_firewall(
                    out, interval=wait_interval, timeout=wait_timeout,
                )
        # The API generates the admin password and reports it here and nowhere
        # else, and every wait above re-fetches the cluster, so this assignment
        # must stay after all of them: carry the password over onto whichever
        # object is being returned. See Cluster.admin_password.
        out._admin_password = body.get('adminPassword')
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
            Cloud provider for the starter cluster (AWS | GCP | Azure). Any
            capitalization is accepted; see below.
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
            # The shared-tier route accepts only the exact spellings AWS,
            # AZURE and GCP: anything else, including the mixed-case 'Azure'
            # that GET /v2/regions itself reports, fails with
            # '500 Unspecified is not a valid CloudServiceProvider'.
            # POST /v2/clusters is case-insensitive, so this is local to here.
            'provider': provider.upper(),
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
