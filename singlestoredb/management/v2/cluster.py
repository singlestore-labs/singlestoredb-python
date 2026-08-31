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
import re
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

from .. import timing
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

#: Shape of a project ID. Anywhere a project can be named, a name is accepted
#: in place of an ID, and this is how the two are told apart. Sending a project
#: ID that is not a UUID comes back as ``400 uuid: incorrect UUID length``, so
#: a value that does not match this could never have been a valid ID and
#: nothing is lost by reading it as a name.
PROJECT_ID_RE = re.compile(
    r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}'
    r'-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$',
)


def _project_from_id(
    manager: 'ClusterManager',
    project_id: Optional[str],
) -> Optional[Project]:
    """
    Return the project with the given ID, as reported by ``manager``.

    A deployment reports only its ``projectID``, so the rest of the project is
    recovered from :attr:`ClusterManager.projects` -- a cached list, so this
    costs nothing per deployment after the first. An ID that matches no project
    still yields a :class:`Project`, carrying the ID and nothing else, so that
    ``cluster.project.id`` is always readable.
    """
    if project_id is None:
        return None
    return next(
        (x for x in manager.projects if x.id == project_id),
        Project(id=project_id, name='<unknown>'),
    )


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
        A cluster object, or the name or ID of a cluster. If not given,
        ``SINGLESTOREDB_WORKSPACE`` is used: the notebook environment publishes
        no ``SINGLESTOREDB_CLUSTER``, and that variable carries the cluster ID
        at v2 just as it carried the workspace ID at v1.
        ``SINGLESTOREDB_WORKSPACE_GROUP`` is *not* consulted -- it holds a group
        ID, which v2 reports only as the read-only :attr:`Cluster.group` and
        offers no route to look up.

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
    if 'SINGLESTOREDB_WORKSPACE' in os.environ:
        return mgr.clusters[os.environ['SINGLESTOREDB_WORKSPACE']]
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
    group: Optional[str]
    size: Optional[str]
    scale_factor: Optional[float]
    state: str
    created_at: Optional[datetime.datetime]
    terminated_at: Optional[datetime.datetime]
    expires_at: Optional[datetime.datetime]
    last_resumed_at: Optional[datetime.datetime]
    endpoint: Optional[str]
    provider: Optional[str]
    region: Optional[Region]
    project: Optional[Project]
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
        group: Optional[str] = None,
        size: Optional[str] = None,
        scale_factor: Optional[float] = None,
        created_at: Optional[Union[str, datetime.datetime]] = None,
        terminated_at: Optional[Union[str, datetime.datetime]] = None,
        expires_at: Optional[Union[str, datetime.datetime]] = None,
        last_resumed_at: Optional[Union[str, datetime.datetime]] = None,
        endpoint: Optional[str] = None,
        provider: Optional[str] = None,
        region: Union[str, Region, None] = None,
        project: Union[str, Project, None] = None,
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

        #: Unique ID of the group the cluster belongs to. v2 has no group
        #: route, so this is an opaque ID rather than a lookup key.
        self.group = group

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

        #: Region the cluster is deployed in. Unlike v1, v2 does not report a
        #: region ID; a region is identified by the
        #: ``(provider, region_name)`` pair. A string is taken as the provider
        #: region name, e.g., ``us-east-1``; :meth:`from_dict` resolves it
        #: against :attr:`ClusterManager.regions` so that the display name is
        #: filled in too.
        if isinstance(region, str):
            region = Region(
                name=region,
                provider=provider or '<unknown>',
                region_name=region,
            )
        self.region = region

        #: Project the cluster belongs to. A string is taken as the project
        #: ID; :meth:`from_dict` resolves it against
        #: :attr:`ClusterManager.projects` so that the name and edition are
        #: filled in too.
        if isinstance(project, str):
            project = Project(id=project, name='<unknown>')
        self.project = project

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
        #
        # The rename of this field to ``sizeConfig`` shipped on 2026-08-26, was
        # backed out the next morning, and landed again by 2026-08-28, when
        # ``POST /v2/clusters`` began answering ``400 request body contains an
        # unknown field "size"``. The request bodies below send ``sizeConfig``
        # accordingly; both keys are read here, since a field that has already
        # been reverted once may be reverted again, and a response carrying the
        # other name would otherwise silently leave
        # :attr:`Cluster.size` as None. The ``size`` argument and
        # :attr:`Cluster.size` are wrapper-side names either way.
        size_spec = obj.get('sizeConfig') or obj.get('size') or {}

        # v2 reports the provider region name and no region ID, so the region
        # is matched on the ``(provider, region_name)`` pair to recover the
        # display name. An unmatched region still yields a Region, built from
        # what the cluster itself reports.
        provider = obj.get('provider')
        region_name = obj.get('region')
        region: Optional[Region] = None
        if region_name is not None:
            region = next(
                (
                    x for x in manager.regions
                    if x.region_name == region_name and x.provider == provider
                ),
                None,
            )
            if region is None:
                region = Region(
                    name=region_name,
                    provider=provider or '<unknown>',
                    region_name=region_name,
                )

        out = cls(
            name=obj['name'],
            id=obj['clusterID'],
            state=obj.get('state', 'Unknown'),
            group=obj.get('groupID'),
            size=size_spec.get('size'),
            scale_factor=size_spec.get('scaleFactor'),
            created_at=obj.get('createdAt'),
            terminated_at=obj.get('terminatedAt'),
            expires_at=obj.get('expiresAt'),
            last_resumed_at=obj.get('lastResumedAt'),
            endpoint=obj.get('endpoint'),
            provider=provider,
            region=region,
            project=_project_from_id(manager, obj.get('projectID')),
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
            Sent nested in a ``size`` object alongside ``scale_factor``.
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
            List of allowed CIDR ranges. An empty list denies all inbound
            traffic; omitting it leaves the current ranges alone.
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
                # ``sizeConfig``, not ``size``; see Cluster.from_dict.
                sizeConfig=size_spec,
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
            remaining = float(wait_timeout)
            while True:
                started_at = timing.now()
                self.refresh()
                if self.terminated_at is not None:
                    break
                if remaining <= 0:
                    raise ManagementError(
                        msg='Exceeded waiting time for Cluster to terminate',
                    )
                timing.sleep(wait_interval, 'cluster terminated')
                # Charged by measured time, so the refresh above counts against
                # the timeout too. See timing.poll_cost.
                remaining -= timing.poll_cost(started_at, wait_interval)

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
    project: Optional[Project]

    def __init__(
        self,
        name: str,
        id: str,
        database_name: str,
        endpoint: Optional[str] = None,
        mysql_dml_port: Optional[int] = None,
        websocket_port: Optional[int] = None,
        project: Union[str, Project, None] = None,
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

        #: Project the starter cluster belongs to. A string is taken as the
        #: project ID; :meth:`from_dict` resolves it against
        #: :attr:`ClusterManager.projects` so that the name and edition are
        #: filled in too.
        if isinstance(project, str):
            project = Project(id=project, name='<unknown>')
        self.project = project

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
            project=_project_from_id(manager, obj.get('projectID')),
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

    @ttl_property(datetime.timedelta(hours=1))
    def projects(self) -> NamedList[Project]:
        """
        Return a list of projects in the current organization.

        Cached like :attr:`regions`, because every :class:`Cluster` built by
        :meth:`Cluster.from_dict` resolves its project against this list and
        listing clusters would otherwise cost a ``GET /v2/projects`` per
        cluster.
        """
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

        waited = 0.0
        remaining = float(timeout)
        while not done(out):
            if remaining <= 0:
                wanted = 'to become {}'.format(expected) \
                    if expected is not None else 'to be applied'
                raise ManagementError(
                    msg=f'Exceeded waiting time for the firewall of cluster '
                        f'{out.id} {wanted} ({waited:.0f}s); it reports '
                        f'firewall_ranges={out.firewall_ranges!r}, '
                        f'allow_all_traffic={out.allow_all_traffic!r}. While '
                        'the firewall admits nothing the endpoint refuses all '
                        'inbound connections.',
                )
            started_at = timing.now()
            timing.sleep(interval, 'cluster firewall')
            out = self.get_cluster(out.id)
            # Measured, and charged after the refetch, so a slow or retried GET
            # counts against the timeout. See timing.poll_cost.
            cost = timing.poll_cost(started_at, interval)
            remaining -= cost
            waited += cost

        return out

    def _project_id_for(self, name_or_id: Union[str, Project]) -> str:
        """
        Return the ID of the project named by ``name_or_id``.

        A :class:`Project` is reduced to its ID. A UUID is taken as an ID and
        returned untouched, which keeps an explicit ID free of a
        ``GET /v2/projects`` round trip. Anything else is matched against the
        project names in the current organization. The API does not promise
        that names are unique, so an ambiguous name raises rather than picking
        the first match.

        Parameters
        ----------
        name_or_id : str or Project
            Project, or project name or ID

        Returns
        -------
        str

        Raises
        ------
        ManagementError
            If the name matches no project, or more than one

        """
        if isinstance(name_or_id, Project):
            return name_or_id.id

        if PROJECT_ID_RE.match(name_or_id):
            return name_or_id

        projects = self.projects
        matches = [x for x in projects if x.name == name_or_id]

        if not matches:
            raise ManagementError(
                msg=f'No project named {name_or_id!r} exists in the current '
                    'organization. Its projects are: ' +
                    (
                        ', '.join(f'{x.name} ({x.id})' for x in projects)
                        or 'none'
                    ) + '.',
            )

        if len(matches) > 1:
            raise ManagementError(
                msg=f'More than one project is named {name_or_id!r}; use an ID '
                    'instead. The matching IDs are: ' +
                    ', '.join(x.id for x in matches) + '.',
            )

        return matches[0].id

    def _resolve_project_id(
        self,
        project: Union[str, Project, None] = None,
    ) -> str:
        """
        Return the project ID a new deployment should be created in.

        ``POST /v2/clusters`` requires ``projectID``, where the v1 workspace
        group route assigned one implicitly. In priority order: the project
        named by the caller, the ``SINGLESTOREDB_PROJECT`` environment variable
        the notebook environment sets, or the organization's only project. An
        organization with more than one project has no default -- naming the
        candidates is more useful than picking one.

        The caller may give a :class:`Project`, a project name or a project ID,
        and the environment variable either a name or an ID; see
        :meth:`_project_id_for`.

        Parameters
        ----------
        project : str or Project, optional
            Project, or project name or ID, supplied by the caller

        Returns
        -------
        str

        Raises
        ------
        ManagementError
            If no project ID can be determined

        """
        if project:
            return self._project_id_for(project)

        from_env = os.environ.get('SINGLESTOREDB_PROJECT')
        if from_env:
            return self._project_id_for(from_env)

        projects = self.projects
        if len(projects) == 1:
            return projects[0].id

        if not projects:
            raise ManagementError(
                msg='A project is required to create a cluster, but the '
                    'current organization reports no projects.',
            )

        raise ManagementError(
            msg='A project is required to create a cluster and the current '
                'organization has more than one. Pass project= or set the '
                'SINGLESTOREDB_PROJECT environment variable to the name or ID '
                'of one of: ' +
                ', '.join(f'{x.name} ({x.id})' for x in projects) + '.',
        )

    def create_cluster(
        self,
        name: str,
        region: Union[str, Region, None] = None,
        provider: Optional[str] = None,
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
        project: Union[str, Project, None] = None,
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
            Region to create the cluster in. A :class:`Region` supplies both
            halves of the ``(provider, region_name)`` pair v2 identifies a
            region by; a string is taken as the provider region name, e.g.,
            ``us-east-1``, and needs ``provider`` alongside it. v2 has no
            region IDs.
        provider : str, optional
            Cloud provider for the cluster (AWS | GCP | Azure). Only needed
            when ``region`` is a string; a :class:`Region` carries its own,
            which this overrides if both are given.
        size : str, optional
            Cluster size in cluster size notation (S-00, S-1, etc.). Sent
            nested in a ``size`` object alongside ``scale_factor``.
        scale_factor : float, optional
            Scale factor for the cluster
        firewall_ranges : List[str], optional
            List of allowed CIDR ranges. An empty list denies all inbound
            traffic, which is also what is sent when this is not given:
            ``POST /v2/clusters`` rejects a null ``firewallRanges`` outright,
            so there is no way to leave the choice to the server.
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
        project : str or Project, optional
            Project to create the cluster in. A :class:`Project` is reduced to
            its ID; a string that is not a UUID is looked up as a name.
            Required by the API; if it is not
            given it is resolved by :meth:`_resolve_project_id` from the
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
        region_name: Optional[str] = None
        if isinstance(region, Region):
            provider = provider or region.provider
            region_name = region.region_name or region.name
        elif region is not None:
            region_name = region

        project_id = self._resolve_project_id(project)

        # POST /v2/clusters rejects a null firewallRanges -- "indicate empty
        # list [] to disallow all inbound traffic" -- so the field cannot be
        # dropped the way every other unset field is. Deny-all is the only
        # safe default for a cluster nobody asked to expose.
        if firewall_ranges is None:
            firewall_ranges = []

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
                    # ``sizeConfig``, not ``size``; see Cluster.from_dict.
                    sizeConfig=size_spec,
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
        provider: Optional[str] = None,
        region: Union[str, Region, None] = None,
        project: Union[str, Project, None] = None,
    ) -> StarterCluster:
        """
        Create a new starter (shared tier) cluster.

        Parameters
        ----------
        name : str
            Name of the starter cluster
        database_name : str
            Name of the database for the starter cluster
        provider : str, optional
            Cloud provider for the starter cluster (AWS | GCP | Azure). Any
            capitalization is accepted; see below. Only needed when ``region``
            is a string; a :class:`Region` carries its own, which this
            overrides if both are given.
        region : str or Region
            Region to create the starter cluster in. A :class:`Region` supplies
            both the provider and the provider region name; a string is taken
            as the provider region name (e.g., 'us-east-1') and needs
            ``provider`` alongside it. See :attr:`shared_tier_regions` for the
            regions this route accepts.
        project : str or Project, optional
            Project to associate the starter cluster with. A :class:`Project`
            is reduced to its ID; a string that is not a UUID is looked up as a
            name. Unlike
            :meth:`create_cluster` this route does not require one, so nothing
            is resolved when it is omitted.

        Returns
        -------
        :class:`StarterCluster`

        """
        region_name: Optional[str] = None
        if isinstance(region, Region):
            provider = provider or region.provider
            region_name = region.region_name or region.name
        elif region is not None:
            region_name = region

        if not provider or not region_name:
            raise ValueError(
                'a provider and a region name are required; pass a Region, '
                'or a provider region name together with provider=',
            )

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
        if project is not None:
            payload['projectID'] = self._project_id_for(project)

        res = self._post(SHAREDTIER_PATH, json=payload)
        cluster_id = res.json().get('virtualClusterID')
        if not cluster_id:
            raise ManagementError(msg='No virtualClusterID returned from API')

        return self.get_starter_cluster(cluster_id)

    @property
    def shared_tier_regions(self) -> NamedList[Region]:
        """
        Return a list of regions that support starter clusters.

        ``GET /v2/regions/sharedtier`` answers with the same shape as
        ``GET /v2/regions`` (verified live 2026-08-24), so this returns
        :class:`Region` objects just like :attr:`regions`.

        """
        res = self._get('regions/sharedtier')
        return NamedList([Region.from_dict(item, self) for item in res.json()])
