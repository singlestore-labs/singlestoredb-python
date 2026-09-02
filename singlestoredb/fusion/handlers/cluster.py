#!/usr/bin/env python3
"""
Fusion SQL handlers for the management API v2 cluster vocabulary.

Kept separate from :mod:`singlestoredb.fusion.handlers.workspace` on purpose.
That module is pinned to v1 through :func:`get_workspace_manager` and this one
is pinned to v2 through :func:`get_cluster_manager`; mixing two API versions in
one module invites reaching for the wrong manager. Keeping them apart also
makes retiring the v1 surface a matter of deleting a file.

The two vocabularies coexist: v1's nested ``WorkspaceGroup``/``Workspace`` pair
and v2's single flat ``Cluster``. A cluster is created in one statement, where
a workspace needed two, and v2 has no region IDs -- so there is deliberately no
``IN REGION ID`` alternate here, unlike ``CREATE WORKSPACE GROUP``.
"""
import json
from typing import Any
from typing import Dict
from typing import Optional

from .. import result
from ..handler import SQLHandler
from ..result import FusionSQLResult
from .utils import dt_isoformat
from .utils import get_cluster
from .utils import get_cluster_manager
from .utils import get_project
from .utils import get_starter_cluster

#: Seconds per unit for the ``AUTO SUSPEND AFTER`` clause.
_SUSPEND_UNIT_SECONDS = dict(
    SECONDS=1,
    MINUTES=60,
    HOURS=60 * 60,
    DAYS=60 * 60 * 24,
)


def _auto_suspend(params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Convert an ``AUTO SUSPEND AFTER`` clause to API parameters."""
    if not params.get('auto_suspend'):
        return None
    # The clause parses to one flat dict, not a list of one dict per
    # sub-rule. CreateWorkspaceHandler indexes it as a list, which is why
    # ``CREATE WORKSPACE ... AUTO SUSPEND`` raises; do not copy that.
    clause = params['auto_suspend']
    units = clause['suspend_after_units'].upper()
    return dict(
        suspend_after_seconds=(
            clause['suspend_after_value'] * _SUSPEND_UNIT_SECONDS[units]
        ),
        suspend_type=clause['suspend_type'].upper(),
    )


def _update_window(params: Dict[str, Any]) -> Optional[Dict[str, int]]:
    """Convert a ``WITH UPDATE WINDOW '<day>:<hour>'`` clause to a dict."""
    if not params.get('with_update_window'):
        return None
    day, hour = params['with_update_window'].split(':', 1)
    return dict(day=int(day), hour=int(hour))


def _cluster_region(cluster: Any) -> Optional[str]:
    """Return a cluster's provider region name, e.g. ``us-east-1``."""
    region = cluster.region
    if region is None:
        return None
    return region.region_name or region.name


def _cluster_project_id(cluster: Any) -> Optional[str]:
    """Return the ID of the project a deployment belongs to."""
    project = cluster.project
    if project is None:
        return None
    return project.id


def _resolve_region(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Resolve an ``IN REGION`` clause to ``create_cluster`` keywords.

    v2 has no region IDs, so a region is identified by its
    ``(provider, region_name)`` pair. ``GET /v2/regions`` reports both a
    display name (``region``, e.g. ``US East 1 (N. Virginia)``) and a provider
    slug (``regionName``, e.g. ``us-east-1``), and a cluster's own ``region``
    field is the *slug* -- so a display name has to be translated before it is
    posted. Matching accepts either spelling.

    An unmatched literal is passed through untouched rather than rejected: the
    region list is cached, and the API gives a clearer error for an unknown
    region than a stale local list can.
    """
    region_name = params['in_region']['region_name']
    provider = params.get('with_provider') or None

    manager = get_cluster_manager()
    matches = [
        x for x in manager.regions
        if region_name in (x.name, x.region_name)
    ]
    if provider:
        matches = [
            x for x in matches
            if (x.provider or '').upper() == provider.upper()
        ]

    if len(matches) > 1:
        found = ', '.join(
            f'{x.provider} {x.region_name}' for x in matches
        )
        raise ValueError(
            f'more than one region matches "{region_name}": {found}; '
            'use the WITH PROVIDER clause to select one',
        )

    if matches:
        return dict(
            provider=matches[0].provider,
            region=matches[0].region_name,
        )

    # Unknown to the cached region list; let the API rule on it.
    return dict(provider=provider, region=region_name)


class ShowClustersHandler(SQLHandler):
    """
    SHOW CLUSTERS [ <like> ]
        [ <extended> ] [ <order-by> ]
        [ <limit> ];

    Description
    -----------
    Displays information on clusters. A cluster is the flat deployment
    resource of management API v2, replacing the v1 pairing of a workspace
    group with the workspaces inside it.

    Arguments
    ---------
    * ``<pattern>``: A pattern similar to SQL LIKE clause.
      Uses ``%`` as the wildcard character.

    Remarks
    -------
    * Use the ``LIKE`` clause to specify a pattern and return only the
      clusters that match the specified pattern.
    * The ``LIMIT`` clause limits the number of results to the
      specified number.
    * Use the ``ORDER BY`` clause to sort the results by the specified
      key. By default, the results are sorted in the ascending order.
    * To return more information about the clusters, use the
      ``EXTENDED`` clause.

    Example
    -------
    The following command displays a list of clusters with names that
    match the specified pattern::

        SHOW CLUSTERS LIKE 'analytics%' EXTENDED ORDER BY Name;

    See Also
    --------
    * ``SHOW STARTER CLUSTERS``
    * ``CREATE CLUSTER``

    """

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        manager = get_cluster_manager()

        res = FusionSQLResult()
        res.add_field('Name', result.STRING)
        res.add_field('ID', result.STRING)
        res.add_field('Region', result.STRING)
        res.add_field('Size', result.STRING)
        res.add_field('State', result.STRING)

        if params['extended']:
            res.add_field('Provider', result.STRING)
            res.add_field('Endpoint', result.STRING)
            res.add_field('DeploymentType', result.STRING)
            res.add_field('FirewallRanges', result.JSON)
            res.add_field('ProjectID', result.STRING)
            res.add_field('CreatedAt', result.DATETIME)
            res.add_field('TerminatedAt', result.DATETIME)

            def fields(x: Any) -> Any:
                return (
                    x.name, x.id, _cluster_region(x), x.size, x.state,
                    x.provider, x.endpoint, x.deployment_type,
                    json.dumps(x.firewall_ranges or []),
                    _cluster_project_id(x),
                    dt_isoformat(x.created_at),
                    dt_isoformat(x.terminated_at),
                )
        else:
            def fields(x: Any) -> Any:
                # Report the provider slug, not the region's display name.
                return (x.name, x.id, _cluster_region(x), x.size, x.state)

        res.set_rows([fields(x) for x in manager.clusters])

        if params['like']:
            res = res.like(Name=params['like'])

        return res.order_by(**params['order_by']).limit(params['limit'])


ShowClustersHandler.register(overwrite=True)


class ShowClusterRegionsHandler(SQLHandler):
    """
    SHOW CLUSTER REGIONS [ <like> ]
        [ <order-by> ]
        [ <limit> ];

    Description
    -----------
    Returns the regions available for creating clusters.

    Arguments
    ---------
    * ``<pattern>``: A pattern similar to SQL LIKE clause.
      Uses ``%`` as the wildcard character.

    Remarks
    -------
    * Use the ``LIKE`` clause to specify a pattern and return only the
      regions that match the specified pattern.
    * The ``LIMIT`` clause limits the number of results to the
      specified number.
    * Use the ``ORDER BY`` clause to sort the results by the specified
      key. By default, the results are sorted in the ascending order.
    * There is no ``ID`` column. Management API v2 assigns no region IDs;
      a region is identified by its provider and region name, which is why
      ``CREATE CLUSTER`` has no ``IN REGION ID`` clause.
    * ``Name`` is the display name, for example
      ``US East 1 (N. Virginia)``. ``RegionName`` is the cloud provider's
      own name for it, for example ``us-east-1``. Either may be given to
      ``CREATE CLUSTER``.

    Example
    -------
    The following command returns the regions in the US, sorted by name::

        SHOW CLUSTER REGIONS LIKE 'US%' ORDER BY Name;

    See Also
    --------
    * ``SHOW REGIONS``, the management API v1 equivalent, which reports an
      ``ID`` column.

    """

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        manager = get_cluster_manager()

        res = FusionSQLResult()
        res.add_field('Name', result.STRING)
        res.add_field('Provider', result.STRING)
        res.add_field('RegionName', result.STRING)

        res.set_rows([
            (x.name, x.provider, x.region_name)
            for x in manager.regions
        ])

        if params['like']:
            res = res.like(Name=params['like'])

        return res.order_by(**params['order_by']).limit(params['limit'])


ShowClusterRegionsHandler.register(overwrite=True)


class ShowProjectsHandler(SQLHandler):
    """
    SHOW PROJECTS [ <like> ]
        [ <order-by> ]
        [ <limit> ];

    Description
    -----------
    Displays the projects in the current organization.

    Arguments
    ---------
    * ``<pattern>``: A pattern similar to SQL LIKE clause.
      Uses ``%`` as the wildcard character.

    Remarks
    -------
    * Use the ``LIKE`` clause to specify a pattern and return only the
      projects that match the specified pattern.
    * The ``LIMIT`` clause limits the number of results to the
      specified number.
    * Use the ``ORDER BY`` clause to sort the results by the specified
      key. By default, the results are sorted in the ascending order.
    * Projects cannot be created or dropped from Fusion SQL. This command
      exists so that the project required by ``CREATE CLUSTER`` can be
      discovered.
    * ``CREATE CLUSTER`` needs a project. If the organization has exactly
      one, it is used automatically; otherwise name one with the
      ``IN PROJECT`` clause.

    Example
    -------
    The following command displays the projects in the current
    organization::

        SHOW PROJECTS ORDER BY Name;

    See Also
    --------
    * ``CREATE CLUSTER``

    """

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        manager = get_cluster_manager()

        res = FusionSQLResult()
        res.add_field('Name', result.STRING)
        res.add_field('ID', result.STRING)
        res.add_field('Edition', result.STRING)
        res.add_field('CreatedAt', result.DATETIME)

        res.set_rows([
            (x.name, x.id, x.edition, dt_isoformat(x.created_at))
            for x in manager.projects
        ])

        if params['like']:
            res = res.like(Name=params['like'])

        return res.order_by(**params['order_by']).limit(params['limit'])


ShowProjectsHandler.register(overwrite=True)


class CreateClusterHandler(SQLHandler):
    """
    CREATE CLUSTER [ if_not_exists ] cluster_name
        in_region
        [ with_provider ]
        [ in_project ]
        [ with_size ]
        [ using_scale_factor ]
        [ auto_suspend ]
        [ enable_kai ]
        [ with_cache_config ]
        [ with_firewall_ranges ]
        [ allow_all_traffic ]
        [ with_update_window ]
        [ expires_at ]
        [ wait_on_active ]
    ;

    # Only create the cluster if it doesn't exist already
    if_not_exists = IF NOT EXISTS

    # Name of the cluster
    cluster_name = '<cluster-name>'

    # Region to create the cluster in
    in_region = IN REGION region_name
    region_name = '<region-name>'

    # Cloud provider, to disambiguate a region name
    with_provider = WITH PROVIDER '<provider>'

    # Project to create the cluster in
    in_project = IN PROJECT { project_id | project_name }
    project_id = ID '<project-id>'
    project_name = '<project-name>'

    # Runtime size
    with_size = WITH SIZE '<size>'

    # Scale factor
    using_scale_factor = USING SCALE FACTOR <number>

    # Auto-suspend
    auto_suspend = AUTO SUSPEND AFTER suspend_after_value suspend_after_units suspend_type
    suspend_after_value = <integer>
    suspend_after_units = { SECONDS | MINUTES | HOURS | DAYS }
    suspend_type = WITH TYPE { IDLE | SCHEDULED | DISABLED }

    # Enable Kai
    enable_kai = ENABLE KAI

    # Cache config
    with_cache_config = WITH CACHE CONFIG <integer>

    # Incoming IP ranges
    with_firewall_ranges = WITH FIREWALL RANGES '<ip-range>',...

    # Allow all incoming traffic
    allow_all_traffic = ALLOW ALL TRAFFIC

    # Update window
    with_update_window = WITH UPDATE WINDOW '<day>:<hour>'

    # Datetime or interval for expiration date/time of the cluster
    expires_at = EXPIRES AT '<iso-datetime-or-interval>'

    # Wait for the cluster to be active before continuing
    wait_on_active = WAIT ON ACTIVE

    Description
    -----------
    Creates a cluster. A cluster is created in a single statement, unlike
    management API v1, which needed a ``CREATE WORKSPACE GROUP`` followed by
    a ``CREATE WORKSPACE``.

    Arguments
    ---------
    * ``<cluster-name>``: The name of the cluster. Must be 1-32 characters
      of lowercase letters, digits and hyphens, and must start and end with
      a letter or digit.
    * ``<region-name>``: The display name or the cloud provider name of the
      region to create the cluster in, as reported by
      ``SHOW CLUSTER REGIONS``.
    * ``<provider>``: The cloud provider (AWS, GCP or Azure), if the region
      name alone is ambiguous.
    * ``<project-id>`` or ``<project-name>``: The ID or name of the project
      to create the cluster in.
    * ``<size>``: The size of the cluster in cluster size notation, for
      example ``S-1``.
    * ``<day>:<hour>``: The day of the week (0-6) and the hour of the day
      (0-23) when engine updates are applied.
    * ``<ip-range>``: A list of allowed IP addresses or CIDR ranges.

    Remarks
    -------
    * Specify the ``IF NOT EXISTS`` clause to create the cluster only if one
      with the given name does not already exist.
    * ``IN PROJECT`` is optional in an organization with a single project,
      which is then used automatically. In an organization with several, the
      clause is required; ``SHOW PROJECTS`` lists the candidates.
    * There is no ``IN REGION ID`` clause. Management API v2 assigns no
      region IDs, so a region is named rather than identified.
    * To allow incoming traffic from any IP address, use the
      ``ALLOW ALL TRAFFIC`` clause.
    * The ``WAIT ON ACTIVE`` clause pauses execution until the cluster
      reaches the ``ACTIVE`` state.
    * Unlike ``CREATE WORKSPACE GROUP``, this command returns a row. The
      admin password is generated by the API and reported when the cluster
      is created and at no later point, so it is returned here; a cluster
      created without capturing it has no reachable ``admin`` user.
    * There are no KMS key or ``SMART DR`` clauses. Management API v2 has no
      equivalent of v1's ``backupBucketKMSKeyID``, ``dataBucketKMSKeyID`` or
      ``smartDR``, so such clauses would be silently dropped.
    * The clause list deliberately stops at what ``CREATE WORKSPACE GROUP`` and
      ``CREATE WORKSPACE`` between them expose, so a v1 script has a v2
      counterpart for everything it says. The API's ``deploymentType`` and
      ``multiAZ`` have no such counterpart and are not surfaced here; reach
      them through ``ClusterManager.create_cluster``, which still takes both.

    Example
    -------
    The following command creates a cluster named **analytics** in the
    ``US East 1 (N. Virginia)`` region and waits for it to become active::

        CREATE CLUSTER 'analytics' IN REGION 'US East 1 (N. Virginia)'
            WITH SIZE 'S-00' WAIT ON ACTIVE;

    See Also
    --------
    * ``SHOW CLUSTERS``
    * ``SHOW CLUSTER REGIONS``
    * ``DROP CLUSTER``

    """  # noqa: E501

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        manager = get_cluster_manager()

        # Only create if a live one doesn't exist. A terminated cluster keeps
        # its name in the listing, so it must not count as existing or the
        # name would be unusable ever after.
        if params['if_not_exists']:
            live = [
                x for x in manager.clusters
                if x.name == params['cluster_name']
                and x.terminated_at is None
            ]
            if live:
                return None

        project = get_project(params)
        region = _resolve_region(params)

        cluster = manager.create_cluster(
            params['cluster_name'],
            provider=region['provider'],
            region=region['region'],
            size=params['with_size'],
            scale_factor=params['using_scale_factor'],
            firewall_ranges=params['with_firewall_ranges'],
            allow_all_traffic=params['allow_all_traffic'],
            auto_suspend=_auto_suspend(params),
            cache_config=params['with_cache_config'],
            expires_at=params['expires_at'],
            update_window=_update_window(params),
            kai=params['enable_kai'],
            project=project,
            wait_on_active=params['wait_on_active'],
        )

        res = FusionSQLResult()
        res.add_field('Name', result.STRING)
        res.add_field('ID', result.STRING)
        res.add_field('Endpoint', result.STRING)
        res.add_field('AdminPassword', result.STRING)
        res.set_rows([
            (
                cluster.name, cluster.id, cluster.endpoint,
                cluster.admin_password,
            ),
        ])
        return res


CreateClusterHandler.register(overwrite=True)


class SuspendClusterHandler(SQLHandler):
    """
    SUSPEND CLUSTER cluster
        [ wait_on_suspended ];

    # Cluster
    cluster = { cluster_id | cluster_name }

    # ID of the cluster
    cluster_id = ID '<cluster-id>'

    # Name of the cluster
    cluster_name = '<cluster-name>'

    # Wait for the cluster to be suspended before continuing
    wait_on_suspended = WAIT ON SUSPENDED

    Description
    -----------
    Suspends a cluster.

    Arguments
    ---------
    * ``<cluster-id>``: The ID of the cluster to suspend.
    * ``<cluster-name>``: The name of the cluster to suspend.

    Remarks
    -------
    * Use the ``WAIT ON SUSPENDED`` clause to pause query execution
      until the cluster is in the ``SUSPENDED`` state.
    * There is no ``IN GROUP`` clause. A cluster is flat, so there is no
      containing group to name.

    Example
    -------
    The following example suspends a cluster named **analytics**::

        SUSPEND CLUSTER 'analytics' WAIT ON SUSPENDED;

    See Also
    --------
    * ``RESUME CLUSTER``

    """

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        cluster = get_cluster(params)
        cluster.suspend(wait_on_suspended=params['wait_on_suspended'])
        return None


SuspendClusterHandler.register(overwrite=True)


class ResumeClusterHandler(SQLHandler):
    """
    RESUME CLUSTER cluster
        [ disable_auto_suspend ]
        [ wait_on_resumed ];

    # Cluster
    cluster = { cluster_id | cluster_name }

    # ID of the cluster
    cluster_id = ID '<cluster-id>'

    # Name of the cluster
    cluster_name = '<cluster-name>'

    # Disable auto-suspend
    disable_auto_suspend = DISABLE AUTO SUSPEND

    # Wait for the cluster to be resumed before continuing
    wait_on_resumed = WAIT ON RESUMED

    Description
    -----------
    Resumes a cluster.

    Arguments
    ---------
    * ``<cluster-id>``: The ID of the cluster to resume.
    * ``<cluster-name>``: The name of the cluster to resume.

    Remarks
    -------
    * Use the ``WAIT ON RESUMED`` clause to pause query execution
      until the cluster is in the ``RESUMED`` state.
    * Specify the ``DISABLE AUTO SUSPEND`` clause to disable
      auto-suspend for the resumed cluster.

    Example
    -------
    The following example resumes a cluster named **analytics** and
    disables its auto-suspend setting::

        RESUME CLUSTER 'analytics' DISABLE AUTO SUSPEND WAIT ON RESUMED;

    See Also
    --------
    * ``SUSPEND CLUSTER``

    """

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        cluster = get_cluster(params)
        cluster.resume(
            wait_on_resumed=params['wait_on_resumed'],
            disable_auto_suspend=params['disable_auto_suspend'],
        )
        return None


ResumeClusterHandler.register(overwrite=True)


class DropClusterHandler(SQLHandler):
    """
    DROP CLUSTER [ if_exists ]
        cluster
        [ wait_on_terminated ];

    # Only run the command if the cluster exists
    if_exists = IF EXISTS

    # Cluster
    cluster = { cluster_id | cluster_name }

    # ID of the cluster to delete
    cluster_id = ID '<cluster-id>'

    # Name of the cluster to delete
    cluster_name = '<cluster-name>'

    # Wait for termination to complete before continuing
    wait_on_terminated = WAIT ON TERMINATED

    Description
    -----------
    Deletes the specified cluster.

    Arguments
    ---------
    * ``<cluster-id>``: The ID of the cluster to delete.
    * ``<cluster-name>``: The name of the cluster to delete.

    Remarks
    -------
    * Specify the ``IF EXISTS`` clause to attempt the delete operation
      only if a cluster with the specified ID or name exists.
    * Use the ``WAIT ON TERMINATED`` clause to pause query execution until
      the cluster is in the ``TERMINATED`` state.
    * There is no ``FORCE`` clause. At v1 it meant "terminate the workspace
      group even though it still contains workspaces", and a cluster is flat,
      so there are no children for it to override. ``DELETE /v2/clusters``
      does still take a ``force`` query parameter, which
      ``Cluster.terminate()`` documents as "even if it is in use" -- a
      different meaning that has not been confirmed against the live API. The
      clause is withheld rather than guessed at; see item 14 of
      ``docs/management-api-audit.md``.
    * All databases attached to the cluster are detached when the cluster
      is deleted.

    Example
    -------
    The following example deletes a cluster named **analytics** if it
    exists, waiting for the termination to finish::

        DROP CLUSTER IF EXISTS 'analytics' WAIT ON TERMINATED;

    See Also
    --------
    * ``CREATE CLUSTER``
    * ``DROP STARTER CLUSTER``

    """

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        try:
            cluster = get_cluster(params)
            if cluster.terminated_at is not None:
                raise KeyError('cluster is already terminated')
            cluster.terminate(
                wait_on_terminated=params['wait_on_terminated'],
            )

        except KeyError:
            if not params['if_exists']:
                raise

        return None


DropClusterHandler.register(overwrite=True)


class UseClusterHandler(SQLHandler):
    """
    USE CLUSTER cluster [ with_database ];

    # Cluster
    cluster = { cluster_id | cluster_name | current_cluster }

    # ID of the cluster
    cluster_id = ID '<cluster-id>'

    # Name of the cluster
    cluster_name = '<cluster-name>'

    # Current cluster
    current_cluster = @@CURRENT

    # Name of database
    with_database = WITH DATABASE '<database-name>'

    Description
    -----------
    Change the cluster and database in the notebook.

    Arguments
    ---------
    * ``<cluster-id>``: The ID of the cluster to use.
    * ``<cluster-name>``: The name of the cluster to use.
    * ``<database-name>``: The name of the database to select.

    Remarks
    -------
    * If you want to specify a database in the current cluster, the
      cluster name can be specified as ``@@CURRENT``.
    * Specify the ``WITH DATABASE`` clause to select a default
      database for the session.
    * There is no ``IN GROUP`` clause. A cluster is flat, so unlike
      ``USE WORKSPACE`` there is no containing group to search in.
    * This command only works in a notebook session in the
      Managed Service.

    Example
    -------
    The following command sets the cluster to ``analytics`` and selects
    ``dbname`` as the default database::

        USE CLUSTER 'analytics' WITH DATABASE 'dbname';

    See Also
    --------
    * ``SHOW CLUSTERS``

    """

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        from singlestoredb.notebook import portal

        # Handle current cluster case
        if params['cluster'].get('current_cluster'):
            if params.get('with_database'):
                portal.default_database = params['with_database']
            return None

        cluster_name = params['cluster'].get('cluster_name')
        cluster_id = params['cluster'].get('cluster_id')

        try:
            if params.get('with_database'):
                portal.connection = (
                    cluster_name or cluster_id,
                    params['with_database'],
                )
            else:
                portal.workspace = cluster_name or cluster_id

        except RuntimeError as exc:
            if 'timeout' not in str(exc):
                raise

        return None


UseClusterHandler.register(overwrite=True)


class ShowStarterClustersHandler(SQLHandler):
    """
    SHOW STARTER CLUSTERS [ <like> ]
        [ <extended> ] [ <order-by> ]
        [ <limit> ];

    Description
    -----------
    Displays information on starter clusters, the shared-tier deployments
    of management API v2.

    Arguments
    ---------
    * ``<pattern>``: A pattern similar to SQL LIKE clause.
      Uses ``%`` as the wildcard character.

    Remarks
    -------
    * Use the ``LIKE`` clause to specify a pattern and return only the
      starter clusters that match the specified pattern.
    * The ``LIMIT`` clause limits the number of results to the
      specified number.
    * Use the ``ORDER BY`` clause to sort the results by the specified
      key. By default, the results are sorted in the ascending order.
    * To return more information about the starter clusters, use the
      ``EXTENDED`` clause.

    Example
    -------
    The following command displays the starter clusters, sorted by name::

        SHOW STARTER CLUSTERS ORDER BY Name;

    See Also
    --------
    * ``SHOW CLUSTERS``
    * ``CREATE STARTER CLUSTER``

    """

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        manager = get_cluster_manager()

        res = FusionSQLResult()
        res.add_field('Name', result.STRING)
        res.add_field('ID', result.STRING)
        res.add_field('DatabaseName', result.STRING)

        if params['extended']:
            res.add_field('Endpoint', result.STRING)
            res.add_field('ProjectID', result.STRING)

            def fields(x: Any) -> Any:
                return (
                    x.name, x.id, x.database_name,
                    x.endpoint, _cluster_project_id(x),
                )
        else:
            def fields(x: Any) -> Any:
                return (x.name, x.id, x.database_name)

        res.set_rows([fields(x) for x in manager.starter_clusters])

        if params['like']:
            res = res.like(Name=params['like'])

        return res.order_by(**params['order_by']).limit(params['limit'])


ShowStarterClustersHandler.register(overwrite=True)


class CreateStarterClusterHandler(SQLHandler):
    """
    CREATE STARTER CLUSTER [ if_not_exists ] cluster_name
        with_database
        in_region
        with_provider
    ;

    # Only create the starter cluster if it doesn't exist already
    if_not_exists = IF NOT EXISTS

    # Name of the starter cluster
    cluster_name = '<cluster-name>'

    # Database to create in the starter cluster
    with_database = WITH DATABASE '<database-name>'

    # Region to create the starter cluster in
    in_region = IN REGION '<region-name>'

    # Cloud provider to create the starter cluster in
    with_provider = WITH PROVIDER '<provider>'

    Description
    -----------
    Creates a starter cluster, the shared-tier deployment of management
    API v2.

    Arguments
    ---------
    * ``<cluster-name>``: The name of the starter cluster.
    * ``<database-name>``: The name of the database to create in it.
    * ``<region-name>``: The cloud provider name of the region, for
      example ``us-east-1``.
    * ``<provider>``: The cloud provider: AWS, GCP or Azure.

    Remarks
    -------
    * Specify the ``IF NOT EXISTS`` clause to create the starter cluster
      only if one with the given name does not already exist.
    * Not every region supports starter clusters. Only the regions
      reported by ``SHOW CLUSTER REGIONS`` are accepted, and both the
      provider and the region name are required because there is nothing
      to infer them from.

    Example
    -------
    The following command creates a starter cluster named **scratch** with
    a database named **scratchdb**::

        CREATE STARTER CLUSTER 'scratch' WITH DATABASE 'scratchdb'
            IN REGION 'us-east-1' WITH PROVIDER 'AWS';

    See Also
    --------
    * ``SHOW STARTER CLUSTERS``
    * ``DROP STARTER CLUSTER``

    """

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        manager = get_cluster_manager()

        if params['if_not_exists']:
            try:
                get_starter_cluster(
                    {'cluster_name': params['cluster_name']},
                )
                return None
            except (ValueError, KeyError):
                pass

        manager.create_starter_cluster(
            params['cluster_name'],
            database_name=params['with_database'],
            provider=params['with_provider'],
            region=params['in_region'],
        )

        return None


CreateStarterClusterHandler.register(overwrite=True)


class DropStarterClusterHandler(SQLHandler):
    """
    DROP STARTER CLUSTER [ if_exists ] cluster;

    # Only run the command if the starter cluster exists
    if_exists = IF EXISTS

    # Starter cluster
    cluster = { cluster_id | cluster_name }

    # ID of the starter cluster to delete
    cluster_id = ID '<cluster-id>'

    # Name of the starter cluster to delete
    cluster_name = '<cluster-name>'

    Description
    -----------
    Deletes the specified starter cluster.

    Arguments
    ---------
    * ``<cluster-id>``: The ID of the starter cluster to delete.
    * ``<cluster-name>``: The name of the starter cluster to delete.

    Remarks
    -------
    * Specify the ``IF EXISTS`` clause to attempt the delete operation
      only if a starter cluster with the specified ID or name exists.
    * There is no ``WAIT ON TERMINATED`` clause. The shared-tier
      termination route reports no state to wait on.

    Example
    -------
    The following example deletes a starter cluster named **scratch** if
    it exists::

        DROP STARTER CLUSTER IF EXISTS 'scratch';

    See Also
    --------
    * ``CREATE STARTER CLUSTER``
    * ``DROP CLUSTER``

    """

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        try:
            get_starter_cluster(params).terminate()

        except KeyError:
            if not params['if_exists']:
                raise

        return None


DropStarterClusterHandler.register(overwrite=True)
