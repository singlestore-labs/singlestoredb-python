#!/usr/bin/env python
import datetime
import os
from typing import Any
from typing import Dict
from typing import Optional
from typing import Union

from ...exceptions import ManagementError
from ...management import files as mgmt_files
from ...management.cluster import Cluster
from ...management.cluster import ClusterManager
from ...management.cluster import manage_clusters
from ...management.cluster import Project
from ...management.cluster import PROJECT_ID_RE
from ...management.cluster import StarterCluster
from ...management.files import FilesManager
from ...management.files import FileSpace
from ...management.files import manage_files
from ...management.v1.inference_api import InferenceAPIInfo
from ...management.v1.inference_api import InferenceAPIManager
from ...management.workspace import _manage_workspaces_v1
from ...management.workspace import Workspace
from ...management.workspace import WorkspaceGroup
from ...management.workspace import WorkspaceManager


def get_workspace_manager() -> WorkspaceManager:
    """
    Return a new workspace manager.

    Pinned to v1. The ``WORKSPACE`` and ``WORKSPACE GROUP`` commands are the
    v1 vocabulary -- v2 replaced both with the flat ``Cluster`` -- so they must
    not follow the ``management.version`` option out of v1. The v2 equivalent
    is :func:`get_cluster_manager`.
    """
    return _manage_workspaces_v1()


def get_cluster_manager() -> ClusterManager:
    """
    Return a new cluster manager.

    Pinned to v2 for the mirror image of the reason
    :func:`get_workspace_manager` is pinned to v1: the ``CLUSTER`` commands
    *are* the v2 vocabulary, so they must not follow the ``management.version``
    option out of v2 -- at v1 there is no cluster resource at all.
    """
    return manage_clusters(version='v2')


def get_files_manager() -> FilesManager:
    """
    Return a new files manager.

    Pinned to v2. ``management/files.py`` is version-neutral -- the personal,
    shared and models spaces are the same resource at both versions and only
    the URL differs -- so the pin is about which URL the Fusion FILES commands
    address, not about which implementation they get. It is explicit rather
    than left to the ``management.version`` option so that the FILES commands
    do not change which API they talk to when an unrelated option is set.
    """
    return manage_files(version='v2')


def dt_isoformat(dt: Optional[datetime.datetime]) -> Optional[str]:
    """Convert datetime to string."""
    if dt is None:
        return None
    return dt.isoformat()


def get_workspace_group(params: Dict[str, Any]) -> WorkspaceGroup:
    """
    Find a workspace group matching group_id or group_name.

    This function will get a workspace group or ID from the
    following parameters:

        * params['group_name']
        * params['group_id']
        * params['group']['group_name']
        * params['group']['group_id']
        * params['in_group']['group_name']
        * params['in_group']['group_id']

    Or, from the SINGLESTOREDB_WORKSPACE_GROUP environment variable, which the
    notebook environment sets to the deployment's group ID. This resolves it
    against v1, where a group is a resource in its own right; at v2 the same ID
    is only reported back as ``Cluster.group`` and cannot be looked up, which
    is why :func:`get_deployment` refuses it rather than guessing.

    """
    manager = get_workspace_manager()

    group_name = params.get('group_name') or \
        (params.get('in_group') or {}).get('group_name') or \
        (params.get('group') or {}).get('group_name')
    if group_name:
        workspace_groups = [
            x for x in manager.workspace_groups
            if x.name == group_name
        ]

        if not workspace_groups:
            raise KeyError(
                f'no workspace group found with name: {group_name}',
            )

        if len(workspace_groups) > 1:
            ids = ', '.join(x.id for x in workspace_groups)
            raise ValueError(
                f'more than one workspace group with given name was found: {ids}',
            )

        return workspace_groups[0]

    group_id = params.get('group_id') or \
        (params.get('in_group') or {}).get('group_id') or \
        (params.get('group') or {}).get('group_id')
    if group_id:
        try:
            return manager.get_workspace_group(group_id)
        except ManagementError as exc:
            if exc.errno == 404:
                raise KeyError(f'no workspace group found with ID: {group_id}')
            raise

    if os.environ.get('SINGLESTOREDB_WORKSPACE_GROUP'):
        try:
            return manager.get_workspace_group(
                os.environ['SINGLESTOREDB_WORKSPACE_GROUP'],
            )
        except ManagementError as exc:
            if exc.errno == 404:
                raise KeyError(
                    'no workspace found with ID: '
                    f'{os.environ["SINGLESTOREDB_WORKSPACE_GROUP"]}',
                )
            raise

    raise KeyError('no workspace group was specified')


def get_workspace(params: Dict[str, Any]) -> Workspace:
    """
    Retrieve the specified workspace.

    This function will get a workspace group or ID from the
    following parameters:

        * params['workspace_name']
        * params['workspace_id']
        * params['workspace']['workspace_name']
        * params['workspace']['workspace_id']

    Or, from the SINGLESTOREDB_WORKSPACE environment variable, which the
    notebook environment sets to the current deployment. Its value is a
    *workspace* ID only in a v1 environment; from v2 onward the same variable
    carries a cluster ID, which these v1 commands cannot resolve -- use the
    ``CLUSTER`` commands, or :func:`get_cluster`, there.

    """
    manager = get_workspace_manager()
    workspace_name = params.get('workspace_name') or \
        (params.get('workspace') or {}).get('workspace_name')
    if workspace_name:
        wg = get_workspace_group(params)
        workspaces = [
            x for x in wg.workspaces
            if x.name == workspace_name
        ]

        if not workspaces:
            raise KeyError(
                f'no workspace found with name: {workspace_name}',
            )

        if len(workspaces) > 1:
            ids = ', '.join(x.id for x in workspaces)
            raise ValueError(
                f'more than one workspace with given name was found: {ids}',
            )

        return workspaces[0]

    workspace_id = params.get('workspace_id') or \
        (params.get('workspace') or {}).get('workspace_id')
    if workspace_id:
        try:
            return manager.get_workspace(workspace_id)
        except ManagementError as exc:
            if exc.errno == 404:
                raise KeyError(f'no workspace found with ID: {workspace_id}')
            raise

    if os.environ.get('SINGLESTOREDB_WORKSPACE'):
        try:
            return manager.get_workspace(
                os.environ['SINGLESTOREDB_WORKSPACE'],
            )
        except ManagementError as exc:
            if exc.errno == 404:
                raise KeyError(
                    'no workspace found with ID: '
                    f'{os.environ["SINGLESTOREDB_WORKSPACE"]}',
                )
            raise

    raise KeyError('no workspace was specified')


def _is_missing(exc: ManagementError) -> bool:
    """
    Return True if ``exc`` means "no such deployment".

    A well-formed but unknown ID draws ``404``, but a *malformed* one draws
    ``400 uuid: incorrect UUID length`` from the v2 routes, which v1's
    non-UUID IDs never did. Both mean the caller named something that does not
    exist, so both become a ``KeyError`` rather than leaking a raw 400 for
    what is usually a typo. Other 400s -- a real request-body problem -- are
    left alone.
    """
    if exc.errno == 404:
        return True
    return exc.errno == 400 and 'uuid' in str(exc.msg or '').lower()


def get_cluster(params: Dict[str, Any]) -> Cluster:
    """
    Retrieve the specified cluster.

    The v2 counterpart of :func:`get_workspace`, and flat where that one is
    nested: a cluster has no containing group, so there is nothing to resolve
    first.

    This function will get a cluster name or ID from the following parameters:

        * params['cluster_name']
        * params['cluster_id']
        * params['cluster']['cluster_name']
        * params['cluster']['cluster_id']

    Or, from ``SINGLESTOREDB_WORKSPACE``, which is what the notebook
    environment calls the current deployment whatever the API version calls it.

    """
    manager = get_cluster_manager()

    cluster_name = params.get('cluster_name') or \
        (params.get('cluster') or {}).get('cluster_name')
    if cluster_name:
        clusters = [x for x in manager.clusters if x.name == cluster_name]

        if not clusters:
            raise KeyError(f'no cluster found with name: {cluster_name}')

        if len(clusters) > 1:
            ids = ', '.join(x.id for x in clusters)
            raise ValueError(
                f'more than one cluster with given name was found: {ids}',
            )

        return clusters[0]

    cluster_id = params.get('cluster_id') or \
        (params.get('cluster') or {}).get('cluster_id')
    if cluster_id:
        try:
            return manager.get_cluster(cluster_id)
        except ManagementError as exc:
            if _is_missing(exc):
                raise KeyError(f'no cluster found with ID: {cluster_id}')
            raise

    from_env = os.environ.get('SINGLESTOREDB_WORKSPACE')
    if from_env:
        try:
            return manager.get_cluster(from_env)
        except ManagementError as exc:
            if _is_missing(exc):
                raise KeyError(
                    f'no cluster found with ID: {from_env} '
                    '(from SINGLESTOREDB_WORKSPACE)',
                )
            raise

    raise KeyError('no cluster was specified')


def get_starter_cluster(params: Dict[str, Any]) -> StarterCluster:
    """
    Retrieve the specified starter cluster.

    This function will get a starter cluster name or ID from the following
    parameters:

        * params['cluster_name']
        * params['cluster_id']
        * params['cluster']['cluster_name']
        * params['cluster']['cluster_id']

    """
    manager = get_cluster_manager()

    cluster_name = params.get('cluster_name') or \
        (params.get('cluster') or {}).get('cluster_name')
    if cluster_name:
        clusters = [
            x for x in manager.starter_clusters
            if x.name == cluster_name
        ]

        if not clusters:
            raise KeyError(
                f'no starter cluster found with name: {cluster_name}',
            )

        if len(clusters) > 1:
            ids = ', '.join(x.id for x in clusters)
            raise ValueError(
                'more than one starter cluster with given name was '
                f'found: {ids}',
            )

        return clusters[0]

    cluster_id = params.get('cluster_id') or \
        (params.get('cluster') or {}).get('cluster_id')
    if cluster_id:
        try:
            return manager.get_starter_cluster(cluster_id)
        except ManagementError as exc:
            if _is_missing(exc):
                raise KeyError(
                    f'no starter cluster found with ID: {cluster_id}',
                )
            raise

    raise KeyError('no starter cluster was specified')


def get_project(params: Dict[str, Any]) -> Optional[Project]:
    """
    Resolve an ``IN PROJECT`` clause, or the project named by the environment.

    Returns ``None`` when neither names a project, so that ``CREATE CLUSTER``
    falls through to ``ClusterManager._resolve_project_id``, which picks the
    organization's only project or raises naming the candidates. The clause is
    therefore optional in a single-project organization and required in one
    with several.

    This function will get a project name or ID from the following parameters:

        * params['project_name']
        * params['project_id']
        * params['in_project']['project_name']
        * params['in_project']['project_id']

    Or, from ``SINGLESTOREDB_PROJECT``, which the SingleStore notebook
    environment sets and which may hold either a project name or a project ID.

    """
    project_name = params.get('project_name') or \
        (params.get('in_project') or {}).get('project_name')
    project_id = params.get('project_id') or \
        (params.get('in_project') or {}).get('project_id')

    source = ''
    if not project_name and not project_id:
        from_env = os.environ.get('SINGLESTOREDB_PROJECT')
        if not from_env:
            return None
        source = ' (from SINGLESTOREDB_PROJECT)'
        # The environment variable is a single value for both spellings, so it
        # is read as an ID only when it is shaped like one; see PROJECT_ID_RE.
        if PROJECT_ID_RE.match(from_env):
            project_id = from_env
        else:
            project_name = from_env

    manager = get_cluster_manager()

    if project_name:
        projects = [x for x in manager.projects if x.name == project_name]

        if not projects:
            raise KeyError(
                f'no project found with name: {project_name}{source}',
            )

        if len(projects) > 1:
            ids = ', '.join(x.id for x in projects)
            raise ValueError(
                f'more than one project with given name was found: {ids}',
            )

        return projects[0]

    assert project_id is not None
    try:
        return manager.get_project(project_id)
    except ManagementError as exc:
        if _is_missing(exc):
            raise KeyError(f'no project found with ID: {project_id}{source}')
        raise


def get_deployment(
        params: Dict[str, Any],
) -> Union[Cluster, StarterCluster]:
    """
    Find a cluster or starter cluster matching deployment_id or deployment_name.

    Resolves against management API v2, so a "deployment" here is a
    :class:`Cluster` or a :class:`StarterCluster`. ``stage.py`` is the only
    consumer, and it touches nothing but ``deployment.stage``, which both
    classes provide.

    This function will get a deployment name or ID from the
    following parameters:

        * params['deployment_name']
        * params['deployment_id']
        * params['group']['deployment_name']
        * params['group']['deployment_id']
        * params['in_deployment']['deployment_name']
        * params['in_deployment']['deployment_id']
        * params['in']['in_cluster']['deployment_name']
        * params['in']['in_cluster']['deployment_id']
        * params['in']['in_group']['deployment_name']
        * params['in']['in_group']['deployment_id']
        * params['in']['in_deployment']['deployment_name']
        * params['in']['in_deployment']['deployment_id']

    The ``group`` and ``in_group`` keys stay wired so that the existing
    ``IN GROUP`` spelling keeps parsing as a synonym for ``IN CLUSTER``.

    Or, from ``SINGLESTOREDB_WORKSPACE``, which is what the notebook
    environment calls the current deployment whatever the API version calls it.

    """
    manager = get_cluster_manager()

    #
    # Search for deployment by name
    #
    deployment_name = params.get('deployment_name') or \
        (params.get('in_deployment') or {}).get('deployment_name') or \
        (params.get('group') or {}).get('deployment_name') or \
        ((params.get('in') or {}).get('in_cluster') or {}).get('deployment_name') or \
        ((params.get('in') or {}).get('in_group') or {}).get('deployment_name') or \
        ((params.get('in') or {}).get('in_deployment') or {}).get('deployment_name')

    if deployment_name:
        # Standard cluster
        clusters = [
            x for x in manager.clusters
            if x.name == deployment_name
        ]

        if len(clusters) == 1:
            return clusters[0]

        elif len(clusters) > 1:
            ids = ', '.join(x.id for x in clusters)
            raise ValueError(
                f'more than one cluster with given name was found: {ids}',
            )

        # Starter cluster
        starter_clusters = [
            x for x in manager.starter_clusters
            if x.name == deployment_name
        ]

        if len(starter_clusters) == 1:
            return starter_clusters[0]

        elif len(starter_clusters) > 1:
            ids = ', '.join(x.id for x in starter_clusters)
            raise ValueError(
                'more than one starter cluster with given name was '
                f'found: {ids}',
            )

        raise KeyError(f'no deployment found with name: {deployment_name}')

    #
    # Search for deployment by ID
    #
    deployment_id = params.get('deployment_id') or \
        (params.get('in_deployment') or {}).get('deployment_id') or \
        (params.get('group') or {}).get('deployment_id') or \
        ((params.get('in') or {}).get('in_cluster') or {}).get('deployment_id') or \
        ((params.get('in') or {}).get('in_group') or {}).get('deployment_id') or \
        ((params.get('in') or {}).get('in_deployment') or {}).get('deployment_id')

    if deployment_id:
        return _deployment_by_id(manager, deployment_id)

    #
    # Use the deployment named by the environment. v1 had a branch per
    # environment variable because a group, a workspace and a legacy cluster
    # were different resources; at v2 there is one deployment resource and the
    # environment names it once, so one lookup tries cluster then starter
    # cluster.
    #
    from_env = os.environ.get('SINGLESTOREDB_WORKSPACE')
    if from_env:
        return _deployment_by_id(
            manager, from_env, 'SINGLESTOREDB_WORKSPACE',
        )

    if os.environ.get('SINGLESTOREDB_WORKSPACE_GROUP'):
        # Deliberately not resolved. The value is a group ID, which v2 exposes
        # only as the read-only Cluster.group attribute -- there is no group
        # route to look it up with, so guessing which cluster was meant could
        # target the wrong deployment.
        raise KeyError(
            'SINGLESTOREDB_WORKSPACE_GROUP holds a group ID, which management '
            'API v2 reports as a cluster attribute rather than something that '
            'can be looked up -- clusters are flat. Set '
            'SINGLESTOREDB_WORKSPACE to the cluster ID instead, or name the '
            'deployment with IN CLUSTER.',
        )

    raise KeyError('no deployment was specified')


def _deployment_by_id(
    manager: ClusterManager,
    deployment_id: str,
    envvar: Optional[str] = None,
) -> Union[Cluster, StarterCluster]:
    """Look an ID up as a cluster, then as a starter cluster."""
    source = f' (from {envvar})' if envvar else ''
    try:
        return manager.get_cluster(deployment_id)
    except ManagementError as exc:
        if not _is_missing(exc):
            raise
    try:
        return manager.get_starter_cluster(deployment_id)
    except ManagementError as exc:
        if _is_missing(exc):
            raise KeyError(
                f'no deployment found with ID: {deployment_id}{source}',
            )
        raise


def get_file_space(params: Dict[str, Any]) -> FileSpace:
    """
    Retrieve the specified file space.

    This function will get a file space from the
    following parameters:

        * params['file_location']
    """
    manager = get_files_manager()

    file_location = params.get('file_location')
    if file_location:
        file_location_lower_case = file_location.lower()

        if file_location_lower_case == mgmt_files.PERSONAL_SPACE:
            return manager.personal_space
        elif file_location_lower_case == mgmt_files.SHARED_SPACE:
            return manager.shared_space
        elif file_location_lower_case == mgmt_files.MODELS_SPACE:
            return manager.models_space
        else:
            raise ValueError(f'invalid file location: {file_location}')

    raise KeyError('no file space was specified')


def get_inference_api_manager() -> InferenceAPIManager:
    """
    Return the inference API manager for the current project.

    Stays on the v1 manager while files and jobs move to v2, because unlike
    those two there is no v2 route to move to: ``Organization.inference_apis``
    raises for every version past v1, and the implementation is imported from
    ``management/v1/inference_api.py`` by that name -- there is deliberately no
    version-neutral alias for it. The handlers this feeds are hidden for the
    same reason (see ``handlers/models.py``). Revisit when the models and
    inference surface gains a v2 equivalent.
    """
    wm = get_workspace_manager()
    return wm.organization.inference_apis


def get_inference_api(params: Dict[str, Any]) -> InferenceAPIInfo:
    """Return an inference API based on model name in params."""
    inference_apis = get_inference_api_manager()
    model_name = params['model_name']
    return inference_apis.get(model_name)
