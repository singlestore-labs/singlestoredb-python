#!/usr/bin/env python
import datetime
import os
from typing import Any
from typing import Dict
from typing import Optional
from typing import Union

from ...exceptions import ManagementError
from ...management import files as mgmt_files
from ...management import manage_workspaces
from ...management.files import FilesManager
from ...management.files import FileSpace
from ...management.files import manage_files
from ...management.workspace import StarterWorkspace
from ...management.workspace import Workspace
from ...management.workspace import WorkspaceGroup
from ...management.workspace import WorkspaceManager


def get_workspace_manager() -> WorkspaceManager:
    """Return a new workspace manager."""
    return manage_workspaces()


def get_files_manager() -> FilesManager:
    """Return a new files manager."""
    return manage_files()


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

    Or, from the SINGLESTOREDB_WORKSPACE_GROUP environment variable.

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

    if os.environ.get('SINGLESTOREDB_CLUSTER'):
        raise ValueError('clusters and shared workspaces are not currently supported')

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

    Or, from the SINGLESTOREDB_WORKSPACE environment variable.

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

    if os.environ.get('SINGLESTOREDB_CLUSTER'):
        raise ValueError('clusters and shared workspaces are not currently supported')

    raise KeyError('no workspace was specified')


def get_deployment(
        params: Dict[str, Any],
) -> Union[WorkspaceGroup, StarterWorkspace]:
    """
    Find a starter workspace matching deployment_id or deployment_name.

    This function will get a starter workspace or ID from the
    following parameters:

        * params['deployment_name']
        * params['deployment_id']
        * params['group']['deployment_name']
        * params['group']['deployment_id']
        * params['in_deployment']['deployment_name']
        * params['in_deployment']['deployment_id']
        * params['in']['in_group']['deployment_name']
        * params['in']['in_group']['deployment_id']
        * params['in']['in_deployment']['deployment_name']
        * params['in']['in_deployment']['deployment_id']

    Or, from the SINGLESTOREDB_WORKSPACE_GROUP
    or SINGLESTOREDB_CLUSTER environment variables.

    """
    manager = get_workspace_manager()

    #
    # Search for deployment by name
    #
    deployment_name = params.get('deployment_name') or \
        (params.get('in_deployment') or {}).get('deployment_name') or \
        (params.get('group') or {}).get('deployment_name') or \
        ((params.get('in') or {}).get('in_group') or {}).get('deployment_name') or \
        ((params.get('in') or {}).get('in_deployment') or {}).get('deployment_name')

    if deployment_name:
        # Standard workspace group
        workspace_groups = [
            x for x in manager.workspace_groups
            if x.name == deployment_name
        ]

        if len(workspace_groups) == 1:
            return workspace_groups[0]

        elif len(workspace_groups) > 1:
            ids = ', '.join(x.id for x in workspace_groups)
            raise ValueError(
                f'more than one workspace group with given name was found: {ids}',
            )

        # Starter workspace
        starter_workspaces = [
            x for x in manager.starter_workspaces
            if x.name == deployment_name
        ]

        if len(starter_workspaces) == 1:
            return starter_workspaces[0]

        elif len(starter_workspaces) > 1:
            ids = ', '.join(x.id for x in starter_workspaces)
            raise ValueError(
                f'more than one starter workspace with given name was found: {ids}',
            )

        raise KeyError(f'no deployment found with name: {deployment_name}')

    #
    # Search for deployment by ID
    #
    deployment_id = params.get('deployment_id') or \
        (params.get('in_deployment') or {}).get('deployment_id') or \
        (params.get('group') or {}).get('deployment_id') or \
        ((params.get('in') or {}).get('in_group') or {}).get('deployment_id') or \
        ((params.get('in') or {}).get('in_deployment') or {}).get('deployment_id')

    if deployment_id:
        try:
            # Standard workspace group
            return manager.get_workspace_group(deployment_id)
        except ManagementError as exc:
            if exc.errno == 404:
                try:
                    # Starter workspace
                    return manager.get_starter_workspace(deployment_id)
                except ManagementError as exc:
                    if exc.errno == 404:
                        raise KeyError(f'no deployment found with ID: {deployment_id}')
                    raise
            else:
                raise

    # Use workspace group from environment
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

    # Use cluster from environment
    if os.environ.get('SINGLESTOREDB_CLUSTER'):
        try:
            return manager.get_starter_workspace(
                os.environ['SINGLESTOREDB_CLUSTER'],
            )
        except ManagementError as exc:
            if exc.errno == 404:
                raise KeyError(
                    'no starter workspace found with ID: '
                    f'{os.environ["SINGLESTOREDB_CLUSTER"]}',
                )
            raise

    raise KeyError('no deployment was specified')


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
