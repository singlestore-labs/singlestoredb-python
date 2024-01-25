#!/usr/bin/env python
import datetime
import os
from typing import Any
from typing import Dict
from typing import Optional

from ...exceptions import ManagementError
from ...management import manage_workspaces
from ...management.workspace import Workspace
from ...management.workspace import WorkspaceGroup
from ...management.workspace import WorkspaceManager


def get_workspace_manager() -> WorkspaceManager:
    """Return a new workspace manager."""
    return manage_workspaces()


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
