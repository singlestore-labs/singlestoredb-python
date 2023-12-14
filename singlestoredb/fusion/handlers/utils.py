#!/usr/bin/env python
import datetime
import os
from typing import Any
from typing import Dict
from typing import Optional

from ...management import get_token
from ...management import manage_workspaces
from ...management.workspace import WorkspaceGroup
from ...management.workspace import WorkspaceManager


def get_workspace_manager() -> WorkspaceManager:
    """Return a new workspace manager."""
    return manage_workspaces(get_token())


def dt_isoformat(dt: Optional[datetime.datetime]) -> Optional[str]:
    """Convert datetime to string."""
    if dt is None:
        return None
    return dt.isoformat()


def get_workspace_group(params: Dict[str, Any]) -> WorkspaceGroup:
    """Find a workspace group matching group_id or group_name."""
    manager = get_workspace_manager()

    group_name = params.get('group_name') or \
        (params.get('in_group') or {}).get('group_name')
    if group_name:
        workspace_groups = [
            x for x in manager.workspace_groups
            if x.name == group_name
        ]

        if not workspace_groups:
            raise KeyError(
                'no workspace group found with name "{}"'.format(group_name),
            )

        if len(workspace_groups) > 1:
            ids = ', '.join(x.id for x in workspace_groups)
            raise ValueError(
                f'more than one workspace group with given name was found: {ids}',
            )

        return workspace_groups[0]

    group_id = params.get('group_id') or \
        (params.get('in_group') or {}).get('group_id')
    if group_id:
        return manager.get_workspace_group(group_id)

    if os.environ.get('SINGLESTOREDB_WORKSPACE_GROUP'):
        return manager.get_workspace_group(
            os.environ['SINGLESTOREDB_WORKSPACE_GROUP'],
        )

    if os.environ.get('SINGLESTOREDB_CLUSTER'):
        raise ValueError('clusters and shared workspaces are not currently supported')

    raise KeyError('no workspace group was specified')
