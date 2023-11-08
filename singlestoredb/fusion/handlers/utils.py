#!/usr/bin/env python
import datetime
import os
from typing import Any
from typing import Dict
from typing import Optional
from urllib.parse import urlparse

import jwt

from ...config import get_option
from ...management import manage_workspaces
from ...management.workspace import WorkspaceGroup
from ...management.workspace import WorkspaceManager


def get_management_token() -> Optional[str]:
    """Return the token for the Management API."""
    # See if an API key is configured
    tok = get_option('management.token')
    if tok:
        return tok

    # See if the connection URL contains a JWT
    url = os.environ.get('SINGLESTOREDB_URL')
    if not url:
        return None

    urlp = urlparse(url, scheme='singlestoredb', allow_fragments=True)
    if urlp.password:
        try:
            jwt.decode(urlp.password, options={'verify_signature': False})
            return urlp.password
        except jwt.DecodeError:
            pass

    # Didn't find a key anywhere
    return None


def get_workspace_manager() -> WorkspaceManager:
    """Return a new workspace manager."""
    return manage_workspaces(get_management_token())


def dt_isoformat(dt: Optional[datetime.datetime]) -> Optional[str]:
    """Convert datetime to string."""
    if dt is None:
        return None
    return dt.isoformat()


def get_workspace_group(params: Dict[str, Any]) -> WorkspaceGroup:
    """Find a workspace group matching group_id or group_name."""
    manager = get_workspace_manager()

    if params['group_name']:
        workspace_groups = [
            x for x in manager.workspace_groups
            if x.name == params['group_name']
        ]

        if not workspace_groups:
            raise KeyError(
                'no workspace group found with name "{}"'.format(params['group_name']),
            )

        if len(workspace_groups) > 1:
            ids = ', '.join(x.id for x in workspace_groups)
            raise ValueError(
                f'more than one workspace group with given name was found: {ids}',
            )

        return workspace_groups[0]

    return manager.get_workspace_group(params['group_id'])
