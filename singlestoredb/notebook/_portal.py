#!/usr/bin/env python
import os
import urllib.parse
from typing import Any
from typing import Dict
from typing import Optional

try:
    from IPython import get_ipython
    has_ipython = True
except ImportError:
    has_ipython = False


class Portal(object):
    """SingleStore Portal information."""

    def __init__(self) -> None:
        self._connection_info: Dict[str, Any] = {}

        if has_ipython:
            try:
                handlers = get_ipython().kernel.control_handlers
                handlers['singlestore_portal_request'] = self._request
            except AttributeError:
                return

    def __str__(self) -> str:
        attrs = []
        for name in sorted(dir(type(self))):
            if name.startswith('_'):
                continue
            if isinstance(getattr(type(self), name), property):
                if name == 'password':
                    if self.password is not None:
                        attrs.append("password='***'")
                    else:
                        attrs.append('password=None')
                else:
                    attrs.append(f'{name}={getattr(self, name)!r}')
        return f'{type(self).__name__}({", ".join(attrs)})'

    def __repr__(self) -> str:
        return str(self)

    def _request(self, stream: Any, ident: Any, msg: Dict[str, Any]) -> None:
        if not isinstance(msg, dict):
            return

        content = msg.get('content', {})
        kind = content.get('kind', 'unknown')

        if kind == 'UPDATE_CONNECTION':
            self._connection_info.update(content)

        elif kind == 'UPDATE_THEME':
            pass

        elif kind == 'UPDATE_SETTINGS':
            pass

    @property
    def organization_id(self) -> Optional[str]:
        """Organization ID."""
        try:
            return self._connection_info['orgID']
        except KeyError:
            return os.environ.get('SINGLESTOREDB_ORGANIZATION')

    @property
    def workspace_group_id(self) -> Optional[str]:
        """Workspace Group ID."""
        try:
            return self._connection_info['workspaceGroupID']
        except KeyError:
            return os.environ.get('SINGLESTOREDB_WORKSPACE_GROUP')

    @property
    def workspace_id(self) -> Optional[str]:
        """Workspace ID."""
        try:
            return self._connection_info['workspaceID']
        except KeyError:
            return os.environ.get('SINGLESTOREDB_WORKSPACE')

    @property
    def cluster_id(self) -> Optional[str]:
        """Cluster ID."""
        try:
            return self._connection_info['clusterID']
        except KeyError:
            return os.environ.get('SINGLESTOREDB_CLUSTER')

    def _parse_url(self) -> Dict[str, Any]:
        url = urllib.parse.urlparse(
            os.environ.get('SINGLESTOREDB_URL', ''),
        )
        return dict(
            host=url.hostname or None,
            port=url.port or None,
            user=url.username or None,
            password=url.password or None,
            defaultDatabase=url.path.split('/')[-1] or None,
        )

    @property
    def host(self) -> Optional[str]:
        """Hostname."""
        try:
            return self._connection_info['host']
        except KeyError:
            return self._parse_url()['host']

    @property
    def port(self) -> Optional[int]:
        """Database server port."""
        try:
            return self._connection_info['port']
        except KeyError:
            return self._parse_url()['port']

    @property
    def user(self) -> Optional[str]:
        """Username."""
        try:
            return self._connection_info['user']
        except KeyError:
            return self._parse_url()['user']

    @property
    def password(self) -> Optional[str]:
        """Password."""
        try:
            return self._connection_info['password']
        except KeyError:
            return self._parse_url()['password']

    @property
    def default_database(self) -> Optional[str]:
        """Default database."""
        try:
            return self._connection_info['defaultDatabase']
        except KeyError:
            return self._parse_url()['defaultDatabase']

    @property
    def version(self) -> Optional[str]:
        """Version."""
        return self._connection_info.get('version')


portal = Portal()
