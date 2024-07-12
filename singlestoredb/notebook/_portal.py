#!/usr/bin/env python
import json
import os
import re
import time
import urllib.parse
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

from . import _objects as obj
from ..management import workspace as mgr
from ..utils import events

try:
    from IPython import display
    has_ipython = True
except ImportError:
    has_ipython = False


class Portal(object):
    """SingleStore Portal information."""

    def __init__(self) -> None:
        self._connection_info: Dict[str, Any] = {}
        self._authentication_info: Dict[str, Any] = {}
        self._theme_info: Dict[str, Any] = {}
        events.subscribe(self._request)

    def __str__(self) -> str:
        attrs = []
        for name in [
            'organization_id', 'workspace_group_id', 'workspace_id',
            'host', 'port', 'user', 'password', 'default_database',
        ]:
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

    def _call_javascript(
        self,
        func: str,
        args: Optional[List[Any]] = None,
        wait_on_condition: Optional[Callable[[], bool]] = None,
        timeout_message: str = 'timed out waiting on condition',
        wait_interval: float = 0.2,
        timeout: float = 5.0,
    ) -> None:
        if not has_ipython or not func:
            return

        if not re.match(r'^[A-Z_][\w\._]*$', func, flags=re.I):
            raise ValueError(f'function name is not valid: {func}')

        args = args if args else []

        code = f'''
            if (window.singlestore && window.singlestore.portal) {{
                window.singlestore.portal.{func}.apply(
                    window,
                    JSON.parse({repr(json.dumps(args))})
                )
            }}
        '''

        display.display(display.Javascript(code))

        if wait_on_condition is not None:
            elapsed = 0.0
            while True:
                if wait_on_condition():
                    break
                if elapsed > timeout:
                    raise RuntimeError(timeout_message)
                time.sleep(wait_interval)
                elapsed += wait_interval

    def _request(self, msg: Dict[str, Any]) -> None:
        """Handle request on the control stream."""
        func = getattr(self, '_handle_' + msg.get('name', 'unknown').split('.')[-1])
        if func is not None:
            func(msg.get('data', {}))

    def _handle_connection_updated(self, data: Dict[str, Any]) -> None:
        """Handle connection_updated event."""
        self._connection_info = dict(data)

    def _handle_authentication_updated(self, data: Dict[str, Any]) -> None:
        """Handle authentication_updated event."""
        self._authentication_info = dict(data)

    def _handle_theme_updated(self, data: Dict[str, Any]) -> None:
        """Handle theme_updated event."""
        self._theme_info = dict(data)

    def _handle_unknown(self, data: Dict[str, Any]) -> None:
        """Handle unknown events."""
        pass

    @property
    def organization_id(self) -> Optional[str]:
        """Organization ID."""
        try:
            return self._connection_info['organization']
        except KeyError:
            return os.environ.get('SINGLESTOREDB_ORGANIZATION')

    @property
    def organization(self) -> obj.Organization:
        """Organization."""
        return obj.organization

    @property
    def stage(self) -> obj.Stage:
        """Stage."""
        return obj.stage

    @property
    def secrets(self) -> obj.Secrets:
        """Secrets."""
        return obj.secrets

    @property
    def workspace_group_id(self) -> Optional[str]:
        """Workspace Group ID."""
        try:
            return self._connection_info['workspace_group']
        except KeyError:
            return os.environ.get('SINGLESTOREDB_WORKSPACE_GROUP')

    @property
    def workspace_group(self) -> obj.WorkspaceGroup:
        """Workspace group."""
        return obj.workspace_group

    @workspace_group.setter
    def workspace_group(self) -> None:
        """Set workspace group."""
        raise AttributeError(
            'workspace group can not be set explictly; ' +
            'you can only set a workspace',
        )

    @property
    def workspace_id(self) -> Optional[str]:
        """Workspace ID."""
        try:
            return self._connection_info['workspace']
        except KeyError:
            return os.environ.get('SINGLESTOREDB_WORKSPACE')

    @property
    def workspace(self) -> obj.Workspace:
        """Workspace."""
        return obj.workspace

    @workspace.setter
    def workspace(self, name_or_id: str) -> None:
        """Set workspace."""
        if re.match(
            r'[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}',
            name_or_id, flags=re.I,
        ):
            w = mgr.get_workspace(name_or_id)
        else:
            w = mgr.get_workspace_group(self.workspace_group_id).workspaces[name_or_id]

        if w.state and w.state.lower() not in ['active', 'resumed']:
            raise RuntimeError('workspace is not active')

        id = w.id

        self._call_javascript(
            'changeDeployment', [id],
            wait_on_condition=lambda: self.workspace_id == id,  # type: ignore
            timeout_message='timeout waiting for workspace update',
        )

    deployment = workspace

    @property
    def connection(self) -> Tuple[obj.Workspace, Optional[str]]:
        """Workspace and default database name."""
        return self.workspace, self.default_database

    @connection.setter
    def connection(self, workspace_and_default_database: Tuple[str, str]) -> None:
        """Set workspace and default database name."""
        name_or_id, default_database = workspace_and_default_database
        if re.match(
            r'[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}',
            name_or_id, flags=re.I,
        ):
            w = mgr.get_workspace(name_or_id)
        else:
            w = mgr.get_workspace_group(self.workspace_group_id).workspaces[name_or_id]

        if w.state and w.state.lower() not in ['active', 'resumed']:
            raise RuntimeError('workspace is not active')

        id = w.id

        self._call_javascript(
            'changeConnection', [id, default_database],
            wait_on_condition=lambda: self.workspace_id == id and
            self.default_database == default_database,  # type: ignore
            timeout_message='timeout waiting for workspace update',
        )

    @property
    def cluster_id(self) -> Optional[str]:
        """Cluster ID."""
        try:
            return self._connection_info['cluster']
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
            default_database=url.path.split('/')[-1] or None,
        )

    @property
    def connection_url(self) -> Optional[str]:
        """Connection URL."""
        try:
            return self._connection_info['connection_url']
        except KeyError:
            return os.environ.get('SINGLESTOREDB_URL')

    @property
    def connection_url_kai(self) -> Optional[str]:
        """Kai connectionURL."""
        try:
            return self._connection_info.get('connection_url_kai')
        except KeyError:
            return os.environ.get('SINGLESTOREDB_URL_KAI')

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
            return self._authentication_info['user']
        except KeyError:
            return self._parse_url()['user']

    @property
    def password(self) -> Optional[str]:
        """Password."""
        try:
            return self._authentication_info['password']
        except KeyError:
            return self._parse_url()['password']

    @property
    def default_database(self) -> Optional[str]:
        """Default database."""
        try:
            return self._connection_info['default_database']
        except KeyError:
            return self._parse_url()['default_database']

    @default_database.setter
    def default_database(self, name: str) -> None:
        """Set default database."""
        self._call_javascript(
            'changeDefaultDatabase', [name],
            wait_on_condition=lambda: self.default_database == name,  # type: ignore
            timeout_message='timeout waiting for database update',
        )

    @property
    def version(self) -> Optional[str]:
        """Version."""
        return self._connection_info.get('version')


portal = Portal()
