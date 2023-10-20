#!/usr/bin/env python3
import json
from typing import Any
from typing import Dict

from .. import result
from ...management.workspace import WorkspaceGroup
from ..handler import SQLHandler


def get_workspace_group(manager: Any, params: Dict[str, Any]) -> WorkspaceGroup:
    """Find a workspace group matching group_id or group_name."""
    if 'group_name' in params:
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


class ShowRegionsHandler(SQLHandler):
    """
    SHOW REGIONS [ like ];

    like = LIKE '<pattern>'

    """

    def run(self, params: Dict[str, Any]) -> result.SQLResult:
        desc = [
            ('Name', result.STRING),
            ('ID', result.STRING),
            ('Provider', result.STRING),
        ]

        is_like = self.build_like_func(params.get('like', None))

        return desc, [(x.name, x.id, x.provider)
                      for x in self.manager.regions if is_like(x.name)]


ShowRegionsHandler.register()


class ShowWorkspaceGroupsHandler(SQLHandler):
    """
    SHOW WORKSPACE GROUPS [ like ] [ extended ];

    like = LIKE '<pattern>'
    extended = EXTENDED

    """

    def run(self, params: Dict[str, Any]) -> result.SQLResult:
        desc = [
            ('Name', result.STRING),
            ('ID', result.STRING),
            ('Region Name', result.STRING),
            ('Firewall Ranges', result.JSON),
        ]

        if params.get('extended'):
            desc += [
                ('Created At', result.DATETIME),
                ('Terminated At', result.DATETIME),
            ]

            def fields(x: Any) -> Any:
                return (
                    x.name, x.id, x.region.name,
                    json.dumps(x.firewall_ranges),
                    x.created_at, x.terminated_at,
                )
        else:
            def fields(x: Any) -> Any:
                return (x.name, x.id, x.region.name, x.firewall_ranges)

        is_like = self.build_like_func(params.get('like', None))

        return (
            desc,
            [fields(x) for x in self.manager.workspace_groups if is_like(x.name)],
        )


ShowWorkspaceGroupsHandler.register()


class ShowWorkspacesHandler(SQLHandler):
    """
    SHOW WORKSPACES IN GROUP { group_id | group_name } [ like ] [ extended ];

    group_id = ID '<group-id>'
    group_name = '<group-name>'
    like = LIKE '<pattern>'
    extended = EXTENDED

    """

    def run(self, params: Dict[str, Any]) -> result.SQLResult:
        desc = [
            ('Name', result.STRING),
            ('ID', result.STRING),
            ('Size', result.STRING),
            ('State', result.STRING),
        ]

        workspace_group = get_workspace_group(self.manager, params)

        if params.get('extended'):
            desc += [
                ('Endpoint', result.STRING),
                ('Created At', result.DATETIME),
                ('Terminated At', result.DATETIME),
            ]

            def fields(x: Any) -> Any:
                return (
                    x.name, x.id, x.size, x.state,
                    x.endpoint, x.created_at, x.terminated_at,
                )
        else:
            def fields(x: Any) -> Any:
                return (x.name, x.id, x.size, x.state)

        is_like = self.build_like_func(params.get('like', None))

        return desc, [fields(x) for x in workspace_group.workspaces if is_like(x.name)]


ShowWorkspacesHandler.register()


class CreateWorkspaceGroupHandler(SQLHandler):
    """
    CREATE WORKSPACE GROUP [ if_not_exists ] group_name
        IN REGION { region_id | region_name }
        [ with_password ]
        [ expires_at ]
        [ with_firewall_ranges ]
    ;

    if_not_exists = IF NOT EXISTS
    group_name = '<group-name>'
    region_id = ID '<region-id>'
    region_name = '<region-name>'
    with_password = WITH PASSWORD '<password>'
    expires_at = EXPIRES AT '<iso-datetime-or-interval>'
    with_firewall_ranges = WITH FIREWALL RANGES '<ip-range>',...

    """

    def run(self, params: Dict[str, Any]) -> result.SQLResult:
        # Only create if one doesn't exist
        if params.get('if_not_exists'):
            try:
                get_workspace_group(self.manager, params)
                return [], []
            except (ValueError, KeyError):
                pass

        # Get region ID
        if 'region_name' in params:
            regs = [x for x in self.manager.regions if x.name == params['region_name']]
            if not regs:
                raise ValueError(f'no region found with name "{params["region_name"]}"')
            if len(regs) > 1:
                raise ValueError(
                    f'multiple regions found with the name "{params["region_name"]}"',
                )
            region_id = regs[0].id
        else:
            region_id = params['region_id']

        self.manager.create_workspace_group(
            params['group_name'],
            region=region_id,
            admin_password=params.get('with_password'),
            expires_at=params.get('expires_at'),
            firewall_ranges=params.get('with_firewall_ranges', []),
        )

        return [], []


CreateWorkspaceGroupHandler.register()


class CreateWorkspaceHandler(SQLHandler):
    """
    CREATE WORKSPACE [ if_not_exists ] workspace_name
        IN GROUP { group_id | group_name }
        WITH SIZE size [ wait_on_active ];

    if_not_exists = IF NOT EXISTS
    workspace_name = '<workspace-name>'
    group_id = ID '<group-id>'
    group_name = '<group-name>'
    size = '<size>'
    wait_on_active = WAIT ON ACTIVE

    """

    def run(self, params: Dict[str, Any]) -> result.SQLResult:
        workspace_group = get_workspace_group(self.manager, params)

        # Only create if one doesn't exist
        if params.get('if_not_exists'):
            try:
                workspace_group.workspaces[params['workspace_name']]
                return [], []
            except KeyError:
                pass

        workspace_group.create_workspace(
            params['workspace_name'], size=params['size'],
            wait_on_active=params.get('wait_on_active', False),
        )

        return [], []


CreateWorkspaceHandler.register()


class DropWorkspaceGroupHandler(SQLHandler):
    """
    DROP WORKSPACE GROUP [ if_exists ] { group_id | group_name }
        [ wait_on_terminated ];

    if_exists = IF EXISTS
    group_id = ID '<group-id>'
    group_name = '<group-name>'
    wait_on_terminated = WAIT ON TERMINATED

    """

    def run(self, params: Dict[str, Any]) -> result.SQLResult:
        try:
            name_or_id = params.get('group_name', params.get('group_id'))
            wg = self.manager.workspace_groups[name_or_id]
            wg.terminate(wait_on_terminated=params.get('wait_on_terminated', False))

        except KeyError:
            if not params.get('if_exists'):
                raise ValueError(f"could not find workspace group '{name_or_id}'")

        return [], []


DropWorkspaceGroupHandler.register()


class DropWorkspaceHandler(SQLHandler):
    """
    DROP WORKSPACE [ if_exists ] { workspace_id | workspace_name }
        IN GROUP { group_id | group_name } [ wait_on_terminated ];

    if_exists = IF EXISTS
    workspace_id = ID '<workspace-id>'
    workspace_name = '<workspace-name>'
    group_id = ID '<group-id>'
    group_name = '<group-name>'
    wait_on_terminated = WAIT ON TERMINATED

    """

    def run(self, params: Dict[str, Any]) -> result.SQLResult:
        try:
            workspace_name_or_id = params.get(
                'workspace_name', params.get('workspace_id'),
            )
            group_name_or_id = params.get('group_name', params.get('group_id'))
            wg = self.manager.workspace_groups[group_name_or_id]
            ws = wg.workspaces[workspace_name_or_id]
            ws.terminate(wait_on_terminated=params.get('wait_on_terminated', False))

        except KeyError:
            if not params.get('if_exists'):
                raise ValueError(
                    f"could not find workspace '{workspace_name_or_id}' "
                    f"in group '{group_name_or_id}'",
                )

        return [], []


DropWorkspaceHandler.register()
