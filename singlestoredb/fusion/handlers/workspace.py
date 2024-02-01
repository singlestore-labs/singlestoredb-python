#!/usr/bin/env python3
import json
from typing import Any
from typing import Dict
from typing import Optional

from .. import result
from ..handler import SQLHandler
from ..result import FusionSQLResult
from .utils import dt_isoformat
from .utils import get_workspace
from .utils import get_workspace_group
from .utils import get_workspace_manager


class ShowRegionsHandler(SQLHandler):
    """
    SHOW REGIONS [ <like> ] [ <order-by> ] [ <limit> ];

    """

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        manager = get_workspace_manager()

        res = FusionSQLResult()
        res.add_field('Name', result.STRING)
        res.add_field('ID', result.STRING)
        res.add_field('Provider', result.STRING)

        res.set_rows([(x.name, x.id, x.provider) for x in manager.regions])

        if params['like']:
            res = res.like(Name=params['like'])

        return res.order_by(**params['order_by']).limit(params['limit'])


ShowRegionsHandler.register(overwrite=True)


class ShowWorkspaceGroupsHandler(SQLHandler):
    """
    SHOW WORKSPACE GROUPS [ <like> ] [ <extended> ] [ <order-by> ] [ <limit> ];

    """

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        manager = get_workspace_manager()

        res = FusionSQLResult()
        res.add_field('Name', result.STRING)
        res.add_field('ID', result.STRING)
        res.add_field('Region', result.STRING)
        res.add_field('FirewallRanges', result.JSON)

        if params['extended']:
            res.add_field('CreatedAt', result.DATETIME)
            res.add_field('TerminatedAt', result.DATETIME)

            def fields(x: Any) -> Any:
                return (
                    x.name, x.id, x.region.name,
                    json.dumps(x.firewall_ranges),
                    dt_isoformat(x.created_at),
                    dt_isoformat(x.terminated_at),
                )
        else:
            def fields(x: Any) -> Any:
                return (x.name, x.id, x.region.name, json.dumps(x.firewall_ranges))

        res.set_rows([fields(x) for x in manager.workspace_groups])

        if params['like']:
            res = res.like(Name=params['like'])

        return res.order_by(**params['order_by']).limit(params['limit'])


ShowWorkspaceGroupsHandler.register(overwrite=True)


class ShowWorkspacesHandler(SQLHandler):
    """
    SHOW WORKSPACES [ in_group ] [ <like> ] [ <extended> ] [ <order-by> ] [ <limit> ];

    # Workspace group
    in_group = IN GROUP { group_id | group_name }

    # ID of group
    group_id = ID '<group-id>'

    # Name of group
    group_name = '<group-name>'

    """

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        res = FusionSQLResult()
        res.add_field('Name', result.STRING)
        res.add_field('ID', result.STRING)
        res.add_field('Size', result.STRING)
        res.add_field('State', result.STRING)

        workspace_group = get_workspace_group(params)

        if params['extended']:
            res.add_field('Endpoint', result.STRING)
            res.add_field('CreatedAt', result.DATETIME)
            res.add_field('TerminatedAt', result.DATETIME)

            def fields(x: Any) -> Any:
                return (
                    x.name, x.id, x.size, x.state,
                    x.endpoint, dt_isoformat(x.created_at),
                    dt_isoformat(x.terminated_at),
                )
        else:
            def fields(x: Any) -> Any:
                return (x.name, x.id, x.size, x.state)

        res.set_rows([fields(x) for x in workspace_group.workspaces])

        if params['like']:
            res = res.like(Name=params['like'])

        return res.order_by(**params['order_by']).limit(params['limit'])


ShowWorkspacesHandler.register(overwrite=True)


class CreateWorkspaceGroupHandler(SQLHandler):
    """
    CREATE WORKSPACE GROUP [ if_not_exists ] group_name
        IN REGION { region_id | region_name }
        [ with_password ]
        [ expires_at ]
        [ with_firewall_ranges ]
    ;

    # Only create workspace group if it doesn't exist already
    if_not_exists = IF NOT EXISTS

    # Name of the workspace group
    group_name = '<group-name>'

    # ID of region to create workspace group in
    region_id = ID '<region-id>'

    # Name of region to create workspace group in
    region_name = '<region-name>'

    # Admin password
    with_password = WITH PASSWORD '<password>'

    # Datetime or interval for expiration date/time of workspace group
    expires_at = EXPIRES AT '<iso-datetime-or-interval>'

    # Incoming IP ranges
    with_firewall_ranges = WITH FIREWALL RANGES '<ip-range>',...

    """

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        manager = get_workspace_manager()

        # Only create if one doesn't exist
        if params['if_not_exists']:
            try:
                get_workspace_group(params)
                return None
            except (ValueError, KeyError):
                pass

        # Get region ID
        if params['region_name']:
            regs = [x for x in manager.regions if x.name == params['region_name']]
            if not regs:
                raise KeyError(f'no region found with name "{params["region_name"]}"')
            if len(regs) > 1:
                raise ValueError(
                    f'multiple regions found with the name "{params["region_name"]}"',
                )
            region_id = regs[0].id
        else:
            region_id = params['region_id']

        manager.create_workspace_group(
            params['group_name'],
            region=region_id,
            admin_password=params['with_password'],
            expires_at=params['expires_at'],
            firewall_ranges=params['with_firewall_ranges'],
        )

        return None


CreateWorkspaceGroupHandler.register(overwrite=True)


class CreateWorkspaceHandler(SQLHandler):
    """
    CREATE WORKSPACE [ if_not_exists ] workspace_name [ in_group ]
        WITH SIZE size [ wait_on_active ];

    # Create workspace in workspace group
    in_group = IN GROUP { group_id | group_name }

    # Only run command if workspace doesn't already exist
    if_not_exists = IF NOT EXISTS

    # Name of the workspace
    workspace_name = '<workspace-name>'

    # ID of the group to create workspace in
    group_id = ID '<group-id>'

    # Name of the group to create workspace in
    group_name = '<group-name>'

    # Runtime size
    size = '<size>'

    # Wait for workspace to be active before continuing
    wait_on_active = WAIT ON ACTIVE

    """

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        workspace_group = get_workspace_group(params)

        # Only create if one doesn't exist
        if params['if_not_exists']:
            try:
                workspace_group.workspaces[params['workspace_name']]
                return None
            except KeyError:
                pass

        workspace_group.create_workspace(
            params['workspace_name'], size=params['size'],
            wait_on_active=params['wait_on_active'],
        )

        return None


CreateWorkspaceHandler.register(overwrite=True)


class SuspendWorkspaceHandler(SQLHandler):
    """
    SUSPEND WORKSPACE workspace [ in_group ] [ wait_on_suspended ];

    # Workspace
    workspace = { workspace_id | workspace_name }

    # ID of workspace
    workspace_id = ID '<workspace-id>'

    # Name of workspace
    workspace_name = '<workspace-name>'

    # Workspace group
    in_group = IN GROUP { group_id | group_name }

    # ID of workspace group
    group_id = ID '<group-id>'

    # Name of workspace group
    group_name = '<group-name>'

    # Wait for workspace to be suspended before continuing
    wait_on_suspended = WAIT ON SUSPENDED

    """

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        ws = get_workspace(params)
        ws.suspend(wait_on_suspended=params['wait_on_suspended'])
        return None


SuspendWorkspaceHandler.register(overwrite=True)


class ResumeWorkspaceHandler(SQLHandler):
    """
    RESUME WORKSPACE workspace [ in_group ] [ wait_on_resumed ];

    # Workspace
    workspace = { workspace_id | workspace_name }

    # ID of workspace
    workspace_id = ID '<workspace-id>'

    # Name of workspace
    workspace_name = '<workspace-name>'

    # Workspace group
    in_group = IN GROUP { group_id | group_name }

    # ID of workspace group
    group_id = ID '<group-id>'

    # Name of workspace group
    group_name = '<group-name>'

    # Wait for workspace to be resumed before continuing
    wait_on_resumed = WAIT ON RESUMED

    """

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        ws = get_workspace(params)
        ws.resume(wait_on_resumed=params['wait_on_resumed'])
        return None


ResumeWorkspaceHandler.register(overwrite=True)


class DropWorkspaceGroupHandler(SQLHandler):
    """
    DROP WORKSPACE GROUP [ if_exists ] group [ wait_on_terminated ] [ force ];

    # Only run command if the workspace group exists
    if_exists = IF EXISTS

    # Workspace group
    group = { group_id | group_name }

    # ID of the workspace group to delete
    group_id = ID '<group-id>'

    # Name of the workspace group to delete
    group_name = '<group-name>'

    # Wait for termination to complete before continuing
    wait_on_terminated = WAIT ON TERMINATED

    # Should the workspace group be terminated even if it has workspaces?
    force = FORCE

    """

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        try:
            workspace_group = get_workspace_group(params)
            if workspace_group.terminated_at is not None:
                raise KeyError('workspace group is alread terminated')
            workspace_group.terminate(
                wait_on_terminated=params['wait_on_terminated'],
                force=params['force'],
            )

        except KeyError:
            if not params['if_exists']:
                raise

        return None


DropWorkspaceGroupHandler.register(overwrite=True)


class DropWorkspaceHandler(SQLHandler):
    """
    DROP WORKSPACE [ if_exists ] workspace [ in_group ] [ wait_on_terminated ];

    # Only drop workspace if it exists
    if_exists = IF EXISTS

    # Workspace
    workspace = { workspace_id | workspace_name }

    # ID of workspace
    workspace_id = ID '<workspace-id>'

    # Name of workspace
    workspace_name = '<workspace-name>'

    # Workspace group
    in_group = IN GROUP { group_id | group_name }

    # ID of workspace group
    group_id = ID '<group-id>'

    # Name of workspace group
    group_name = '<group-name>'

    # Wait for workspace to be terminated before continuing
    wait_on_terminated = WAIT ON TERMINATED

    """

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        try:
            ws = get_workspace(params)
            if ws.terminated_at is not None:
                raise KeyError('workspace is already terminated')
            ws.terminate(wait_on_terminated=params['wait_on_terminated'])

        except KeyError:
            if not params['if_exists']:
                raise

        return None


DropWorkspaceHandler.register(overwrite=True)
