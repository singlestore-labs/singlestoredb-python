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

    Description
    -----------
    Show all available regions.

    Remarks
    -------
    * ``LIKE`` specifies a pattern to match. ``%`` is a wildcard.
    * ``ORDER BY`` specifies the column names to sort by.
    * ``LIMIT`` indicates a maximum number of results to return.

    Example
    -------
    Show all regions in the US::

        SHOW REGIONS LIKE 'US%' ORDER BY Name;

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

    Description
    -----------
    Show workspace group information.

    Remarks
    -------
    * ``LIKE`` specifies a pattern to match. ``%`` is a wildcard.
    * ``EXTENDED`` indicates that extra workspace group information should
      be returned in the result set.
    * ``ORDER BY`` specifies the column names to sort by.
    * ``LIMIT`` indicates a maximum number of results to return.

    Example
    -------
    Display workspace groups that match a pattern incuding extended information::

        SHOW WORKSPACE GROUPS LIKE 'Marketing%' EXTENDED ORDER BY Name;

    See Also
    --------
    * SHOW WORKSPACES

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

    Description
    -----------
    Show workspaces in a workspace group.

    Remarks
    -------
    * ``IN GROUP`` specifies the workspace group to list workspaces for. If a
      workspace group ID is specified, you should use ``IN GROUP ID``.
    * ``LIKE`` specifies a pattern to match. ``%`` is a wildcard.
    * ``EXTENDED`` indicates that extra workspace group information should
      be returned in the result set.
    * ``ORDER BY`` specifies the column names to sort by.
    * ``LIMIT`` indicates a maximum number of results to return.

    Example
    -------
    Display workspaces in a workspace group including extended information::

        SHOW WORKSPACES IN GROUP 'My Group' EXTENDED ORDER BY Name;

    See Also
    --------
    * SHOW WORKSPACE GROUPS

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
        [ with_backup_bucket_kms_key_id ]
        [ with_data_bucket_kms_key_id ]
        [ with_smart_dr ]
        [ allow_all_traffic ]
        [ with_update_window ]
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

    # Backup bucket key
    with_backup_bucket_kms_key_id = WITH BACKUP BUCKET KMS KEY ID '<key-id>'

    # Data bucket key
    with_data_bucket_kms_key_id = WITH DATA BUCKET KMS KEY ID '<key-id>'

    # Smart DR
    with_smart_dr = WITH SMART DR

    # Allow all incoming traffic
    allow_all_traffic = ALLOW ALL TRAFFIC

    # Update window
    with_update_window = WITH UPDATE WINDOW '<day>:<hour>'

    Description
    -----------
    Create a workspace group.

    Remarks
    -------
    * ``IF NOT EXISTS`` indicates that the creation of the workspace group
      will only be attempted if a workspace group with that name doesn't
      already exist.
    * ``IN REGION`` specifies the region to create the workspace group in.
      If a region ID is used, ``IN REGION ID`` should be used.
    * ``EXPIRES AT`` specifies an expiration date/time or interval.
    * ``WITH FIREWALL RANGES`` indicates IP ranges to allow access to the
       workspace group.
    * ``WITH BACKUP BUCKET KMS KEY ID`` is the key ID associated with the
      backup bucket.
    * ``WITH DATA BUCKET KMS KEY ID`` is the key ID associated with the
      data bucket.
    * ``WITH SMART DR`` enables smart disaster recovery.
    * ``ALLOW ALL TRAFFIC`` allows all incoming traffic.
    * ``WITH UPDATE WINDOW`` specifies tha day (0-6) and hour (0-23) of the
      update window.

    Examples
    --------
    Example 1: Create workspace group in US East 2 (Ohio)::

        CREATE WORKSPACE GROUP 'My Group' IN REGION 'US East 2 (Ohio)';

    Example 2: Create workspace group with region ID and accessible from anywhere::

        CREATE WORKSPACE GROUP 'My Group'
               IN REGION ID '93b61160-0cae-4e11-a5de-977b8e2e3ee5'
               WITH FIREWALL RANGES '0.0.0.0/0';

    See Also
    --------
    * SHOW WORKSPACE GROUPS

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

        with_update_window = None
        if params['with_update_window']:
            day, hour = params['with_update_window'].split(':', 1)
            with_update_window = dict(day=int(day), hour=int(hour))

        manager.create_workspace_group(
            params['group_name'],
            region=region_id,
            admin_password=params['with_password'],
            expires_at=params['expires_at'],
            firewall_ranges=params['with_firewall_ranges'],
            backup_bucket_kms_key_id=params['with_backup_bucket_kms_key_id'],
            data_bucket_kms_key_id=params['with_data_bucket_kms_key_id'],
            smart_dr=params['with_smart_dr'],
            allow_all_traffic=params['allow_all_traffic'],
            update_window=with_update_window,
        )

        return None


CreateWorkspaceGroupHandler.register(overwrite=True)


class CreateWorkspaceHandler(SQLHandler):
    """
    CREATE WORKSPACE [ if_not_exists ] workspace_name [ in_group ]
        WITH SIZE size [ auto_suspend ] [ enable_kai ]
        [ with_cache_config ] [ wait_on_active ];

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

    # Auto-suspend
    auto_suspend = AUTO SUSPEND suspend_after_seconds SECONDS suspend_type
    suspend_after_seconds = AFTER <integer>
    suspend_type = WITH TYPE { IDLE | SCHEDULED | DISABLED }

    # Enable Kai
    enable_kai = ENABLE KAI

    # Cache config
    with_cache_config = WITH CACHE CONFIG <integer>

    # Wait for workspace to be active before continuing
    wait_on_active = WAIT ON ACTIVE

    Description
    -----------
    Create a workspace in a workspace group.

    Remarks
    -------
    * ``IF NOT EXISTS`` indicates that the creation of the workspace
      will only be attempted if a workspace with that name doesn't
      already exist.
    * ``IN GROUP`` indicates the workspace group to create the workspace
      in. If an ID is used, ``IN GROUP ID`` should be used.
    * ``SIZE`` indicates a cluster size specification such as 'S-00'.
    * ``WITH CACHE CONFIG`` specifies the multiplier for the persistent cache
      associated with the workspace. It must be 1, 2, or 4.
    * ``WAIT ON ACTIVE`` indicates that execution should be paused until
      the workspace has reached the ACTIVE state.

    Example
    -------
    Create a workspace group and wait until it is active::

        CREATE WORKSPACE 'my-workspace' IN GROUP 'My Group'
               WITH SIZE 'S-00' WAIT ON ACTIVE;

    See Also
    --------
    * CREATE WORKSPACE GROUP

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

        auto_suspend = None
        if params['auto_suspend']:
            auto_suspend = dict(
                suspend_after_seconds=params['auto_suspend'][0]['suspend_after_seconds'],
                suspend_type=params['auto_suspend'][-1]['suspend_type'].upper(),
            )

        workspace_group.create_workspace(
            params['workspace_name'],
            size=params['size'],
            auto_suspend=auto_suspend,
            enable_kai=params['enable_kai'],
            cache_config=params['with_cache_config'],
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

    Description
    -----------
    Suspend a workspace.

    Remarks
    -------
    * ``IN GROUP`` indicates the workspace group of the workspace.
      If an ID is used, ``IN GROUP ID`` should be used.
    * ``WAIT ON SUSPENDED`` indicates that execution should be paused until
      the workspace has reached the SUSPENDED state.

    See Also
    --------
    * RESUME WORKSPACE

    """

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        ws = get_workspace(params)
        ws.suspend(wait_on_suspended=params['wait_on_suspended'])
        return None


SuspendWorkspaceHandler.register(overwrite=True)


class ResumeWorkspaceHandler(SQLHandler):
    """
    RESUME WORKSPACE workspace [ in_group ]
        [ disable_auto_suspend ] [ wait_on_resumed ];

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

    # Disable auto-suspend
    disable_auto_suspend = DISABLE AUTO SUSPEND

    # Wait for workspace to be resumed before continuing
    wait_on_resumed = WAIT ON RESUMED

    Description
    -----------
    Resume a workspace.

    Remarks
    -------
    * ``IN GROUP`` indicates the workspace group of the workspace.
      If an ID is used, ``IN GROUP ID`` should be used.
    * ``WAIT ON RESUMED`` indicates that execution should be paused until
      the workspace has reached the RESUMED state.

    """

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        ws = get_workspace(params)
        ws.resume(
            wait_on_resumed=params['wait_on_resumed'],
            disable_auto_suspend=params['disable_auto_suspend'],
        )
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

    Description
    -----------
    Drop a workspace group.

    Remarks
    -------
    * ``IF EXISTS`` indicates that the dropping of the workspace group should
      only be attempted if a workspace group with the given name exists.
    * ``WAIT ON TERMINATED`` specifies that execution should be paused
      until the workspace group reaches the TERMINATED state.
    * ``FORCE`` specifies that the workspace group should be terminated
      even if it contains workspaces.

    Example
    -------
    Drop a workspace group and all workspaces within it::

        DROP WORKSPACE GROUP 'My Group' FORCE;

    See Also
    --------
    * DROP WORKSPACE

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

    Description
    -----------
    Drop a workspace.

    Remarks
    -------
    * ``IF EXISTS`` indicates that the dropping of the workspace should
      only be attempted if a workspace with the given name exists.
    * ``IN GROUP`` indicates the workspace group of the workspace.
      If an ID is used, ``IN GROUP ID`` should be used.
    * ``WAIT ON TERMINATED`` specifies that execution should be paused
      until the workspace reaches the TERMINATED state.

    Example
    -------
    Drop a workspace if it exists::

        DROP WORKSPACE IF EXISTS 'my-workspace' IN GROUP 'My Group';

    See Also
    --------
    * DROP WORKSPACE GROUP

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
