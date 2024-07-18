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


class UseWorkspaceHandler(SQLHandler):
    """
    USE WORKSPACE workspace [ with_database ];

    # Workspace
    workspace = { workspace_id | workspace_name | current_workspace }

    # ID of workspace
    workspace_id = ID '<workspace-id>'

    # Name of workspace
    workspace_name = '<workspace-name>'

    # Current workspace
    current_workspace = @@CURRENT

    # Name of database
    with_database = WITH DATABASE 'database-name'

    Description
    -----------
    Change the workspace and database in the notebook.

    Arguments
    ---------
    * ``<workspace-id>``: The ID of the workspace to delete.
    * ``<workspace-name>``: The name of the workspace to delete.

    Remarks
    -------
    * If you want to specify a database in the current workspace,
      the workspace name can be specified as ``@@CURRENT``.
    * Specify the ``WITH DATABASE`` clause to select a default
      database for the session.
    * This command only works in a notebook session in the
      Managed Service.

    Example
    -------
    The following command sets the workspace to ``examplews`` and
    select 'dbname' as the default database::

        USE WORKSPACE 'examplews' WITH DATABASE 'dbname';

    """
    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        from singlestoredb.notebook import portal
        if params['workspace'].get('current_workspace'):
            if params.get('with_database'):
                portal.default_database = params['with_database']
        elif params.get('with_database'):
            if params['workspace'].get('workspace_name'):
                portal.connection = params['workspace']['workspace_name'], \
                                    params['with_database']
            else:
                portal.connection = params['workspace']['workspace_id'], \
                                    params['with_database']
        elif params['workspace'].get('workspace_name'):
            portal.workspace = params['workspace']['workspace_name']
        else:
            portal.workspace = params['workspace']['workspace_id']
        return None


UseWorkspaceHandler.register(overwrite=True)


class ShowRegionsHandler(SQLHandler):
    """
    SHOW REGIONS [ <like> ]
        [ <order-by> ]
        [ <limit> ];

    Description
    -----------
    Returns a list of all the valid regions for the user.

    Arguments
    ---------
    * ``<pattern>``: A pattern similar to SQL LIKE clause.
      Uses ``%`` as the wildcard character.

    Remarks
    -------
    * Use the ``LIKE`` clause to specify a pattern and return only the
      regions that match the specified pattern.
    * The ``LIMIT`` clause limits the number of results to the
      specified number.
    * Use the ``ORDER BY`` clause to sort the results by the specified
      key. By default, the results are sorted in the ascending order.

    Example
    -------
    The following command returns a list of all the regions in the US
    and sorts the results in ascending order by their ``Name``::

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
    SHOW WORKSPACE GROUPS [ <like> ]
        [ <extended> ] [ <order-by> ]
        [ <limit> ];

    Description
    -----------
    Displays information on workspace groups.

    Arguments
    ---------
    * ``<pattern>``: A pattern similar to SQL LIKE clause.
      Uses ``%`` as the wildcard character.

    Remarks
    -------
    * Use the ``LIKE`` clause to specify a pattern and return only the
      workspace groups that match the specified pattern.
    * The ``LIMIT`` clause limits the number of results to the
      specified number.
    * Use the ``ORDER BY`` clause to sort the results by the specified
      key. By default, the results are sorted in the ascending order.
    * To return more information about the workspace groups, use the
      ``EXTENDED`` clause.

    Example
    -------
    The following command displays a list of workspace groups with names
    that match the specified pattern::

        SHOW WORKSPACE GROUPS LIKE 'Marketing%' EXTENDED ORDER BY Name;

    See Also
    --------
    * ``SHOW WORKSPACES``

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
    SHOW WORKSPACES [ in_group ]
        [ <like> ] [ <extended> ]
        [ <order-by> ] [ <limit> ];

    # Workspace group
    in_group = IN GROUP { group_id | group_name }

    # ID of group
    group_id = ID '<group-id>'

    # Name of group
    group_name = '<group-name>'

    Description
    -----------
    Displays information on workspaces in a workspace group.

    Arguments
    ---------
    * ``<group_id>``: The ID of the workspace group that contains
      the workspace.
    * ``<group_name>``: The name of the workspace group that
      contains the workspace.
    * ``<pattern>``: A pattern similar to SQL LIKE clause.
      Uses ``%`` as the wildcard character.

    Remarks
    -------
    * The ``IN GROUP`` clause specifies the ID or the name of the
      workspace group that contains the workspace.
    * Use the ``LIKE`` clause to specify a pattern and return only
      the workspaces that match the specified pattern.
    * The ``LIMIT`` clause limits the number of results to the
      specified number.
    * Use the ``ORDER BY`` clause to sort the results by the
      specified key. By default, the results are sorted in the
      ascending order.
    * To return more information about the workspaces, use the
      ``EXTENDED`` clause.

    Example
    -------
    The following command displays information on all the workspaces
    in a workspace group named **wsg1** and sorts the results by
    workspace name in the ascending order::

        SHOW WORKSPACES IN GROUP 'wsg1' EXTENDED ORDER BY Name;

    See Also
    --------
    * ``SHOW WORKSPACE GROUPS``

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
    Creates a workspace group.

    Arguments
    ---------
    * ``<group-name>``: The name of the workspace group.
    * ``<region_id>`` or ``<region_name>``: The ID or the name of the region
      in which the new workspace group is created.
    * ``<password>``: The admin password of the workspace group.
      The password must contain:
        - At least 8 characters
        - At least one uppercase character
        - At least one lowercase character
        - At least one number or special character
    * ``<expiry_time>``: The timestamp of when the workspace group terminates.
      Expiration time can be specified as a timestamp or duration.
    * ``<ip_range>``: A list of allowed IP addresses or CIDR ranges.
    * ``<backup_key_id>``: The KMS key ID associated with the backup bucket.
    * ``<data_key_id>``: The KMS key ID associated with the data bucket.
    * ``<day>:<hour>``: The day of the week (0-6) and the hour of the day
      (0-23) when the engine updates are applied to the workspace group.

    Remarks
    -------
    * Specify the ``IF NOT EXISTS`` clause to create a new workspace group only
      if a workspace group with the specified name does not exist.
    * If the ``WITH BACKUP BUCKET KMS KEY ID '<backup_key_id>'`` clause is
      specified, Customer-Managed Encryption Keys (CMEK) encryption is enabled
      for the data bucket of the workspace group.
    * If the ``WITH DATA BUCKET KMS KEY ID '<data_key_id>'`` clause is specified,
      CMEK encryption for the data bucket and Amazon Elastic Block Store (EBS)
      volumes of the workspace group is enabled.
    * To enable Smart Disaster Recovery (SmartDR) for the workspace group, specify
      the WITH SMART DR clause. Refer to Smart Disaster Recovery (DR):
      SmartDR for more information.
    * To allow incoming traffic from any IP address, use the ``ALLOW ALL TRAFFIC``
      clause.

    Examples
    --------
    The following command creates a workspace group named wsg1 in the
    US East 2 (Ohio) region::

        CREATE WORKSPACE GROUP 'wsg1' IN REGION 'US East 2 (Ohio)';

    The following command specifies additional workspace group configuration
    options::

        CREATE WORKSPACE GROUP 'wsg1'
            IN REGION ID '93b61160-0000-1000-9000-977b8e2e3ee5'
            WITH FIREWALL RANGES '0.0.0.0/0';

    See Also
    --------
    * ``SHOW WORKSPACE GROUPS``

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
    CREATE WORKSPACE [ if_not_exists ] workspace_name
        [ in_group ]
        WITH SIZE size
        [ auto_suspend ]
        [ enable_kai ]
        [ with_cache_config ]
        [ wait_on_active ];

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
    auto_suspend = AUTO SUSPEND AFTER suspend_after_value suspend_after_units suspend_type
    suspend_after_value = <integer>
    suspend_after_units = { SECONDS | MINUTES | HOURS | DAYS }
    suspend_type = WITH TYPE { IDLE | SCHEDULED | DISABLED }

    # Enable Kai
    enable_kai = ENABLE KAI

    # Cache config
    with_cache_config = WITH CACHE CONFIG <integer>

    # Wait for workspace to be active before continuing
    wait_on_active = WAIT ON ACTIVE

    Description
    -----------
    Creates a new workspace. Refer to
    `Creating and Using Workspaces <https://docs.singlestore.com/cloud/getting-started-with-singlestore-helios/about-workspaces/creating-and-using-workspaces/>`_
    for more information.

    Arguments
    ---------
    * ``<workspace_name>``: The name of the workspace.
    * ``<group_id>`` or ``<group_name>``: The ID or name of the workspace group
      in which the workspace is created.
    * ``<workspace_size>``: The size of the workspace in workspace size notation,
      for example "S-1".
    * ``<suspend_time>``: The time (in given units) after which the workspace is
      suspended, according to the specified auto-suspend type.
    * ``<multiplier>``: The multiplier for the persistent cache associated with
      the workspace.

    Remarks
    -------
    * Use the ``IF NOT EXISTS`` clause to create a new workspace only if a workspace
      with the specified name does not exist.
    * If the ``WITH CACHE CONFIG <multiplier>`` clause is specified, the cache
      configuration multiplier is enabled for the workspace. It can have the
      following values: 1, 2, or 4.
    * The ``WAIT ON ACTIVE`` clause indicates that the execution is paused until this
      workspace is in ACTIVE state.
    * Specify the ``ENABLE KAI`` clause to enable SingleStore Kai and the MongoDB®
      API for the workspace.

    Example
    -------
    The following command creates a workspace named **examplews** in a workspace
    group named **wsg1**::

        CREATE WORKSPACE 'examplews' IN GROUP 'wsgroup1'
            WITH SIZE 'S-00' WAIT ON ACTIVE;

    See Also
    --------
    * ``CREATE WORKSPACE GROUP``

    """  # noqa: E501

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
            mult = dict(
                SECONDS=1,
                MINUTES=60,
                HOURS=60*60,
                DAYS=60*60*24,
            )
            val = params['auto_suspend'][0]['suspend_after_value']
            val = val * mult[params['auto_suspend'][1]['suspend_after_units'].upper()]
            auto_suspend = dict(
                suspend_after_seconds=val,
                suspend_type=params['auto_suspend'][2]['suspend_type'].upper(),
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
    SUSPEND WORKSPACE workspace
        [ in_group ]
        [ wait_on_suspended ];

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
    Suspends a workspace.

    Refer to `Manage Workspaces <https://docs.singlestore.com/cloud/user-and-workspace-administration/manage-organizations/manage-workspaces/>`_
    for more information.

    Arguments
    ---------
    * ``<workspace-id>``: The ID of the workspace to suspend.
    * ``<workspace-name>``: The name of the workspace to suspend.
    * ``<group-id>``: The ID of the workspace group that contains
      the workspace.
    * ``<group-name>``: The name of the workspace group that
      contains the workspace.

    Remarks
    -------
    * Use the ``WAIT ON SUSPENDED`` clause to pause query execution
      until the workspace is in the ``SUSPENDED`` state.

    Example
    -------
    The following example suspends a workspace named examplews in
    a workspace group named **wsg1**::

        SUSPEND WORKSPACE 'examplews' IN GROUP 'wsg1';

    See Also
    --------
    * ``RESUME WORKSPACE``

    """  # noqa: E501

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        ws = get_workspace(params)
        ws.suspend(wait_on_suspended=params['wait_on_suspended'])
        return None


SuspendWorkspaceHandler.register(overwrite=True)


class ResumeWorkspaceHandler(SQLHandler):
    """
    RESUME WORKSPACE workspace
        [ in_group ]
        [ disable_auto_suspend ]
        [ wait_on_resumed ];

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
    Resumes a workspace.

    Refer to `Manage Workspaces <https://docs.singlestore.com/cloud/user-and-workspace-administration/manage-organizations/manage-workspaces/>`_
    for more information.

    Arguments
    ---------
    * ``<workspace-id>``: The ID of the workspace to resume.
    * ``<workspace-name>``: The name of the workspace to resume.
    * ``<group_id>``: The ID of the workspace group that contains
      the workspace.
    * ``<group_name>``: The name of the workspace group that
      contains the workspace.

    Remarks
    -------
    * Use the ``IN GROUP`` clause to specify the ID or name of the
      workspace group that contains the workspace to resume.
    * Use the ``WAIT ON RESUMED`` clause to pause query execution
      until the workspace is in the ``RESUMED`` state.
    * Specify the ``DISABLE AUTO SUSPEND`` clause to disable
      auto-suspend for the resumed workspace.

    Example
    -------
    The following example resumes a workspace with the specified ID
    in a workspace group named **wsg1**::

        RESUME WORKSPACE ID '93b61160-0000-1000-9000-977b8e2e3ee5'
            IN GROUP 'wsg1';

    """  # noqa: E501

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
    DROP WORKSPACE GROUP [ if_exists ]
        group
        [ wait_on_terminated ]
        [ force ];

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
    Deletes the specified workspace group.

    Arguments
    ---------
    * ``<group_id>``: The ID of the workspace group to delete.
    * ``<group_name>``: The name of the workspace group to delete.

    Remarks
    -------
    * Specify the ``IF EXISTS`` clause to attempt the delete operation
      only if a workspace group with the specified ID or name exists.
    * Use the ``WAIT ON TERMINATED`` clause to pause query execution until
      the workspace group is in the ``TERMINATED`` state.
    * If the ``FORCE`` clause is specified, the workspace group is
      terminated even if it contains workspaces.

    Example
    -------
    The following command deletes a workspace group named **wsg1** even
    if it contains workspaces::

        DROP WORKSPACE GROUP 'wsg1' FORCE;

    See Also
    --------
    * ``DROP WORKSPACE``

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
    DROP WORKSPACE [ if_exists ]
        workspace
        [ in_group ]
        [ wait_on_terminated ];

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
    Deletes a workspace.

    Arguments
    ---------
    * ``<workspace-id>``: The ID of the workspace to delete.
    * ``<workspace-name>``: The name of the workspace to delete.
    * ``<group_id>``: The ID of the workspace group that contains
      the workspace.
    * ``<group_name>``: The name of the workspace group that
      contains the workspace.

    Remarks
    -------
    * Specify the ``IF EXISTS`` clause to attempt the delete operation
      only if a workspace with the specified ID or name exists.
    * Use the ``IN GROUP`` clause to specify the ID or name of the workspace
      group that contains the workspace to delete.
    * Use the ``WAIT ON TERMINATED`` clause to pause query execution until
      the workspace is in the ``TERMINATED`` state.
    * All databases attached to the workspace are detached when the
      workspace is deleted (terminated).

    Example
    -------
    The following example deletes a workspace named **examplews** in
    a workspace group **wsg1**::

        DROP WORKSPACE IF EXISTS 'examplews' IN GROUP 'wsg1';

    See Also
    --------
    * ``DROP WORKSPACE GROUP``

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
