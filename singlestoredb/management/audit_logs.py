#!/usr/bin/env python
"""SingleStoreDB Audit Logs Management."""
import datetime
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

from .manager import Manager
from .utils import camel_to_snake_dict
from .utils import to_datetime
from .utils import vars_to_str


class AuditLog(object):
    """
    SingleStoreDB audit log entry definition.

    This object is not instantiated directly. It is used in the results
    of API calls on the :class:`AuditLogsManager`. Audit logs are retrieved using
    :meth:`AuditLogsManager.list_audit_logs`.

    Represents an audit log entry from the SingleStore Management API.
    Contains information about user actions in the Control Plane that can be
    used to track user activity, including Portal activities, workspace operations,
    team management, authentication events, and more.

    See Also
    --------
    :meth:`AuditLogsManager.list_audit_logs`
    :attr:`AuditLogsManager.audit_logs`

    """

    def __init__(
        self,
        audit_id: str,
        created_at: Union[str, datetime.datetime],
        user_id: Optional[str] = None,
        user_email: Optional[str] = None,
        type: Optional[str] = None,
        reason: Optional[str] = None,
        source: Optional[str] = None,
        user_type: Optional[str] = None,
        org_id: Optional[str] = None,
        project_id: Optional[str] = None,
        workspace_id: Optional[str] = None,
        cluster_id: Optional[str] = None,
        team_id: Optional[str] = None,
        session_id: Optional[str] = None,
        labels: Optional[List[str]] = None,
        attributes: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
        first_name: Optional[str] = None,
        last_name: Optional[str] = None,
    ):
        #: Unique ID of the audit log entry
        self.id = audit_id

        #: Timestamp of when the audit log entry was created (RFC3339Nano format)
        self.created_at = to_datetime(created_at)

        #: ID of the user who performed the action
        self.user_id = user_id

        #: Email of the user who performed the action
        self.user_email = user_email

        #: The audit log entry type
        self.type = type

        #: A human-readable description of what happened
        self.reason = reason

        #: The audit log entry source (Portal, Admin, SystemJob)
        self.source = source

        #: The type of user that triggered the audit log entry
        self.user_type = user_type

        #: Organization ID tied to this event
        self.organization_id = org_id

        #: Project ID tied to this event
        self.project_id = project_id

        #: Workspace ID tied to this event
        self.workspace_id = workspace_id

        #: Database cluster ID tied to this event
        self.cluster_id = cluster_id

        #: Team ID tied to this event
        self.team_id = team_id

        #: Authorization session ID tied to this event
        self.session_id = session_id

        #: A list of audit keywords
        self.labels = labels or []

        #: Additional keys and values that are specific to the audit log type
        self.attributes = camel_to_snake_dict(attributes) if attributes else None

        #: Text error message, if any relating to this entry
        self.error = error

        #: The first name of a redacted user
        self.first_name = first_name

        #: The last name of a redacted user
        self.last_name = last_name

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)

    @classmethod
    def from_dict(cls, obj: Dict[str, Any]) -> 'AuditLog':
        """
        Construct an AuditLog from a dictionary of values.

        Parameters
        ----------
        obj : dict
            Dictionary of values

        Returns
        -------
        :class:`AuditLog`

        """
        return cls(
            audit_id=obj['auditID'],
            created_at=obj['createdAt'],
            user_id=obj.get('userID'),
            user_email=obj.get('userEmail'),
            type=obj.get('type'),
            reason=obj.get('reason'),
            source=obj.get('source'),
            user_type=obj.get('userType'),
            org_id=obj.get('orgID'),
            project_id=obj.get('projectID'),
            workspace_id=obj.get('workspaceID'),
            cluster_id=obj.get('clusterID'),
            team_id=obj.get('teamID'),
            session_id=obj.get('sessionID'),
            labels=obj.get('labels'),
            attributes=obj.get('attributes'),
            error=obj.get('error'),
            first_name=obj.get('firstName'),
            last_name=obj.get('lastName'),
        )


class AuditLogsManager(Manager):
    """
    SingleStoreDB audit logs manager.

    This class should be instantiated using :func:`singlestoredb.manage_audit_logs`
    or accessed via :attr:`WorkspaceManager.audit_logs`.

    Parameters
    ----------
    access_token : str, optional
        The API key or other access token for the management API
    version : str, optional
        Version of the API to use
    base_url : str, optional
        Base URL of the management API

    """

    #: Object type
    obj_type = 'audit_log'

    def list_audit_logs(
        self,
        start_date: Optional[datetime.datetime] = None,
        end_date: Optional[datetime.datetime] = None,
        log_type: Optional[str] = None,
        source: Optional[str] = None,
        limit: Optional[int] = None,
        next_token: Optional[str] = None,
        first_name: Optional[str] = None,
        last_name: Optional[str] = None,
    ) -> List[AuditLog]:
        """
        List audit log entries for the organization.

        Parameters
        ----------
        start_date : datetime.datetime, optional
            Start date (inclusive) for filtering audit logs in RFC3339 format
        end_date : datetime.datetime, optional
            End date (inclusive) for filtering audit logs in RFC3339 format
        log_type : str, optional
            Filter by audit log entry type
        source : str, optional
            Filter by source (Portal, Admin, SystemJob)
        limit : int, optional
            Maximum number of entries to return
        next_token : str, optional
            Token from previous query for pagination
        first_name : str, optional
            Filter by first name (for user redaction)
        last_name : str, optional
            Filter by last name (for user redaction)

        Returns
        -------
        List[AuditLog]
            List of audit log entries

        Examples
        --------
        >>> audit_mgr = singlestoredb.manage_audit_logs()
        >>> logs = audit_mgr.list_audit_logs(
        ...     log_type="Login",
        ...     limit=100
        ... )
        >>> for log in logs:
        ...     print(f"{log.created_at}: {log.type} by {log.user_email}")
        >>>
        >>> # Filter by time range
        >>> import datetime
        >>> start = datetime.datetime.now() - datetime.timedelta(days=7)
        >>> recent_logs = audit_mgr.list_audit_logs(start_date=start)

        """
        params = {}

        if start_date:
            params['startDate'] = start_date.isoformat()
        if end_date:
            params['endDate'] = end_date.isoformat()
        if log_type:
            params['type'] = log_type
        if source:
            params['source'] = source
        if limit:
            params['limit'] = str(limit)
        if next_token:
            params['nextToken'] = next_token
        if first_name:
            params['firstName'] = first_name
        if last_name:
            params['lastName'] = last_name

        res = self._get('auditLogs', params=params if params else None)
        return [AuditLog.from_dict(item) for item in res.json()['auditLogs']]

    @property
    def audit_logs(self) -> List[AuditLog]:
        """Return a list of recent audit logs."""
        return self.list_audit_logs(limit=100)

    def get_audit_logs_for_user(
        self,
        user_email: str,
        start_date: Optional[datetime.datetime] = None,
        end_date: Optional[datetime.datetime] = None,
        limit: Optional[int] = None,
    ) -> List[AuditLog]:
        """
        Get audit logs for a specific user by email.

        Note: The API doesn't support filtering by user_id directly, so this method
        retrieves all logs and filters them client-side by user_email.

        Parameters
        ----------
        user_email : str
            Email address of the user
        start_date : datetime.datetime, optional
            Start date for filtering audit logs
        end_date : datetime.datetime, optional
            End date for filtering audit logs
        limit : int, optional
            Maximum number of entries to return

        Returns
        -------
        List[AuditLog]
            List of audit log entries for the user

        Examples
        --------
        >>> audit_mgr = singlestoredb.manage_audit_logs()
        >>> user_logs = audit_mgr.get_audit_logs_for_user("user@example.com")
        >>> print(f"Found {len(user_logs)} log entries for user")

        """
        # Get all logs and filter client-side since API doesn't support user_id filter
        all_logs = self.list_audit_logs(
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )
        # Filter for logs that match the user email
        return [log for log in all_logs if log.user_email == user_email]

    def get_audit_logs_for_resource(
        self,
        resource_type: str,
        resource_id: str,
        start_date: Optional[datetime.datetime] = None,
        end_date: Optional[datetime.datetime] = None,
        limit: Optional[int] = None,
    ) -> List[AuditLog]:
        """
        Get audit logs for a specific resource.

        Note: The API doesn't support filtering by resource IDs directly, so this method
        retrieves all logs and filters them client-side by checking the attributes.

        Parameters
        ----------
        resource_type : str
            Type of the resource (workspace, cluster, team, project, organization)
        resource_id : str
            ID of the resource
        start_date : datetime.datetime, optional
            Start date for filtering audit logs
        end_date : datetime.datetime, optional
            End date for filtering audit logs
        limit : int, optional
            Maximum number of entries to return

        Returns
        -------
        List[AuditLog]
            List of audit log entries for the resource

        Examples
        --------
        >>> audit_mgr = singlestoredb.manage_audit_logs()
        >>> workspace_logs = audit_mgr.get_audit_logs_for_resource(
        ...     "workspace", "ws-123"
        ... )
        >>> print(f"Found {len(workspace_logs)} log entries for workspace")

        """
        # Get all logs and filter client-side since API doesn't support resource filters
        all_logs = self.list_audit_logs(
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )

        # Filter for logs that match the resource
        filtered_logs = []
        for log in all_logs:
            if resource_type.lower() == 'workspace' and log.workspace_id == resource_id:
                filtered_logs.append(log)
            elif resource_type.lower() == 'cluster' and log.cluster_id == resource_id:
                filtered_logs.append(log)
            elif resource_type.lower() == 'team' and log.team_id == resource_id:
                filtered_logs.append(log)
            elif resource_type.lower() == 'project' and log.project_id == resource_id:
                filtered_logs.append(log)
            elif (
                resource_type.lower() == 'organization' and
                log.organization_id == resource_id
            ):
                filtered_logs.append(log)

        return filtered_logs

    def get_failed_actions(
        self,
        start_date: Optional[datetime.datetime] = None,
        end_date: Optional[datetime.datetime] = None,
        limit: Optional[int] = None,
    ) -> List[AuditLog]:
        """
        Get audit logs that contain error messages.

        Note: This method filters for logs that have error messages, as the
        audit log schema doesn't have a simple success/failure boolean field.

        Parameters
        ----------
        start_date : datetime.datetime, optional
            Start date for filtering audit logs
        end_date : datetime.datetime, optional
            End date for filtering audit logs
        limit : int, optional
            Maximum number of entries to return

        Returns
        -------
        List[AuditLog]
            List of audit log entries that contain error messages

        Examples
        --------
        >>> audit_mgr = singlestoredb.manage_audit_logs()
        >>> failed_logs = audit_mgr.get_failed_actions(limit=50)
        >>> for log in failed_logs:
        ...     print(f"{log.created_at}: {log.type} - {log.error}")

        """
        # Get all logs and filter for those with error messages
        all_logs = self.list_audit_logs(
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )
        # Filter for logs that have error messages
        return [log for log in all_logs if log.error]

    def get_actions_by_type(
        self,
        log_type: str,
        start_date: Optional[datetime.datetime] = None,
        end_date: Optional[datetime.datetime] = None,
        limit: Optional[int] = None,
    ) -> List[AuditLog]:
        """
        Get audit logs for a specific log type.

        Parameters
        ----------
        log_type : str
            Type of audit log entry to filter by (e.g., "Login", "Logout", etc.)
        start_date : datetime.datetime, optional
            Start date for filtering audit logs
        end_date : datetime.datetime, optional
            End date for filtering audit logs
        limit : int, optional
            Maximum number of entries to return

        Returns
        -------
        List[AuditLog]
            List of audit log entries for the log type

        Examples
        --------
        >>> audit_mgr = singlestoredb.manage_audit_logs()
        >>> login_logs = audit_mgr.get_actions_by_type("Login")
        >>> print(f"Found {len(login_logs)} login events")

        """
        return self.list_audit_logs(
            log_type=log_type,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )


def manage_audit_logs(
    access_token: Optional[str] = None,
    version: Optional[str] = None,
    base_url: Optional[str] = None,
    *,
    organization_id: Optional[str] = None,
) -> AuditLogsManager:
    """
    Retrieve a SingleStoreDB audit logs manager.

    Parameters
    ----------
    access_token : str, optional
        The API key or other access token for the management API
    version : str, optional
        Version of the API to use
    base_url : str, optional
        Base URL of the management API
    organization_id : str, optional
        ID of organization, if using a JWT for authentication

    Returns
    -------
    :class:`AuditLogsManager`

    Examples
    --------
    >>> import singlestoredb as s2
    >>> audit_mgr = s2.manage_audit_logs()
    >>> logs = audit_mgr.audit_logs
    >>> print(f"Found {len(logs)} recent audit log entries")

    """
    return AuditLogsManager(
        access_token=access_token,
        base_url=base_url,
        version=version,
        organization_id=organization_id,
    )
