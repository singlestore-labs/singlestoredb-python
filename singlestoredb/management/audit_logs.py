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

    See Also
    --------
    :meth:`AuditLogsManager.list_audit_logs`
    :attr:`AuditLogsManager.audit_logs`

    """

    def __init__(
        self,
        log_id: str,
        timestamp: Union[str, datetime.datetime],
        user_id: str,
        user_email: Optional[str] = None,
        action: Optional[str] = None,
        resource_type: Optional[str] = None,
        resource_id: Optional[str] = None,
        resource_name: Optional[str] = None,
        organization_id: Optional[str] = None,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        success: Optional[bool] = None,
        error_message: Optional[str] = None,
    ):
        #: Unique ID of the audit log entry
        self.id = log_id

        #: Timestamp of when the action occurred
        self.timestamp = to_datetime(timestamp)

        #: ID of the user who performed the action
        self.user_id = user_id

        #: Email of the user who performed the action
        self.user_email = user_email

        #: Action that was performed
        self.action = action

        #: Type of resource the action was performed on
        self.resource_type = resource_type

        #: ID of the resource the action was performed on
        self.resource_id = resource_id

        #: Name of the resource the action was performed on
        self.resource_name = resource_name

        #: Organization ID where the action occurred
        self.organization_id = organization_id

        #: IP address of the user
        self.ip_address = ip_address

        #: User agent string
        self.user_agent = user_agent

        #: Additional details about the action
        self.details = camel_to_snake_dict(details) if details else None

        #: Whether the action was successful
        self.success = success

        #: Error message if the action failed
        self.error_message = error_message

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
            log_id=obj['logID'],
            timestamp=obj['timestamp'],
            user_id=obj['userID'],
            user_email=obj.get('userEmail'),
            action=obj.get('action'),
            resource_type=obj.get('resourceType'),
            resource_id=obj.get('resourceID'),
            resource_name=obj.get('resourceName'),
            organization_id=obj.get('organizationID'),
            ip_address=obj.get('ipAddress'),
            user_agent=obj.get('userAgent'),
            details=obj.get('details'),
            success=obj.get('success'),
            error_message=obj.get('errorMessage'),
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
        start_time: Optional[datetime.datetime] = None,
        end_time: Optional[datetime.datetime] = None,
        user_id: Optional[str] = None,
        action: Optional[str] = None,
        resource_type: Optional[str] = None,
        resource_id: Optional[str] = None,
        success: Optional[bool] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> List[AuditLog]:
        """
        List audit log entries for the organization.

        Parameters
        ----------
        start_time : datetime.datetime, optional
            Start time for filtering audit logs
        end_time : datetime.datetime, optional
            End time for filtering audit logs
        user_id : str, optional
            Filter by user ID
        action : str, optional
            Filter by action type
        resource_type : str, optional
            Filter by resource type
        resource_id : str, optional
            Filter by resource ID
        success : bool, optional
            Filter by success status
        limit : int, optional
            Maximum number of entries to return
        offset : int, optional
            Number of entries to skip

        Returns
        -------
        List[AuditLog]
            List of audit log entries

        Examples
        --------
        >>> audit_mgr = singlestoredb.manage_audit_logs()
        >>> logs = audit_mgr.list_audit_logs(
        ...     action="CREATE_WORKSPACE",
        ...     limit=100
        ... )
        >>> for log in logs:
        ...     print(f"{log.timestamp}: {log.action} by {log.user_email}")
        >>>
        >>> # Filter by time range
        >>> import datetime
        >>> start = datetime.datetime.now() - datetime.timedelta(days=7)
        >>> recent_logs = audit_mgr.list_audit_logs(start_time=start)

        """
        params = {}

        if start_time:
            params['startTime'] = start_time.isoformat()
        if end_time:
            params['endTime'] = end_time.isoformat()
        if user_id:
            params['userID'] = user_id
        if action:
            params['action'] = action
        if resource_type:
            params['resourceType'] = resource_type
        if resource_id:
            params['resourceID'] = resource_id
        if success is not None:
            params['success'] = str(success).lower()
        if limit:
            params['limit'] = str(limit)
        if offset:
            params['offset'] = str(offset)

        res = self._get('auditLogs', params=params if params else None)
        return [AuditLog.from_dict(item) for item in res.json()]

    @property
    def audit_logs(self) -> List[AuditLog]:
        """Return a list of recent audit logs."""
        return self.list_audit_logs(limit=100)

    def get_audit_logs_for_user(
        self,
        user_id: str,
        start_time: Optional[datetime.datetime] = None,
        end_time: Optional[datetime.datetime] = None,
        limit: Optional[int] = None,
    ) -> List[AuditLog]:
        """
        Get audit logs for a specific user.

        Parameters
        ----------
        user_id : str
            ID of the user
        start_time : datetime.datetime, optional
            Start time for filtering audit logs
        end_time : datetime.datetime, optional
            End time for filtering audit logs
        limit : int, optional
            Maximum number of entries to return

        Returns
        -------
        List[AuditLog]
            List of audit log entries for the user

        Examples
        --------
        >>> audit_mgr = singlestoredb.manage_audit_logs()
        >>> user_logs = audit_mgr.get_audit_logs_for_user("user-123")
        >>> print(f"Found {len(user_logs)} log entries for user")

        """
        return self.list_audit_logs(
            user_id=user_id,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
        )

    def get_audit_logs_for_resource(
        self,
        resource_type: str,
        resource_id: str,
        start_time: Optional[datetime.datetime] = None,
        end_time: Optional[datetime.datetime] = None,
        limit: Optional[int] = None,
    ) -> List[AuditLog]:
        """
        Get audit logs for a specific resource.

        Parameters
        ----------
        resource_type : str
            Type of the resource
        resource_id : str
            ID of the resource
        start_time : datetime.datetime, optional
            Start time for filtering audit logs
        end_time : datetime.datetime, optional
            End time for filtering audit logs
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
        return self.list_audit_logs(
            resource_type=resource_type,
            resource_id=resource_id,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
        )

    def get_failed_actions(
        self,
        start_time: Optional[datetime.datetime] = None,
        end_time: Optional[datetime.datetime] = None,
        limit: Optional[int] = None,
    ) -> List[AuditLog]:
        """
        Get audit logs for failed actions.

        Parameters
        ----------
        start_time : datetime.datetime, optional
            Start time for filtering audit logs
        end_time : datetime.datetime, optional
            End time for filtering audit logs
        limit : int, optional
            Maximum number of entries to return

        Returns
        -------
        List[AuditLog]
            List of audit log entries for failed actions

        Examples
        --------
        >>> audit_mgr = singlestoredb.manage_audit_logs()
        >>> failed_logs = audit_mgr.get_failed_actions(limit=50)
        >>> for log in failed_logs:
        ...     print(f"{log.timestamp}: {log.action} failed - {log.error_message}")

        """
        return self.list_audit_logs(
            success=False,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
        )

    def get_actions_by_type(
        self,
        action: str,
        start_time: Optional[datetime.datetime] = None,
        end_time: Optional[datetime.datetime] = None,
        limit: Optional[int] = None,
    ) -> List[AuditLog]:
        """
        Get audit logs for a specific action type.

        Parameters
        ----------
        action : str
            Type of action to filter by
        start_time : datetime.datetime, optional
            Start time for filtering audit logs
        end_time : datetime.datetime, optional
            End time for filtering audit logs
        limit : int, optional
            Maximum number of entries to return

        Returns
        -------
        List[AuditLog]
            List of audit log entries for the action type

        Examples
        --------
        >>> audit_mgr = singlestoredb.manage_audit_logs()
        >>> create_logs = audit_mgr.get_actions_by_type("CREATE_WORKSPACE")
        >>> print(f"Found {len(create_logs)} workspace creation events")

        """
        return self.list_audit_logs(
            action=action,
            start_time=start_time,
            end_time=end_time,
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
