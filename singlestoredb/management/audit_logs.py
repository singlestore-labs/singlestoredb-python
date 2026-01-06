#!/usr/bin/env python
"""SingleStoreDB Audit Log Management."""
from __future__ import annotations

import datetime
from typing import Any
from typing import cast
from typing import Dict
from typing import List
from typing import Optional
from typing import TYPE_CHECKING

from .utils import NamedList
from .utils import normalize_audit_log_source
from .utils import normalize_audit_log_type
from .utils import to_datetime
from .utils import vars_to_str

if TYPE_CHECKING:
    from .workspace import WorkspaceManager


class AuditLog:
    """
    SingleStoreDB audit log entry.

    This object is not instantiated directly. It is used in the results
    of API calls on the :class:`WorkspaceManager`. Audit logs are accessed using
    :meth:`WorkspaceManager.get_audit_logs`.

    See Also
    --------
    :meth:`WorkspaceManager.get_audit_logs`

    """

    id: str
    type: str
    source: Optional[str]
    timestamp: Optional[datetime.datetime]
    user_id: Optional[str]
    user_email: Optional[str]
    first_name: Optional[str]
    last_name: Optional[str]
    resource_type: Optional[str]
    resource_id: Optional[str]
    resource_name: Optional[str]
    details: Optional[Dict[str, Any]]

    def __init__(
        self,
        audit_log_id: str,
        type: str,
        source: Optional[str] = None,
        timestamp: Optional[datetime.datetime] = None,
        user_id: Optional[str] = None,
        user_email: Optional[str] = None,
        first_name: Optional[str] = None,
        last_name: Optional[str] = None,
        resource_type: Optional[str] = None,
        resource_id: Optional[str] = None,
        resource_name: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ):
        #: Unique ID of the audit log entry
        self.id = audit_log_id

        #: Type of action (e.g., 'Login')
        self.type = normalize_audit_log_type(type) or type

        #: Source of the action (e.g., 'Portal', 'Admin', 'SystemJob')
        self.source = normalize_audit_log_source(source) if source else source

        #: Timestamp of when the action occurred
        self.timestamp = timestamp

        #: ID of the user who performed the action
        self.user_id = user_id

        #: Email of the user who performed the action
        self.user_email = user_email

        #: First name of the user who performed the action
        self.first_name = first_name

        #: Last name of the user who performed the action
        self.last_name = last_name

        #: Type of resource affected
        self.resource_type = resource_type

        #: ID of the resource affected
        self.resource_id = resource_id

        #: Name of the resource affected
        self.resource_name = resource_name

        #: Additional details about the action
        self.details = details

        self._manager: Optional[WorkspaceManager] = None

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)

    @classmethod
    def from_dict(
        cls,
        obj: Dict[str, Any],
        manager: 'WorkspaceManager',
    ) -> 'AuditLog':
        """
        Construct an AuditLog from a dictionary of values.

        Parameters
        ----------
        obj : dict
            Dictionary of values
        manager : WorkspaceManager
            The WorkspaceManager the AuditLog belongs to

        Returns
        -------
        :class:`AuditLog`

        """
        # Get ID - try auditLogID first, fall back to id
        audit_log_id = obj.get('auditLogID') or obj.get('id')
        if not audit_log_id:
            raise KeyError('Missing required field(s): auditLogID or id')

        out = cls(
            audit_log_id=audit_log_id,
            type=obj.get('type', ''),
            source=obj.get('source'),
            timestamp=to_datetime(obj.get('timestamp')),
            user_id=obj.get('userID'),
            user_email=obj.get('userEmail'),
            first_name=obj.get('firstName'),
            last_name=obj.get('lastName'),
            resource_type=obj.get('resourceType'),
            resource_id=obj.get('resourceID'),
            resource_name=obj.get('resourceName'),
            details=obj.get('details'),
        )
        out._manager = manager
        return out


class AuditLogResult:
    """
    Result from audit log query with pagination support.

    Attributes
    ----------
    logs : List[AuditLog]
        List of audit log entries
    next_token : str, optional
        Token for fetching the next page of results

    """

    def __init__(
        self,
        logs: List[AuditLog],
        next_token: Optional[str] = None,
    ):
        self.logs = logs
        self.next_token = next_token

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)


class AuditLogsMixin:
    """Mixin class that adds audit log methods to WorkspaceManager."""

    def get_audit_logs(
        self,
        type: Optional[str] = None,
        source: Optional[str] = None,
        start_date: Optional[datetime.datetime] = None,
        end_date: Optional[datetime.datetime] = None,
        limit: Optional[int] = None,
        next_token: Optional[str] = None,
        first_name: Optional[str] = None,
        last_name: Optional[str] = None,
    ) -> AuditLogResult:
        """
        Get audit logs for the organization.

        Parameters
        ----------
        type : str, optional
            Filter by action type (e.g., 'Login')
        source : str, optional
            Filter by source (e.g., 'Portal', 'Admin', 'SystemJob')
        start_date : datetime, optional
            Start date for the query range
        end_date : datetime, optional
            End date for the query range
        limit : int, optional
            Maximum number of results to return
        next_token : str, optional
            Token for pagination (from previous response)
        first_name : str, optional
            Filter by user's first name
        last_name : str, optional
            Filter by user's last name

        Returns
        -------
        :class:`AuditLogResult`
            Contains list of audit logs and optional next_token for pagination

        """
        params: Dict[str, Any] = {}
        if type is not None:
            params['type'] = normalize_audit_log_type(type)
        if source is not None:
            params['source'] = normalize_audit_log_source(source)
        if start_date is not None:
            params['startDate'] = start_date.isoformat()
        if end_date is not None:
            params['endDate'] = end_date.isoformat()
        if limit is not None:
            params['limit'] = limit
        if next_token is not None:
            params['nextToken'] = next_token
        if first_name is not None:
            params['firstName'] = first_name
        if last_name is not None:
            params['lastName'] = last_name

        manager = cast('WorkspaceManager', self)
        res = manager._get('auditLogs', params=params)
        data = res.json()

        logs = NamedList([
            AuditLog.from_dict(item, manager)
            for item in data.get('auditLogs', [])
        ])

        return AuditLogResult(
            logs=logs,
            next_token=data.get('nextToken'),
        )
