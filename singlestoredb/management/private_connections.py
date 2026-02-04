#!/usr/bin/env python
"""SingleStoreDB Private Connection Management."""
from __future__ import annotations

import datetime
from typing import Any
from typing import cast
from typing import Dict
from typing import List
from typing import Optional
from typing import TYPE_CHECKING

from ..exceptions import ManagementError
from .utils import normalize_connection_status
from .utils import normalize_connection_type
from .utils import to_datetime
from .utils import vars_to_str

if TYPE_CHECKING:
    from .workspace import WorkspaceManager


class PrivateConnection:
    """
    SingleStoreDB private connection definition.

    This object is not instantiated directly. It is used in the results
    of API calls on the :class:`WorkspaceManager`. Private connections are created
    using :meth:`WorkspaceManager.create_private_connection`.

    See Also
    --------
    :meth:`WorkspaceManager.create_private_connection`
    :meth:`WorkspaceManager.get_private_connection`

    """

    id: str
    workspace_group_id: str
    type: str
    status: Optional[str]
    service_name: Optional[str]
    endpoint_id: Optional[str]
    allow_list: List[str]
    created_at: Optional[datetime.datetime]
    updated_at: Optional[datetime.datetime]

    def __init__(
        self,
        connection_id: str,
        workspace_group_id: str,
        type: str,
        status: Optional[str] = None,
        service_name: Optional[str] = None,
        endpoint_id: Optional[str] = None,
        allow_list: Optional[List[str]] = None,
        created_at: Optional[datetime.datetime] = None,
        updated_at: Optional[datetime.datetime] = None,
    ):
        #: Unique ID of the private connection
        self.id = connection_id

        #: ID of the workspace group this connection belongs to
        self.workspace_group_id = workspace_group_id

        #: Type of private connection (e.g., 'INBOUND', 'OUTBOUND')
        self.type = normalize_connection_type(type) or type

        #: Status of the connection
        self.status = normalize_connection_status(status) if status else status

        #: Service name for the private connection
        self.service_name = service_name

        #: Endpoint ID
        self.endpoint_id = endpoint_id

        #: List of allowed principals/accounts
        self.allow_list = allow_list or []

        #: Timestamp of when the connection was created
        self.created_at = created_at

        #: Timestamp of when the connection was last updated
        self.updated_at = updated_at

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
    ) -> 'PrivateConnection':
        """
        Construct a PrivateConnection from a dictionary of values.

        Parameters
        ----------
        obj : dict
            Dictionary of values
        manager : WorkspaceManager
            The WorkspaceManager the PrivateConnection belongs to

        Returns
        -------
        :class:`PrivateConnection`

        """
        # Get ID - try connectionID first, fall back to privateConnectionID
        connection_id = obj.get('connectionID') or obj.get('privateConnectionID')
        if not connection_id:
            raise KeyError(
                'Missing required field(s): connectionID or privateConnectionID',
            )

        workspace_group_id = obj.get('workspaceGroupID')
        if not workspace_group_id:
            raise KeyError('Missing required field(s): workspaceGroupID')

        out = cls(
            connection_id=connection_id,
            workspace_group_id=workspace_group_id,
            type=obj.get('type', ''),
            status=obj.get('status'),
            service_name=obj.get('serviceName'),
            endpoint_id=obj.get('endpointID'),
            allow_list=obj.get('allowList', []),
            created_at=to_datetime(obj.get('createdAt')),
            updated_at=to_datetime(obj.get('updatedAt')),
        )
        out._manager = manager
        return out

    def refresh(self) -> None:
        """Refresh the private connection data from the server."""
        if self._manager is None:
            raise ManagementError(
                msg='No workspace manager is associated with this object.',
            )
        res = self._manager._get(f'privateConnections/{self.id}')
        obj = res.json()
        status = obj.get('status')
        self.status = normalize_connection_status(status) if status else status
        self.service_name = obj.get('serviceName')
        self.endpoint_id = obj.get('endpointID')
        self.allow_list = obj.get('allowList', [])
        self.updated_at = to_datetime(obj.get('updatedAt'))

    def update(self, allow_list: List[str]) -> None:
        """
        Update the private connection's allow list.

        Parameters
        ----------
        allow_list : List[str]
            New list of allowed principals/accounts

        """
        if self._manager is None:
            raise ManagementError(
                msg='No workspace manager is associated with this object.',
            )

        data = {'allowList': allow_list}
        self._manager._patch(f'privateConnections/{self.id}', json=data)
        self.refresh()

    def delete(self) -> None:
        """Delete the private connection."""
        if self._manager is None:
            raise ManagementError(
                msg='No workspace manager is associated with this object.',
            )
        self._manager._delete(f'privateConnections/{self.id}')


class OutboundAllowListEntry:
    """
    SingleStoreDB outbound allow list entry.

    Represents a destination that a workspace is allowed to connect to.

    """

    destination: str
    description: Optional[str]

    def __init__(
        self,
        destination: str,
        description: Optional[str] = None,
    ):
        #: The allowed destination (e.g., IP address, hostname)
        self.destination = destination

        #: Description of the allow list entry
        self.description = description

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)

    @classmethod
    def from_dict(cls, obj: Dict[str, Any]) -> 'OutboundAllowListEntry':
        """Construct from a dictionary."""
        return cls(
            destination=obj.get('destination', ''),
            description=obj.get('description'),
        )


class KaiPrivateConnectionInfo:
    """
    SingleStoreDB Kai private connection information.

    Contains details about the Kai (MongoDB-compatible) private connection endpoint.

    """

    service_name: Optional[str]
    endpoint_id: Optional[str]
    status: Optional[str]

    def __init__(
        self,
        service_name: Optional[str] = None,
        endpoint_id: Optional[str] = None,
        status: Optional[str] = None,
    ):
        #: Service name for Kai private connection
        self.service_name = service_name

        #: Endpoint ID for Kai private connection
        self.endpoint_id = endpoint_id

        #: Status of the Kai private connection
        self.status = normalize_connection_status(status) if status else status

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)

    @classmethod
    def from_dict(cls, obj: Dict[str, Any]) -> 'KaiPrivateConnectionInfo':
        """Construct from a dictionary."""
        return cls(
            service_name=obj.get('serviceName'),
            endpoint_id=obj.get('endpointID'),
            status=obj.get('status'),
        )


class PrivateConnectionsMixin:
    """Mixin class that adds private connection methods to WorkspaceManager."""

    def create_private_connection(
        self,
        workspace_group_id: str,
        type: str,
        allow_list: Optional[List[str]] = None,
    ) -> PrivateConnection:
        """
        Create a new private connection.

        Parameters
        ----------
        workspace_group_id : str
            ID of the workspace group
        type : str
            Type of private connection (e.g., 'PRIVATELINK', 'PRIVATECONNECT')
        allow_list : List[str], optional
            List of allowed principals/accounts

        Returns
        -------
        :class:`PrivateConnection`

        """
        manager = cast('WorkspaceManager', self)
        data: Dict[str, Any] = {
            'workspaceGroupID': workspace_group_id,
            'type': normalize_connection_type(type),
        }
        if allow_list is not None:
            data['allowList'] = allow_list

        res = manager._post('privateConnections', json=data)
        return self.get_private_connection(res.json()['privateConnectionID'])

    def get_private_connection(self, connection_id: str) -> PrivateConnection:
        """
        Retrieve a private connection by ID.

        Parameters
        ----------
        connection_id : str
            ID of the private connection

        Returns
        -------
        :class:`PrivateConnection`

        """
        manager = cast('WorkspaceManager', self)
        res = manager._get(f'privateConnections/{connection_id}')
        return PrivateConnection.from_dict(res.json(), manager)
