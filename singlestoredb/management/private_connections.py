#!/usr/bin/env python
"""SingleStoreDB Private Connections Management."""
import datetime
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

from ..exceptions import ManagementError
from .manager import Manager
from .utils import NamedList
from .utils import to_datetime
from .utils import vars_to_str


class PrivateConnection(object):
    """
    SingleStoreDB private connection definition.

    This object is not instantiated directly. It is used in the results
    of API calls on the :class:`PrivateConnectionsManager`. Private connections are
    created using :meth:`PrivateConnectionsManager.create_private_connection`, or
    existing private connections are accessed by either
    :attr:`PrivateConnectionsManager.private_connections` or by calling
    :meth:`PrivateConnectionsManager.get_private_connection`.

    See Also
    --------
    :meth:`PrivateConnectionsManager.create_private_connection`
    :meth:`PrivateConnectionsManager.get_private_connection`
    :attr:`PrivateConnectionsManager.private_connections`

    """

    def __init__(
        self,
        private_connection_id: str,
        workspace_group_id: str,
        service_name: Optional[str] = None,
        connection_type: Optional[str] = None,
        status: Optional[str] = None,
        allow_list: Optional[str] = None,
        outbound_allow_list: Optional[str] = None,
        allowed_private_link_ids: Optional[List[str]] = None,
        kai_endpoint_id: Optional[str] = None,
        sql_port: Optional[int] = None,
        websockets_port: Optional[int] = None,
        endpoint: Optional[str] = None,
        workspace_id: Optional[str] = None,
        created_at: Optional[Union[str, datetime.datetime]] = None,
        updated_at: Optional[Union[str, datetime.datetime]] = None,
        active_at: Optional[Union[str, datetime.datetime]] = None,
        deleted_at: Optional[Union[str, datetime.datetime]] = None,
    ):
        #: Unique ID of the private connection
        self.id = private_connection_id

        #: ID of the workspace group containing the private connection
        self.workspace_group_id = workspace_group_id

        #: Name of the private connection service
        self.service_name = service_name

        #: The private connection type (INBOUND, OUTBOUND)
        self.type = connection_type

        #: Status of the private connection (PENDING, ACTIVE, DELETED)
        self.status = status

        #: The private connection allow list (account ID for AWS,
        #: subscription ID for Azure, project name for GCP)
        self.allow_list = allow_list

        #: The account ID allowed for outbound connections
        self.outbound_allow_list = outbound_allow_list

        #: List of allowed Private Link IDs
        self.allowed_private_link_ids = allowed_private_link_ids or []

        #: VPC Endpoint ID for AWS
        self.kai_endpoint_id = kai_endpoint_id

        #: The SQL port
        self.sql_port = sql_port

        #: The websockets port
        self.websockets_port = websockets_port

        #: The service endpoint
        self.endpoint = endpoint

        #: ID of the workspace to connect with
        self.workspace_id = workspace_id

        #: Timestamp of when the private connection was created
        self.created_at = to_datetime(created_at)

        #: Timestamp of when the private connection was last updated
        self.updated_at = to_datetime(updated_at)

        #: Timestamp of when the private connection became active
        self.active_at = to_datetime(active_at)

        #: Timestamp of when the private connection was deleted
        self.deleted_at = to_datetime(deleted_at)

        self._manager: Optional['PrivateConnectionsManager'] = None

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)

    @classmethod
    def from_dict(
        cls, obj: Dict[str, Any],
        manager: 'PrivateConnectionsManager',
    ) -> 'PrivateConnection':
        """
        Construct a PrivateConnection from a dictionary of values.

        Parameters
        ----------
        obj : dict
            Dictionary of values
        manager : PrivateConnectionsManager
            The PrivateConnectionsManager the PrivateConnection belongs to

        Returns
        -------
        :class:`PrivateConnection`

        """
        out = cls(
            private_connection_id=obj['privateConnectionID'],
            workspace_group_id=obj['workspaceGroupID'],
            service_name=obj.get('serviceName'),
            connection_type=obj.get('type'),
            status=obj.get('status'),
            allow_list=obj.get('allowList'),
            outbound_allow_list=obj.get('outboundAllowList'),
            allowed_private_link_ids=obj.get('allowedPrivateLinkIDs', []),
            kai_endpoint_id=obj.get('kaiEndpointID'),
            sql_port=obj.get('sqlPort'),
            websockets_port=obj.get('websocketsPort'),
            endpoint=obj.get('endpoint'),
            workspace_id=obj.get('workspaceID'),
            created_at=obj.get('createdAt'),
            updated_at=obj.get('updatedAt'),
            active_at=obj.get('activeAt'),
            deleted_at=obj.get('deletedAt'),
        )
        out._manager = manager
        return out

    def update(
        self,
        allow_list: Optional[str] = None,
    ) -> None:
        """
        Update the private connection definition.

        Parameters
        ----------
        allow_list : str, optional
            The private connection allow list

        """
        if self._manager is None:
            raise ManagementError(
                msg='No private connections manager is associated with this object.',
            )

        data = {}
        if allow_list is not None:
            data['allowList'] = allow_list

        if not data:
            return

        self._manager._patch(f'privateConnections/{self.id}', json=data)
        self.refresh()

    def delete(self) -> None:
        """Delete the private connection."""
        if self._manager is None:
            raise ManagementError(
                msg='No private connections manager is associated with this object.',
            )
        self._manager._delete(f'privateConnections/{self.id}')

    def refresh(self) -> 'PrivateConnection':
        """Update the object to the current state."""
        if self._manager is None:
            raise ManagementError(
                msg='No private connections manager is associated with this object.',
            )
        new_obj = self._manager.get_private_connection(self.id)
        for name, value in vars(new_obj).items():
            setattr(self, name, value)
        return self


class PrivateConnectionKaiInfo(object):
    """
    SingleStore Kai private connection information.

    This object contains information needed to create a private connection
    to SingleStore Kai for a workspace.

    """

    def __init__(
        self,
        service_name: str,
    ):
        #: VPC Endpoint Service Name for AWS
        self.service_name = service_name

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)

    @classmethod
    def from_dict(cls, obj: Dict[str, Any]) -> 'PrivateConnectionKaiInfo':
        """
        Construct a PrivateConnectionKaiInfo from a dictionary of values.

        Parameters
        ----------
        obj : dict
            Dictionary of values

        Returns
        -------
        :class:`PrivateConnectionKaiInfo`
        """
        return cls(
            service_name=obj['serviceName'],
        )


class PrivateConnectionOutboundAllowList(object):
    """
    Outbound allow list for a workspace.

    """

    def __init__(
        self,
        outbound_allow_list: str,
    ):
        #: The account ID allowed for outbound connections
        self.outbound_allow_list = outbound_allow_list

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)

    @classmethod
    def from_dict(cls, obj: Dict[str, Any]) -> 'PrivateConnectionOutboundAllowList':
        """
        Construct a PrivateConnectionOutboundAllowList from a dictionary of values.

        Parameters
        ----------
        obj : dict
            Dictionary of values

        Returns
        -------
        :class:`PrivateConnectionOutboundAllowList`

        """
        return cls(
            outbound_allow_list=obj['outboundAllowList'],
        )


class PrivateConnectionsManager(Manager):
    """
    SingleStoreDB private connections manager.

    This class should be instantiated using
    :func:`singlestoredb.manage_private_connections` or accessed via
    :attr:`WorkspaceManager.private_connections`.

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
    obj_type = 'private_connection'

    def create_private_connection(
        self,
        workspace_group_id: str,
        service_name: Optional[str] = None,
        connection_type: Optional[str] = None,
        kai_endpoint_id: Optional[str] = None,
        allow_list: Optional[str] = None,
        sql_port: Optional[int] = None,
        websockets_port: Optional[int] = None,
        workspace_id: Optional[str] = None,
    ) -> PrivateConnection:
        """
        Create a new private connection.

        Parameters
        ----------
        workspace_group_id : str
            The ID of the workspace group containing the private connection
        service_name : str, optional
            The name of the private connection service
        connection_type : str, optional
            The private connection type ('INBOUND', 'OUTBOUND')
        kai_endpoint_id : str, optional
            VPC Endpoint ID for AWS
        allow_list : str, optional
            The private connection allow list
        sql_port : int, optional
            The SQL port
        websockets_port : int, optional
            The websockets port
        workspace_id : str, optional
            The ID of the workspace to connect with

        Returns
        -------
        :class:`PrivateConnection`

        Examples
        --------
        >>> pc_mgr = singlestoredb.manage_private_connections()
        >>> connection = pc_mgr.create_private_connection(
        ...     workspace_group_id="wg-123",
        ...     service_name="My PrivateLink",
        ...     connection_type="INBOUND",
        ...     kai_endpoint_id="vpce-123456789abcdef01"
        ... )

        """
        data = {
            k: v for k, v in dict(
                workspaceGroupID=workspace_group_id,
                serviceName=service_name,
                type=connection_type,
                kaiEndpointID=kai_endpoint_id,
                allowList=allow_list,
                sqlPort=sql_port,
                websocketsPort=websockets_port,
                workspaceID=workspace_id,
            ).items() if v is not None
        }

        res = self._post('privateConnections', json=data)
        return self.get_private_connection(res.json()['privateConnectionID'])

    def get_private_connection(self, connection_id: str) -> PrivateConnection:
        """
        Retrieve a private connection definition.

        Parameters
        ----------
        connection_id : str
            ID of the private connection

        Returns
        -------
        :class:`PrivateConnection`

        Examples
        --------
        >>> pc_mgr = singlestoredb.manage_private_connections()
        >>> connection = pc_mgr.get_private_connection("conn-123")

        """
        res = self._get(f'privateConnections/{connection_id}')
        return PrivateConnection.from_dict(res.json(), manager=self)

    @property
    def private_connections(self) -> NamedList[PrivateConnection]:
        """
        List all private connections.

        Returns
        -------
        NamedList[PrivateConnection]
            List of private connections

        Examples
        --------
        >>> pc_mgr = singlestoredb.manage_private_connections()
        >>> connections = pc_mgr.private_connections
        >>> for conn in connections:
        ...     print(f"{conn.service_name}: {conn.type}")

        """
        res = self._get('privateConnections')
        return NamedList([PrivateConnection.from_dict(item, self) for item in res.json()])

    def delete_private_connection(self, connection_id: str) -> None:
        """
        Delete a private connection.

        Parameters
        ----------
        connection_id : str
            ID of the private connection to delete

        Examples
        --------
        >>> pc_mgr = singlestoredb.manage_private_connections()
        >>> pc_mgr.delete_private_connection("conn-123")
        """
        self._delete(f'privateConnections/{connection_id}')

    def update_private_connection(
        self,
        connection_id: str,
        allow_list: Optional[str] = None,
    ) -> PrivateConnection:
        """
        Update a private connection.

        Parameters
        ----------
        connection_id : str
            ID of the private connection to update
        allow_list : str, optional
            The private connection allow list

        Returns
        -------
        :class:`PrivateConnection`
            Updated private connection object

        Examples
        --------
        >>> pc_mgr = singlestoredb.manage_private_connections()
        >>> connection = pc_mgr.update_private_connection(
        ...     "conn-123",
        ...     allow_list="my-allow-list"
        ... )

        """
        data = {}
        if allow_list is not None:
            data['allowList'] = allow_list

        if not data:
            return self.get_private_connection(connection_id)

        self._patch(f'privateConnections/{connection_id}', json=data)
        return self.get_private_connection(connection_id)


def manage_private_connections(
    access_token: Optional[str] = None,
    version: Optional[str] = None,
    base_url: Optional[str] = None,
    *,
    organization_id: Optional[str] = None,
) -> PrivateConnectionsManager:
    """
    Retrieve a SingleStoreDB private connections manager.

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
    :class:`PrivateConnectionsManager`

    Examples
    --------
    >>> import singlestoredb as s2
    >>> pc_mgr = s2.manage_private_connections()
    >>> connections = pc_mgr.private_connections
    >>> print(f"Found {len(connections)} private connections")

    """
    return PrivateConnectionsManager(
        access_token=access_token,
        base_url=base_url,
        version=version,
        organization_id=organization_id,
    )
