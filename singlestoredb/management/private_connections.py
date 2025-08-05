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
from .utils import camel_to_snake_dict
from .utils import NamedList
from .utils import snake_to_camel_dict
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
        connection_id: str,
        name: str,
        service_type: str,
        created_at: Union[str, datetime.datetime],
        updated_at: Optional[Union[str, datetime.datetime]] = None,
        status: Optional[str] = None,
        endpoint_service_id: Optional[str] = None,
        aws_private_link: Optional[Dict[str, Any]] = None,
        azure_private_link: Optional[Dict[str, Any]] = None,
        gcp_private_service_connect: Optional[Dict[str, Any]] = None,
    ):
        #: Unique ID of the private connection
        self.id = connection_id

        #: Name of the private connection
        self.name = name

        #: Service type (e.g., 'aws-privatelink', 'azure-privatelink',
        #: 'gcp-private-service-connect')
        self.service_type = service_type

        #: Timestamp of when the private connection was created
        self.created_at = to_datetime(created_at)

        #: Timestamp of when the private connection was last updated
        self.updated_at = to_datetime(updated_at)

        #: Status of the private connection
        self.status = status

        #: Endpoint service ID
        self.endpoint_service_id = endpoint_service_id

        #: AWS PrivateLink configuration
        self.aws_private_link = camel_to_snake_dict(
            aws_private_link,
        ) if aws_private_link else None

        #: Azure Private Link configuration
        self.azure_private_link = camel_to_snake_dict(
            azure_private_link,
        ) if azure_private_link else None

        #: GCP Private Service Connect configuration
        self.gcp_private_service_connect = camel_to_snake_dict(
            gcp_private_service_connect,
        ) if gcp_private_service_connect else None

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
            connection_id=obj['connectionID'],
            name=obj['name'],
            service_type=obj['serviceType'],
            created_at=obj['createdAt'],
            updated_at=obj.get('updatedAt'),
            status=obj.get('status'),
            endpoint_service_id=obj.get('endpointServiceID'),
            aws_private_link=obj.get('awsPrivateLink'),
            azure_private_link=obj.get('azurePrivateLink'),
            gcp_private_service_connect=obj.get('gcpPrivateServiceConnect'),
        )
        out._manager = manager
        return out

    def update(
        self,
        name: Optional[str] = None,
        aws_private_link: Optional[Dict[str, Any]] = None,
        azure_private_link: Optional[Dict[str, Any]] = None,
        gcp_private_service_connect: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Update the private connection definition.

        Parameters
        ----------
        name : str, optional
            New name for the private connection
        aws_private_link : Dict[str, Any], optional
            AWS PrivateLink configuration
        azure_private_link : Dict[str, Any], optional
            Azure Private Link configuration
        gcp_private_service_connect : Dict[str, Any], optional
            GCP Private Service Connect configuration

        """
        if self._manager is None:
            raise ManagementError(
                msg='No private connections manager is associated with this object.',
            )

        data = {
            k: v for k, v in dict(
                name=name,
                awsPrivateLink=snake_to_camel_dict(aws_private_link),
                azurePrivateLink=snake_to_camel_dict(azure_private_link),
                gcpPrivateServiceConnect=snake_to_camel_dict(gcp_private_service_connect),
            ).items() if v is not None
        }

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
        endpoint_service_id: str,
        availability_zones: List[str],
        service_type: str,
    ):
        #: Endpoint service ID for Kai
        self.endpoint_service_id = endpoint_service_id

        #: Available zones for the connection
        self.availability_zones = availability_zones

        #: Service type
        self.service_type = service_type

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
            endpoint_service_id=obj['endpointServiceID'],
            availability_zones=obj.get('availabilityZones', []),
            service_type=obj['serviceType'],
        )


class PrivateConnectionOutboundAllowList(object):
    """
    Outbound allow list for a workspace.

    """

    def __init__(
        self,
        allowed_endpoints: List[str],
    ):
        #: List of allowed outbound endpoints
        self.allowed_endpoints = allowed_endpoints

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
            allowed_endpoints=obj.get('allowedEndpoints', []),
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
        name: str,
        service_type: str,
        aws_private_link: Optional[Dict[str, Any]] = None,
        azure_private_link: Optional[Dict[str, Any]] = None,
        gcp_private_service_connect: Optional[Dict[str, Any]] = None,
    ) -> PrivateConnection:
        """
        Create a new private connection.

        Parameters
        ----------
        name : str
            Name of the private connection
        service_type : str
            Service type ('aws-privatelink', 'azure-privatelink',
            'gcp-private-service-connect')
        aws_private_link : Dict[str, Any], optional
            AWS PrivateLink configuration
        azure_private_link : Dict[str, Any], optional
            Azure Private Link configuration
        gcp_private_service_connect : Dict[str, Any], optional
            GCP Private Service Connect configuration

        Returns
        -------
        :class:`PrivateConnection`

        Examples
        --------
        >>> pc_mgr = singlestoredb.manage_private_connections()
        >>> connection = pc_mgr.create_private_connection(
        ...     name="My AWS PrivateLink",
        ...     service_type="aws-privatelink",
        ...     aws_private_link={
        ...         "vpc_endpoint_id": "vpce-123456789abcdef01"
        ...     }
        ... )

        """
        data = {
            k: v for k, v in dict(
                name=name,
                serviceType=service_type,
                awsPrivateLink=snake_to_camel_dict(aws_private_link),
                azurePrivateLink=snake_to_camel_dict(azure_private_link),
                gcpPrivateServiceConnect=snake_to_camel_dict(gcp_private_service_connect),
            ).items() if v is not None
        }

        res = self._post('privateConnections', json=data)
        return self.get_private_connection(res.json()['connectionID'])

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
        ...     print(f"{conn.name}: {conn.service_type}")

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
        name: Optional[str] = None,
        aws_private_link: Optional[Dict[str, Any]] = None,
        azure_private_link: Optional[Dict[str, Any]] = None,
        gcp_private_service_connect: Optional[Dict[str, Any]] = None,
    ) -> PrivateConnection:
        """
        Update a private connection.

        Parameters
        ----------
        connection_id : str
            ID of the private connection to update
        name : str, optional
            New name for the private connection
        aws_private_link : Dict[str, Any], optional
            AWS PrivateLink configuration
        azure_private_link : Dict[str, Any], optional
            Azure Private Link configuration
        gcp_private_service_connect : Dict[str, Any], optional
            GCP Private Service Connect configuration

        Returns
        -------
        :class:`PrivateConnection`
            Updated private connection object

        Examples
        --------
        >>> pc_mgr = singlestoredb.manage_private_connections()
        >>> connection = pc_mgr.update_private_connection(
        ...     "conn-123",
        ...     name="Updated Connection Name"
        ... )

        """
        data = {
            k: v for k, v in dict(
                name=name,
                awsPrivateLink=snake_to_camel_dict(aws_private_link),
                azurePrivateLink=snake_to_camel_dict(azure_private_link),
                gcpPrivateServiceConnect=snake_to_camel_dict(gcp_private_service_connect),
            ).items() if v is not None
        }

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
