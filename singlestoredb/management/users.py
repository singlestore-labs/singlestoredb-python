#!/usr/bin/env python
"""SingleStoreDB Users Management."""
import datetime
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

from ..exceptions import ManagementError
from .manager import Manager
from .utils import to_datetime
from .utils import vars_to_str


class IdentityRole(object):
    """
    Identity role definition for users.

    This object is not instantiated directly. It is used in results
    of API calls on users and teams.
    """

    def __init__(
        self,
        role_id: str,
        role_name: str,
        resource_type: str,
        resource_id: str,
        granted_at: Union[str, datetime.datetime],
        granted_by: str,
    ):
        #: Role ID
        self.role_id = role_id

        #: Role name
        self.role_name = role_name

        #: Resource type the role applies to
        self.resource_type = resource_type

        #: Resource ID the role applies to
        self.resource_id = resource_id

        #: When the role was granted
        self.granted_at = to_datetime(granted_at)

        #: Who granted the role
        self.granted_by = granted_by

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)

    @classmethod
    def from_dict(cls, obj: Dict[str, Any]) -> 'IdentityRole':
        """
        Construct an IdentityRole from a dictionary of values.

        Parameters
        ----------
        obj : dict
            Dictionary of values

        Returns
        -------
        :class:`IdentityRole`
        """
        return cls(
            role_id=obj['roleID'],
            role_name=obj['roleName'],
            resource_type=obj['resourceType'],
            resource_id=obj['resourceID'],
            granted_at=obj['grantedAt'],
            granted_by=obj['grantedBy'],
        )


class User(object):
    """
    SingleStoreDB user definition.

    This object is not instantiated directly. It is used in the results
    of API calls on the :class:`UsersManager`. Users are accessed by calling
    :meth:`UsersManager.get_user` or :meth:`UsersManager.get_user_identity_roles`.

    See Also
    --------
    :meth:`UsersManager.get_user_identity_roles`
    """

    def __init__(
        self,
        user_id: str,
        email: Optional[str] = None,
        name: Optional[str] = None,
        created_at: Optional[Union[str, datetime.datetime]] = None,
        last_login: Optional[Union[str, datetime.datetime]] = None,
        status: Optional[str] = None,
    ):
        #: Unique ID of the user
        self.id = user_id

        #: Email address of the user
        self.email = email

        #: Display name of the user
        self.name = name

        #: Timestamp of when the user was created
        self.created_at = to_datetime(created_at)

        #: Timestamp of user's last login
        self.last_login = to_datetime(last_login)

        #: Status of the user account
        self.status = status

        self._manager: Optional['UsersManager'] = None

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)

    @classmethod
    def from_dict(cls, obj: Dict[str, Any], manager: 'UsersManager') -> 'User':
        """
        Construct a User from a dictionary of values.

        Parameters
        ----------
        obj : dict
            Dictionary of values
        manager : UsersManager
            The UsersManager the User belongs to

        Returns
        -------
        :class:`User`
        """
        out = cls(
            user_id=obj['userID'],
            email=obj.get('email'),
            name=obj.get('name'),
            created_at=obj.get('createdAt'),
            last_login=obj.get('lastLogin'),
            status=obj.get('status'),
        )
        out._manager = manager
        return out

    def get_identity_roles(self) -> List[IdentityRole]:
        """
        Get identity roles granted to this user.

        Returns
        -------
        List[IdentityRole]
            List of identity roles granted to the user

        Examples
        --------
        >>> user = users_mgr.get_user("user-123")
        >>> roles = user.get_identity_roles()
        >>> for role in roles:
        ...     print(f"{role.role_name} on {role.resource_type}")
        """
        if self._manager is None:
            raise ManagementError(
                msg='No users manager is associated with this object.',
            )
        return self._manager.get_user_identity_roles(self.id)


class UsersManager(Manager):
    """
    SingleStoreDB users manager.

    This class should be instantiated using :func:`singlestoredb.manage_users`
    or accessed via :attr:`WorkspaceManager.users`.

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
    obj_type = 'user'

    def get_user_identity_roles(self, user_id: str) -> List[IdentityRole]:
        """
        Get identity roles granted to a user.

        Parameters
        ----------
        user_id : str
            ID of the user

        Returns
        -------
        List[IdentityRole]
            List of identity roles granted to the user

        Examples
        --------
        >>> users_mgr = singlestoredb.manage_users()
        >>> roles = users_mgr.get_user_identity_roles("user-123")
        >>> for role in roles:
        ...     print(f"{role.role_name} on {role.resource_type} ({role.resource_id})")
        ...     print(f"  Granted by {role.granted_by} at {role.granted_at}")
        """
        res = self._get(f'users/{user_id}/identityRoles')
        return [IdentityRole.from_dict(item) for item in res.json()]

    def get_user(self, user_id: str) -> User:
        """
        Get basic user information.

        Note: This method creates a User object with the provided user_id.
        Full user details may not be available through the current API.

        Parameters
        ----------
        user_id : str
            ID of the user

        Returns
        -------
        :class:`User`
            User object

        Examples
        --------
        >>> users_mgr = singlestoredb.manage_users()
        >>> user = users_mgr.get_user("user-123")
        >>> roles = user.get_identity_roles()
        """
        # Note: The API doesn't seem to have a direct GET /users/{userID} endpoint
        # based on the documentation provided. We create a basic User object
        # that can be used to get identity roles.
        user = User(user_id=user_id)
        user._manager = self
        return user

    def list_user_roles_by_resource(
        self,
        resource_type: str,
        resource_id: str,
    ) -> Dict[str, List[IdentityRole]]:
        """
        Get all user roles for a specific resource.

        This is a convenience method that could be used to understand
        which users have access to a particular resource.

        Parameters
        ----------
        resource_type : str
            Type of the resource
        resource_id : str
            ID of the resource

        Returns
        -------
        Dict[str, List[IdentityRole]]
            Dictionary mapping user IDs to their roles on the resource

        Note
        ----
        This method would require additional API endpoints or organization-level
        access to list all users. Currently it returns an empty dict as a placeholder.

        Examples
        --------
        >>> users_mgr = singlestoredb.manage_users()
        >>> user_roles = users_mgr.list_user_roles_by_resource(
        ...     "workspace", "ws-123"
        ... )
        >>> for user_id, roles in user_roles.items():
        ...     print(f"User {user_id} has {len(roles)} roles on this workspace")
        """
        # This would require additional API endpoints or organization-level access
        # to list all users and then get their roles. For now, return empty dict.
        return {}


def manage_users(
    access_token: Optional[str] = None,
    version: Optional[str] = None,
    base_url: Optional[str] = None,
    *,
    organization_id: Optional[str] = None,
) -> UsersManager:
    """
    Retrieve a SingleStoreDB users manager.

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
    :class:`UsersManager`

    Examples
    --------
    >>> import singlestoredb as s2
    >>> users_mgr = s2.manage_users()
    >>> # Get roles for a specific user
    >>> roles = users_mgr.get_user_identity_roles("user-123")
    >>> print(f"User has {len(roles)} identity roles")
    """
    return UsersManager(
        access_token=access_token,
        base_url=base_url,
        version=version,
        organization_id=organization_id,
    )
