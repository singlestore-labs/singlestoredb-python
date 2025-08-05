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
from .utils import NamedList
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


class UserInvitation(object):
    """
    SingleStoreDB user invitation definition.

    This object is not instantiated directly. It is used in the results
    of API calls on the :class:`UsersManager`.

    """

    def __init__(
        self,
        invitation_id: str,
        email: str,
        state: str,
        created_at: Union[str, datetime.datetime],
        acted_at: Optional[Union[str, datetime.datetime]] = None,
        message: Optional[str] = None,
        team_ids: Optional[List[str]] = None,
    ):
        #: Unique ID of the invitation
        self.id = invitation_id

        #: Email address of the invited user
        self.email = email

        #: State of the invitation (Pending, Accepted, Refused, Revoked)
        self.state = state

        #: Timestamp of when the invitation was created
        self.created_at = to_datetime(created_at)

        #: Timestamp of most recent state change
        self.acted_at = to_datetime(acted_at)

        #: Welcome message
        self.message = message

        #: List of team IDs the user will be added to
        self.team_ids = team_ids or []

        self._manager: Optional['UsersManager'] = None

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)

    @classmethod
    def from_dict(cls, obj: Dict[str, Any], manager: 'UsersManager') -> 'UserInvitation':
        """
        Construct a UserInvitation from a dictionary of values.

        Parameters
        ----------
        obj : dict
            Dictionary of values
        manager : UsersManager
            The UsersManager the UserInvitation belongs to

        Returns
        -------
        :class:`UserInvitation`

        """
        out = cls(
            invitation_id=obj['invitationID'],
            email=obj['email'],
            state=obj['state'],
            created_at=obj['createdAt'],
            acted_at=obj.get('actedAt'),
            message=obj.get('message'),
            team_ids=obj.get('teamIDs', []),
        )
        out._manager = manager
        return out


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
        email: str,
        first_name: str,
        last_name: str,
    ):
        #: Unique ID of the user
        self.id = user_id

        #: Email address of the user
        self.email = email

        #: First name of the user
        self.first_name = first_name

        #: Last name of the user
        self.last_name = last_name

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
            email=obj['email'],
            first_name=obj['firstName'],
            last_name=obj['lastName'],
        )
        out._manager = manager
        return out

    @property
    def identity_roles(self) -> List[IdentityRole]:
        """
        Get identity roles granted to this user.

        Returns
        -------
        List[IdentityRole]
            List of identity roles granted to the user

        Examples
        --------
        >>> user = users_mgr.get_user("user-123")
        >>> roles = user.identity_roles
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
        >>> roles = user.identity_roles

        """
        # Note: The API doesn't seem to have a direct GET /users/{userID} endpoint
        # based on the documentation provided. We create a basic User object
        # that can be used to get identity roles.
        user = User(
            user_id=user_id,
            email='',  # Will be populated if/when user details endpoint is available
            first_name='',
            last_name='',
        )
        user._manager = self
        return user

    def create_user_invitation(
        self,
        email: str,
        team_ids: Optional[List[str]] = None,
    ) -> UserInvitation:
        """
        Create a user invitation.

        Parameters
        ----------
        email : str
            Email address of the user to invite
        team_ids : List[str], optional
            List of team IDs to add the user to upon acceptance

        Returns
        -------
        :class:`UserInvitation`
            Created user invitation

        Examples
        --------
        >>> users_mgr = singlestoredb.manage_users()
        >>> invitation = users_mgr.create_user_invitation(
        ...     email="user@example.com",
        ...     team_ids=["team-123"]
        ... )
        >>> print(invitation.state)
        Pending

        """
        data: Dict[str, Any] = {
            'email': email,
        }
        if team_ids is not None:
            data['teamIDs'] = team_ids

        res = self._post('userInvitations', json=data)
        return self.get_user_invitation(res.json()['invitationID'])

    def get_user_invitation(self, invitation_id: str) -> UserInvitation:
        """
        Get a user invitation.

        Parameters
        ----------
        invitation_id : str
            ID of the invitation

        Returns
        -------
        :class:`UserInvitation`
            User invitation object

        Examples
        --------
        >>> users_mgr = singlestoredb.manage_users()
        >>> invitation = users_mgr.get_user_invitation("invitation-123")
        >>> print(f"Invitation for {invitation.email} is {invitation.state}")

        """
        res = self._get(f'userInvitations/{invitation_id}')
        return UserInvitation.from_dict(res.json(), manager=self)

    def list_user_invitations(self) -> NamedList[UserInvitation]:
        """
        List all user invitations for the current organization.

        Returns
        -------
        NamedList[UserInvitation]
            List of user invitations

        Examples
        --------
        >>> users_mgr = singlestoredb.manage_users()
        >>> invitations = users_mgr.list_user_invitations()
        >>> for invitation in invitations:
        ...     print(f"{invitation.email}: {invitation.state}")

        """
        res = self._get('userInvitations')
        return NamedList([UserInvitation.from_dict(item, self) for item in res.json()])

    def delete_user_invitation(self, invitation_id: str) -> None:
        """
        Delete (revoke) a user invitation.

        Parameters
        ----------
        invitation_id : str
            ID of the invitation to delete

        Examples
        --------
        >>> users_mgr = singlestoredb.manage_users()
        >>> users_mgr.delete_user_invitation("invitation-123")

        """
        self._delete(f'userInvitations/{invitation_id}')

    @property
    def user_invitations(self) -> NamedList[UserInvitation]:
        """Return a list of user invitations."""
        return self.list_user_invitations()


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
