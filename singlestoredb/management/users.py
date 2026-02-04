#!/usr/bin/env python
"""SingleStoreDB User Management."""
from __future__ import annotations

import datetime
from typing import Any
from typing import cast
from typing import Dict
from typing import List
from typing import Optional
from typing import TYPE_CHECKING

from ..exceptions import ManagementError
from .utils import NamedList
from .utils import normalize_resource_type
from .utils import require_fields
from .utils import to_datetime
from .utils import vars_to_str

if TYPE_CHECKING:
    from .workspace import WorkspaceManager


class User:
    """
    SingleStoreDB user definition.

    This object is not instantiated directly. It is used in the results
    of API calls on the :class:`WorkspaceManager`. Users are accessed by
    either :attr:`WorkspaceManager.users` or by calling
    :meth:`WorkspaceManager.get_user`.

    See Also
    --------
    :meth:`WorkspaceManager.get_user`
    :meth:`WorkspaceManager.add_user`
    :attr:`WorkspaceManager.users`
    :attr:`WorkspaceManager.current_user`

    """

    id: str
    email: str
    first_name: Optional[str]
    last_name: Optional[str]
    created_at: Optional[datetime.datetime]

    def __init__(
        self,
        user_id: str,
        email: str,
        first_name: Optional[str] = None,
        last_name: Optional[str] = None,
        created_at: Optional[datetime.datetime] = None,
    ):
        #: Unique ID of the user
        self.id = user_id

        #: Email address of the user
        self.email = email

        #: First name of the user
        self.first_name = first_name

        #: Last name of the user
        self.last_name = last_name

        #: Timestamp of when the user was created
        self.created_at = created_at

        self._manager: Optional[WorkspaceManager] = None

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)

    @classmethod
    def from_dict(cls, obj: Dict[str, Any], manager: 'WorkspaceManager') -> 'User':
        """
        Construct a User from a dictionary of values.

        Parameters
        ----------
        obj : dict
            Dictionary of values
        manager : WorkspaceManager
            The WorkspaceManager the User belongs to

        Returns
        -------
        :class:`User`

        """
        require_fields(obj, 'userID', 'email')
        out = cls(
            user_id=obj['userID'],
            email=obj['email'],
            first_name=obj.get('firstName'),
            last_name=obj.get('lastName'),
            created_at=to_datetime(obj.get('createdAt')),
        )
        out._manager = manager
        return out

    def get_identity_roles(self, resource_type: str) -> List[Dict[str, Any]]:
        """
        Get the roles assigned to this user for a specific resource type.

        Parameters
        ----------
        resource_type : str
            The type of resource (e.g., 'Organization', 'WorkspaceGroup')

        Returns
        -------
        List[Dict[str, Any]]
            List of role assignments

        """
        if self._manager is None:
            raise ManagementError(
                msg='No workspace manager is associated with this object.',
            )
        res = self._manager._get(
            f'users/{self.id}/identityRoles',
            params={'resourceType': normalize_resource_type(resource_type)},
        )
        return res.json()

    def remove(self) -> None:
        """Remove the user from the organization."""
        if self._manager is None:
            raise ManagementError(
                msg='No workspace manager is associated with this object.',
            )
        self._manager._delete(f'users/{self.id}')


class UsersMixin:
    """Mixin class that adds user management methods to WorkspaceManager."""

    @property
    def users(self) -> NamedList[User]:
        """Return a list of all users in the organization."""
        manager = cast('WorkspaceManager', self)
        res = manager._get('users')
        return NamedList([
            User.from_dict(item, manager)
            for item in res.json()
        ])

    @property
    def current_user(self) -> User:
        """Return the current authenticated user."""
        manager = cast('WorkspaceManager', self)
        res = manager._get('users/current')
        return User.from_dict(res.json(), manager)

    def get_user(self, user_id: str) -> User:
        """
        Retrieve a user by ID.

        Parameters
        ----------
        user_id : str
            ID of the user

        Returns
        -------
        :class:`User`

        """
        manager = cast('WorkspaceManager', self)
        res = manager._get(f'users/{user_id}')
        return User.from_dict(res.json(), manager)

    def add_user(self, email: str) -> User:
        """
        Add an existing user to the organization by email.

        Parameters
        ----------
        email : str
            Email address of the user to add

        Returns
        -------
        :class:`User`

        """
        manager = cast('WorkspaceManager', self)
        res = manager._post('users', json={'email': email})
        return self.get_user(res.json()['userID'])
