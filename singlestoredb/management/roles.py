#!/usr/bin/env python
"""SingleStoreDB Role Management."""
from __future__ import annotations

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
from .utils import vars_to_str

if TYPE_CHECKING:
    from .workspace import WorkspaceManager


class Role:
    """
    SingleStoreDB role definition.

    This object is not instantiated directly. It is used in the results
    of API calls on the :class:`WorkspaceManager`. Roles are accessed using
    :meth:`WorkspaceManager.get_roles` or :meth:`WorkspaceManager.get_role`.

    See Also
    --------
    :meth:`WorkspaceManager.get_roles`
    :meth:`WorkspaceManager.get_role`
    :meth:`WorkspaceManager.create_role`

    """

    name: str
    resource_type: str
    permissions: List[str]
    inherits: List[str]
    is_custom: bool

    def __init__(
        self,
        name: str,
        resource_type: str,
        permissions: Optional[List[str]] = None,
        inherits: Optional[List[str]] = None,
        is_custom: bool = False,
    ):
        #: Name of the role
        self.name = name

        #: Resource type this role applies to (e.g., 'organization', 'workspace')
        self.resource_type = normalize_resource_type(resource_type) or resource_type

        #: List of permissions granted by this role
        self.permissions = permissions or []

        #: List of roles this role inherits from
        self.inherits = inherits or []

        #: Whether this is a custom role
        self.is_custom = is_custom

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
        resource_type: str,
        manager: 'WorkspaceManager',
    ) -> 'Role':
        """
        Construct a Role from a dictionary of values.

        Parameters
        ----------
        obj : dict
            Dictionary of values
        resource_type : str
            The resource type this role applies to
        manager : WorkspaceManager
            The WorkspaceManager the Role belongs to

        Returns
        -------
        :class:`Role`

        """
        require_fields(obj, 'name')
        out = cls(
            name=obj['name'],
            resource_type=resource_type,
            permissions=obj.get('permissions', []),
            inherits=obj.get('inherits', []),
            is_custom=obj.get('isCustom', False),
        )
        out._manager = manager
        return out

    def update(
        self,
        name: Optional[str] = None,
        permissions: Optional[List[str]] = None,
        inherits: Optional[List[str]] = None,
    ) -> None:
        """
        Update the role.

        Parameters
        ----------
        name : str, optional
            New name for the role
        permissions : List[str], optional
            New list of permissions for the role
        inherits : List[str], optional
            New list of roles to inherit from

        """
        if self._manager is None:
            raise ManagementError(
                msg='No workspace manager is associated with this object.',
            )

        if not self.is_custom:
            raise ManagementError(
                msg='Cannot update a built-in role.',
            )

        data: Dict[str, Any] = {}
        if name is not None:
            data['name'] = name
        if permissions is not None:
            data['permissions'] = permissions
        if inherits is not None:
            data['inherits'] = inherits

        if not data:
            return  # No parameters provided, nothing to update

        old_name = self.name
        self._manager._patch(
            f'roles/{self.resource_type}/{old_name}',
            json=data,
        )
        # Update local state (refresh not possible if name changed)
        if name is not None:
            self.name = name
        if permissions is not None:
            self.permissions = permissions
        if inherits is not None:
            self.inherits = inherits

    def delete(self) -> None:
        """Delete the role."""
        if self._manager is None:
            raise ManagementError(
                msg='No workspace manager is associated with this object.',
            )

        if not self.is_custom:
            raise ManagementError(
                msg='Cannot delete a built-in role.',
            )

        self._manager._delete(f'roles/{self.resource_type}/{self.name}')


class RolesMixin:
    """Mixin class that adds role management methods to WorkspaceManager."""

    def get_roles(self, resource_type: str) -> NamedList[Role]:
        """
        Get all roles for a resource type.

        Parameters
        ----------
        resource_type : str
            The resource type (e.g., 'Organization', 'WorkspaceGroup', 'Secret')

        Returns
        -------
        :class:`NamedList` of :class:`Role`

        """
        manager = cast('WorkspaceManager', self)
        resource_type = normalize_resource_type(resource_type) or resource_type
        res = manager._get(f'roles/{resource_type}')
        return NamedList([
            Role.from_dict(item, resource_type, manager)
            for item in res.json()
        ])

    def get_role(self, resource_type: str, role_name: str) -> Role:
        """
        Get a specific role.

        Parameters
        ----------
        resource_type : str
            The resource type (e.g., 'Organization', 'WorkspaceGroup', 'Secret')
        role_name : str
            Name of the role

        Returns
        -------
        :class:`Role`

        """
        manager = cast('WorkspaceManager', self)
        resource_type = normalize_resource_type(resource_type) or resource_type
        res = manager._get(f'roles/{resource_type}/{role_name}')
        return Role.from_dict(res.json(), resource_type, manager)

    def create_role(
        self,
        resource_type: str,
        name: str,
        permissions: Optional[List[str]] = None,
        inherits: Optional[List[str]] = None,
    ) -> Role:
        """
        Create a new custom role.

        Parameters
        ----------
        resource_type : str
            The resource type (e.g., 'Organization', 'WorkspaceGroup', 'Secret')
        name : str
            Name for the new role
        permissions : List[str], optional
            List of permissions to grant
        inherits : List[str], optional
            List of roles to inherit from

        Returns
        -------
        :class:`Role`

        """
        manager = cast('WorkspaceManager', self)
        resource_type = normalize_resource_type(resource_type) or resource_type
        data: Dict[str, Any] = {'name': name}
        if permissions is not None:
            data['permissions'] = permissions
        if inherits is not None:
            data['inherits'] = inherits

        manager._post(f'roles/{resource_type}', json=data)
        return self.get_role(resource_type, name)
