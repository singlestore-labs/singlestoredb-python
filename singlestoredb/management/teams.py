#!/usr/bin/env python
"""SingleStoreDB Team Management."""
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


class Team:
    """
    SingleStoreDB team definition.

    This object is not instantiated directly. It is used in the results
    of API calls on the :class:`WorkspaceManager`. Teams are created using
    :meth:`WorkspaceManager.create_team`, or existing teams are accessed by
    either :attr:`WorkspaceManager.teams` or by calling
    :meth:`WorkspaceManager.get_team`.

    See Also
    --------
    :meth:`WorkspaceManager.create_team`
    :meth:`WorkspaceManager.get_team`
    :attr:`WorkspaceManager.teams`

    """

    id: str
    name: str
    description: Optional[str]
    member_ids: List[str]
    created_at: Optional[datetime.datetime]

    def __init__(
        self,
        team_id: str,
        name: str,
        description: Optional[str] = None,
        member_ids: Optional[List[str]] = None,
        created_at: Optional[datetime.datetime] = None,
    ):
        #: Unique ID of the team
        self.id = team_id

        #: Name of the team
        self.name = name

        #: Description of the team
        self.description = description

        #: List of user IDs that are members of this team
        self.member_ids = member_ids or []

        #: Timestamp of when the team was created
        self.created_at = created_at

        self._manager: Optional[WorkspaceManager] = None

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)

    @classmethod
    def from_dict(cls, obj: Dict[str, Any], manager: 'WorkspaceManager') -> 'Team':
        """
        Construct a Team from a dictionary of values.

        Parameters
        ----------
        obj : dict
            Dictionary of values
        manager : WorkspaceManager
            The WorkspaceManager the Team belongs to

        Returns
        -------
        :class:`Team`

        """
        require_fields(obj, 'teamID', 'name')
        out = cls(
            team_id=obj['teamID'],
            name=obj['name'],
            description=obj.get('description'),
            member_ids=obj.get('memberIDs', []),
            created_at=to_datetime(obj.get('createdAt')),
        )
        out._manager = manager
        return out

    def refresh(self) -> 'Team':
        """Update the object to the current state."""
        if self._manager is None:
            raise ManagementError(
                msg='No workspace manager is associated with this object.',
            )
        new_obj = self._manager.get_team(self.id)
        for name, value in vars(new_obj).items():
            setattr(self, name, value)
        return self

    def update(
        self,
        name: Optional[str] = None,
        description: Optional[str] = None,
        member_ids: Optional[List[str]] = None,
    ) -> None:
        """
        Update the team definition.

        Parameters
        ----------
        name : str, optional
            New name for the team
        description : str, optional
            New description for the team
        member_ids : List[str], optional
            New list of member user IDs

        """
        if self._manager is None:
            raise ManagementError(
                msg='No workspace manager is associated with this object.',
            )
        data = {
            k: v for k, v in dict(
                name=name,
                description=description,
                memberIDs=member_ids,
            ).items() if v is not None
        }
        self._manager._patch(f'teams/{self.id}', json=data)
        self.refresh()

    def delete(self) -> None:
        """Delete the team."""
        if self._manager is None:
            raise ManagementError(
                msg='No workspace manager is associated with this object.',
            )
        self._manager._delete(f'teams/{self.id}')

    def get_access_controls(self) -> Dict[str, Any]:
        """
        Get the access controls (RBAC) for this team.

        Returns
        -------
        Dict[str, Any]
            Dictionary containing access control information

        """
        if self._manager is None:
            raise ManagementError(
                msg='No workspace manager is associated with this object.',
            )
        res = self._manager._get(f'teams/{self.id}/accessControls')
        return res.json()

    def update_access_controls(
        self,
        grants: Optional[List[Dict[str, Any]]] = None,
        revokes: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        """
        Update the access controls (RBAC) for this team.

        Parameters
        ----------
        grants : List[Dict[str, Any]], optional
            List of access grants to add. Each grant should contain
            'resourceID', 'resourceType', and 'role'.
        revokes : List[Dict[str, Any]], optional
            List of access grants to remove. Each revoke should contain
            'resourceID', 'resourceType', and 'role'.

        """
        if self._manager is None:
            raise ManagementError(
                msg='No workspace manager is associated with this object.',
            )
        data = {}
        if grants is not None:
            data['grants'] = grants
        if revokes is not None:
            data['revokes'] = revokes
        self._manager._patch(f'teams/{self.id}/accessControls', json=data)

    def get_identity_roles(self, resource_type: str) -> List[Dict[str, Any]]:
        """
        Get the roles assigned to this team for a specific resource type.

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
            f'teams/{self.id}/identityRoles',
            params={'resourceType': normalize_resource_type(resource_type)},
        )
        return res.json()


class TeamsMixin:
    """Mixin class that adds team management methods to WorkspaceManager."""

    @property
    def teams(self) -> NamedList[Team]:
        """Return a list of all teams in the organization."""
        manager = cast('WorkspaceManager', self)
        res = manager._get('teams')
        return NamedList([
            Team.from_dict(item, manager)
            for item in res.json()
        ])

    def get_team(self, team_id: str) -> Team:
        """
        Retrieve a team by ID.

        Parameters
        ----------
        team_id : str
            ID of the team

        Returns
        -------
        :class:`Team`

        """
        manager = cast('WorkspaceManager', self)
        res = manager._get(f'teams/{team_id}')
        return Team.from_dict(res.json(), manager)

    def create_team(
        self,
        name: str,
        description: Optional[str] = None,
        member_ids: Optional[List[str]] = None,
    ) -> Team:
        """
        Create a new team.

        Parameters
        ----------
        name : str
            Name of the team
        description : str, optional
            Description of the team
        member_ids : List[str], optional
            List of user IDs to add as members

        Returns
        -------
        :class:`Team`

        """
        manager = cast('WorkspaceManager', self)
        data: Dict[str, Any] = {'name': name}
        if description is not None:
            data['description'] = description
        if member_ids is not None:
            data['memberIDs'] = member_ids

        res = manager._post('teams', json=data)
        return self.get_team(res.json()['teamID'])
