#!/usr/bin/env python
"""SingleStoreDB Teams Management."""
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
    Identity role definition.

    This object is not instantiated directly. It is used in results
    of API calls on teams and users.

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


class Team(object):
    """
    SingleStoreDB team definition.

    This object is not instantiated directly. It is used in the results
    of API calls on the :class:`TeamsManager`. Teams are created using
    :meth:`TeamsManager.create_team`, or existing teams are accessed by either
    :attr:`TeamsManager.teams` or by calling :meth:`TeamsManager.get_team`.

    See Also
    --------
    :meth:`TeamsManager.create_team`
    :meth:`TeamsManager.get_team`
    :attr:`TeamsManager.teams`

    """

    def __init__(
        self,
        team_id: str,
        name: str,
        description: str,
        member_users: Optional[List[Dict[str, Any]]] = None,
        member_teams: Optional[List[Dict[str, Any]]] = None,
        created_at: Optional[Union[str, datetime.datetime]] = None,
    ):
        #: Unique ID of the team
        self.id = team_id

        #: Name of the team
        self.name = name

        #: Description of the team
        self.description = description

        #: List of member users with user info
        self.member_users = member_users or []

        #: List of member teams with team info
        self.member_teams = member_teams or []

        #: Timestamp of when the team was created
        self.created_at = to_datetime(created_at)

        self._manager: Optional['TeamsManager'] = None

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)

    @classmethod
    def from_dict(cls, obj: Dict[str, Any], manager: 'TeamsManager') -> 'Team':
        """
        Construct a Team from a dictionary of values.

        Parameters
        ----------
        obj : dict
            Dictionary of values
        manager : TeamsManager
            The TeamsManager the Team belongs to

        Returns
        -------
        :class:`Team`

        """
        out = cls(
            team_id=obj['teamID'],
            name=obj['name'],
            description=obj['description'],
            member_users=obj.get('memberUsers', []),
            member_teams=obj.get('memberTeams', []),
            created_at=obj.get('createdAt'),
        )
        out._manager = manager
        return out

    def update(
        self,
        name: Optional[str] = None,
        description: Optional[str] = None,
        add_member_user_ids: Optional[List[str]] = None,
        add_member_user_emails: Optional[List[str]] = None,
        add_member_team_ids: Optional[List[str]] = None,
        remove_member_user_ids: Optional[List[str]] = None,
        remove_member_user_emails: Optional[List[str]] = None,
        remove_member_team_ids: Optional[List[str]] = None,
    ) -> None:
        """
        Update the team definition.

        Parameters
        ----------
        name : str, optional
            New name for the team
        description : str, optional
            New description for the team
        add_member_user_ids : List[str], optional
            List of user IDs to add as members
        add_member_user_emails : List[str], optional
            List of user emails to add as members
        add_member_team_ids : List[str], optional
            List of team IDs to add as members
        remove_member_user_ids : List[str], optional
            List of user IDs to remove from members
        remove_member_user_emails : List[str], optional
            List of user emails to remove from members
        remove_member_team_ids : List[str], optional
            List of team IDs to remove from members

        """
        if self._manager is None:
            raise ManagementError(
                msg='No teams manager is associated with this object.',
            )

        data = {
            k: v for k, v in dict(
                name=name,
                description=description,
                addMemberUserIDs=add_member_user_ids,
                addMemberUserEmails=add_member_user_emails,
                addMemberTeamIDs=add_member_team_ids,
                removeMemberUserIDs=remove_member_user_ids,
                removeMemberUserEmails=remove_member_user_emails,
                removeMemberTeamIDs=remove_member_team_ids,
            ).items() if v is not None
        }

        if not data:
            return

        self._manager._patch(f'teams/{self.id}', json=data)
        self.refresh()

    def delete(self) -> None:
        """Delete the team."""
        if self._manager is None:
            raise ManagementError(
                msg='No teams manager is associated with this object.',
            )
        self._manager._delete(f'teams/{self.id}')

    def refresh(self) -> 'Team':
        """Update the object to the current state."""
        if self._manager is None:
            raise ManagementError(
                msg='No teams manager is associated with this object.',
            )
        new_obj = self._manager.get_team(self.id)
        for name, value in vars(new_obj).items():
            setattr(self, name, value)
        return self

    @property
    def identity_roles(self) -> List[IdentityRole]:
        """
        Get identity roles granted to this team.

        Returns
        -------
        List[IdentityRole]
            List of identity roles granted to the team

        """
        if self._manager is None:
            raise ManagementError(
                msg='No teams manager is associated with this object.',
            )
        res = self._manager._get(f'teams/{self.id}/identityRoles')
        return [IdentityRole.from_dict(item) for item in res.json()]


class TeamsManager(Manager):
    """
    SingleStoreDB teams manager.

    This class should be instantiated using :func:`singlestoredb.manage_teams`
    or accessed via :attr:`WorkspaceManager.teams`.

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
    obj_type = 'team'

    def create_team(
        self,
        name: str,
        description: Optional[str] = None,
    ) -> Team:
        """
        Create a new team.

        Parameters
        ----------
        name : str
            Name of the team
        description : str, optional
            Description of the team

        Returns
        -------
        :class:`Team`

        Examples
        --------
        >>> teams_mgr = singlestoredb.manage_teams()
        >>> team = teams_mgr.create_team(
        ...     name="Data Science Team",
        ...     description="Team for data science projects"
        ... )
        >>> print(team.name)
        Data Science Team

        """
        data = {
            'name': name,
        }
        if description is not None:
            data['description'] = description

        res = self._post('teams', json=data)
        return self.get_team(res.json()['teamID'])

    def get_team(self, team_id: str) -> Team:
        """
        Retrieve a team definition.

        Parameters
        ----------
        team_id : str
            ID of the team

        Returns
        -------
        :class:`Team`

        Examples
        --------
        >>> teams_mgr = singlestoredb.manage_teams()
        >>> team = teams_mgr.get_team("team-123")
        >>> print(team.name)
        My Team

        """
        res = self._get(f'teams/{team_id}')
        return Team.from_dict(res.json(), manager=self)

    def list_teams(
        self,
        name_filter: Optional[str] = None,
        description_filter: Optional[str] = None,
    ) -> NamedList[Team]:
        """
        List all teams for the current organization.

        Parameters
        ----------
        name_filter : str, optional
            Filter teams by name (substring match)
        description_filter : str, optional
            Filter teams by description (substring match)

        Returns
        -------
        NamedList[Team]
            List of teams

        Examples
        --------
        >>> teams_mgr = singlestoredb.manage_teams()
        >>> teams = teams_mgr.list_teams()
        >>> for team in teams:
        ...     print(f"{team.name}: {team.description}")

        >>> # Filter by name
        >>> data_teams = teams_mgr.list_teams(name_filter="data")

        """
        params = {
            k: v for k, v in dict(
                name=name_filter,
                description=description_filter,
            ).items() if v is not None
        }

        res = self._get('teams', params=params if params else None)
        return NamedList([Team.from_dict(item, self) for item in res.json()])

    @property
    def teams(self) -> NamedList[Team]:
        """Return a list of available teams."""
        return self.list_teams()

    def delete_team(self, team_id: str) -> None:
        """
        Delete a team.

        Parameters
        ----------
        team_id : str
            ID of the team to delete

        Examples
        --------
        >>> teams_mgr = singlestoredb.manage_teams()
        >>> teams_mgr.delete_team("team-123")

        """
        self._delete(f'teams/{team_id}')

    def update_team(
        self,
        team_id: str,
        name: Optional[str] = None,
        description: Optional[str] = None,
        add_member_user_ids: Optional[List[str]] = None,
        add_member_user_emails: Optional[List[str]] = None,
        add_member_team_ids: Optional[List[str]] = None,
        remove_member_user_ids: Optional[List[str]] = None,
        remove_member_user_emails: Optional[List[str]] = None,
        remove_member_team_ids: Optional[List[str]] = None,
    ) -> Team:
        """
        Update a team.

        Parameters
        ----------
        team_id : str
            ID of the team to update
        name : str, optional
            New name for the team
        description : str, optional
            New description for the team
        add_member_user_ids : List[str], optional
            List of user IDs to add as members
        add_member_user_emails : List[str], optional
            List of user emails to add as members
        add_member_team_ids : List[str], optional
            List of team IDs to add as members
        remove_member_user_ids : List[str], optional
            List of user IDs to remove from members
        remove_member_user_emails : List[str], optional
            List of user emails to remove from members
        remove_member_team_ids : List[str], optional
            List of team IDs to remove from members

        Returns
        -------
        :class:`Team`
            Updated team object

        Examples
        --------
        >>> teams_mgr = singlestoredb.manage_teams()
        >>> team = teams_mgr.update_team(
        ...     "team-123",
        ...     name="Updated Team Name",
        ...     description="Updated description",
        ...     add_member_user_emails=["user@example.com"]
        ... )

        """
        data = {
            k: v for k, v in dict(
                name=name,
                description=description,
                addMemberUserIDs=add_member_user_ids,
                addMemberUserEmails=add_member_user_emails,
                addMemberTeamIDs=add_member_team_ids,
                removeMemberUserIDs=remove_member_user_ids,
                removeMemberUserEmails=remove_member_user_emails,
                removeMemberTeamIDs=remove_member_team_ids,
            ).items() if v is not None
        }

        if not data:
            return self.get_team(team_id)

        self._patch(f'teams/{team_id}', json=data)
        return self.get_team(team_id)

    def get_team_identity_roles(self, team_id: str) -> List[IdentityRole]:
        """
        Get identity roles granted to a team.

        Parameters
        ----------
        team_id : str
            ID of the team

        Returns
        -------
        List[IdentityRole]
            List of identity roles granted to the team

        Examples
        --------
        >>> teams_mgr = singlestoredb.manage_teams()
        >>> roles = teams_mgr.get_team_identity_roles("team-123")
        >>> for role in roles:
        ...     print(f"{role.role_name} on {role.resource_type}")

        """
        res = self._get(f'teams/{team_id}/identityRoles')
        return [IdentityRole.from_dict(item) for item in res.json()]


def manage_teams(
    access_token: Optional[str] = None,
    version: Optional[str] = None,
    base_url: Optional[str] = None,
    *,
    organization_id: Optional[str] = None,
) -> TeamsManager:
    """
    Retrieve a SingleStoreDB teams manager.

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
    :class:`TeamsManager`

    Examples
    --------
    >>> import singlestoredb as s2
    >>> teams_mgr = s2.manage_teams()
    >>> teams = teams_mgr.teams
    >>> print(f"Found {len(teams)} teams")

    """
    return TeamsManager(
        access_token=access_token,
        base_url=base_url,
        version=version,
        organization_id=organization_id,
    )
