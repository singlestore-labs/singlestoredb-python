#!/usr/bin/env python
"""SingleStoreDB Invitation Management."""
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
from .utils import normalize_invitation_state
from .utils import require_fields
from .utils import to_datetime
from .utils import vars_to_str

if TYPE_CHECKING:
    from .workspace import WorkspaceManager


class Invitation:
    """
    SingleStoreDB invitation definition.

    This object is not instantiated directly. It is used in the results
    of API calls on the :class:`WorkspaceManager`. Invitations are created using
    :meth:`WorkspaceManager.create_invitation`, or existing invitations are accessed
    by either :attr:`WorkspaceManager.invitations` or by calling
    :meth:`WorkspaceManager.get_invitation`.

    See Also
    --------
    :meth:`WorkspaceManager.create_invitation`
    :meth:`WorkspaceManager.get_invitation`
    :attr:`WorkspaceManager.invitations`

    """

    id: str
    email: str
    state: Optional[str]
    message: Optional[str]
    teams: List[str]
    created_at: Optional[datetime.datetime]
    expires_at: Optional[datetime.datetime]

    def __init__(
        self,
        invitation_id: str,
        email: str,
        state: Optional[str] = None,
        message: Optional[str] = None,
        teams: Optional[List[str]] = None,
        created_at: Optional[datetime.datetime] = None,
        expires_at: Optional[datetime.datetime] = None,
    ):
        #: Unique ID of the invitation
        self.id = invitation_id

        #: Email address the invitation was sent to
        self.email = email

        #: State of the invitation (e.g., 'Pending', 'Accepted', 'Revoked')
        self.state = normalize_invitation_state(state) if state else state

        #: Optional message included with the invitation
        self.message = message

        #: List of team IDs the invited user will be added to
        self.teams = teams or []

        #: Timestamp of when the invitation was created
        self.created_at = created_at

        #: Timestamp of when the invitation expires
        self.expires_at = expires_at

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
    ) -> 'Invitation':
        """
        Construct an Invitation from a dictionary of values.

        Parameters
        ----------
        obj : dict
            Dictionary of values
        manager : WorkspaceManager
            The WorkspaceManager the Invitation belongs to

        Returns
        -------
        :class:`Invitation`

        """
        require_fields(obj, 'invitationID', 'email')
        out = cls(
            invitation_id=obj['invitationID'],
            email=obj['email'],
            state=obj.get('state'),
            message=obj.get('message'),
            teams=obj.get('teams', []),
            created_at=to_datetime(obj.get('createdAt')),
            expires_at=to_datetime(obj.get('expiresAt')),
        )
        out._manager = manager
        return out

    def revoke(self) -> None:
        """Revoke the invitation."""
        if self._manager is None:
            raise ManagementError(
                msg='No workspace manager is associated with this object.',
            )
        self._manager._delete(f'invitations/{self.id}')


class InvitationsMixin:
    """Mixin class that adds invitation management methods to WorkspaceManager."""

    @property
    def invitations(self) -> NamedList[Invitation]:
        """Return a list of all open invitations in the organization."""
        manager = cast('WorkspaceManager', self)
        res = manager._get('invitations')
        return NamedList([
            Invitation.from_dict(item, manager)
            for item in res.json()
        ])

    def get_invitation(self, invitation_id: str) -> Invitation:
        """
        Retrieve an invitation by ID.

        Parameters
        ----------
        invitation_id : str
            ID of the invitation

        Returns
        -------
        :class:`Invitation`

        """
        manager = cast('WorkspaceManager', self)
        res = manager._get(f'invitations/{invitation_id}')
        return Invitation.from_dict(res.json(), manager)

    def create_invitation(
        self,
        email: str,
        message: Optional[str] = None,
        teams: Optional[List[str]] = None,
    ) -> Invitation:
        """
        Send an invitation to join the organization.

        Parameters
        ----------
        email : str
            Email address to send the invitation to
        message : str, optional
            Custom message to include with the invitation
        teams : List[str], optional
            List of team IDs to add the user to upon acceptance

        Returns
        -------
        :class:`Invitation`

        """
        manager = cast('WorkspaceManager', self)
        data: Dict[str, Any] = {'email': email}
        if message is not None:
            data['message'] = message
        if teams is not None:
            data['teams'] = teams

        res = manager._post('invitations', json=data)
        return self.get_invitation(res.json()['invitationID'])
