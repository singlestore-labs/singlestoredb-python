#!/usr/bin/env python
"""SingleStoreDB Cloud Organization."""
import datetime
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

from ..exceptions import ManagementError
from .manager import Manager
from .utils import vars_to_str


def listify(x: Union[str, List[str]]) -> List[str]:
    if isinstance(x, list):
        return x
    return [x]


def stringify(x: Union[str, List[str]]) -> str:
    if isinstance(x, list):
        return x[0]
    return x


class Secret(object):
    """
    SingleStoreDB secrets definition.

    This object is not directly instantiated. It is used in results
    of API calls on the :class:`Organization`. See :meth:`Organization.get_secret`.
    """

    def __init__(
        self,
        id: str,
        name: str,
        value: str,
        created_by: str,
        created_at: Union[str, datetime.datetime],
        last_updated_by: str,
        last_updated_at: Union[str, datetime.datetime],
        deleted_by: Optional[str] = None,
        deleted_at: Optional[Union[str, datetime.datetime]] = None,
    ):
        # UUID of the secret
        self.id = id

        # Name of the secret
        self.name = name

        # Value of the secret
        self.value = value

        # User who created the secret
        self.created_by = created_by

        # Time when the secret was created
        self.created_at = created_at

        # UUID of the user who last updated the secret
        self.last_updated_by = last_updated_by

        # Time when the secret was last updated
        self.last_updated_at = last_updated_at

        # UUID of the user who deleted the secret
        self.deleted_by = deleted_by

        # Time when the secret was deleted
        self.deleted_at = deleted_at

    @classmethod
    def from_dict(cls, obj: Dict[str, str]) -> 'Secret':
        """
        Construct a Secret from a dictionary of values.

        Parameters
        ----------
        obj : dict
            Dictionary of values

        Returns
        -------
        :class:`Secret`

        """
        out = cls(
            id=obj['secretID'],
            name=obj['name'],
            value=obj['value'],
            created_by=obj['createdBy'],
            created_at=obj['createdAt'],
            last_updated_by=obj['lastUpdatedBy'],
            last_updated_at=obj['lastUpdatedAt'],
            deleted_by=obj.get('deletedBy'),
            deleted_at=obj.get('deletedAt'),
        )

        return out

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)


class Organization(object):
    """
    Organization in SingleStoreDB Cloud portal.

    This object is not directly instantiated. It is used in results
    of ``WorkspaceManager`` API calls.

    See Also
    --------
    :attr:`WorkspaceManager.organization`

    """

    def __init__(self, id: str, name: str, firewall_ranges: List[str]):
        """Use :attr:`WorkspaceManager.organization` instead."""
        #: Unique ID of the organization
        self.id = id

        #: Name of the organization
        self.name = name

        #: Firewall ranges of the organization
        self.firewall_ranges = list(firewall_ranges)

        self._manager: Optional[Manager] = None

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)

    def get_secret(self, name: str) -> Secret:
        if self._manager is None:
            raise ManagementError(msg='Organization not initialized')

        res = self._manager._get('secrets', params=dict(name=name))

        secrets = [Secret.from_dict(item) for item in res.json()['secrets']]

        if len(secrets) == 0:
            raise ManagementError(msg=f'Secret {name} not found')

        if len(secrets) > 1:
            raise ManagementError(msg=f'Multiple secrets found for {name}')

        return secrets[0]

    @classmethod
    def from_dict(
        cls,
        obj: Dict[str, Union[str, List[str]]],
        manager: Manager,
    ) -> 'Organization':
        """
        Convert dictionary to an ``Organization`` object.

        Parameters
        ----------
        obj : dict
            Key-value pairs to retrieve organization information from
        manager : WorkspaceManager, optional
            The WorkspaceManager the Organization belongs to

        Returns
        -------
        :class:`Organization`

        """
        out = cls(
            id=stringify(obj['orgID']),
            name=stringify(obj.get('name', '<unknown>')),
            firewall_ranges=listify(obj.get('firewallRanges', [])),
        )
        out._manager = manager
        return out
