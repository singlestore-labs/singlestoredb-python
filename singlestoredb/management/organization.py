#!/usr/bin/env python
"""SingleStoreDB Cloud Organization."""
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

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
