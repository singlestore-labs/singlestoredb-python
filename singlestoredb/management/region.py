#!/usr/bin/env python
"""SingleStoreDB Cluster Management."""
from __future__ import annotations

from typing import Dict
from typing import Optional

from .manager import Manager
from .utils import vars_to_str


class Region(object):
    """
    Cluster region information.

    This object is not directly instantiated. It is used in results
    of `ClusterManager` API calls.

    See Also
    --------
    :attr:`ClusterManager.regions`

    """

    def __init__(self, id: str, name: str, provider: str):
        """Use :attr:`ClusterManager.regions` instead."""
        #: Unique ID of the region
        self.id = id

        #: Name of the region
        self.name = name

        #: Name of the cloud provider
        self.provider = provider

        self._manager: Optional[Manager] = None

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)

    @classmethod
    def from_dict(cls, obj: Dict[str, str], manager: Manager) -> Region:
        """
        Convert dictionary to a ``Region`` object.

        Parameters
        ----------
        obj : dict
            Key-value pairs to retrieve region information from
        manager : ClusterManager, optional
            The ClusterManager the Region belongs to

        Returns
        -------
        :class:`Region`

        """
        out = cls(
            id=obj['regionID'],
            name=obj['region'],
            provider=obj['provider'],
        )
        out._manager = manager
        return out
