#!/usr/bin/env python
"""SingleStoreDB Cluster Management."""
from typing import Dict
from typing import Optional

from .manager import Manager
from .utils import NamedList
from .utils import vars_to_str


class Region(object):
    """
    Cluster region information.

    This object is not directly instantiated. It is used in results
    of ``WorkspaceManager`` API calls.

    See Also
    --------
    :attr:`WorkspaceManager.regions`

    """

    def __init__(
        self, name: str, provider: str, id: Optional[str] = None,
        region_name: Optional[str] = None,
    ) -> None:
        """Use :attr:`WorkspaceManager.regions` instead."""
        #: Unique ID of the region
        self.id = id

        #: Name of the region
        self.name = name

        #: Name of the cloud provider
        self.provider = provider

        #: Name of the provider region
        self.region_name = region_name

        self._manager: Optional[Manager] = None

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)

    @classmethod
    def from_dict(cls, obj: Dict[str, str], manager: Manager) -> 'Region':
        """
        Convert dictionary to a ``Region`` object.

        Parameters
        ----------
        obj : dict
            Key-value pairs to retrieve region information from
        manager : WorkspaceManager, optional
            The WorkspaceManager the Region belongs to

        Returns
        -------
        :class:`Region`

        """
        id = obj.get('regionID', None)
        region_name = obj.get('regionName', None)

        out = cls(
            id=id,
            name=obj['region'],
            provider=obj['provider'],
            region_name=region_name,
        )
        out._manager = manager
        return out


class RegionManager(Manager):
    """
    SingleStoreDB region manager.

    This class should be instantiated using :func:`singlestoredb.manage_regions`.

    Parameters
    ----------
    access_token : str, optional
        The API key or other access token for the workspace management API
    version : str, optional
        Version of the API to use
    base_url : str, optional
        Base URL of the workspace management API

    See Also
    --------
    :func:`singlestoredb.manage_regions`
    """

    #: Object type
    obj_type = 'region'

    def list_regions(self) -> NamedList[Region]:
        """
        List all available regions.

        Returns
        -------
        NamedList[Region]
            List of available regions

        Raises
        ------
        ManagementError
            If there is an error getting the regions
        """
        res = self._get('regions')
        return NamedList(
            [Region.from_dict(item, self) for item in res.json()],
        )

    def list_shared_tier_regions(self) -> NamedList[Region]:
        """
        List regions that support shared tier workspaces.

        Returns
        -------
        NamedList[Region]
            List of regions that support shared tier workspaces

        Raises
        ------
        ManagementError
            If there is an error getting the regions
        """
        res = self._get('regions/sharedtier')
        return NamedList(
            [Region.from_dict(item, self) for item in res.json()],
        )


def manage_regions(
    access_token: Optional[str] = None,
    version: Optional[str] = None,
    base_url: Optional[str] = None,
) -> RegionManager:
    """
    Retrieve a SingleStoreDB region manager.

    Parameters
    ----------
    access_token : str, optional
        The API key or other access token for the workspace management API
    version : str, optional
        Version of the API to use
    base_url : str, optional
        Base URL of the workspace management API

    Returns
    -------
    :class:`RegionManager`

    """
    return RegionManager(
        access_token=access_token,
        version=version,
        base_url=base_url,
    )
