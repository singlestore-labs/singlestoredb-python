#!/usr/bin/env python
"""SingleStoreDB Region Management."""
from typing import Dict
from typing import Optional

from ..exceptions import ManagementError
from .manager import Manager
from .utils import NamedList
from .utils import vars_to_str


class Region:
    """
    Cluster region information.

    This object is not directly instantiated. It is used in results
    of ``ClusterManager`` API calls.

    See Also
    --------
    :attr:`ClusterManager.regions`

    """

    def __init__(
        self, name: str, provider: str, id: Optional[str] = None,
        region_name: Optional[str] = None,
    ) -> None:
        """Use :attr:`ClusterManager.regions` instead."""
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
        manager : ClusterManager, optional
            The ClusterManager the Region belongs to

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
        The API key or other access token for the management API
    version : str, optional
        Version of the API to use
    base_url : str, optional
        Base URL of the management API

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
        Not available past API v1.

        The shared-tier region route exists at v1 only. There is no later
        equivalent -- ``GET /v2/regions/sharedtier`` returns
        ``404 page not found``, and no alternate spelling responds either
        (``sharedTier/regions``, ``regions/sharedTier``,
        ``sharedtier/virtualClusters/regions``, ``clusters/regions``, ...) --
        so this raises rather than returning a misleading empty list. The v1
        ``RegionManager`` overrides it with the real implementation.

        Raises
        ------
        ManagementError
            Always.

        """
        raise ManagementError(
            msg='Listing shared tier regions is not supported by this version '
                'of the management API; there is no equivalent of '
                'GET /v1/regions/sharedtier past v1.',
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
        The API key or other access token for the management API
    version : str, optional
        Version of the API to use
    base_url : str, optional
        Base URL of the management API

    Returns
    -------
    :class:`RegionManager`

    """
    from .. import config
    from ._version_import import _import_versioned_module
    ver = version or config.get_option('management.version') or 'v1'
    mod = _import_versioned_module(ver, 'region')
    return mod.RegionManager(
        access_token=access_token,
        version=ver,
        base_url=base_url,
    )
