#!/usr/bin/env python
"""SingleStoreDB Region Management API v2."""
from ...exceptions import ManagementError
from ..region import Region as Region
from ..region import RegionManager as _RegionManager
from ..utils import NamedList


class RegionManager(_RegionManager):
    """
    SingleStoreDB region manager (API v2).

    ``GET /v2/regions`` returns entries containing ``provider``, ``region``,
    and ``regionName`` only -- no ``regionID``. :class:`Region` instances
    therefore have ``id is None`` and ``region_name`` set; v2 identifies a
    region by ``(provider, region_name)``.

    There is no v2 shared-tier region route, so
    :meth:`list_shared_tier_regions` raises here rather than returning a
    misleading empty list.
    """

    def list_shared_tier_regions(self) -> NamedList[Region]:
        """
        Not available at API v2.

        Raises
        ------
        ManagementError
            Always. ``GET /v2/regions/sharedtier`` does not exist, and neither
            does any alternate spelling (``sharedTier/regions``,
            ``regions/sharedTier``, ``sharedtier/virtualClusters/regions``,
            ``clusters/regions``, ...) -- all return ``404 page not found`` or
            are swallowed by the ``virtualClusters/{id}`` route.
        """
        raise ManagementError(
            msg='Listing shared tier regions is not supported by management '
                'API v2; there is no v2 equivalent of '
                'GET /v1/regions/sharedtier. Use a v1 region manager '
                "(manage_regions(version='v1')) for this call.",
        )
