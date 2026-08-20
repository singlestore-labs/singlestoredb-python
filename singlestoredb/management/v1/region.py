#!/usr/bin/env python
"""SingleStoreDB Region Management API v1."""
from ..region import Region as Region
from ..region import RegionManager as _RegionManager
from ..utils import NamedList


class RegionManager(_RegionManager):
    """
    SingleStoreDB region manager (API v1).

    ``GET /v1/regions`` is what the shared base implements. What v1 adds is
    ``GET /v1/regions/sharedtier``, which has no equivalent from v2 onward.
    """

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
