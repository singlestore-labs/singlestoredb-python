#!/usr/bin/env python
"""SingleStoreDB Region Management API v2."""
from ...exceptions import ManagementError
from ..utils import NamedList
from ..v1.region import Region as Region
from ..v1.region import RegionManager as V1RegionManager


class RegionManager(V1RegionManager):
    """
    SingleStoreDB region manager (API v2).

    Calls ``GET /v2/regions``, which returns ``RegionV2`` entries containing
    ``provider``, ``region``, and ``regionName`` only — no ``regionID``.
    Region instances therefore have ``id is None`` and ``region_name`` set.

    The v1 ``GET /v1/regions/sharedtier`` endpoint has no v2 counterpart;
    :meth:`list_shared_tier_regions` raises here. Use ``mgr.v1`` for that
    endpoint.
    """

    def list_regions(self) -> NamedList[Region]:
        """
        List all available regions via ``GET /v2/regions``.

        Returns
        -------
        NamedList[Region]
            List of available regions. Each entry has ``id=None`` and
            ``region_name`` populated; v2 identifies regions by
            ``(provider, region_name)``.
        """
        res = self._get('regions')
        return NamedList(
            [Region.from_dict(item, self) for item in res.json()],
        )

    def list_shared_tier_regions(self) -> NamedList[Region]:
        """Not available in API v2 — use ``mgr.v1.list_shared_tier_regions()``."""
        raise ManagementError(
            msg='list_shared_tier_regions is not available in API v2; '
                'use mgr.v1.list_shared_tier_regions() instead',
        )
