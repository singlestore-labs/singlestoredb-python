#!/usr/bin/env python
"""SingleStoreDB Region Management."""
from typing import Optional

from .v1.region import Region as Region
from .v1.region import RegionManager as RegionManager
from .versioned import _import_versioned_module
# Re-export from default version for backward compatibility


def manage_regions(
    access_token: Optional[str] = None,
    version: Optional[str] = None,
    base_url: Optional[str] = None,
) -> 'RegionManager':
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
    from .. import config
    ver = version or config.get_option('management.version') or 'v1'
    mod = _import_versioned_module(ver, 'region')
    return mod.RegionManager(
        access_token=access_token, base_url=base_url,
        version=ver,
    )
