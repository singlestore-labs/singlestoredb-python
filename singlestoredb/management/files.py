#!/usr/bin/env python
"""SingleStore Cloud Files Management."""
from typing import Optional

from .v1.files import FileLocation as FileLocation
from .v1.files import FilesManager as FilesManager
from .v1.files import FilesObject as FilesObject
from .v1.files import FilesObjectBytesReader as FilesObjectBytesReader
from .v1.files import FilesObjectBytesWriter as FilesObjectBytesWriter
from .v1.files import FilesObjectTextReader as FilesObjectTextReader
from .v1.files import FilesObjectTextWriter as FilesObjectTextWriter
from .v1.files import FileSpace as FileSpace
from .v1.files import MODELS_SPACE as MODELS_SPACE
from .v1.files import PERSONAL_SPACE as PERSONAL_SPACE
from .v1.files import SHARED_SPACE as SHARED_SPACE
from .versioned import _import_versioned_module
# Re-export from default version for backward compatibility


def manage_files(
    access_token: Optional[str] = None,
    version: Optional[str] = None,
    base_url: Optional[str] = None,
    *,
    organization_id: Optional[str] = None,
) -> 'FilesManager':
    """
    Retrieve a SingleStoreDB files manager.

    Parameters
    ----------
    access_token : str, optional
        The API key or other access token for the workspace management API
    version : str, optional
        Version of the API to use
    base_url : str, optional
        Base URL of the workspace management API
    organization_id : str, optional
        ID of organization, if using a JWT for authentication

    Returns
    -------
    :class:`FilesManager`

    """
    from .. import config
    ver = version or config.get_option('management.version') or 'v1'
    mod = _import_versioned_module(ver, 'files')
    return mod.FilesManager(
        access_token=access_token, base_url=base_url,
        version=ver, organization_id=organization_id,
    )
