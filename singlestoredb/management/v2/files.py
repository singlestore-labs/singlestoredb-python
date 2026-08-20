#!/usr/bin/env python
"""
SingleStoreDB Files Management API v2.

The Files API is unchanged at v2 -- ``files/fs/{space}/...`` returns identical
responses at both versions -- so the implementation lives in the shared
:mod:`singlestoredb.management.files` module and this module only re-exports it.
Nothing here may import from :mod:`singlestoredb.management.v1`; see
``TestV1IsDeletable``.
"""
from ..files import FileLocation as FileLocation
from ..files import FilesManager as FilesManager
from ..files import FilesObject as FilesObject
from ..files import FilesObjectBytesReader as FilesObjectBytesReader
from ..files import FilesObjectBytesWriter as FilesObjectBytesWriter
from ..files import FilesObjectTextReader as FilesObjectTextReader
from ..files import FilesObjectTextWriter as FilesObjectTextWriter
from ..files import FileSpace as FileSpace
from ..files import MODELS_SPACE as MODELS_SPACE
from ..files import PERSONAL_SPACE as PERSONAL_SPACE
from ..files import SHARED_SPACE as SHARED_SPACE
