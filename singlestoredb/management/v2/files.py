#!/usr/bin/env python
"""
SingleStoreDB Files Management API v2.

The ``files/fs/{space}/...`` routes are implemented in
:mod:`singlestoredb.management.files`, so this module only re-exports it.
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
