#!/usr/bin/env python
"""
SingleStoreDB Files Management API v1 -- **deprecated**.

.. deprecated::
   Only the module path is deprecated, along with the rest of
   :mod:`singlestoredb.management.v1`; the names re-exported below are the
   shared implementations, not v1-specific ones. Import them from
   :mod:`singlestoredb.management.files`, and call ``manage_files()`` without
   ``version='v1'``.

The Files API is identical at v1 and v2 -- ``files/fs/{space}/...`` is
live-confirmed at both versions -- so the implementation lives in the shared
:mod:`singlestoredb.management.files` module and this package only re-exports
it, so ``manage_files(version='v1')`` can resolve this module by name.
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
