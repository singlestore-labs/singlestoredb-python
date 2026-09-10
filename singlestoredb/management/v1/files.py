#!/usr/bin/env python
"""
SingleStoreDB Files Management API v1 -- **deprecated**.

.. deprecated::
   Only the module path is deprecated; the names re-exported below are the
   shared implementations. Import them from
   :mod:`singlestoredb.management.files`, and call ``manage_files()`` without
   ``version='v1'``.

The ``files/fs/{space}/...`` routes are implemented in
:mod:`singlestoredb.management.files`, and this module re-exports them so that
``manage_files(version='v1')`` can resolve the implementation by name.
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
