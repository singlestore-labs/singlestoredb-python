#!/usr/bin/env python
"""
SingleStoreDB export service.

The names below come from :mod:`singlestoredb.management.v2.export`, matching
:mod:`singlestoredb.management.cluster`: table egress is driven through
``clusters/{id}/egress/...``, so an export is owned by a
:class:`~singlestoredb.management.v2.cluster.Cluster`.

This is a version-locked shim, not a version-neutral one -- it does not consult
the ``management.version`` option, because the two implementations take
different objects (a ``Cluster`` at v2, a ``WorkspaceGroup`` at v1) and so
cannot be swapped behind one name. Import from :mod:`.v1.export` to pin v1.
"""
from .v2.export import _get_exports as _get_exports
from .v2.export import ExportService as ExportService
from .v2.export import ExportStatus as ExportStatus
