#!/usr/bin/env python
"""
SingleStoreDB Stage Management API v1 -- **deprecated**.

.. deprecated::
   Use :mod:`singlestoredb.management.stage`.
"""
from ..stage import Stage as _Stage
from ..utils import PathLike


class Stage(_Stage):
    """
    Stage file space for a workspace group or starter workspace.

    .. deprecated::
       Use :class:`singlestoredb.management.stage.Stage`, reached from
       :attr:`Cluster.stage` or :func:`singlestoredb.management.get_stage`.

    Here Stage is a top-level resource keyed by deployment ID:
    ``stage/{id}/fs/``, which is why this subclass overrides the path.
    """

    def _fs_path(self, path: PathLike = '') -> str:
        return f'stage/{self._deployment_id}/fs/{path}'
