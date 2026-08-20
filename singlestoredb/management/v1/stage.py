#!/usr/bin/env python
"""SingleStoreDB Stage Management API v1."""
from ..stage import Stage as _Stage
from ..utils import PathLike


class Stage(_Stage):
    """
    Stage file space for a v1 workspace group or starter workspace.

    At v1 Stage is a top-level resource keyed by deployment ID:
    ``stage/{id}/fs/``. From v2 onward it is nested under the cluster, which
    is what the shared base implements.
    """

    def _fs_path(self, path: PathLike = '') -> str:
        return f'stage/{self._deployment_id}/fs/{path}'
