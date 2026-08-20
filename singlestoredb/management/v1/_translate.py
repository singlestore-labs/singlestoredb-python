#!/usr/bin/env python
"""
Field translation between v1 workspace payloads and v2 cluster payloads.

All of the knowledge that v1's workspaces and workspace groups became v2's
clusters lives here, inside the v1 package. That is deliberate: when the v1
endpoints are retired the whole ``v1/`` directory is removed and this mapping
goes with it, and until then no v2 module has to name a v1 resource. See
``TestV1IsDeletable``.

The translators are reached through :attr:`VersionedMixin._version_map` and
:meth:`VersionedMixin._version_response`.
"""
from collections.abc import Iterable
from typing import Any
from typing import Dict
from typing import Optional
from typing import Tuple


def _rename(
    obj: Dict[str, Any],
    renames: Iterable[Tuple[str, str]],
) -> Dict[str, Any]:
    """Copy `obj`, renaming the given keys."""
    out = dict(obj)
    for old, new in renames:
        if old in out:
            out[new] = out.pop(old)
    return out


def _pack_size(obj: Dict[str, Any]) -> Dict[str, Any]:
    """Fold a flat ``size``/``scaleFactor`` pair into a v2 size object."""
    size = obj.pop('size', None)
    scale_factor = obj.pop('scaleFactor', None)
    if size is None and scale_factor is None:
        return obj
    spec: Dict[str, Any] = {}
    if size is not None:
        spec['size'] = size
    if scale_factor is not None:
        spec['scaleFactor'] = scale_factor
    obj['size'] = spec
    return obj


def _unpack_size(obj: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten a v2 size object back into ``size``/``scaleFactor``."""
    spec = obj.get('size')
    if not isinstance(spec, dict):
        return obj
    obj.pop('size')
    if spec.get('size') is not None:
        obj['size'] = spec['size']
    if spec.get('scaleFactor') is not None:
        obj['scaleFactor'] = spec['scaleFactor']
    return obj


def workspace_to_cluster(obj: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Re-key a v1 workspace response body as a v2 cluster response body."""
    if obj is None:
        return None
    return _pack_size(
        _rename(
            obj, (
                ('workspaceID', 'clusterID'),
                ('workspaceGroupID', 'groupID'),
                ('kaiEnabled', 'kai'),
            ),
        ),
    )


def starter_workspace_to_starter_cluster(
    obj: Optional[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """Re-key a v1 starter workspace response body for v2."""
    if obj is None:
        return None
    return _rename(obj, (('virtualWorkspaceID', 'virtualClusterID'),))


def cluster_to_workspace(obj: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Re-key a v2 cluster response body as a v1 workspace response body."""
    if obj is None:
        return None
    return _rename(
        _unpack_size(dict(obj)), (
            ('clusterID', 'workspaceID'),
            ('groupID', 'workspaceGroupID'),
            ('kai', 'kaiEnabled'),
            ('region', 'regionName'),
            ('multiAZ', 'highAvailabilityTwoZones'),
        ),
    )


def starter_cluster_to_starter_workspace(
    obj: Optional[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """Re-key a v2 starter cluster response body for v1."""
    if obj is None:
        return None
    return _rename(obj, (('virtualClusterID', 'virtualWorkspaceID'),))
