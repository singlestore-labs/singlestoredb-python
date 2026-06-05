#!/usr/bin/env python
"""Version switching mixin for management API objects."""
import importlib
import re
from typing import Any
from typing import Dict
from typing import Optional

from ..exceptions import ManagementError


_VERSION_RE = re.compile(r'^v\d+$')


class VersionedMixin:
    """Mixin providing version-switching via attribute access (e.g., obj.v2)."""

    _version_cache: Optional[Dict[str, Any]] = None
    _response: Optional[Dict[str, Any]] = None

    @property
    def _module_name(self) -> str:
        return self.__class__.__module__.rsplit('.', 1)[-1]

    def _get_version_cache(self) -> Dict[str, Any]:
        if self._version_cache is None:
            self._version_cache = {}
        return self._version_cache

    def __getattr__(self, name: str) -> Any:
        if _VERSION_RE.match(name):
            cache = self._get_version_cache()
            if name not in cache:
                cache[name] = self._get_versioned(name)
            return cache[name]
        raise AttributeError(
            f"'{type(self).__name__}' object has no attribute '{name}'",
        )

    def _get_versioned(self, version: str) -> Any:
        mod = _import_versioned_module(version, self._module_name)
        target_cls = getattr(mod, type(self).__name__, None)
        if target_cls is None:
            raise ManagementError(
                msg=f"'{type(self).__name__}' is not available in API {version}",
            )

        if hasattr(self, '_access_token'):
            # Manager path: clone with same credentials at new version
            return target_cls(
                access_token=self._access_token,
                version=version,
                base_url=self._base_url_root,
                organization_id=self._organization_id,
            )
        elif hasattr(self, '_manager') and self._response is not None:
            # Entity path: construct versioned entity with versioned manager
            versioned_mgr = self._manager._get_versioned(version)
            return target_cls.from_dict(self._response, versioned_mgr)
        elif hasattr(self, '_manager'):
            # Wrapper manager path (e.g., JobsManager, InferenceAPIManager):
            # clone with versioned parent manager
            versioned_mgr = self._manager._get_versioned(version)
            return target_cls(versioned_mgr)
        else:
            raise ManagementError(
                msg=f"Cannot version-switch '{type(self).__name__}': "
                    f'no credentials or manager reference',
            )


def _import_versioned_module(version: str, module_name: str) -> Any:
    """Import a versioned module, raising a friendly error if not found."""
    path = f'singlestoredb.management.{version}.{module_name}'
    try:
        return importlib.import_module(path)
    except ImportError:
        raise ManagementError(
            msg=f"Unsupported API version: '{version}'",
        )
