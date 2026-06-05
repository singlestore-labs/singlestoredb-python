#!/usr/bin/env python
"""Version switching mixin for management API objects."""
import copy
import importlib
import inspect
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
            mgr = target_cls(
                access_token=self._access_token,
                version=version,
                base_url=self._base_url_root,
                organization_id=self._organization_id,
            )
            # Propagate JWT state so the clone continues refreshing tokens
            mgr._is_jwt = self._is_jwt
            return mgr
        elif hasattr(self, '_manager') and self._response is not None:
            # Entity path: reconstruct from stored response with versioned
            # manager. Pass manager to from_dict only if its signature
            # accepts one (named 'manager').
            if self._manager is None:
                raise ManagementError(
                    msg=f"Cannot version-switch '{type(self).__name__}': "
                        f'manager reference is None',
                )
            versioned_mgr = getattr(self._manager, version)
            sig = inspect.signature(target_cls.from_dict)
            params = list(sig.parameters.keys())
            if 'manager' in params:
                out = target_cls.from_dict(self._response, versioned_mgr)
            else:
                out = target_cls.from_dict(self._response)
                out._manager = versioned_mgr
            # Propagate context that from_dict can't reconstruct alone
            if hasattr(self, '_location') and self._location is not None:
                out._location = copy.copy(self._location)
                if hasattr(out._location, '_manager'):
                    out._location._manager = versioned_mgr
            if hasattr(self, 'region') and hasattr(out, 'region'):
                out.region = self.region
            return out
        elif hasattr(self, '_manager'):
            # Wrapper manager path (e.g., JobsManager, InferenceAPIManager):
            # clone with versioned parent manager
            if self._manager is None:
                raise ManagementError(
                    msg=f"Cannot version-switch '{type(self).__name__}': "
                        f'manager reference is None',
                )
            versioned_mgr = getattr(self._manager, version)
            return target_cls(versioned_mgr)
        else:
            raise ManagementError(
                msg=f"Cannot version-switch '{type(self).__name__}': "
                    f'no credentials or manager reference',
            )


def _import_versioned_module(version: str, module_name: str) -> Any:
    """Import a versioned module, raising a friendly error if not found."""
    if not _VERSION_RE.match(version):
        raise ManagementError(
            msg=f"Invalid API version format: '{version}'",
        )
    path = f'singlestoredb.management.{version}.{module_name}'
    try:
        return importlib.import_module(path)
    except ModuleNotFoundError as e:
        if e.name and (e.name == path or path.startswith(e.name)):
            raise ManagementError(
                msg=f"Unsupported API version: '{version}'",
            )
        raise
