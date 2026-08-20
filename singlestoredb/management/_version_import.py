#!/usr/bin/env python
"""Importer for version-specific management API modules."""
import importlib
import re
from typing import Any

from ..exceptions import ManagementError


_VERSION_RE = re.compile(r'^v\d+$')


def _import_versioned_module(version: str, module_name: str) -> Any:
    """Import a versioned module, raising a friendly error if not found."""
    if not _VERSION_RE.match(version):
        raise ManagementError(
            msg=f"Invalid API version format: '{version}'",
        )
    version_pkg = f'singlestoredb.management.{version}'
    path = f'{version_pkg}.{module_name}'
    try:
        return importlib.import_module(path)
    except ModuleNotFoundError as e:
        if e.name is None or (e.name != path and not path.startswith(e.name)):
            # Failure originated deeper than the requested module
            # (e.g., a transitive import inside a valid module). Don't mask.
            raise
        try:
            importlib.import_module(version_pkg)
        except ModuleNotFoundError:
            raise ManagementError(
                msg=f"Unsupported API version: '{version}'",
            )
        raise ManagementError(
            msg=f"API version '{version}' does not provide "
                f"module '{module_name}'",
        )
