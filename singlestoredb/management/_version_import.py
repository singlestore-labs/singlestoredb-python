#!/usr/bin/env python
"""Importer for version-specific management API modules."""
import importlib
import re
import warnings
from typing import Any
from typing import Optional

from ..exceptions import ManagementError


_VERSION_RE = re.compile(r'^v\d+$')

#: API version used when neither the caller nor the ``management.version``
#: option names one -- i.e. when the option has been explicitly blanked out,
#: since it otherwise carries this same default itself.
DEFAULT_VERSION = 'v2'

#: The version this SDK is winding down. Everything under
#: ``singlestoredb.management.v1`` goes away with it, so any *public* entry
#: point that resolves to it warns -- see :func:`_warn_if_deprecated_version`.
DEPRECATED_VERSION = 'v1'


def _warn_if_deprecated_version(version: str, stacklevel: int = 3) -> None:
    """
    Warn if ``version`` names a management API version being wound down.

    Called from the public version-neutral entry points -- the ``manage_*``
    factories and the ``get_organization``/``get_secret``/``get_stage``
    helpers -- *after* the version has been resolved, so it fires whether v1
    was named by the caller or inherited from the ``management.version``
    option.

    Deliberately not called from :func:`_resolve_version` itself. Several
    internal paths are v1-only by design and resolve v1 with no v2 route to
    move to -- ``workspace._manage_workspaces_v1`` and the inference API
    behind it -- so warning at the resolver would emit noise the caller can do
    nothing about. :func:`manage_workspaces` is likewise excluded: it raises
    its own, more specific warning naming ``manage_clusters``.

    Parameters
    ----------
    version : str
        The already-resolved version
    stacklevel : int, optional
        Passed through to :func:`warnings.warn`. The default of 3 is right for
        a public entry point calling this directly: 1 is this function, 2 is
        the entry point, 3 is the user. Add one per intervening frame.

    """
    if version != DEPRECATED_VERSION:
        return
    warnings.warn(
        f'management API {DEPRECATED_VERSION} is deprecated and will be '
        'removed; it has been replaced by '
        f'{DEFAULT_VERSION}. Stop passing version='
        f'"{DEPRECATED_VERSION}", and unset the management.version option '
        '(the SINGLESTOREDB_MANAGEMENT_VERSION environment variable) if it '
        f'names {DEPRECATED_VERSION}.',
        DeprecationWarning,
        stacklevel=stacklevel + 1,
    )


def _resolve_version(
    version: Optional[str] = None,
    default: Optional[str] = None,
) -> str:
    """
    Resolve the management API version to use.

    An explicit argument wins; otherwise the ``management.version`` option (the
    ``SINGLESTOREDB_MANAGEMENT_VERSION`` environment variable) decides. This is
    the one place that rule is written down; every version-neutral entry point
    goes through here so they cannot drift apart.

    Parameters
    ----------
    version : str, optional
        Version named by the caller, if any
    default : str, optional
        Version to use when neither the caller nor the option names one.
        Defaults to :data:`DEFAULT_VERSION`.

    Returns
    -------
    str

    """
    from .. import config
    return version or config.get_option('management.version') \
        or default or DEFAULT_VERSION


def _import_versioned_package(version: str) -> Any:
    """Import a version package, raising a friendly error if not found."""
    if not _VERSION_RE.match(version):
        raise ManagementError(
            msg=f"Invalid API version format: '{version}'",
        )
    try:
        return importlib.import_module(f'singlestoredb.management.{version}')
    except ModuleNotFoundError:
        raise ManagementError(
            msg=f"Unsupported API version: '{version}'",
        )


def _versioned_attr(name: str, version: Optional[str] = None) -> Any:
    """
    Look a name up in the resolved version package.

    Version-neutral helpers dispatch through here rather than naming the module
    that holds their implementation, because that module differs by version --
    the v1 helpers hang off workspaces, the v2 helpers off clusters -- and a
    future version is free to put them somewhere else again. Each version
    package re-exports its own, so this layer only has to resolve the version.

    Every caller is a public entry point one frame up
    (``organization.get_organization``, ``organization.get_secret``,
    ``stage.get_stage``), so the deprecated-version warning is raised here
    rather than repeated in each of them.

    Parameters
    ----------
    name : str
        Name to look up in the version package
    version : str, optional
        Version of the API to use. Defaults to the ``management.version``
        option.

    Returns
    -------
    Any

    Raises
    ------
    :class:`ManagementError`
        If the resolved version does not provide the name

    """
    ver = _resolve_version(version)
    # +1 for this frame sitting between the helper and the user.
    _warn_if_deprecated_version(ver, stacklevel=4)
    pkg = _import_versioned_package(ver)
    try:
        return getattr(pkg, name)
    except AttributeError:
        raise ManagementError(
            msg=f"management API {ver} does not provide '{name}'",
        )


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
