"""SingleStoreDB extensions for Ibis.

This package adds SingleStoreDB-specific features to the Ibis backend.
Features are automatically registered on import.

Usage
-----
>>> import ibis
>>> import singlestoredb.ibis_extras  # Auto-registers extensions
>>>
>>> con = ibis.singlestoredb.connect(host="...", database="...")
>>>
>>> # Variable accessors (from old ibis_singlestoredb)
>>> con.show.databases()
>>> con.globals["max_connections"]
>>> con.vars["autocommit"]
>>>
>>> # Backend methods
>>> con.get_storage_info()
>>> con.get_workload_metrics()
>>> con.optimize_table("users")
>>>
>>> # Table methods (work on any table from SingleStoreDB)
>>> t = con.table("users")
>>> t.optimize()
>>> t.get_stats()
"""
from __future__ import annotations

import warnings

from .mixins import BackendExtensionsMixin
from .mixins import TableExtensionsMixin

__all__ = [
    'BackendExtensionsMixin',
    'TableExtensionsMixin',
    'is_registered',
    'register',
]

_registered = False


def _check_collisions(cls: type, mixin: type) -> None:
    """Check for method collisions between mixin and target class."""
    mixin_attrs = {
        name
        for name in dir(mixin)
        if not name.startswith('_') and callable(getattr(mixin, name, None))
    }
    mixin_props = {
        name
        for name in dir(mixin)
        if not name.startswith('_')
        and isinstance(getattr(mixin, name, None), property)
    }
    mixin_members = mixin_attrs | mixin_props

    existing_attrs = {name for name in dir(cls) if not name.startswith('_')}

    collisions = mixin_members & existing_attrs
    if collisions:
        warnings.warn(
            f'Mixin {mixin.__name__} has methods that collide with '
            f'{cls.__name__}: {collisions}',
            stacklevel=3,
        )


def register() -> None:
    """Register mixins on Backend and ir.Table.

    This is called automatically on import, but can be called
    explicitly if needed.
    """
    global _registered  # noqa: PLW0603
    if _registered:
        return

    import ibis.expr.types as ir
    from ibis.backends.singlestoredb import Backend

    # Check for collisions before adding mixins
    _check_collisions(Backend, BackendExtensionsMixin)
    _check_collisions(ir.Table, TableExtensionsMixin)

    # Add mixin to Backend
    if BackendExtensionsMixin not in Backend.__bases__:
        Backend.__bases__ = (BackendExtensionsMixin,) + Backend.__bases__

    # Add mixin to ir.Table
    if TableExtensionsMixin not in ir.Table.__bases__:
        ir.Table.__bases__ = (TableExtensionsMixin,) + ir.Table.__bases__

    _registered = True


def is_registered() -> bool:
    """Check if extensions have been registered."""
    return _registered


# Auto-register on import
register()
