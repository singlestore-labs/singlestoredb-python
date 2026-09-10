#!/usr/bin/env python
"""
Management API versions this SDK is built against.

This module imports nothing, so both :mod:`singlestoredb.config` -- which
registers the ``management.version`` option -- and
:mod:`singlestoredb.management._version_import` can read these without an
import cycle. Neither package can host them: ``config`` is imported before
``management``, and ``management.manager`` imports ``config``.
"""
#: Management API version used when nothing else names one. Changing this
#: retargets the ``management.version`` option default, the ``manage_*``
#: factories, and the ``default_version`` of every version-neutral manager
#: class. Classes that implement one specific version name it literally
#: instead, and do not follow this.
DEFAULT_MANAGEMENT_VERSION = 'v2'

#: Management API version being wound down. Public entry points that resolve
#: to it raise a :class:`DeprecationWarning`, and everything under
#: ``singlestoredb.management.v1`` goes away with it.
DEPRECATED_MANAGEMENT_VERSION = 'v1'
