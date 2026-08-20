#!/usr/bin/env python
"""
SingleStoreDB Organization API v1.

``organizations/current`` and ``secrets`` respond identically at v1 and v2, so
:class:`Organization` and :class:`Secret` live in the shared
:mod:`singlestoredb.management.organization` module.
"""
from ..organization import Organization as Organization
from ..organization import Secret as Secret
