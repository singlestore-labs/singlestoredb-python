#!/usr/bin/env python
"""
SingleStoreDB Organization API v2.

``GET /v2/organizations/current`` and ``GET /v2/secrets`` return the same
payloads as their v1 counterparts, and the shared
:mod:`singlestoredb.management.organization` module already hands out the v2
sub-managers, so this module only re-exports it.
"""
from ..organization import Organization as Organization
from ..organization import Organizations as Organizations
from ..organization import Secret as Secret
