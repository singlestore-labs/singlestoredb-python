#!/usr/bin/env python
"""
SingleStoreDB Organization API v2.

``GET /v2/organizations/current`` and ``GET /v2/secrets`` are implemented in
:mod:`singlestoredb.management.organization`, so this module only re-exports it.
"""
from ..organization import Organization as Organization
from ..organization import Organizations as Organizations
from ..organization import Secret as Secret
