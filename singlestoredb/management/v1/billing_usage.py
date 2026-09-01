#!/usr/bin/env python
"""
SingleStoreDB Billing Usage API v1 -- **deprecated**.

.. deprecated::
   Only the module path is deprecated, along with the rest of
   :mod:`singlestoredb.management.v1`; the names re-exported below are the
   shared implementations, not v1-specific ones. Import them from
   :mod:`singlestoredb.management.billing_usage`.

``GET /v1/billing/usage`` and ``GET /v2/billing/usage`` are identical, so the
implementation lives in the shared
:mod:`singlestoredb.management.billing_usage` module.
"""
from ..billing_usage import BillingUsageItem as BillingUsageItem
from ..billing_usage import UsageItem as UsageItem
