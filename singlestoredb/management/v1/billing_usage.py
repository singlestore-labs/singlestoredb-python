#!/usr/bin/env python
"""
SingleStoreDB Billing Usage API v1 -- **deprecated**.

.. deprecated::
   Only the module path is deprecated; the names re-exported below are the
   shared implementations. Import them from
   :mod:`singlestoredb.management.billing_usage`.

``GET /v1/billing/usage`` is implemented in
:mod:`singlestoredb.management.billing_usage`, so this module only re-exports it.
"""
from ..billing_usage import BillingUsageItem as BillingUsageItem
from ..billing_usage import UsageItem as UsageItem
