#!/usr/bin/env python
"""
SingleStoreDB billing information.

``GET /v1/billing/usage`` and ``GET /v2/billing/usage`` are the same route with
the same response, so this is version-neutral.
"""
import datetime
from typing import List
from typing import Optional

from .billing_usage import BillingUsageItem
from .manager import Manager
from .utils import from_datetime
from .utils import snake_to_camel


class Billing(object):
    """Billing information."""

    COMPUTE_CREDIT = 'compute_credit'
    STORAGE_AVG_BYTE = 'storage_avg_byte'

    HOUR = 'hour'
    DAY = 'day'
    MONTH = 'month'

    def __init__(self, manager: Manager):
        self._manager = manager

    def usage(
        self,
        start_time: datetime.datetime,
        end_time: datetime.datetime,
        metric: Optional[str] = None,
        aggregate_by: Optional[str] = None,
    ) -> List[BillingUsageItem]:
        """
        Get usage information.

        Parameters
        ----------
        start_time : datetime.datetime
            Start time for usage interval
        end_time : datetime.datetime
            End time for usage interval
        metric : str, optional
            Possible metrics are ``mgr.billing.COMPUTE_CREDIT`` and
            ``mgr.billing.STORAGE_AVG_BYTE`` (default is all)
        aggregate_by : str, optional
            Aggregate type used to group usage: ``mgr.billing.HOUR``,
            ``mgr.billing.DAY``, or ``mgr.billing.MONTH``

        Returns
        -------
        List[BillingUsage]

        """
        res = self._manager._get(
            'billing/usage',
            params={
                k: v for k, v in dict(
                    metric=snake_to_camel(metric),
                    startTime=from_datetime(start_time),
                    endTime=from_datetime(end_time),
                    aggregateBy=aggregate_by.lower() if aggregate_by else None,
                ).items() if v is not None
            },
        )
        return [
            BillingUsageItem.from_dict(x, self._manager)
            for x in res.json()['billingUsage']
        ]
