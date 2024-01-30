#!/usr/bin/env python
"""SingleStoreDB Cloud Billing Usage."""
import datetime
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

from .manager import Manager
from .utils import camel_to_snake
from .utils import vars_to_str


class UsageItem(object):
    """Usage statistics."""

    def __init__(
        self,
        start_time: datetime.datetime,
        end_time: datetime.datetime,
        owner_id: str,
        resource_id: str,
        resource_name: str,
        resource_type: str,
        value: str,
    ):
        #: Starting time for the usage duration
        self.start_time = start_time

        #: Ending time for the usage duration
        self.end_time = end_time

        #: Owner ID
        self.owner_id = owner_id

        #: Resource ID
        self.resource_id = resource_id

        #: Resource name
        self.resource_name = resource_name

        #: Resource type
        self.resource_type = resource_type

        #: Usage statistic value
        self.value = value

        self._manager: Optional[Manager] = None

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)

    @classmethod
    def from_dict(
        cls,
        obj: Dict[str, Any],
        manager: Manager,
    ) -> 'UsageItem':
        """
        Convert dictionary to a ``UsageItem`` object.

        Parameters
        ----------
        obj : dict
            Key-value pairs to retrieve billling usage information from
        manager : WorkspaceManager, optional
            The WorkspaceManager the UsageItem belongs to

        Returns
        -------
        :class:`UsageItem`

        """
        out = cls(
            end_time=datetime.datetime.fromisoformat(obj['endTime']),
            start_time=datetime.datetime.fromisoformat(obj['startTime']),
            owner_id=obj['ownerId'],
            resource_id=obj['resourceId'],
            resource_name=obj['resourceName'],
            resource_type=obj['resource_type'],
            value=obj['value'],
        )
        out._manager = manager
        return out


class BillingUsageItem(object):
    """Billing usage item."""

    def __init__(
        self,
        description: str,
        metric: str,
        usage: List[UsageItem],
    ):
        """Use :attr:`WorkspaceManager.billing.usage` instead."""
        #: Description of the usage metric
        self.description = description

        #: Name of the usage metric
        self.metric = metric

        #: Usage statistics
        self.usage = list(usage)

        self._manager: Optional[Manager] = None

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)

    @ classmethod
    def from_dict(
        cls,
        obj: Dict[str, Any],
        manager: Manager,
    ) -> 'BillingUsageItem':
        """
        Convert dictionary to a ``BillingUsageItem`` object.

        Parameters
        ----------
        obj : dict
            Key-value pairs to retrieve billling usage information from
        manager : WorkspaceManager, optional
            The WorkspaceManager the BillingUsageItem belongs to

        Returns
        -------
        :class:`BillingUsageItem`

        """
        out = cls(
            description=obj['description'],
            metric=str(camel_to_snake(obj['metric'])),
            usage=[UsageItem.from_dict(x, manager) for x in obj['Usage']],
        )
        out._manager = manager
        return out
