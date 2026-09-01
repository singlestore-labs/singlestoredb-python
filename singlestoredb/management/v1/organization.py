#!/usr/bin/env python
"""
SingleStoreDB Organization API v1 -- **deprecated**.

.. deprecated::
   Deprecated with the rest of :mod:`singlestoredb.management.v1`. Use
   :mod:`singlestoredb.management.organization`, whose ``Organization`` hands
   out the v2 sub-managers, reached from
   :func:`singlestoredb.management.get_organization`.
"""
from ..organization import Organization as _Organization
from ..organization import Organizations as _Organizations
from ..organization import Secret as Secret
from .inference_api import InferenceAPIManager
from .job import JobsManager


class Organization(_Organization):
    """
    Organization in SingleStoreDB Cloud portal (API v1).

    .. deprecated::
       Use :class:`singlestoredb.management.organization.Organization`, the v2
       class.

    ``organizations/current`` and ``secrets`` respond identically at v1 and v2,
    so the only v1 difference is which sub-managers this organization hands
    out. Getting this repoint wrong would silently send v2 ``targetType``
    values on v1 job schedules.
    """

    _jobs_manager_class = JobsManager
    _inference_api_manager_class = InferenceAPIManager


class Organizations(_Organizations):
    """
    Organizations (API v1).

    .. deprecated::
       Use :class:`singlestoredb.management.organization.Organizations`, the v2
       class.
    """

    _organization_class = Organization

    @property
    def current(self) -> Organization:
        """Get current organization."""
        out = super().current
        assert isinstance(out, Organization)
        return out
