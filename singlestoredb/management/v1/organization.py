#!/usr/bin/env python
"""
SingleStoreDB Organization API v1 -- **deprecated**.

.. deprecated::
   Use :mod:`singlestoredb.management.organization`, reached from
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
       Use :class:`singlestoredb.management.organization.Organization`.

    ``organizations/current`` and ``secrets`` are the shared routes; all this
    subclass does is hand out the sub-managers that belong to this API version,
    which is what keeps job schedules on the ``targetType`` vocabulary these
    routes expect.
    """

    _jobs_manager_class = JobsManager
    _inference_api_manager_class = InferenceAPIManager


class Organizations(_Organizations):
    """
    Organizations (API v1).

    .. deprecated::
       Use :class:`singlestoredb.management.organization.Organizations`.
    """

    _organization_class = Organization

    @property
    def current(self) -> Organization:
        """Get current organization."""
        out = super().current
        assert isinstance(out, Organization)
        return out
