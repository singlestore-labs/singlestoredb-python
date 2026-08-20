#!/usr/bin/env python
"""SingleStoreDB Organization API v2."""
from ..organization import Organization as _Organization
from ..organization import Secret as Secret
from .inference_api import InferenceAPIManager
from .job import JobsManager


class Organization(_Organization):
    """
    Organization in SingleStoreDB Cloud portal (API v2).

    ``GET /v2/organizations/current`` and ``GET /v2/secrets`` return the same
    payloads as their v1 counterparts, so the only v2 difference is which
    sub-managers this organization hands out.
    """

    _jobs_manager_class = JobsManager
    _inference_api_manager_class = InferenceAPIManager
