#!/usr/bin/env python
"""
SingleStoreDB Inference API Management v2.

.. warning:: The inference APIs are **not available at management API v2.**
   ``GET /v1/models`` returns 200 while ``GET /v2/models`` returns
   ``404 page not found``, and no v2 spelling of ``inferenceapis`` responds.
   Until the service exposes v2 routes, every method on the v2
   :class:`InferenceAPIManager` raises :class:`ManagementError` pointing at v1
   rather than silently issuing a request that cannot succeed.
"""
from typing import List

from ...exceptions import ManagementError
from ..inference_api import InferenceAPIInfo as InferenceAPIInfo
from ..inference_api import InferenceAPIManager as _InferenceAPIManager
from ..inference_api import ModelOperationResult as ModelOperationResult

_NO_V2_ROUTE = (
    'The inference APIs are not available at management API v2; there is no '
    'v2 equivalent of the v1 "models" and "inferenceapis" routes. Use a v1 '
    "manager (manage_workspaces(version='v1').organization.inference_apis) "
    'for this call.'
)


class InferenceAPIManager(_InferenceAPIManager):
    """
    SingleStoreDB Inference APIs manager (API v2) -- not implemented.

    Every method raises :class:`ManagementError`. See the module docstring.
    """

    def get(self, model_name: str) -> InferenceAPIInfo:
        """Not available at API v2. Always raises."""
        raise ManagementError(msg=_NO_V2_ROUTE)

    def start(self, model_name: str) -> ModelOperationResult:
        """Not available at API v2. Always raises."""
        raise ManagementError(msg=_NO_V2_ROUTE)

    def stop(self, model_name: str) -> ModelOperationResult:
        """Not available at API v2. Always raises."""
        raise ManagementError(msg=_NO_V2_ROUTE)

    def show(self) -> List[ModelOperationResult]:
        """Not available at API v2. Always raises."""
        raise ManagementError(msg=_NO_V2_ROUTE)

    def drop(self, model_name: str) -> ModelOperationResult:
        """Not available at API v2. Always raises."""
        raise ManagementError(msg=_NO_V2_ROUTE)
