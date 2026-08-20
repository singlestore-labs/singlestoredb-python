#!/usr/bin/env python
"""
SingleStoreDB Inference API Management v1.

The inference API routes (``models``, ``inferenceapis/{project}/{model}``)
exist at v1 only, but the code itself is version-neutral, so it lives in the
shared :mod:`singlestoredb.management.inference_api` module and this module
only re-exports it. The v2 subclass raises instead of calling.
"""
from ..inference_api import InferenceAPIInfo as InferenceAPIInfo
from ..inference_api import InferenceAPIManager as InferenceAPIManager
from ..inference_api import ModelOperationResult as ModelOperationResult
