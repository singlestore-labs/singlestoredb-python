#!/usr/bin/env python
"""
SingleStoreDB Cloud Inference API.

The inference API exists at v1 only -- none of the ``inferenceapis/`` routes
respond at v2 -- so the implementation lives in
:mod:`singlestoredb.management.v1.inference_api`. This module re-exports it
for the v1-only callers (Fusion) that import it by this name.
"""
from .v1.inference_api import InferenceAPIInfo as InferenceAPIInfo
from .v1.inference_api import InferenceAPIManager as InferenceAPIManager
from .v1.inference_api import ModelOperationResult as ModelOperationResult
