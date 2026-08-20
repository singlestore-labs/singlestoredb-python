#!/usr/bin/env python
"""
SingleStoreDB Job Management API v1.

The jobs routes are unchanged at v2; only the ``targetConfig.targetType``
vocabulary differs. The implementation therefore lives in the shared
:mod:`singlestoredb.management.job` module, whose defaults are the v1
vocabulary, and this module only re-exports it.
"""
from ..job import Execution as Execution
from ..job import ExecutionConfig as ExecutionConfig
from ..job import ExecutionMetadata as ExecutionMetadata
from ..job import ExecutionsData as ExecutionsData
from ..job import Job as Job
from ..job import JobMetadata as JobMetadata
from ..job import JobsManager as JobsManager
from ..job import Mode as Mode
from ..job import Parameter as Parameter
from ..job import Runtime as Runtime
from ..job import Schedule as Schedule
from ..job import Status as Status
from ..job import TargetConfig as TargetConfig
from ..job import TargetType as TargetType
