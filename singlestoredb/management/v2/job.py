#!/usr/bin/env python
"""SingleStoreDB Job Management API v2."""
from ..job import Execution as Execution
from ..job import ExecutionConfig as ExecutionConfig
from ..job import ExecutionMetadata as ExecutionMetadata
from ..job import ExecutionsData as ExecutionsData
from ..job import Job as Job
from ..job import JobMetadata as JobMetadata
from ..job import JobsManager as _JobsManager
from ..job import Mode as Mode
from ..job import Parameter as Parameter
from ..job import Runtime as Runtime
from ..job import Schedule as Schedule
from ..job import Status as Status
from ..job import TargetConfig as TargetConfig
from ..job import TargetType as TargetType


class JobsManager(_JobsManager):
    """
    SingleStoreDB scheduled notebook jobs manager (API v2).

    The ``jobs`` routes themselves are unchanged at v2. What changed is the
    ``targetConfig.targetType`` vocabulary: v1's ``'Workspace'`` and
    ``'VirtualWorkspace'`` became ``'Cluster'`` and ``'VirtualCluster'``, and
    v1's legacy self-managed ``'Cluster'`` target has no v2 equivalent.

    Note that ``'Cluster'`` means different things at the two versions: a
    legacy self-managed cluster at v1, and the resource v1 called a workspace
    at v2.
    """

    _deployment_target_type = TargetType.CLUSTER
    _starter_target_type = TargetType.VIRTUAL_CLUSTER

    #: v2 has no legacy self-managed cluster concept -- everything is a
    #: cluster -- so ``SINGLESTOREDB_CLUSTER`` is not a distinct target here.
    _legacy_cluster_target_type = None
