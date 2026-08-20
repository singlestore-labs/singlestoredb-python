#!/usr/bin/env python
"""SingleStoreDB Job Management API v1."""
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
    SingleStoreDB scheduled notebook jobs manager (API v1).

    The ``jobs`` routes themselves are unchanged from v1 to v2. What changed
    is the ``targetConfig.targetType`` vocabulary: v1's ``'Workspace'`` and
    ``'VirtualWorkspace'`` became ``'Cluster'`` and ``'VirtualCluster'``, and
    v1's legacy self-managed ``'Cluster'`` target has no later equivalent.

    Note that ``'Cluster'`` means different things at the two versions: a
    legacy self-managed cluster at v1, and the resource v1 called a workspace
    from v2 onward.
    """

    _deployment_target_type = TargetType.WORKSPACE
    _starter_target_type = TargetType.VIRTUAL_WORKSPACE

    #: v1 keeps a distinct legacy self-managed cluster target, named by
    #: ``SINGLESTOREDB_CLUSTER``.
    _legacy_cluster_target_type = TargetType.CLUSTER
