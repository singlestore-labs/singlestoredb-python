#!/usr/bin/env python
"""
SingleStoreDB Job Management API v1 -- **deprecated**.

.. deprecated::
   Use :mod:`singlestoredb.management.job`, reached from
   :attr:`Organization.jobs`.
"""
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

    .. deprecated::
       Use :class:`singlestoredb.management.job.JobsManager`.

    The ``jobs`` routes are the shared ones; what is specific here is the
    ``targetConfig.targetType`` vocabulary this manager schedules with,
    ``'Workspace'`` and ``'VirtualWorkspace'``.

    ``'Cluster'`` is a third value these routes accept, naming a legacy
    self-managed cluster. Only the read path ever sees it -- the deployment a
    job is scheduled against comes from ``SINGLESTOREDB_WORKSPACE``, which
    never names a legacy cluster.
    """

    _deployment_target_type = TargetType.WORKSPACE
    _starter_target_type = TargetType.VIRTUAL_WORKSPACE
