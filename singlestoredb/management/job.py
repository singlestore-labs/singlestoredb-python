#!/usr/bin/env python
"""SingleStoreDB Cloud Scheduled Notebook Job."""
import datetime
import threading
from enum import Enum
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

from ..exceptions import ManagementError
from .manager import Manager
from .utils import camel_to_snake
from .utils import from_datetime
from .utils import get_cluster_id
from .utils import get_database_name
from .utils import get_workspace_id
from .utils import is_virtual_workspace
from .utils import vars_to_str
from .utils import to_datetime_strict


class TargetType(Enum):
    WORKSPACE = 'Workspace'
    CLUSTER = 'Cluster'
    VIRTUAL_WORKSPACE = 'VirtualWorkspace'

    @classmethod
    def from_str(cls, s: Optional[str]) -> Optional['TargetType']:
        if s is None:
            return None
        try:
            return cls[str(camel_to_snake(s)).upper()]
        except KeyError:
            return None
        
    def __str__(self) -> str:
        """Return string representation."""
        return self.value

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)


class Status(Enum):
    UNKNOWN = 'Unknown'
    SCHEDULED = 'Scheduled'
    RUNNING = 'Running'
    COMPLETED = 'Completed'
    FAILED = 'Failed'
    ERROR = 'Error'
    CANCELED = 'Canceled'

    @classmethod
    def from_str(cls, s: str) -> 'Status':
        try:
            return cls[str(camel_to_snake(s)).upper()]
        except KeyError:
            return cls.UNKNOWN
        
    def __str__(self) -> str:
        """Return string representation."""
        return self.value

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)


class ExecutionMetadata(object):

    avg_duration_in_seconds: int
    max_duration_in_seconds: int
    status: Status
    count: int

    def __init__(
        self,
        avg_duration_in_seconds: int,
        max_duration_in_seconds: int,
        status: Status,
        count: int,
    ):
        self.avg_duration_in_seconds = avg_duration_in_seconds
        self.max_duration_in_seconds = max_duration_in_seconds
        self.status = status
        self.count = count

    @classmethod
    def from_dict(cls, obj: Dict[str, Any]) -> 'ExecutionMetadata':
        """
        Construct an ExecutionMetadata from a dictionary of values.

        Parameters
        ----------
        obj : dict
            Dictionary of values

        Returns
        -------
        :class:`ExecutionMetadata`

        """
        out = cls(
            avg_duration_in_seconds=int(obj['avgDurationInSeconds']),
            max_duration_in_seconds=int(obj['maxDurationInSeconds']),
            status=Status.from_str(obj['status']),
            count=int(obj['count']),
        )

        return out

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)


class ExecutionConfig(object):

    create_snapshot: Optional[bool]
    max_duration_in_mins: Optional[int]
    notebook_path: Optional[str]

    def __init__(
        self,
        create_snapshot: Optional[bool],
        max_duration_in_mins: Optional[int],
        notebook_path: Optional[str],
    ):
        self.create_snapshot = create_snapshot
        self.max_duration_in_mins = max_duration_in_mins
        self.notebook_path = notebook_path

    @classmethod
    def from_dict(cls, obj: Dict[str, Any]) -> 'ExecutionConfig':
        """
        Construct an ExecutionConfig from a dictionary of values.

        Parameters
        ----------
        obj : dict
            Dictionary of values

        Returns
        -------
        :class:`ExecutionConfig`

        """
        out = cls(
            create_snapshot=obj.get('createSnapshot'),
            max_duration_in_mins=obj.get('maxAllowedExecutionDurationInMinutes'),
            notebook_path=obj.get('notebookPath'),
        )

        return out

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)


class Schedule(object):

    execution_interval_in_minutes: Optional[int]
    start_at: datetime.datetime
    mode: Optional[str]

    def __init__(
        self,
        execution_interval_in_minutes: Optional[int],
        start_at: datetime.datetime,
        mode: Optional[str],
    ):
        self.execution_interval_in_minutes = execution_interval_in_minutes
        self.start_at = start_at
        self.mode = mode

    @classmethod
    def from_dict(cls, obj: Dict[str, Any]) -> 'Schedule':
        """
        Construct a Schedule from a dictionary of values.

        Parameters
        ----------
        obj : dict
            Dictionary of values

        Returns
        -------
        :class:`Schedule`

        """
        out = cls(
            execution_interval_in_minutes=obj.get('executionIntervalInMinutes'),
            start_at=to_datetime_strict(obj['startAt']),
            mode=obj.get('mode'),
        )

        return out

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)


class TargetConfig(object):

    database_name: Optional[str]
    resume_target: Optional[bool]
    target_id: Optional[str]
    target_type: Optional[TargetType]

    def __init__(
        self,
        database_name: Optional[str],
        resume_target: Optional[bool],
        target_id: Optional[str],
        target_type: Optional[TargetType],
    ):
        self.database_name = database_name
        self.resume_target = resume_target
        self.target_id = target_id
        self.target_type = target_type

    @classmethod
    def from_dict(cls, obj: Dict[str, Any]) -> 'TargetConfig':
        """
        Construct a TargetConfig from a dictionary of values.

        Parameters
        ----------
        obj : dict
            Dictionary of values

        Returns
        -------
        :class:`TargetConfig`

        """
        out = cls(
            database_name=obj.get('databaseName'),
            resume_target=obj.get('resumeTarget'),
            target_id=obj.get('targetID'),
            target_type=TargetType.from_str(obj.get('targetType')),
        )

        return out

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)


class Job(object): # TODO: Check which fields are optional
    """
    Scheduled Notebook Job definition.

    This object is not directly instantiated. It is used in results
    of API calls on the :class:`JobsManager`. See :meth:`JobsManager.run`.
    """

    job_id: str
    name: str
    description: str
    project_id: str
    created_at: datetime.datetime
    enqueued_by: str
    completed_executions_count: int
    terminated_at: datetime.datetime
    job_metadata: List[ExecutionMetadata]
    execution_config: ExecutionConfig
    schedule: Schedule
    target_config: Optional[TargetConfig]

    def __init__(
        self,
        job_id: str,
        name: str,
        description: str,
        project_id: str,
        created_at: datetime.datetime,
        enqueued_by: str,
        completed_executions_count: int,
        terminated_at: datetime.datetime,
        job_metadata: List[ExecutionMetadata],
        execution_config: ExecutionConfig,
        schedule: Schedule,
        target_config: Optional[TargetConfig],
    ):
        self.job_id = job_id
        self.name = name
        self.description = description
        self.project_id = project_id
        self.created_at = created_at
        self.enqueued_by = enqueued_by
        self.completed_executions_count = completed_executions_count
        self.terminated_at = terminated_at
        self.job_metadata = job_metadata
        self.execution_config = execution_config
        self.schedule = schedule
        self.target_config = target_config

    @classmethod
    def from_dict(cls, obj: Dict[str, Any]) -> 'Job':
        """
        Construct a Job from a dictionary of values.

        Parameters
        ----------
        obj : dict
            Dictionary of values

        Returns
        -------
        :class:`Job`

        """

        out = cls(
            job_id=obj['jobID'],
            name=obj['name'],
            description=obj['description'],
            project_id=obj['projectID'],
            created_at=to_datetime_strict(obj['createdAt']),
            enqueued_by=obj['enqueuedBy'],
            completed_executions_count=int(obj['completedExecutionsCount']),
            terminated_at=to_datetime_strict(obj['terminatedAt']),
            job_metadata=[ExecutionMetadata.from_dict(x) for x in obj['jobMetadata']],
            execution_config=ExecutionConfig.from_dict(obj['executionConfig']),
            schedule=Schedule.from_dict(obj['schedule']),
            target_config=TargetConfig.from_dict(obj['targetConfig']) if 'targetConfig' in obj else None,
        )

        return out

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)


class JobsManager(object):
    """
    TODO: Add more details about the job manager
    """

    def __init__(self, manager: Optional[Manager]):
        self._manager = manager

    def run(
        self,
        notebook_path: str,
        pool_name: Optional[str] = None,
    ) -> Job:
        """Creates and returns the created scheduled notebook job."""
        if self._manager is None:
            raise ManagementError(msg='JobsManager not initialized')

        target_config = dict(
            resumeTarget=False,
        ) # type: Dict[str, Any]

        workspace_id = get_workspace_id()
        cluster_id = get_cluster_id()
        if workspace_id is not None:
            target_config['targetID'] = workspace_id
            if is_virtual_workspace():
                target_config['targetType'] = TargetType.VIRTUAL_WORKSPACE.value
            else:
                target_config['targetType'] = TargetType.WORKSPACE.value

        elif cluster_id is not None:
            target_config['targetID'] = cluster_id
            target_config['targetType'] = TargetType.CLUSTER.value

        database_name = get_database_name()
        if database_name is not None and database_name != '':
            target_config['databaseName'] = database_name

        job_run_json = dict(
            executionConfig=dict(
                createSnapshot=False,
                notebookPath=notebook_path,
            ),
            schedule=dict(
                mode='Once',
                startAt=from_datetime(datetime.datetime.now()),
            ),
            targetConfig=target_config,
        )  # type: Dict[str, Any]

        if pool_name is not None:
            job_run_json['poolName'] = pool_name

        res = self._manager._post('jobs', json=job_run_json).json()
        print(res)
        return Job.from_dict(res)

    def wait(self, jobs: List[Union[str, Job]]) -> None:
        for job in jobs:
            self.wait_for_job(job)

    def wait_for_job(self, job: Union[str, Job]) -> None:
        if isinstance(job, str):
            job_id = job
        else:
            job_id = job.job_id

        while True:
            res = self._manager._get(f'jobs/{job_id}').json()
            job = Job.from_dict(res)
            if job.schedule.mode == 'Once' and job.completed_executions_count > 0:
                return
            if job.schedule.mode == 'Recurring':
                raise ValueError('Cannot wait for recurring job')
            threading.sleep(1)