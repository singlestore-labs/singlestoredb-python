#!/usr/bin/env python
"""SingleStoreDB Cloud Scheduled Notebook Job."""
import datetime
import time
from enum import Enum
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Type
from typing import Union

from ..exceptions import ManagementError
from .manager import Manager
from .utils import camel_to_snake
from .utils import from_datetime
from .utils import get_cluster_id
from .utils import get_database_name
from .utils import get_virtual_workspace_id
from .utils import get_workspace_id
from .utils import to_datetime
from .utils import to_datetime_strict
from .utils import vars_to_str


type_to_parameter_conversion_map = {
    str: 'string',
    int: 'integer',
    float: 'float',
    bool: 'boolean',
}


class Mode(Enum):
    ONCE = 'Once'
    RECURRING = 'Recurring'

    @classmethod
    def from_str(cls, s: str) -> 'Mode':
        try:
            return cls[str(camel_to_snake(s)).upper()]
        except KeyError:
            raise ValueError(f'Unknown Mode: {s}')

    def __str__(self) -> str:
        """Return string representation."""
        return self.value

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)


class TargetType(Enum):
    WORKSPACE = 'Workspace'
    CLUSTER = 'Cluster'
    VIRTUAL_WORKSPACE = 'VirtualWorkspace'

    @classmethod
    def from_str(cls, s: str) -> 'TargetType':
        try:
            return cls[str(camel_to_snake(s)).upper()]
        except KeyError:
            raise ValueError(f'Unknown TargetType: {s}')

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


class Parameter(object):

    name: str
    value: str
    type: str

    def __init__(
        self,
        name: str,
        value: str,
        type: str,
    ):
        self.name = name
        self.value = value
        self.type = type

    @classmethod
    def from_dict(cls, obj: Dict[str, Any]) -> 'Parameter':
        """
        Construct a Parameter from a dictionary of values.

        Parameters
        ----------
        obj : dict
            Dictionary of values

        Returns
        -------
        :class:`Parameter`

        """
        out = cls(
            name=obj['name'],
            value=obj['value'],
            type=obj['type'],
        )

        return out

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)


class Runtime(object):

    name: str
    description: str

    def __init__(
        self,
        name: str,
        description: str,
    ):
        self.name = name
        self.description = description

    @classmethod
    def from_dict(cls, obj: Dict[str, Any]) -> 'Runtime':
        """
        Construct a Runtime from a dictionary of values.

        Parameters
        ----------
        obj : dict
            Dictionary of values

        Returns
        -------
        :class:`Runtime`

        """
        out = cls(
            name=obj['name'],
            description=obj['description'],
        )

        return out

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)


class JobMetadata(object):

    avg_duration_in_seconds: Optional[float]
    count: int
    max_duration_in_seconds: Optional[float]
    status: Status

    def __init__(
        self,
        avg_duration_in_seconds: Optional[float],
        count: int,
        max_duration_in_seconds: Optional[float],
        status: Status,
    ):
        self.avg_duration_in_seconds = avg_duration_in_seconds
        self.count = count
        self.max_duration_in_seconds = max_duration_in_seconds
        self.status = status

    @classmethod
    def from_dict(cls, obj: Dict[str, Any]) -> 'JobMetadata':
        """
        Construct a JobMetadata from a dictionary of values.

        Parameters
        ----------
        obj : dict
            Dictionary of values

        Returns
        -------
        :class:`JobMetadata`

        """
        out = cls(
            avg_duration_in_seconds=obj.get('avgDurationInSeconds'),
            count=obj['count'],
            max_duration_in_seconds=obj.get('maxDurationInSeconds'),
            status=Status.from_str(obj['status']),
        )

        return out

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)


class ExecutionMetadata(object):

    start_execution_number: int
    end_execution_number: int

    def __init__(
        self,
        start_execution_number: int,
        end_execution_number: int,
    ):
        self.start_execution_number = start_execution_number
        self.end_execution_number = end_execution_number

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
            start_execution_number=obj['startExecutionNumber'],
            end_execution_number=obj['endExecutionNumber'],
        )

        return out

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)


class Execution(object):

    execution_id: str
    job_id: str
    status: Status
    snapshot_notebook_path: Optional[str]
    scheduled_start_time: datetime.datetime
    started_at: Optional[datetime.datetime]
    finished_at: Optional[datetime.datetime]
    execution_number: int

    def __init__(
        self,
        execution_id: str,
        job_id: str,
        status: Status,
        scheduled_start_time: datetime.datetime,
        started_at: Optional[datetime.datetime],
        finished_at: Optional[datetime.datetime],
        execution_number: int,
        snapshot_notebook_path: Optional[str],
    ):
        self.execution_id = execution_id
        self.job_id = job_id
        self.status = status
        self.scheduled_start_time = scheduled_start_time
        self.started_at = started_at
        self.finished_at = finished_at
        self.execution_number = execution_number
        self.snapshot_notebook_path = snapshot_notebook_path

    @classmethod
    def from_dict(cls, obj: Dict[str, Any]) -> 'Execution':
        """
        Construct an Execution from a dictionary of values.

        Parameters
        ----------
        obj : dict
            Dictionary of values

        Returns
        -------
        :class:`Execution`

        """
        out = cls(
            execution_id=obj['executionID'],
            job_id=obj['jobID'],
            status=Status.from_str(obj['status']),
            snapshot_notebook_path=obj.get('snapshotNotebookPath'),
            scheduled_start_time=to_datetime_strict(obj['scheduledStartTime']),
            started_at=to_datetime(obj.get('startedAt')),
            finished_at=to_datetime(obj.get('finishedAt')),
            execution_number=obj['executionNumber'],
        )

        return out

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)


class ExecutionsData(object):

    executions: List[Execution]
    metadata: ExecutionMetadata

    def __init__(
        self,
        executions: List[Execution],
        metadata: ExecutionMetadata,
    ):
        self.executions = executions
        self.metadata = metadata

    @classmethod
    def from_dict(cls, obj: Dict[str, Any]) -> 'ExecutionsData':
        """
        Construct an ExecutionsData from a dictionary of values.

        Parameters
        ----------
        obj : dict
            Dictionary of values

        Returns
        -------
        :class:`ExecutionsData`

        """
        out = cls(
            executions=[Execution.from_dict(x) for x in obj['executions']],
            metadata=ExecutionMetadata.from_dict(obj['executionsMetadata']),
        )

        return out

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)


class ExecutionConfig(object):

    create_snapshot: bool
    max_duration_in_mins: int
    notebook_path: str

    def __init__(
        self,
        create_snapshot: bool,
        max_duration_in_mins: int,
        notebook_path: str,
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
            create_snapshot=obj['createSnapshot'],
            max_duration_in_mins=obj['maxAllowedExecutionDurationInMinutes'],
            notebook_path=obj['notebookPath'],
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
    mode: Mode
    start_at: Optional[datetime.datetime]

    def __init__(
        self,
        execution_interval_in_minutes: Optional[int],
        mode: Mode,
        start_at: Optional[datetime.datetime],
    ):
        self.execution_interval_in_minutes = execution_interval_in_minutes
        self.mode = mode
        self.start_at = start_at

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
            mode=Mode.from_str(obj['mode']),
            start_at=to_datetime(obj.get('startAt')),
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
    resume_target: bool
    target_id: str
    target_type: TargetType

    def __init__(
        self,
        database_name: Optional[str],
        resume_target: bool,
        target_id: str,
        target_type: TargetType,
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
            resume_target=obj['resumeTarget'],
            target_id=obj['targetID'],
            target_type=TargetType.from_str(obj['targetType']),
        )

        return out

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)


class Job(object):
    """
    Scheduled Notebook Job definition.

    This object is not directly instantiated. It is used in results
    of API calls on the :class:`JobsManager`. See :meth:`JobsManager.run`.
    """

    completed_executions_count: int
    created_at: datetime.datetime
    description: Optional[str]
    enqueued_by: str
    execution_config: ExecutionConfig
    job_id: str
    job_metadata: List[JobMetadata]
    name: Optional[str]
    schedule: Schedule
    target_config: Optional[TargetConfig]
    terminated_at: Optional[datetime.datetime]

    def __init__(
        self,
        completed_executions_count: int,
        created_at: datetime.datetime,
        description: Optional[str],
        enqueued_by: str,
        execution_config: ExecutionConfig,
        job_id: str,
        job_metadata: List[JobMetadata],
        name: Optional[str],
        schedule: Schedule,
        target_config: Optional[TargetConfig],
        terminated_at: Optional[datetime.datetime],
    ):
        self.completed_executions_count = completed_executions_count
        self.created_at = created_at
        self.description = description
        self.enqueued_by = enqueued_by
        self.execution_config = execution_config
        self.job_id = job_id
        self.job_metadata = job_metadata
        self.name = name
        self.schedule = schedule
        self.target_config = target_config
        self.terminated_at = terminated_at
        self._manager: Optional[JobsManager] = None

    @classmethod
    def from_dict(cls, obj: Dict[str, Any], manager: 'JobsManager') -> 'Job':
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
        target_config = obj.get('targetConfig')
        if target_config is not None:
            target_config = TargetConfig.from_dict(target_config)

        out = cls(
            completed_executions_count=obj['completedExecutionsCount'],
            created_at=to_datetime_strict(obj['createdAt']),
            description=obj.get('description'),
            enqueued_by=obj['enqueuedBy'],
            execution_config=ExecutionConfig.from_dict(obj['executionConfig']),
            job_id=obj['jobID'],
            job_metadata=[JobMetadata.from_dict(x) for x in obj['jobMetadata']],
            name=obj.get('name'),
            schedule=Schedule.from_dict(obj['schedule']),
            target_config=target_config,
            terminated_at=to_datetime(obj.get('terminatedAt')),
        )
        out._manager = manager
        return out

    def wait(self, timeout: Optional[int] = None) -> bool:
        """Wait for the job to complete."""
        if self._manager is None:
            raise ManagementError(msg='Job not initialized with JobsManager')
        return self._manager._wait_for_job(self, timeout)

    def get_executions(
            self,
            start_execution_number: int,
            end_execution_number: int,
    ) -> ExecutionsData:
        """Get executions for the job."""
        if self._manager is None:
            raise ManagementError(msg='Job not initialized with JobsManager')
        return self._manager.get_executions(
            self.job_id,
            start_execution_number,
            end_execution_number,
        )

    def get_parameters(self) -> List[Parameter]:
        """Get parameters for the job."""
        if self._manager is None:
            raise ManagementError(msg='Job not initialized with JobsManager')
        return self._manager.get_parameters(self.job_id)

    def delete(self) -> bool:
        """Delete the job."""
        if self._manager is None:
            raise ManagementError(msg='Job not initialized with JobsManager')
        return self._manager.delete(self.job_id)

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)


class JobsManager(object):
    """
    SingleStoreDB scheduled notebook jobs manager.

    This class should be instantiated using :attr:`Organization.jobs`.

    Parameters
    ----------
    manager : WorkspaceManager, optional
        The WorkspaceManager the JobsManager belongs to

    See Also
    --------
    :attr:`Organization.jobs`
    """

    def __init__(self, manager: Optional[Manager]):
        self._manager = manager

    def schedule(
        self,
        notebook_path: str,
        mode: Mode,
        create_snapshot: bool,
        name: Optional[str] = None,
        description: Optional[str] = None,
        execution_interval_in_minutes: Optional[int] = None,
        start_at: Optional[datetime.datetime] = None,
        runtime_name: Optional[str] = None,
        resume_target: Optional[bool] = None,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> Job:
        """Creates and returns a scheduled notebook job."""
        if self._manager is None:
            raise ManagementError(msg='JobsManager not initialized')

        schedule = dict(
            mode=mode.value,
        )  # type: Dict[str, Any]

        if start_at is not None:
            schedule['startAt'] = from_datetime(start_at)

        if execution_interval_in_minutes is not None:
            schedule['executionIntervalInMinutes'] = execution_interval_in_minutes

        execution_config = dict(
            createSnapshot=create_snapshot,
            notebookPath=notebook_path,
        )  # type: Dict[str, Any]

        if runtime_name is not None:
            execution_config['runtimeName'] = runtime_name

        target_config = None  # type: Optional[Dict[str, Any]]
        database_name = get_database_name()
        if database_name is not None:
            target_config = dict(
                databaseName=database_name,
            )

            if resume_target is not None:
                target_config['resumeTarget'] = resume_target

            workspace_id = get_workspace_id()
            virtual_workspace_id = get_virtual_workspace_id()
            cluster_id = get_cluster_id()
            if virtual_workspace_id is not None:
                target_config['targetID'] = virtual_workspace_id
                target_config['targetType'] = TargetType.VIRTUAL_WORKSPACE.value

            elif workspace_id is not None:
                target_config['targetID'] = workspace_id
                target_config['targetType'] = TargetType.WORKSPACE.value

            elif cluster_id is not None:
                target_config['targetID'] = cluster_id
                target_config['targetType'] = TargetType.CLUSTER.value

        job_run_json = dict(
            schedule=schedule,
            executionConfig=execution_config,
        )  # type: Dict[str, Any]

        if target_config is not None:
            job_run_json['targetConfig'] = target_config

        if name is not None:
            job_run_json['name'] = name

        if description is not None:
            job_run_json['description'] = description

        if parameters is not None:
            job_run_json['parameters'] = [
                dict(
                    name=k,
                    value=str(parameters[k]),
                    type=type_to_parameter_conversion_map[type(parameters[k])],
                ) for k in parameters
            ]

        res = self._manager._post('jobs', json=job_run_json).json()
        return Job.from_dict(res, self)

    def run(
        self,
        notebook_path: str,
        runtime_name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> Job:
        """Creates and returns a scheduled notebook job that runs once immediately."""
        return self.schedule(
            notebook_path,
            Mode.ONCE,
            False,
            start_at=datetime.datetime.now(),
            runtime_name=runtime_name,
            parameters=parameters,
        )

    def wait(self, jobs: List[Union[str, Job]], timeout: Optional[int] = None) -> bool:
        """Wait for jobs to finish executing."""
        if timeout is not None:
            if timeout <= 0:
                return False
            finish_time = datetime.datetime.now() + datetime.timedelta(seconds=timeout)

        for job in jobs:
            if timeout is not None:
                job_timeout = int((finish_time - datetime.datetime.now()).total_seconds())
            else:
                job_timeout = None

            res = self._wait_for_job(job, job_timeout)
            if not res:
                return False

        return True

    def _wait_for_job(self, job: Union[str, Job], timeout: Optional[int] = None) -> bool:
        if self._manager is None:
            raise ManagementError(msg='JobsManager not initialized')

        if timeout is not None:
            if timeout <= 0:
                return False
            finish_time = datetime.datetime.now() + datetime.timedelta(seconds=timeout)

        if isinstance(job, str):
            job_id = job
        else:
            job_id = job.job_id

        while True:
            if timeout is not None and datetime.datetime.now() >= finish_time:
                return False

            res = self._manager._get(f'jobs/{job_id}').json()
            job = Job.from_dict(res, self)
            if job.schedule.mode == Mode.ONCE and job.completed_executions_count > 0:
                return True
            if job.schedule.mode == Mode.RECURRING:
                raise ValueError(f'Cannot wait for recurring job {job_id}')
            time.sleep(5)

    def get(self, job_id: str) -> Job:
        """Get a job by its ID."""
        if self._manager is None:
            raise ManagementError(msg='JobsManager not initialized')

        res = self._manager._get(f'jobs/{job_id}').json()
        return Job.from_dict(res, self)

    def get_executions(
            self,
            job_id: str,
            start_execution_number: int,
            end_execution_number: int,
    ) -> ExecutionsData:
        """Get executions for a job by its ID."""
        if self._manager is None:
            raise ManagementError(msg='JobsManager not initialized')
        path = (
            f'jobs/{job_id}/executions'
            f'?start={start_execution_number}'
            f'&end={end_execution_number}'
        )
        res = self._manager._get(path).json()
        return ExecutionsData.from_dict(res)

    def get_parameters(self, job_id: str) -> List[Parameter]:
        """Get parameters for a job by its ID."""
        if self._manager is None:
            raise ManagementError(msg='JobsManager not initialized')

        res = self._manager._get(f'jobs/{job_id}/parameters').json()
        return [Parameter.from_dict(p) for p in res]

    def delete(self, job_id: str) -> bool:
        """Delete a job by its ID."""
        if self._manager is None:
            raise ManagementError(msg='JobsManager not initialized')

        return self._manager._delete(f'jobs/{job_id}').json()

    def modes(self) -> Type[Mode]:
        """Get all possible job scheduling modes."""
        return Mode

    def runtimes(self) -> List[Runtime]:
        """Get all available job runtimes."""
        if self._manager is None:
            raise ManagementError(msg='JobsManager not initialized')

        res = self._manager._get('jobs/runtimes').json()
        return [Runtime.from_dict(r) for r in res]
