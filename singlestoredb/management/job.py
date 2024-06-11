#!/usr/bin/env python
"""SingleStoreDB Cloud Scheduled Notebook Job."""
import datetime
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from enum import Enum

from .utils import to_datetime
from .manager import Manager



class TargetType(Enum):
    WORKSPACE = "Workspace"
    CLUSTER = "Cluster"
    VIRTUAL_WORKSPACE = "VirtualWorkspace"


class Status(Enum):
    UNKNOWN = "Unknown"
    SCHEDULED = "Scheduled"
    RUNNING = "Running"
    COMPLETED = "Completed"
    FAILED = "Failed"
    ERROR = "Error"
    CANCELED = "Canceled"


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
            status=obj['status'],
            count=int(obj['count']),
        )

        return out


class ExecutionConfig(object):

    create_snapshot: Optional[bool]
    max_allowed_execution_duration_in_minutes: Optional[int]
    notebook_path: Optional[str]

    def __init__(
        self,
        create_snapshot: Optional[bool],
        max_allowed_execution_duration_in_minutes: Optional[int],
        notebook_path: Optional[str],
    ):
        self.create_snapshot = create_snapshot
        self.max_allowed_execution_duration_in_minutes = max_allowed_execution_duration_in_minutes
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
            create_snapshot=bool(obj.get('createSnapshot')),
            max_allowed_execution_duration_in_minutes=int(obj.get('maxAllowedExecutionDurationInMinutes')),
            notebook_path=obj.get('notebookPath'),
        )

        return out


class Schedule(object):

    execution_interval_in_minutes: int
    start_at: datetime.datetime
    mode: Optional[str]

    def __init__(
        self,
        execution_interval_in_minutes: int,
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
            execution_interval_in_minutes=int(obj['executionIntervalInMinutes']),
            start_at=to_datetime(obj['startAt']),
            mode=obj.get('mode'),
        )

        return out


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
            target_type: Optional[TargetType]
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
                resume_target=bool(obj.get('resumeTarget')),
                target_id=obj.get('targetID'),
                target_type=obj.get('targetType'),
            )

            return out


class Job(object):
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
    job_metadata : List[ExecutionMetadata]
    executionConfig: ExecutionConfig
    schedule: Schedule
    targetConfig: Optional[TargetConfig]

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
        executionConfig: ExecutionConfig,
        schedule: Schedule,
        targetConfig: Optional[TargetConfig],
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
        self.executionConfig = executionConfig
        self.schedule = schedule
        self.targetConfig = targetConfig

    @classmethod
    def from_dict(cls, obj: Dict[str, Any], manager: Manager) -> 'Job':
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
            created_at=to_datetime(obj['createdAt']),
            enqueued_by=obj['enqueuedBy'],
            completed_executions_count=int(obj['completedExecutionsCount']),
            terminated_at=to_datetime(obj['terminatedAt']),
            job_metadata=[ExecutionMetadata.from_dict(x) for x in obj['jobMetadata']],
            executionConfig=ExecutionConfig.from_dict(obj['executionConfig']),
            schedule=Schedule.from_dict(obj['schedule']),
            targetConfig=TargetConfig.from_dict(obj.get('targetConfig'))
        )

        return out


class JobsManager(object):
    """
    TODO: Add more details about the job manager
    """

    def __init__(self, _manager):
        self._manager = _manager
        
    def run(
        self, 
        notebook_path: str,
        execution_interval_in_minutes: int,
        create_snapshot: bool,
        resume_target: bool,
        project_id: str,
        name: Optional[str] = None,
        description: Optional[str] = None,
        target_id: Optional[str] = None,
        target_type: Optional[TargetType] = None,
        database_name: Optional[str] = None,
        start_at: Optional[datetime.datetime] = None,
        pool_name: Optional[str] = None,
        ) -> Job:
        """Creates and returns the created scheduled notebook job."""
        # TODO: check for null values
        # TODO: check if I can pass project_id from org. Are target ID and target type also to be filled by me?
        res = self._post('jobs', json=dict(
            name = name,
            description = description,
            project_id = project_id,
            pool_name = pool_name, #still does not exist in the API
            executionConfig = dict(
                createSnapshot = create_snapshot,
                notebookPath = notebook_path
            ),
            schedule = dict(
                executionMode = "Once",
                executionIntervalInMinutes = execution_interval_in_minutes,
                startAt = start_at
            ),
            targetConfig = dict(
                databaseName = database_name,
                resumeTarget = resume_target,
                targetID = target_id,
                targetType = target_type
            )))
        return Job.from_dict(res, self)
    
    def wait(self):
        #TODO: Implement this
        pass