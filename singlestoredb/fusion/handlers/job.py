#!/usr/bin/env python3
from typing import Any
from typing import Dict
from typing import Optional

import singlestoredb as s2
from .. import result
from ...management.utils import to_datetime
from ..handler import SQLHandler
from ..result import FusionSQLResult
from .utils import dt_isoformat
from singlestoredb.management.job import Mode


class ScheduleJobHandler(SQLHandler):
    """
    SCHEDULE JOB USING NOTEBOOK notebook_path
        with_mode
        [ create_snapshot ]
        [ with_runtime ]
        [ with_name ]
        [ with_description ]
        [ execute_every ]
        [ start_at ]
        [ resume_target ]
    ;

    # Path to notebook file
    notebook_path = '<notebook-path>'

    # Mode to use (either Once or Recurring)
    with_mode = WITH MODE '<mode>'

    # Create snapshot
    create_snapshot = CREATE SNAPSHOT

    # Runtime to use
    with_runtime = WITH RUNTIME '<runtime-name>'

    # Name of the job
    with_name = WITH NAME '<job-name>'

    # Description of the job
    with_description = WITH DESCRIPTION '<job-description>'

    # Execution interval in minutes
    execute_every = EXECUTE EVERY <integer>

    # Start time
    start_at = START AT '<year>-<month>-<day> <hour>:<min>:<sec>'

    # Resume target if suspended
    resume_target = RESUME TARGET

    Description
    -----------
    Creates a scheduled notebook job.

    Arguments
    ---------
    * ``<notebook-path>``: The path in the Stage where the notebook file is
      stored.
    * ``<mode>``: The mode of the job. Either **Once** or **Recurring**.
    * ``<runtime-name>``: The name of the runtime the job will be run with.
    * ``<job-name>``: The name of the job.
    * ``<job-description>``: The description of the job.
    * ``<integer>``: The interval in minutes at which the job will be executed.
    * ``<year>-<month>-<day> <hour>:<min>:<sec>``: The start date and time of the
      job in UTC. The format is **yyyy-MM-dd HH:mm:ss**. The hour is in 24-hour format.

    Remarks
    -------
    * The ``WITH MODE`` clause specifies the mode of the job and is either
      **Once** or **Recurring**.
    * The ``EXECUTE EVERY`` clause specifies the interval in minutes at which the
      job will be executed and is required if the mode is **Recurring**.
    * The ``CREATE SNAPSHOT`` clause creates a snapshot of the notebook executed by
      the job.
    * The ``WITH RUNTIME`` clause specifies the name of the runtime that
      the job will be run with.
    * The ``RESUME TARGET`` clause resumes the job's target if it is suspended.

    Example
    -------
    The following command creates a job that will run the content of notebook
    **example_notebook.ipynb** every 5 minutes starting at **2024-06-25 21:35:06**
    using the runtime **notebooks-cpu-small**. The job's target will be resumed if it
    is suspended, a snapshot of the notebook will be created and the job is named
    **example_job** with the description **This is an example job**::

        SCHEDULE JOB USING NOTEBOOK 'example_notebook.ipynb'
            WITH MODE 'Recurring'
            CREATE SNAPSHOT
            WITH RUNTIME 'notebooks-cpu-small'
            WITH NAME 'example_job'
            WITH DESCRIPTION 'This is an example job'
            EXECUTE EVERY 5
            START AT '2024-06-25 21:35:06'
            RESUME TARGET;
    """

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        res = FusionSQLResult()
        res.add_field('JobID', result.STRING)

        jobs_manager = s2.manage_workspaces(base_url='http://apisvc.default.svc.cluster.local:8080').organizations.current.jobs

        job = jobs_manager.schedule(
            notebook_path=params['notebook_path'],
            mode=Mode.from_str(params['with_mode']),
            runtime_name=params['with_runtime'],
            create_snapshot=params['create_snapshot'],
            name=params['with_name'],
            description=params['with_description'],
            execution_interval_in_minutes=params['execute_every'],
            start_at=to_datetime(params.get('start_at')),
            resume_target=params['resume_target'],
        )
        res.set_rows([(job.job_id,)])

        return res


ScheduleJobHandler.register(overwrite=True)


class RunJobHandler(SQLHandler):
    """
    RUN JOB USING NOTEBOOK notebook_path
        [ with_runtime ]
    ;

    # Path to notebook file
    notebook_path = '<notebook-path>'

    # Runtime to use
    with_runtime = WITH RUNTIME '<runtime-name>'

    Description
    -----------
    Creates a scheduled notebook job that runs once immediately on the specified runtime.

    Arguments
    ---------
    * ``<notebook-path>``: The path in the Stage where the notebook file is stored.
    * ``<runtime-name>``: The name of the runtime the job will be run with.

    Remarks
    -------
    * The job is run immediately after the command is executed.
    * The ``WITH RUNTIME`` clause specifies the name of the runtime that
      the job will be run with.

    Example
    -------
    The following command creates a job that will run the content of notebook
    **example_notebook.ipynb** using the runtime **notebooks-cpu-small** immediately::

        RUN JOB USING NOTEBOOK 'example_notebook.ipynb'
           WITH RUNTIME 'notebooks-cpu-small';

    """

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        res = FusionSQLResult()
        res.add_field('JobID', result.STRING)

        jobs_manager = s2.manage_workspaces(base_url='http://apisvc.default.svc.cluster.local:8080').organizations.current.jobs

        job = jobs_manager.run(
            params['notebook_path'],
            runtime_name=params['with_runtime'],
        )
        res.set_rows([(job.job_id,)])

        return res


RunJobHandler.register(overwrite=True)


class WaitOnJobsHandler(SQLHandler):
    """
    WAIT ON JOBS job_ids
        [ with_timeout ]
    ;

    # Job IDs to wait on
    job_ids = '<job-id>',...

    # Timeout in seconds
    with_timeout = WITH TIMEOUT <integer>

    Description
    -----------
    Waits for the jobs with the specified IDs to complete.

    Arguments
    ---------
    * ``<job-id>``: A list of the IDs of the job to wait on.
    * ``<integer>``: The number of seconds to wait for the jobs to complete.

    Remarks
    -------
    * The ``WITH TIMEOUT`` clause specifies the number of seconds to wait for the jobs
      to complete. If the jobs have not completed after the specified number of seconds,
      the command will return.

    Example
    -------
    The following command waits for the jobs with IDs **job1** and **job2** to complete
    with a timeout of 60 seconds::

        WAIT ON JOBS 'job1', 'job2' WITH TIMEOUT 60;

    """

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        res = FusionSQLResult()
        res.add_field('Success', result.BOOL)

        jobs_manager = s2.manage_workspaces(base_url='http://apisvc.default.svc.cluster.local:8080').organizations.current.jobs

        success = jobs_manager.wait(
            params['job_ids'],
            timeout=params['with_timeout'],
        )
        res.set_rows([(success,)])

        return res


WaitOnJobsHandler.register(overwrite=True)


class ShowJobsHandler(SQLHandler):
    """
    SHOW JOBS job_ids
        [ <extended> ]
        [ <like> ]
    ;

    # Job IDs to show
    job_ids = '<job-id>',...

    Description
    -----------
    Shows the jobs with the specified IDs.

    Arguments
    ---------
    * ``<job-id>``: A list of the IDs of the jobs to show.
    * ``<pattern>``: A pattern similar to SQL LIKE clause.
      Uses ``%`` as the wildcard character.

    Remarks
    -------
    * Use the ``LIKE`` clause to specify a pattern and return only
      the jobs that match the specified pattern.
    * To return more information about the jobs, use the
      ``EXTENDED`` clause.

    Example
    -------
    The following command shows extended information on the jobs with IDs
    **job1** and **job2** and that match the pattern **example_job_name**::

        SHOW JOBS 'job1', 'job2'
          EXTENDED
          LIKE 'example_job_name';

    """

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        res = FusionSQLResult()
        res.add_field('JobID', result.STRING)
        res.add_field('Name', result.STRING)
        res.add_field('CreatedAt', result.DATETIME)
        res.add_field('EnqueuedBy', result.STRING)
        res.add_field('CompletedExecutions', result.INTEGER)
        res.add_field('NotebookPath', result.STRING)
        res.add_field('DatabaseName', result.STRING)
        res.add_field('TargetID', result.STRING)
        res.add_field('TargetType', result.STRING)

        jobs_manager = s2.manage_workspaces(base_url='http://apisvc.default.svc.cluster.local:8080').organizations.current.jobs

        jobs = []
        for job_id in params['job_ids']:
            jobs.append(jobs_manager.get(job_id))

        if params['extended']:
            res.add_field('Description', result.STRING)
            res.add_field('TerminatedAt', result.DATETIME)
            res.add_field('CreateSnapshot', result.BOOL)
            res.add_field('MaxDurationInMins', result.INTEGER)
            res.add_field('ExecutionIntervalInMins', result.INTEGER)
            res.add_field('Mode', result.STRING)
            res.add_field('StartAt', result.DATETIME)
            res.add_field('ResumeTarget', result.BOOL)

            def fields(job: Any) -> Any:
                database_name = None
                resume_target = None
                target_id = None
                target_type = None
                if job.target_config is not None:
                    database_name = job.target_config.database_name
                    resume_target = job.target_config.resume_target
                    target_id = job.target_config.target_id
                    target_type = job.target_config.target_type.value
                return (
                    job.job_id,
                    job.name,
                    dt_isoformat(job.created_at),
                    job.enqueued_by,
                    job.completed_executions_count,
                    job.execution_config.notebook_path,
                    database_name,
                    target_id,
                    target_type,
                    job.description,
                    dt_isoformat(job.terminated_at),
                    job.execution_config.create_snapshot,
                    job.execution_config.max_duration_in_mins,
                    job.schedule.execution_interval_in_minutes,
                    job.schedule.mode.value,
                    dt_isoformat(job.schedule.start_at),
                    resume_target,
                )
        else:
            def fields(job: Any) -> Any:
                database_name = None
                target_id = None
                target_type = None
                if job.target_config is not None:
                    database_name = job.target_config.database_name
                    target_id = job.target_config.target_id
                    target_type = job.target_config.target_type.value
                return (
                    job.job_id,
                    job.name,
                    dt_isoformat(job.created_at),
                    job.enqueued_by,
                    job.completed_executions_count,
                    job.execution_config.notebook_path,
                    database_name,
                    target_id,
                    target_type,
                )

        res.set_rows([fields(job) for job in jobs])

        if params['like']:
            res = res.like(Name=params['like'])

        return res


ShowJobsHandler.register(overwrite=True)


class ShowJobExecutionsHandler(SQLHandler):
    """
    SHOW JOB EXECUTIONS job_id
        from_start
        to_end;

    # Job ID to show executions for
    job_id = '<job-id>'

    # From start execution number
    from_start = FROM <integer>

    # To end execution number
    to_end = TO <integer>

    Description
    -----------
    Shows the executions for the job with the specified ID within the specified range.

    Arguments
    ---------
    * ``<job-id>``: The ID of the job to show executions for.
    * ``<integer>``: The execution number to start from or end at.

    Example
    -------
    The following command shows information on the executions for the job
    with ID **job1**, from execution number **1** to **10**::

        SHOW JOB EXECUTIONS 'job1'
          FROM 1 TO 10;

    """

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        res = FusionSQLResult()
        res.add_field('ExecutionID', result.STRING)
        res.add_field('ExecutionNumber', result.INTEGER)
        res.add_field('JobID', result.STRING)
        res.add_field('Status', result.STRING)
        res.add_field('SnapshotNotebookPath', result.STRING)
        res.add_field('ScheduledStartTime', result.DATETIME)
        res.add_field('StartedAt', result.DATETIME)
        res.add_field('FinishedAt', result.DATETIME)

        jobs_manager = s2.manage_workspaces(base_url='http://apisvc.default.svc.cluster.local:8080').organizations.current.jobs

        executionsData = jobs_manager.get_executions(
              params['job_id'],
              params['from_start'],
              params['to_end'],
        )

        def fields(execution: Any) -> Any:
            return (
              execution.execution_id,
              execution.execution_number,
              execution.job_id,
              execution.status.value,
              execution.snapshot_notebook_path,
              dt_isoformat(execution.scheduled_start_time),
              dt_isoformat(execution.started_at),
              dt_isoformat(execution.finished_at),
            )

        res.set_rows([fields(execution) for execution in executionsData.executions])

        return res


ShowJobExecutionsHandler.register(overwrite=True)
