#!/usr/bin/env python3
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

from .. import result
from ...management.utils import to_datetime
from ..handler import SQLHandler
from ..result import FusionSQLResult
from .utils import dt_isoformat
from .utils import get_workspace_manager
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
        [ with_parameters ]
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

    # Execution interval
    execute_every = EXECUTE EVERY interval time_unit
    interval = <integer>
    time_unit = { MINUTES | HOURS | DAYS }

    # Start time
    start_at = START AT '<year>-<month>-<day> <hour>:<min>:<sec>'

    # Resume target if suspended
    resume_target = RESUME TARGET

    # Parameters to pass to the job
    with_parameters = WITH PARAMETERS <json>

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
    * ``<integer>``: The interval at which the job will be executed.
    * ``<year>-<month>-<day> <hour>:<min>:<sec>``: The start date and time of the
      job in UTC. The format is **yyyy-MM-dd HH:mm:ss**. The hour is in 24-hour format.
    * ``<json>``: The parameters to pass to the job. A JSON object with
      the following format: ``{"<paramName>": "<paramValue>", ...}``.

    Remarks
    -------
    * The ``WITH MODE`` clause specifies the mode of the job and is either
      **Once** or **Recurring**.
    * The ``EXECUTE EVERY`` clause specifies the interval at which the job will be
      executed. The interval can be in minutes, hours, or days. It is mandatory to
      specify the interval if the mode is **Recurring**.
    * The ``CREATE SNAPSHOT`` clause creates a snapshot of the notebook executed by
      the job.
    * The ``WITH RUNTIME`` clause specifies the name of the runtime that
      the job will be run with.
    * The ``RESUME TARGET`` clause resumes the job's target if it is suspended.
    * The ``WITH PARAMETERS`` clause specifies the parameters to pass to the job. The
      only supported parameter value types are strings, integers, floats, and booleans.

    Example
    -------
    The following command creates a job that will run the content of notebook
    **example_notebook.ipynb** every 5 minutes starting at **2024-06-25 21:35:06**
    using the runtime **notebooks-cpu-small**. The job's target will be resumed if it
    is suspended, a snapshot of the notebook will be created and the job is named
    **example_job** with the description **This is an example job**. The job will
    have the following parameters: **strParam** with value **"string"**, **intParam**
    with value **1**, **floatParam** with value **1.0**, and **boolParam** with value
    **true**::

        SCHEDULE JOB USING NOTEBOOK 'example_notebook.ipynb'
            WITH MODE 'Recurring'
            CREATE SNAPSHOT
            WITH RUNTIME 'notebooks-cpu-small'
            WITH NAME 'example_job'
            WITH DESCRIPTION 'This is an example job'
            EXECUTE EVERY 5 MINUTES
            START AT '2024-06-25 21:35:06'
            RESUME TARGET
            WITH PARAMETERS {
                              "strParam": "string",
                              "intParam": 1,
                              "floatParam": 1.0,
                              "boolParam": true
                            }
        ;
    """

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        res = FusionSQLResult()
        res.add_field('JobID', result.STRING)

        jobs_manager = get_workspace_manager().organizations.current.jobs

        parameters = None
        if params.get('with_parameters'):
            parameters = {}
            for name, value in params['with_parameters'].items():
                parameters[name] = value

        execution_interval_in_mins = None
        if params.get('execute_every'):
            execution_interval_in_mins = params['execute_every']['interval']
            time_unit = params['execute_every']['time_unit'].upper()
            if time_unit == 'MINUTES':
                pass
            elif time_unit == 'HOURS':
                execution_interval_in_mins *= 60
            elif time_unit == 'DAYS':
                execution_interval_in_mins *= 60 * 24
            else:
                raise ValueError(f'Invalid time unit: {time_unit}')

        job = jobs_manager.schedule(
            notebook_path=params['notebook_path'],
            mode=Mode.from_str(params['with_mode']),
            runtime_name=params['with_runtime'],
            create_snapshot=params['create_snapshot'],
            name=params['with_name'],
            description=params['with_description'],
            execution_interval_in_minutes=execution_interval_in_mins,
            start_at=to_datetime(params.get('start_at')),
            resume_target=params['resume_target'],
            parameters=parameters,
        )
        res.set_rows([(job.job_id,)])

        return res


ScheduleJobHandler.register(overwrite=True)


class RunJobHandler(SQLHandler):
    """
    RUN JOB USING NOTEBOOK notebook_path
        [ with_runtime ]
        [ with_parameters ]
    ;

    # Path to notebook file
    notebook_path = '<notebook-path>'

    # Runtime to use
    with_runtime = WITH RUNTIME '<runtime-name>'

    # Parameters to pass to the job
    with_parameters = WITH PARAMETERS <json>

    Description
    -----------
    Creates a scheduled notebook job that runs once immediately on the specified runtime.

    Arguments
    ---------
    * ``<notebook-path>``: The path in the Stage where the notebook file is stored.
    * ``<runtime-name>``: The name of the runtime the job will be run with.
    * ``<json>``: The parameters to pass to the job. A JSON object with
      the following format: ``{"<paramName>": "<paramValue>", ...}``.

    Remarks
    -------
    * The job is run immediately after the command is executed.
    * The ``WITH RUNTIME`` clause specifies the name of the runtime that
      the job will be run with.
    * The ``WITH PARAMETERS`` clause specifies the parameters to pass to the job. The
      only supported parameter value types are strings, integers, floats, and booleans.

    Example
    -------
    The following command creates a job that will run the content of notebook
    **example_notebook.ipynb** using the runtime **notebooks-cpu-small** immediately.
    The job will have the following parameters: **strParam** with value **"string"**,
    **intParam** with value **1**, **floatParam** with value **1.0**, and **boolParam**
    with value **true**::

        RUN JOB USING NOTEBOOK 'example_notebook.ipynb'
           WITH RUNTIME 'notebooks-cpu-small'
           WITH PARAMETERS {
                              "strParam": "string",
                              "intParam": 1,
                              "floatParam": 1.0,
                              "boolParam": true
                            }
        ;

    """

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        res = FusionSQLResult()
        res.add_field('JobID', result.STRING)

        jobs_manager = get_workspace_manager().organizations.current.jobs

        parameters = None
        if params.get('with_parameters'):
            parameters = {}
            for name, value in params['with_parameters'].items():
                parameters[name] = value

        job = jobs_manager.run(
            params['notebook_path'],
            runtime_name=params['with_runtime'],
            parameters=parameters,
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

    # Timeout
    with_timeout = WITH TIMEOUT time time_unit
    time = <integer>
    time_unit = { SECONDS | MINUTES | HOURS }

    Description
    -----------
    Waits for the jobs with the specified IDs to complete.

    Arguments
    ---------
    * ``<job-id>``: A list of the IDs of the jobs to wait on.
    * ``<integer>``: The number of seconds to wait for the jobs to complete.

    Remarks
    -------
    * The ``WITH TIMEOUT`` clause specifies the time to wait for the jobs to complete.
      The time can be in seconds, minutes, or hours.

    Example
    -------
    The following command waits for the jobs with IDs **job1** and **job2** to complete
    with a timeout of 60 seconds::

        WAIT ON JOBS 'job1', 'job2' WITH TIMEOUT 60;

    """

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        res = FusionSQLResult()
        res.add_field('Success', result.BOOL)

        jobs_manager = get_workspace_manager().organizations.current.jobs

        timeout_in_secs = None
        if params.get('with_timeout'):
            timeout_in_secs = params['with_timeout']['time']
            time_unit = params['with_timeout']['time_unit'].upper()
            if time_unit == 'SECONDS':
                pass
            elif time_unit == 'MINUTES':
                timeout_in_secs *= 60
            elif time_unit == 'HOURS':
                timeout_in_secs *= 60 * 60
            else:
                raise ValueError(f'Invalid time unit: {time_unit}')

        success = jobs_manager.wait(
            params['job_ids'],
            timeout=timeout_in_secs,
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

        jobs_manager = get_workspace_manager().organizations.current.jobs

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
    SHOW JOB EXECUTIONS FOR job_id
        from_start
        to_end
        [ <extended> ];

    # ID of the job to show executions for
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

    Remarks
    -------
    * Use the ``FROM`` clause to specify the execution number to start from.
    * Use the ``TO`` clause to specify the execution number to end at.
    * To return more information about the executions, use the
      ``EXTENDED`` clause.

    Example
    -------
    The following command shows extended information on the executions for the job
    with ID **job1**, from execution number **1** to **10**::

        SHOW JOB EXECUTIONS FOR 'job1'
          FROM 1 TO 10
          EXTENDED;
    """

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        res = FusionSQLResult()
        res.add_field('ExecutionID', result.STRING)
        res.add_field('ExecutionNumber', result.INTEGER)
        res.add_field('JobID', result.STRING)
        res.add_field('Status', result.STRING)
        res.add_field('ScheduledStartTime', result.DATETIME)
        res.add_field('StartedAt', result.DATETIME)
        res.add_field('FinishedAt', result.DATETIME)

        jobs_manager = get_workspace_manager().organizations.current.jobs

        executionsData = jobs_manager.get_executions(
            params['job_id'],
            params['from_start'],
            params['to_end'],
        )

        if params['extended']:
            res.add_field('SnapshotNotebookPath', result.STRING)

            def fields(execution: Any) -> Any:
                return (
                    execution.execution_id,
                    execution.execution_number,
                    execution.job_id,
                    execution.status.value,
                    dt_isoformat(execution.scheduled_start_time),
                    dt_isoformat(execution.started_at),
                    dt_isoformat(execution.finished_at),
                    execution.snapshot_notebook_path,
                )
        else:
            def fields(execution: Any) -> Any:
                return (
                    execution.execution_id,
                    execution.execution_number,
                    execution.job_id,
                    execution.status.value,
                    dt_isoformat(execution.scheduled_start_time),
                    dt_isoformat(execution.started_at),
                    dt_isoformat(execution.finished_at),
                )

        res.set_rows([fields(execution) for execution in executionsData.executions])

        return res


ShowJobExecutionsHandler.register(overwrite=True)


class ShowJobParametersHandler(SQLHandler):
    """
    SHOW JOB PARAMETERS FOR job_id;

    # ID of the job to show parameters for
    job_id = '<job-id>'

    Description
    -----------
    Shows the parameters for the job with the specified ID.

    Example
    -------
    The following command shows the parameters for the job with ID **job1**::

        SHOW JOB PARAMETERS FOR 'job1';
    """

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        res = FusionSQLResult()
        res.add_field('Name', result.STRING)
        res.add_field('Value', result.STRING)
        res.add_field('Type', result.STRING)

        jobs_manager = get_workspace_manager().organizations.current.jobs

        parameters = jobs_manager.get_parameters(params['job_id'])

        def fields(parameter: Any) -> Any:
            return (
                parameter.name,
                parameter.value,
                parameter.type,
            )

        res.set_rows([fields(parameter) for parameter in parameters])

        return res


ShowJobParametersHandler.register(overwrite=True)


class ShowJobRuntimesHandler(SQLHandler):
    """
    SHOW JOB RUNTIMES;

    Description
    -----------
    Shows the available runtimes for jobs.

    Example
    -------
    The following command shows the available runtimes for jobs::

        SHOW JOB RUNTIMES;
    """

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        res = FusionSQLResult()
        res.add_field('Name', result.STRING)
        res.add_field('Description', result.STRING)

        jobs_manager = get_workspace_manager().organizations.current.jobs

        runtimes = jobs_manager.runtimes()

        def fields(runtime: Any) -> Any:
            return (
                runtime.name,
                runtime.description,
            )

        res.set_rows([fields(runtime) for runtime in runtimes])

        return res


ShowJobRuntimesHandler.register(overwrite=True)


class DropJobHandler(SQLHandler):
    """
    DROP JOBS job_ids;

    # Job IDs to drop
    job_ids = '<job-id>',...

    Description
    -----------
    Drops the jobs with the specified IDs.

    Arguments
    ---------
    * ``<job-id>``: A list of the IDs of the jobs to drop.

    Example
    -------
    The following command drops the jobs with ID **job1** and **job2**::

        DROP JOBS 'job1', 'job2';
    """

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        res = FusionSQLResult()
        res.add_field('JobID', result.STRING)
        res.add_field('Success', result.BOOL)

        jobs_manager = get_workspace_manager().organizations.current.jobs

        results: List[Tuple[Any, ...]] = []
        for job_id in params['job_ids']:
            success = jobs_manager.delete(job_id)
            results.append((job_id, success))
        res.set_rows(results)

        return res


DropJobHandler.register(overwrite=True)
