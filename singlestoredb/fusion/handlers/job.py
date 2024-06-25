#!/usr/bin/env python3
from typing import Any
from typing import Dict
from typing import Optional

import singlestoredb as s2
from .. import result
from ...management.utils import to_datetime
from ..handler import SQLHandler
from ..result import FusionSQLResult
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

    # Execute interval in minutes
    execute_every = EXECUTE EVERY '<interval>'

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
    * ``<interval>``: The interval in minutes at which the job will be executed.
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
            EXECUTE EVERY '5'
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
            execution_interval_in_minutes=int(params['execute_every']) if params.get('execute_every') is not None else None,
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
    Waits for the jobs with the specified IDs to complete. If a timeout is specified,
    the command will return after the specified number of seconds, even if the jobs
    have not completed.

    Arguments
    ---------
    * ``<job-id>``: A list of the IDs of the job to wait on.
    * ``<integer>``: The number of seconds to wait for the jobs to complete.

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

        print(params['with_timeout'])

        success = jobs_manager.wait(
            params['job_ids'],
            timeout=params['with_timeout'],
        )
        res.set_rows([(success,)])

        return res


WaitOnJobsHandler.register(overwrite=True)
