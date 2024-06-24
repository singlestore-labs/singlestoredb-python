#!/usr/bin/env python3
import singlestoredb as s2

from typing import Any
from typing import Dict
from typing import Optional

from .. import result
from ..handler import SQLHandler
from ..result import FusionSQLResult
from .utils import get_workspace_manager


class RunJobHandler(SQLHandler):
    """
    RUN JOB USING notebook_path
        [ with_runtime ];

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

        RUN JOB USING 'example_notebook.ipynb' WITH RUNTIME 'notebooks-cpu-small';

    """

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        res = FusionSQLResult()
        res.add_field('JobID', result.STRING)

        jobs_manager = s2.manage_workspaces(base_url="http://10.42.0.40:8080").organizations.current.jobs

        job = jobs_manager.run(
            params['notebook_path'],
            runtime_name=params['runtime_name'] or None,
        )
        res.set_rows([(job.job_id,)])

        return res
    
RunJobHandler.register(overwrite=True)
