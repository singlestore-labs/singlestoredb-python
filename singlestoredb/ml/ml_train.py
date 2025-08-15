import os
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

import requests
from IPython.core.magic import Magics, magics_class, line_magic
from IPython.core.magic_arguments import argument, magic_arguments, parse_argstring

class SingleStoreAPIError(Exception):
    """Raised when the SingleStore Management API returns an error."""
    def __init__(self, status_code: int, message: str, response: requests.Response):
        super().__init__(f"SingleStore API Error {status_code}: {message}")
        self.status_code = status_code
        self.response = response


class SingleStoreJobsClient:
    """Thin wrapper around the SingleStore Management API for notebook Jobs."""

    def __init__(
        self,
        jwt_token: Optional[str] = None,
        base_url: str = "https://api.singlestore.com/v1",
    ):
        token = jwt_token or "09f75c43e5ed6ceb1cdf34ea35ced436599450967e3772a34f774f4f36a49945"
        # token = jwt_token or os.environ.get("SINGLESTOREDB_USER_TOKEN")
        # token = jwt_token or os.environ.get("SINGLESTOREDB_APP_TOKEN")


        if not token:
            raise ValueError("Set your JWT in SINGLESTORE_JWT or pass it in.")
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        })

    def list_jobs(self) -> List[Dict[str, Any]]:
        """Fetch all jobs; filter client‑side by name."""
        url = f"{self.base_url}/jobs"
        resp = self.session.get(url)
        if not resp.ok:
            raise SingleStoreAPIError(resp.status_code, resp.text, resp)
        return resp.json()  # assume a JSON array

    def create_job(
        self,
        name: str,
        description: str,
        notebook_path: str,
        runtime_name: str,
        parameters: List[Dict[str, Any]],
        schedule_interval_minutes: int,
        schedule_mode: str,
        schedule_start_at: datetime,
        target_config: Dict[str, Any],
        create_snapshot: bool = True,
    ) -> Dict[str, Any]:
        payload = {
            "name": name,
            "description": description,
            "executionConfig": {
                "notebookPath": notebook_path,
                "runtimeName": runtime_name,
                "createSnapshot": create_snapshot,
            },
            "parameters": parameters,
            "schedule": {
                "executionIntervalInMinutes": schedule_interval_minutes,
                "mode": schedule_mode,
                "startAt": schedule_start_at.replace(microsecond=0).isoformat() + "Z",
            },
            "targetConfig": target_config,
        }
        url = f"{self.base_url}/jobs"
        resp = self.session.post(url, json=payload)
        if not resp.ok:
            raise SingleStoreAPIError(resp.status_code, resp.text, resp)
        return resp.json()

    def get_job(self, job_id: str) -> Dict[str, Any]:
        url = f"{self.base_url}/jobs/{job_id}"
        resp = self.session.get(url)
        print(resp)
        if not resp.ok:
            raise SingleStoreAPIError(resp.status_code, resp.text, resp)
        return resp.json()

import os
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

import requests
from IPython.core.magic import Magics, magics_class, line_magic
from IPython.core.magic_arguments import argument, magic_arguments, parse_argstring


class SingleStoreAPIError(Exception):
    """Raised when the SingleStore Management API returns an error."""
    def __init__(self, status_code: int, message: str, response: requests.Response):
        super().__init__(f"SingleStore API Error {status_code}: {message}")
        self.status_code = status_code
        self.response = response


class SingleStoreJobsClient:
    """Thin wrapper around the SingleStore Management API for notebook Jobs."""

    def __init__(
        self,
        jwt_token: Optional[str] = None,
        base_url: str = "https://api.singlestore.com/v1",
    ):
        token = jwt_token or os.environ.get("SINGLESTOREDB_USER_TOKEN")
        # token = jwt_token or os.environ.get("SINGLESTOREDB_APP_TOKEN")

        if not token:
            raise ValueError("Set your JWT in SINGLESTORE_JWT or pass it in.")
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        })



    def list_jobs(self) -> List[Dict[str, Any]]:
        """Fetch all jobs; filter client‑side by name."""
        url = f"{self.base_url}/jobs"
        resp = self.session.get(url)
        if not resp.ok:
            raise SingleStoreAPIError(resp.status_code, resp.text, resp)
        return resp.json()  # assume a JSON array

    def create_job(
        self,
        name: str,
        description: str,
        notebook_path: str,
        runtime_name: str,
        parameters: List[Dict[str, Any]],
        schedule_interval_minutes: int,
        schedule_mode: str,
        schedule_start_at: datetime,
        target_config: Dict[str, Any],
        create_snapshot: bool = True,
    ) -> Dict[str, Any]:
        payload = {
            "name": name,
            "description": description,
            "executionConfig": {
                "notebookPath": notebook_path,
                "runtimeName": runtime_name,
                "createSnapshot": create_snapshot,
            },
            "parameters": parameters,
            "schedule": {
                "executionIntervalInMinutes": schedule_interval_minutes,
                "mode": schedule_mode,
                "startAt": schedule_start_at.replace(microsecond=0).isoformat() + "Z",
            },
            "targetConfig": target_config,
        }
        org_id =   os.environ.get("SINGLESTOREDB_ORGANIZATION")
        if not org_id:
            raise ValueError("Please set SINGLESTOREDB_ORGANIZATION in your env.")
        url = f"{self.base_url}/jobs?organizationID={org_id}"
        resp = self.session.post(url, json=payload)
        if not resp.ok:
            raise SingleStoreAPIError(resp.status_code, resp.text, resp)
        return resp.json()

    def get_job(self, job_id: str) -> Dict[str, Any]:
        url = f"{self.base_url}/jobs/{job_id}"
        resp = self.session.get(url)
        print(resp)
        if not resp.ok:
            raise SingleStoreAPIError(resp.status_code, resp.text, resp)
        return resp.json()


@magics_class
class SSMLFunctionMagics(Magics):
    """Line magic that schedules (or skips) a classification‑train job via REST."""

    def __init__(self, shell):
        super().__init__(shell)
        self.logger = logging.getLogger(__name__)
        if not self.logger.handlers:
            h = logging.StreamHandler()
            h.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
            self.logger.addHandler(h)
        self.logger.setLevel(logging.INFO)

    @magic_arguments()
    @argument('--job_name',            type=str, required=True, help="Job name")
    @argument('--model_name',          type=str, required=True, help="Model filename")
    @argument('--workspace',           type=str, required=True, help="Workspace identifier")
    @argument('--db',                  type=str, required=True, help="Database/schema name")
    @argument('--input_table',         type=str, required=True, help="Source table")
    @argument('--target_column',       type=str, required=True, help="Target column")
    @argument('--model',               type=str, default='auto', help="Model to train")
    @argument('--evaluation_criteria', type=str, required=True, help="Metric (roc_auc, etc.)")
    @argument('--version', type=str, default="1", help="default version")
    @argument('--selected_features',   type=str, nargs='+', help="List of features")
    @line_magic
    def SS_ML_FUNCTION_CLASSIFICATION_TRAIN(self, line: str):
        args = parse_argstring(self.SS_ML_FUNCTION_CLASSIFICATION_TRAIN, line)
        job_name  = args.job_name
        job_descr = f"Train classification on {args.input_table} target={args.target_column} job_name={job_name}"

        # 2) Instantiate client & check for existing job
        client = SingleStoreJobsClient()
        # try:
        #     job = client.get_job(job_name)
        #     print(job)
        #     if job.get("name") == job_name:
        #         existing = True
        # except SingleStoreAPIError as e:
        #     self.logger.error(f"Failed to list jobs: {e}")
        #     return

        # if existing:
        #     job = existing[0]
        #     self.logger.info(
        #         f"Job '{job_name}' already exists (ID={job.get('id')}); skipping creation."
        #     )
        #     return

        # 3) Build parameters payload
        params: List[Dict[str, Any]] = []
        for key in ('job_name','workspace','db','input_table','target_column','model','model_name','evaluation_criteria','version'):
            params.append({
                "name": key,
                "type": "string",
                "value": getattr(args, key),
            })
        if args.selected_features:
            params.append({
                "name": "selected_features",
                "type": "string",
                "value": ",".join(args.selected_features),
            })

        # 4) Schedule + targetConfig defaults (from ENV)
        runtime = os.environ.get("SINGLESTORE_RUNTIME", "notebooks-cpu-small")
        target_id = os.environ.get("SINGLESTOREDB_WORKSPACE")
        if not target_id:
            raise ValueError("Please set SINGLESTORE_TARGET_ID in your env.")
        target_cfg = {
            "databaseName": args.db,
            "resumeTarget": True,
            "targetType": "Workspace",
            "targetID": target_id,
        }

        # 5) Create the job
        try:
            created = client.create_job(
                name=job_name,
                description=job_descr,
                notebook_path="ml_Classification_pipeline.ipynb",
                runtime_name=runtime,
                parameters=params,
                schedule_interval_minutes=None,
                schedule_mode="Once",
                schedule_start_at=datetime.utcnow(),
                target_config=target_cfg,
                create_snapshot=True
            )
        except SingleStoreAPIError as e:
            self.logger.error(f"Failed to create job '{job_name}': {e}")
            return

        self.logger.info(
            f"Created job '{created.get('name')}' (ID={created.get('jobID')})"
        )


# register so the magic is available immediately:
# get_ipython().register_magics(SSMLFunctionMagics)
def register_train_command():
    """Register the SSMLFunctionMagics with the IPython shell."""
    get_ipython().register_magics(SSMLFunctionMagics)
