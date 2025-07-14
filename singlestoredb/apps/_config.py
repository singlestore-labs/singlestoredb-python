import os
from dataclasses import dataclass
from typing import Optional
from singlestoredb.connection import build_params


@dataclass
class AppConfig:
    listen_port: int
    base_url: str
    base_path: str
    notebook_server_id: str
    workspace_group_id: str
    database_name: str
    app_token: Optional[str]
    user_token: Optional[str]
    running_interactively: bool
    is_gateway_enabled: bool
    is_local_dev: bool

    @staticmethod
    def _read_variable(name: str) -> str:
        value = os.environ.get(name)
        if value is None:
            raise RuntimeError(
                f'Missing {name} environment variable. '
                'Is the code running outside SingleStoreDB notebook environment?',
            )
        return value

    @classmethod
    def from_env(cls) -> 'AppConfig':
        port = cls._read_variable('SINGLESTOREDB_APP_LISTEN_PORT')
        base_url = cls._read_variable('SINGLESTOREDB_APP_BASE_URL')
        base_path = cls._read_variable('SINGLESTOREDB_APP_BASE_PATH')
        notebook_server_id = cls._read_variable('SINGLESTOREDB_NOTEBOOK_SERVER_ID')
        is_local_dev_env_var = cls._read_variable('SINGLESTOREDB_IS_LOCAL_DEV')

        workload_type = os.environ.get('SINGLESTOREDB_WORKLOAD_TYPE')
        running_interactively = workload_type == 'InteractiveNotebook'

        is_gateway_enabled = 'SINGLESTOREDB_NOVA_GATEWAY_ENDPOINT' in os.environ

        app_token = os.environ.get('SINGLESTOREDB_APP_TOKEN')
        user_token = os.environ.get('SINGLESTOREDB_USER_TOKEN')

        if 'SINGLESTOREDB_URL' in os.environ:
            dbname = build_params(host=os.environ['SINGLESTOREDB_URL']).get('database')
        elif 'SINGLESTOREDB_HOST' in os.environ:
            dbname = build_params(host=os.environ['SINGLESTOREDB_HOST']).get('database')
        elif 'SINGLESTOREDB_DATABASE' in os.environ:
            dbname = os.environ['SINGLESTOREDB_DATBASE']
        
        workspace_group_id = os.environ.get('SINGLESTOREDB_WORKSPACE_GROUP')

        # Make sure the required variables are present
        # and present useful error message if not
        if running_interactively:
            if is_gateway_enabled:
                app_token = cls._read_variable('SINGLESTOREDB_APP_TOKEN')
            else:
                user_token = cls._read_variable('SINGLESTOREDB_USER_TOKEN')

        return cls(
            listen_port=int(port),
            base_url=base_url,
            base_path=base_path,
            notebook_server_id=notebook_server_id,
            workspace_group_id=workspace_group_id,
            database_name=dbname,
            app_token=app_token,
            user_token=user_token,
            running_interactively=running_interactively,
            is_gateway_enabled=is_gateway_enabled,
            is_local_dev=is_local_dev_env_var == 'true',
        )

    @property
    def token(self) -> Optional[str]:
        """
        Returns None if running non-interactively
        """
        if self.is_gateway_enabled:
            return self.app_token
        else:
            return self.user_token
