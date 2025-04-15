import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class AppConfig:
    listen_port: int
    base_url: str
    base_path: str
    app_token: Optional[str]
    user_token: Optional[str]
    running_interactively: bool
    is_gateway_enabled: bool

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

        workload_type = os.environ.get('SINGLESTOREDB_WORKLOAD_TYPE')
        running_interactively = workload_type == 'InteractiveNotebook'

        is_gateway_enabled = 'SINGLESTOREDB_NOVA_GATEWAY_ENDPOINT' in os.environ

        app_token = os.environ.get('SINGLESTOREDB_APP_TOKEN')
        user_token = os.environ.get('SINGLESTOREDB_USER_TOKEN')

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
            app_token=app_token,
            user_token=user_token,
            running_interactively=running_interactively,
            is_gateway_enabled=is_gateway_enabled,
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


@dataclass
class PythonUdfAppConfig:
    listen_port: int
    base_url: str
    base_path: str
    running_interactively: bool
    is_gateway_enabled: bool

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

        workload_type = os.environ.get('SINGLESTOREDB_WORKLOAD_TYPE')
        running_interactively = workload_type == 'InteractiveNotebook'

        is_gateway_enabled = 'SINGLESTOREDB_NOVA_GATEWAY_ENDPOINT' in os.environ

        if running_interactively:
            if is_gateway_enabled:
                base_url = cls._read_variable('SINGLESTOREDB_PYTHON_UDF_BASE_URL')
                base_path = cls._read_variable('SINGLESTOREDB_PYTHON_UDF_BASE_PATH')
                assert base_url is not None
                assert base_path is not None
            else:
                raise RuntimeError(
                    'Running Python UDFs in interactive mode without nova-gateway enabled is not supported'
                )

        return cls(
            listen_port=int(port),
            base_url=base_url,
            base_path=base_path,
            running_interactively=running_interactively,
            is_gateway_enabled=is_gateway_enabled,
        )
