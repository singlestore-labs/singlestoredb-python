import os
from dataclasses import dataclass


@dataclass
class AppConfig:
    listen_port: int
    url: str
    running_interactively: bool

    @classmethod
    def from_env(cls) -> 'AppConfig':
        port = os.environ.get('SINGLESTOREDB_APP_LISTEN_PORT')
        if port is None:
            raise RuntimeError(
                'Missing SINGLESTOREDB_APP_LISTEN_PORT environment variable. '
                'Is the code running outside SingleStoreDB notebook environment?',
            )
        url = os.environ.get('SINGLESTOREDB_APP_URL')
        if url is None:
            raise RuntimeError(
                'Missing SINGLESTOREDB_APP_URL environment variable. '
                'Is the code running outside SingleStoreDB notebook environment?',
            )

        workload_type = os.environ.get('SINGLESTOREDB_WORKLOAD_TYPE')
        running_interactively = workload_type == 'InteractiveNotebook'

        return cls(
            listen_port=int(port),
            url=url,
            running_interactively=running_interactively,
        )
