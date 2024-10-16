import asyncio
import textwrap
import typing

from ._config import AppConfig
from ._connection_info import ConnectionInfo
from ._process import kill_process_by_port

if typing.TYPE_CHECKING:
    from fastapi import FastAPI
    from ._uvicorn_util import AwaitableUvicornServer

# Keep track of currently running server
_running_server: 'typing.Optional[AwaitableUvicornServer]' = None


async def run_function_app(
    app: 'FastAPI',
    log_level: str = 'error',
    kill_existing_app_server: bool = True,
) -> ConnectionInfo:
    global _running_server
    from ._uvicorn_util import AwaitableUvicornServer

    try:
        import uvicorn
    except ImportError:
        raise ImportError('package uvicorn is required to run cloud functions')
    try:
        import fastapi
    except ImportError:
        raise ImportError('package fastapi is required to run cloud functions')

    if not isinstance(app, fastapi.FastAPI):
        raise TypeError('app is not an instance of FastAPI')

    app_config = AppConfig.from_env()

    if kill_existing_app_server:
        # Shutdown the server gracefully if it was started by us.
        # Since the uvicorn server doesn't start a new subprocess
        # killing the process would result in kernel dying.
        if _running_server is not None:
            await _running_server.shutdown()
            _running_server = None

        # Kill if any other process is occupying the port
        kill_process_by_port(app_config.listen_port)

    # Add `GET /` route, used for liveness check
    @app.get('/')
    def ping() -> str:
        return 'Success!'

    app.root_path = app_config.base_path

    config = uvicorn.Config(
        app,
        host='0.0.0.0',
        port=app_config.listen_port,
        log_level=log_level,
    )
    _running_server = AwaitableUvicornServer(config)

    asyncio.create_task(_running_server.serve())
    await _running_server.wait_for_startup()

    connection_info = ConnectionInfo(app_config.base_url, app_config.token)

    if app_config.running_interactively:
        if app_config.is_gateway_enabled:
            print(
                'Cloud function available at '
                f'{app_config.base_url}docs?authToken={app_config.token}',
            )
        else:
            curl_header = f'-H "Authorization: Bearer {app_config.token}"'
            curl_example = f'curl "{app_config.base_url}" {curl_header}'
            print(
                textwrap.dedent(f"""
                  Cloud function available at {app_config.base_url}

                  Auth Token: {app_config.token}

                  Curl example: {curl_example}

              """).strip(),
            )

    return connection_info
