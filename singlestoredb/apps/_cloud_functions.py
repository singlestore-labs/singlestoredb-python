import asyncio
import typing
import urllib.parse

from ._config import AppConfig
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
) -> None:

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

    base_path = urllib.parse.urlparse(app_config.url).path
    app.root_path = base_path

    config = uvicorn.Config(
        app,
        host='0.0.0.0',
        port=app_config.listen_port,
        log_level=log_level,
    )
    _running_server = AwaitableUvicornServer(config)

    asyncio.create_task(_running_server.serve())
    await _running_server.wait_for_startup()

    if app_config.running_interactively:
        print(f'Cloud function available at {app_config.url}')
