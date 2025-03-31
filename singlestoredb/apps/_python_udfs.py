import asyncio
import textwrap
import typing
import os

from ._config import PythonUdfAppConfig
from ._connection_info import ConnectionInfo, PythonUdfConnectionInfo
from ._process import kill_process_by_port
from ..functions.ext.asgi import Application

if typing.TYPE_CHECKING:
    from ._uvicorn_util import AwaitableUvicornServer

# Keep track of currently running server
_running_server: 'typing.Optional[AwaitableUvicornServer]' = None


async def run_udf_app(
    log_level: str = 'error',
    kill_existing_app_server: bool = True,
) -> ConnectionInfo:
    global _running_server
    from ._uvicorn_util import AwaitableUvicornServer

    try:
        import uvicorn
    except ImportError:
        raise ImportError('package uvicorn is required to run python udfs')

    app_config = PythonUdfAppConfig.from_env()

    if kill_existing_app_server:
        # Shutdown the server gracefully if it was started by us.
        # Since the uvicorn server doesn't start a new subprocess
        # killing the process would result in kernel dying.
        if _running_server is not None:
            await _running_server.shutdown()
            _running_server = None

        # Kill if any other process is occupying the port
        kill_process_by_port(app_config.listen_port)

    app = Application()
    app.root_path = app_config.base_path

    config = uvicorn.Config(
        app,
        host='0.0.0.0',
        port=app_config.listen_port,
        log_level=log_level,
    )
    _running_server = AwaitableUvicornServer(config)

    # In interactive mode this should be set to true
    replace = app_config.running_interactively
    app.register_functions(replace=True)

    asyncio.create_task(_running_server.serve())
    await _running_server.wait_for_startup()

    connection_info = PythonUdfConnectionInfo(app_config.base_url, app.get_function_info())


    return connection_info
