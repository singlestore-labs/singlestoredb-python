import asyncio
import os
import typing

from ..functions.ext.asgi import Application
from ._config import AppConfig
from ._connection_info import UdfConnectionInfo
from ._process import kill_process_by_port

if typing.TYPE_CHECKING:
    from ._uvicorn_util import AwaitableUvicornServer

# Keep track of currently running server
_running_server: 'typing.Optional[AwaitableUvicornServer]' = None


async def run_udf_app(
    replace_existing: bool,
    log_level: str = 'error',
    kill_existing_app_server: bool = True,
) -> UdfConnectionInfo:
    global _running_server
    from ._uvicorn_util import AwaitableUvicornServer

    try:
        import uvicorn
    except ImportError:
        raise ImportError('package uvicorn is required to run python udfs')

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

    base_url = generate_base_url(app_config)

    udf_suffix = ''
    if app_config.running_interactively:
        udf_suffix = '_test'
    app = Application(url=base_url, app_mode='managed', name_suffix=udf_suffix)

    config = uvicorn.Config(
        app,
        host='0.0.0.0',
        port=app_config.listen_port,
        log_level=log_level,
    )
    _running_server = AwaitableUvicornServer(config)

    # Register the functions
    app.register_functions(replace=replace_existing)

    asyncio.create_task(_running_server.serve())
    await _running_server.wait_for_startup()

    print(f'Python UDF registered at {base_url}')

    return UdfConnectionInfo(base_url, app.get_function_info())


def generate_base_url(app_config: AppConfig) -> str:
    if not app_config.is_gateway_enabled:
        raise RuntimeError('Python UDFs are not available if Nova Gateway is not enabled')

    if not app_config.running_interactively:
        return app_config.base_url

    # generate python udf endpoint for interactive notebooks
    gateway_url = os.environ.get('SINGLESTOREDB_NOVA_GATEWAY_ENDPOINT')
    if app_config.is_local_dev:
        gateway_url = os.environ.get('SINGLESTOREDB_NOVA_GATEWAY_DEV_ENDPOINT')
        if gateway_url is None:
            raise RuntimeError(
                'Missing SINGLESTOREDB_NOVA_GATEWAY_DEV_ENDPOINT environment variable.',
            )

    return f'{gateway_url}/pythonudfs/{app_config.notebook_server_id}/interactive/'
