import asyncio
import math
import os
import typing
import warnings

from ..functions.ext.asgi import Application
from ._config import AppConfig
from ._connection_info import UdfConnectionInfo
from ._process import kill_process_by_port

if typing.TYPE_CHECKING:
    from ._uvicorn_util import AwaitableUvicornServer

# Keep track of currently running server
_running_server: 'typing.Optional[AwaitableUvicornServer]' = None
_running_server_task: 'typing.Optional[asyncio.Task[None]]' = None

# Maximum number of UDFs allowed
MAX_UDFS_LIMIT = 10

# Default uvicorn keep-alive timeout (seconds) for managed UDF servers.
# The high keep-alive timeout makes sure uvicorn does not close idle connections so
# eagerly. Whichever side closes first holds the socket in TIME_WAIT (~60s on
# Linux), so server-initiated closes churn sockets under load.
DEFAULT_UDF_KEEPALIVE_TIMEOUT = 120


async def run_udf_app(
    log_level: str = 'error',
    kill_existing_app_server: bool = True,
) -> UdfConnectionInfo:
    global _running_server, _running_server_task
    from ._uvicorn_util import AwaitableUvicornServer

    try:
        import uvicorn
    except ImportError:
        raise ImportError('package uvicorn is required to run python udfs')

    app_config = AppConfig.from_env()

    base_url = generate_base_url(app_config)

    udf_suffix = ''
    if app_config.running_interactively:
        udf_suffix = '_test'
    app = Application(
        url=base_url,
        app_mode='managed',
        name_suffix=udf_suffix,
        log_level=log_level,
    )

    if not app.endpoints:
        raise ValueError('You must define at least one function.')
    if len(app.endpoints) > MAX_UDFS_LIMIT:
        raise ValueError(
            f'You can only define a maximum of {MAX_UDFS_LIMIT} functions.',
        )

    # Prove every name is allowed before killing a live interactive server.
    if app_config.running_interactively:
        app.preflight_interactive_functions()

    if kill_existing_app_server:
        # Shutdown the server gracefully if it was started by us.
        # Since the uvicorn server doesn't start a new subprocess
        # killing the process would result in kernel dying.
        await shutdown_udf_app(timeout=None)

        # Kill if any other process is occupying the port
        kill_process_by_port(app_config.listen_port)

    config = uvicorn.Config(
        app,
        host='0.0.0.0',
        port=app_config.listen_port,
        log_config=app.get_uvicorn_log_config(),
        timeout_keep_alive=get_keep_alive_timeout(),
    )

    # Register the functions only if the app is running interactively.
    if app_config.running_interactively:
        app.register_interactive_functions()

    _running_server = AwaitableUvicornServer(config)
    _running_server_task = asyncio.create_task(_running_server.serve())
    await _running_server.wait_for_startup()

    print(f'Python UDF registered at {base_url}')
    if app_config.running_interactively:
        sql_names = [
            info['signature']['name']
            for _func, info in app.endpoints.values()
        ]
        print(f'Registered SQL functions: {", ".join(sql_names)}')

    return UdfConnectionInfo(base_url, app.get_function_info())


async def shutdown_udf_app(timeout: typing.Optional[float]) -> None:
    """
    Stop the UDF server started by ``run_udf_app`` in this Python kernel.

    Uvicorn closes listeners and idle connections, then allows active requests
    to finish for up to ``timeout`` seconds before cancelling them. ``None``
    waits without a drain limit, preserving the existing server-replacement
    behavior. A finite timeout must be non-negative.

    Call and await this operation on the same event loop as ``run_udf_app``,
    before shutting down the kernel or its networking. The budget covers
    request draining; allow additional time for server cleanup. Python cannot
    forcibly stop arbitrary synchronous UDF code running in worker threads.

    It is safe to call when no server is running. Concurrent calls await the
    same server task; the first shutdown request determines the drain budget.
    """
    global _running_server, _running_server_task

    if timeout is not None and (not math.isfinite(timeout) or timeout < 0):
        raise ValueError('timeout must be a finite non-negative number or None')

    server, task = _running_server, _running_server_task
    if server is None:
        return
    if task is None:
        raise RuntimeError('UDF server has no serving task')

    if not server.should_exit:
        server.config.timeout_graceful_shutdown = timeout
        server.should_exit = True
    try:
        # Cancelling a caller must not interrupt the server's own drain.
        await asyncio.shield(task)
    finally:
        if task.done():
            # Uvicorn can skip shutdown when exit was requested during startup.
            if server.started:
                for listener in server.servers:
                    listener.close()
            # Uvicorn can finish after cancelling requests but before they close.
            for connection in list(server.server_state.connections):
                connection.transport.abort()
            if _running_server is server:
                _running_server = None
                _running_server_task = None


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


def get_keep_alive_timeout() -> int:
    raw = os.environ.get('SINGLESTOREDB_UDF_KEEPALIVE_TIMEOUT')
    if raw is None:
        return DEFAULT_UDF_KEEPALIVE_TIMEOUT
    try:
        value = int(raw)
    except (TypeError, ValueError):
        warnings.warn(
            f'Invalid SINGLESTOREDB_UDF_KEEPALIVE_TIMEOUT={raw!r}; '
            f'falling back to {DEFAULT_UDF_KEEPALIVE_TIMEOUT}s.',
        )
        return DEFAULT_UDF_KEEPALIVE_TIMEOUT
    if value < 0:
        warnings.warn(
            f'Negative SINGLESTOREDB_UDF_KEEPALIVE_TIMEOUT={value}; '
            f'falling back to {DEFAULT_UDF_KEEPALIVE_TIMEOUT}s.',
        )
        return DEFAULT_UDF_KEEPALIVE_TIMEOUT
    return value
