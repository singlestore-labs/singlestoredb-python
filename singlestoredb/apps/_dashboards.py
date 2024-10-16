import typing

from ._config import AppConfig
from ._process import kill_process_by_port
from ._stdout_supress import StdoutSuppressor
from singlestoredb.apps._connection_info import ConnectionInfo

if typing.TYPE_CHECKING:
    from dash import Dash


async def run_dashboard_app(
    app: 'Dash',
    debug: bool = False,
    kill_existing_app_server: bool = True,
) -> ConnectionInfo:
    try:
        from dash import Dash
    except ImportError:
        raise ImportError('package dash is required to run dashboards')

    if not isinstance(app, Dash):
        raise TypeError('app is not an instance of Dash App')

    app_config = AppConfig.from_env()

    if kill_existing_app_server:
        kill_process_by_port(app_config.listen_port)

    if app.config.requests_pathname_prefix is None or \
            app.config.requests_pathname_prefix != app_config.base_path:
        raise RuntimeError('''
requests_pathname_prefix of the Dash App is invalid. Please set
requests_pathname_prefix=os.environ['SINGLESTOREDB_APP_BASE_PATH']
while initializing the Dash App and retry''')

    with StdoutSuppressor():
        app.run(
            host='0.0.0.0',
            debug=debug,
            port=str(app_config.listen_port),
            jupyter_mode='external',
        )

    if app_config.running_interactively:
        print(f'Dash app available at {app_config.base_url}?authToken={app_config.token}')
    return ConnectionInfo(app_config.base_url, app_config.token)
