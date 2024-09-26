import typing
import urllib.parse

from ._config import AppConfig
from ._process import kill_process_by_port
from ._stdout_supress import StdoutSuppressor
from singlestoredb.apps._connection_info import ConnectionInfo

if typing.TYPE_CHECKING:
    from plotly.graph_objs import Figure


async def run_dashboard_app(
    app: 'App',
    debug: bool = False,
    kill_existing_app_server: bool = True,
) -> ConnectionInfo:
    try:
        from dash import Dash
    except ImportError:
        raise ImportError('package dash is required to run dashboards')

    try:
        from plotly.graph_objs import Figure
    except ImportError:
        raise ImportError('package dash is required to run dashboards')

    if not isinstance(app, Dash):
        raise TypeError('app is not an instance of Dash App')

    app_config = AppConfig.from_env()

    if kill_existing_app_server:
        kill_process_by_port(app_config.listen_port)

    base_path = urllib.parse.urlparse(app_config.base_url).path
    app.requests_pathname_prefix = base_path
    
    # Layout needs to refreshed after setting the requests_pathname_prefix
    app.layout = app.layout

    with StdoutSuppressor():
        app.run_server(
            host='0.0.0.0',
            debug=debug,
            port=str(app_config.listen_port),
            jupyter_mode='external',
        )

    if app_config.running_interactively:
        print(f'Dash app available at {app_config.base_url}?authToken={app_config.token}')
    return ConnectionInfo(app_config.base_url, app_config.token)