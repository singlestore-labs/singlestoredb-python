# type: ignore
import asyncio
import http.client
import json
import os
import sys
import threading
import unittest
from collections.abc import Iterator
from typing import Any
from typing import Callable
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple
from unittest.mock import MagicMock
from unittest.mock import patch

from singlestoredb.apps import _python_udfs
from singlestoredb.apps import run_udf_app
from singlestoredb.apps import shutdown_udf_app
from singlestoredb.apps._config import AppConfig
from singlestoredb.functions import udf
from singlestoredb.functions.ext.function_url import classify_interactive_registration
from singlestoredb.functions.ext.function_url import extract_service_url
from singlestoredb.functions.ext.function_url import is_function_not_defined
from singlestoredb.functions.ext.function_url import is_interactive_service_url
from singlestoredb.mysql.constants import ER


class TestFunctionURL(unittest.TestCase):

    def test_extract_managed_double_quoted(self) -> None:
        sql = (
            'CREATE OR REPLACE EXTERNAL FUNCTION `shi_test`(v BIGINT) '
            'RETURNS TEXT AS MANAGED SERVICE '
            '"https://apps.us-east-1.cloud.singlestore.com/pythonudfs/abc/" '
            'FORMAT ROWDAT_1;'
        )
        self.assertEqual(
            extract_service_url(sql),
            'https://apps.us-east-1.cloud.singlestore.com/pythonudfs/abc/',
        )

    def test_extract_service_single_quoted(self) -> None:
        sql = (
            'CREATE EXTERNAL FUNCTION foo() RETURNS INT AS REMOTE SERVICE '
            "'https://example.com/invoke' FORMAT ROWDAT_1;"
        )
        self.assertEqual(extract_service_url(sql), 'https://example.com/invoke')

    def test_extract_none(self) -> None:
        self.assertIsNone(extract_service_url('CREATE FUNCTION foo() RETURNS INT'))

    def test_classify_missing_create(self) -> None:
        self.assertEqual(
            classify_interactive_registration(
                None,
                'https://gw/pythonudfs/sess/interactive/',
            ),
            'create',
        )

    def test_classify_this_session_replace(self) -> None:
        this = 'https://gw/pythonudfs/sess/interactive/'
        self.assertEqual(
            classify_interactive_registration(this, this),
            'replace',
        )
        self.assertEqual(
            classify_interactive_registration(this.rstrip('/'), this),
            'replace',
        )

    def test_classify_published_refuses(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            classify_interactive_registration(
                'https://gw/pythonudfs/published-id/',
                'https://gw/pythonudfs/sess/interactive/',
            )
        self.assertIn('will not replace', str(ctx.exception))
        self.assertIn('this session is', str(ctx.exception))

    def test_classify_other_session_refuses(self) -> None:
        with self.assertRaises(ValueError):
            classify_interactive_registration(
                'https://gw/pythonudfs/other-sess/interactive/',
                'https://gw/pythonudfs/sess/interactive/',
            )

    def test_is_interactive(self) -> None:
        self.assertTrue(
            is_interactive_service_url(
                'https://gw/pythonudfs/sess/interactive/',
            ),
        )
        self.assertFalse(
            is_interactive_service_url(
                'https://gw/pythonudfs/published-id/',
            ),
        )

    def test_function_not_defined(self) -> None:
        missing = type('E', (Exception,), {'errno': ER.FUNCTION_NOT_DEFINED})()
        self.assertTrue(is_function_not_defined(missing))
        sp_missing = type('E', (Exception,), {'errno': ER.SP_DOES_NOT_EXIST})()
        self.assertTrue(is_function_not_defined(sp_missing))
        self.assertTrue(is_function_not_defined(Exception(ER.SP_DOES_NOT_EXIST)))
        self.assertFalse(is_function_not_defined(ValueError('nope')))

    def test_fake_cursor_ownership(self) -> None:
        this_session = 'https://gw/pythonudfs/sess/interactive/'
        published_sql = (
            'CREATE OR REPLACE EXTERNAL FUNCTION `shi_test`(v BIGINT) '
            'RETURNS BIGINT AS MANAGED SERVICE '
            '"https://gw/pythonudfs/published-id/" FORMAT ROWDAT_1;'
        )
        this_sql = (
            'CREATE OR REPLACE EXTERNAL FUNCTION `shi_test`(v BIGINT) '
            'RETURNS BIGINT AS MANAGED SERVICE '
            f'"{this_session}" FORMAT ROWDAT_1;'
        )

        class _Cursor:
            def __init__(
                self,
                rows: Optional[List[Tuple[str, str, str]]] = None,
                exc: Optional[BaseException] = None,
            ) -> None:
                self._rows = rows or []
                self._exc = exc

            def execute(self, sql: str) -> None:
                if self._exc is not None:
                    raise self._exc

            def __iter__(self) -> Iterator[Tuple[str, str, str]]:
                return iter(self._rows)

        def show_url(cur: Any) -> Optional[str]:
            try:
                cur.execute('SHOW CREATE FUNCTION `shi_test`')
            except Exception as exc:
                if is_function_not_defined(exc):
                    return None
                raise
            rows = list(cur)
            if not rows:
                return None
            return extract_service_url(rows[0][2])

        missing = type('E', (Exception,), {'errno': ER.FUNCTION_NOT_DEFINED})()
        self.assertEqual(
            classify_interactive_registration(
                show_url(_Cursor(exc=missing)),
                this_session,
            ),
            'create',
        )
        self.assertEqual(
            classify_interactive_registration(
                show_url(_Cursor(rows=[('shi_test', '', this_sql)])),
                this_session,
            ),
            'replace',
        )
        with self.assertRaises(ValueError):
            classify_interactive_registration(
                show_url(_Cursor(rows=[('shi_test', '', published_sql)])),
                this_session,
            )

    def _interactive_app(self, names: List[str]) -> Any:
        from singlestoredb.functions.ext.asgi import Application

        app = Application.__new__(Application)
        app.function_database = None
        app.url = 'https://gw/pythonudfs/sess/interactive/'
        app.data_format = 'rowdat_1'
        app.app_mode = 'managed'
        app.endpoints = {}
        for name in names:
            app.endpoints[name.encode('utf-8')] = (
                None,
                {
                    'signature': {
                        'name': name,
                        'args': [{'name': 'v', 'sql': 'BIGINT'}],
                        'returns': [{'name': '', 'sql': 'BIGINT'}],
                    },
                },
            )
        return app

    def _patch_connect(self, executed: List[str]) -> Any:
        class _Cursor:
            def execute(self, sql: str) -> None:
                executed.append(sql)

        class _CM:
            def __init__(self, inner: Any) -> None:
                self.inner = inner

            def __enter__(self) -> Any:
                return self.inner

            def __exit__(self, *args: Any) -> bool:
                return False

        class _Conn:
            def cursor(self) -> _CM:
                return _CM(_Cursor())

        return patch(
            'singlestoredb.functions.ext.asgi.connection.connect',
            return_value=_CM(_Conn()),
        )

    def test_register_interactive_preflight(self) -> None:
        app = self._interactive_app(['keep_test', 'steal_test'])
        executed: List[str] = []

        def show(_cur: Any, sql_name: str) -> Optional[str]:
            if sql_name == 'steal_test':
                return 'https://gw/pythonudfs/published-id/'
            return None

        app._show_create_service_url = show  # type: ignore[method-assign]

        with self._patch_connect(executed):
            with self.assertRaises(RuntimeError):
                app.register_interactive_functions()
        self.assertEqual(executed, [])

    def test_register_interactive_rechecks_at_write(self) -> None:
        app = self._interactive_app(['keep_test'])
        executed: List[str] = []
        seen = {'n': 0}

        def show(_cur: Any, _sql_name: str) -> Optional[str]:
            seen['n'] += 1
            if seen['n'] == 1:
                return app.url
            return 'https://gw/pythonudfs/published-id/'

        def locate(_cur: Any) -> Tuple[Set[str], Set[str]]:
            return set(), set()

        app._show_create_service_url = show  # type: ignore[method-assign]
        app._locate_app_functions = locate  # type: ignore[method-assign]

        with self._patch_connect(executed):
            with self.assertRaises(RuntimeError):
                app.register_interactive_functions()
        self.assertEqual(executed, [])

    def test_register_interactive_drops_stale(self) -> None:
        app = self._interactive_app(['keep_test'])
        executed: List[str] = []

        def show(_cur: Any, _sql_name: str) -> Optional[str]:
            return None

        def locate(_cur: Any) -> Tuple[Set[str], Set[str]]:
            return {'`keep_test`', '`stale_test`'}, set()

        def owned(_cur: Any, qualified: str) -> Optional[str]:
            if 'stale_test' in qualified:
                return app.url
            return None

        app._show_create_service_url = show  # type: ignore[method-assign]
        app._locate_app_functions = locate  # type: ignore[method-assign]
        app._service_url_for_qualified = owned  # type: ignore[method-assign]

        with self._patch_connect(executed):
            app.register_interactive_functions()
        self.assertIn('DROP FUNCTION IF EXISTS `stale_test`', executed)
        self.assertTrue(
            any('CREATE' in sql and 'keep_test' in sql for sql in executed),
        )
        self.assertFalse(
            any('DROP' in sql and 'keep_test' in sql for sql in executed),
        )

    def test_register_interactive_skips_stolen_stale_drop(self) -> None:
        app = self._interactive_app(['keep_test'])
        executed: List[str] = []

        def show(_cur: Any, _sql_name: str) -> Optional[str]:
            return None

        def locate(_cur: Any) -> Tuple[Set[str], Set[str]]:
            return {'`keep_test`', '`stale_test`'}, set()

        def owned(_cur: Any, qualified: str) -> Optional[str]:
            if 'stale_test' in qualified:
                return 'https://gw/pythonudfs/published-id/'
            return None

        app._show_create_service_url = show  # type: ignore[method-assign]
        app._locate_app_functions = locate  # type: ignore[method-assign]
        app._service_url_for_qualified = owned  # type: ignore[method-assign]

        with self._patch_connect(executed):
            app.register_interactive_functions()
        self.assertFalse(any('DROP' in sql for sql in executed))
        self.assertTrue(
            any('CREATE' in sql and 'keep_test' in sql for sql in executed),
        )


class TestRunUdfAppInteractiveOrder(unittest.IsolatedAsyncioTestCase):

    def tearDown(self) -> None:
        from singlestoredb.apps import _python_udfs
        _python_udfs._running_server = None
        _python_udfs._running_server_task = None

    def _interactive_config(self) -> AppConfig:
        return AppConfig(
            listen_port=8000,
            base_url='https://example/',
            base_path='/',
            notebook_server_id='nb1',
            app_token='t',
            user_token=None,
            running_interactively=True,
            is_gateway_enabled=True,
            is_local_dev=False,
        )

    def _mock_app(self, order: List[str], *, refuse: bool) -> MagicMock:
        app = MagicMock()
        app.endpoints = {
            b'foo': (None, {'signature': {'name': 'foo_test'}}),
        }
        app.get_uvicorn_log_config.return_value = {}
        app.get_function_info.return_value = {}

        def preflight(*_a: Any, **_k: Any) -> None:
            order.append('preflight')
            if refuse:
                raise RuntimeError('stolen')

        def register(*_a: Any, **_k: Any) -> None:
            order.append('register')

        app.preflight_interactive_functions.side_effect = preflight
        app.register_interactive_functions.side_effect = register
        return app

    def _uvicorn_modules(self, server_cls: Any = None) -> Any:
        fake_util = MagicMock()
        if server_cls is not None:
            fake_util.AwaitableUvicornServer = server_cls
        return patch.dict(
            sys.modules,
            {
                'uvicorn': MagicMock(),
                'singlestoredb.apps._uvicorn_util': fake_util,
            },
        )

    async def test_preflight_failure_leaves_server_running(self) -> None:
        from singlestoredb.apps import _python_udfs
        from singlestoredb.apps._python_udfs import run_udf_app

        order: List[str] = []
        existing = MagicMock()

        async def shutdown() -> None:
            order.append('shutdown')

        existing.shutdown.side_effect = shutdown
        _python_udfs._running_server = existing
        app = self._mock_app(order, refuse=True)

        with self._uvicorn_modules():
            with patch.object(
                AppConfig, 'from_env', return_value=self._interactive_config(),
            ):
                with patch.object(
                    _python_udfs,
                    'generate_base_url',
                    return_value='https://gw/pythonudfs/sess/interactive/',
                ):
                    with patch.object(_python_udfs, 'Application', return_value=app):
                        with patch.object(
                            _python_udfs,
                            'kill_process_by_port',
                            side_effect=lambda _port: order.append('kill'),
                        ):
                            with self.assertRaises(RuntimeError):
                                await run_udf_app()
        self.assertEqual(order, ['preflight'])
        self.assertIs(_python_udfs._running_server, existing)

    async def test_preflight_then_shutdown_then_register(self) -> None:
        from singlestoredb.apps import _python_udfs
        from singlestoredb.apps._python_udfs import run_udf_app

        order: List[str] = []
        existing = MagicMock()
        existing.should_exit = False

        async def serve() -> None:
            while not existing.should_exit:
                await asyncio.sleep(0)
            order.append('shutdown')

        _python_udfs._running_server = existing
        _python_udfs._running_server_task = asyncio.create_task(serve())
        app = self._mock_app(order, refuse=False)

        class FakeServer:
            def __init__(self, _config: Any) -> None:
                pass

            async def serve(self) -> None:
                return None

            async def wait_for_startup(self) -> None:
                order.append('started')

        with self._uvicorn_modules(FakeServer):
            with patch.object(
                AppConfig, 'from_env', return_value=self._interactive_config(),
            ):
                with patch.object(
                    _python_udfs,
                    'generate_base_url',
                    return_value='https://gw/pythonudfs/sess/interactive/',
                ):
                    with patch.object(_python_udfs, 'Application', return_value=app):
                        with patch.object(
                            _python_udfs,
                            'kill_process_by_port',
                            side_effect=lambda _port: order.append('kill'),
                        ):
                            await run_udf_app()
        self.assertEqual(
            order,
            ['preflight', 'shutdown', 'kill', 'register', 'started'],
        )


_request_started = threading.Event()
_release_request = threading.Event()
_request_finished = threading.Event()


@udf
async def _draining_udf(value: float) -> float:
    _request_started.set()
    try:
        while not _release_request.is_set():
            await asyncio.sleep(0.01)
        return value * 2
    finally:
        _request_finished.set()


class TestUdfShutdown(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self) -> None:
        self.env = {
            'SINGLESTOREDB_APP_LISTEN_PORT': '0',
            'SINGLESTOREDB_APP_BASE_URL': 'http://127.0.0.1',
            'SINGLESTOREDB_APP_BASE_PATH': '/',
            'SINGLESTOREDB_NOTEBOOK_SERVER_ID': 'shutdown-test',
            'SINGLESTOREDB_IS_LOCAL_DEV': 'true',
            'SINGLESTOREDB_WORKLOAD_TYPE': 'PythonUdf',
            'SINGLESTOREDB_NOVA_GATEWAY_ENDPOINT': 'http://127.0.0.1',
            'SINGLESTOREDB_EXT_FUNCTIONS': __name__,
        }
        self.previous_env = {name: os.environ.get(name) for name in self.env}
        os.environ.update(self.env)
        _request_started.clear()
        _request_finished.clear()
        _release_request.set()
        self.connections: List[http.client.HTTPConnection] = []
        await run_udf_app(kill_existing_app_server=False)
        self.server = _python_udfs._running_server
        self.server_task = _python_udfs._running_server_task
        assert self.server is not None
        self.port = self.server.servers[0].sockets[0].getsockname()[1]

    async def asyncTearDown(self) -> None:
        _release_request.set()
        try:
            await asyncio.wait_for(shutdown_udf_app(timeout=0.5), 3)
        finally:
            for connection in self.connections:
                connection.close()
            for name, value in self.previous_env.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value

    async def _wait_for(self, predicate: Callable[[], bool]) -> None:
        async def wait() -> None:
            while not predicate():
                await asyncio.sleep(0.01)

        await asyncio.wait_for(wait(), 3)

    def _connect(self) -> http.client.HTTPConnection:
        connection = http.client.HTTPConnection('127.0.0.1', self.port, timeout=3)
        self.connections.append(connection)
        return connection

    async def _invoke(
        self, connection: http.client.HTTPConnection,
    ) -> Tuple[int, bytes]:
        def request() -> Tuple[int, bytes]:
            connection.request(
                'POST', '/invoke',
                json.dumps({'data': [[0, 2.125]]}),
                {
                    'Content-Type': 'application/json',
                    'Accepts': 'application/json',
                    's2-ef-name': '_draining_udf',
                    's2-ef-version': '1.0',
                },
            )
            response = connection.getresponse()
            return response.status, response.read()

        return await asyncio.to_thread(request)

    async def test_closes_idle_connections_without_waiting_for_budget(self) -> None:
        connection = self._connect()
        status, body = await self._invoke(connection)
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body), {'data': [[0, 4.25]]})
        sock = connection.sock
        assert sock is not None

        start = asyncio.get_running_loop().time()
        await asyncio.wait_for(shutdown_udf_app(timeout=30), 3)
        self.assertLess(asyncio.get_running_loop().time() - start, 3)
        self.assertEqual(await asyncio.to_thread(sock.recv, 1), b'')
        assert self.server_task is not None
        self.assertTrue(self.server_task.done())
        self.assertIsNone(_python_udfs._running_server)
        self.assertIsNone(_python_udfs._running_server_task)
        await shutdown_udf_app(timeout=0)

    async def test_drains_active_request_and_refuses_new_connections(self) -> None:
        idle = self._connect()
        self.assertEqual((await self._invoke(idle))[0], 200)
        idle_socket = idle.sock
        assert idle_socket is not None
        _request_started.clear()
        _release_request.clear()
        request = asyncio.create_task(self._invoke(self._connect()))
        await self._wait_for(_request_started.is_set)

        shutdown = asyncio.create_task(shutdown_udf_app(timeout=2))
        server = self.server
        assert server is not None
        await self._wait_for(
            lambda: all(not listener.is_serving() for listener in server.servers),
        )
        self.assertFalse(shutdown.done())
        self.assertEqual(await asyncio.to_thread(idle_socket.recv, 1), b'')
        with self.assertRaises(OSError):
            await asyncio.open_connection('127.0.0.1', self.port)

        _release_request.set()
        status, body = await asyncio.wait_for(request, 3)
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body), {'data': [[0, 4.25]]})
        await asyncio.wait_for(shutdown, 3)
        self.assertTrue(_request_finished.is_set())

    async def test_deadline_closes_over_budget_request(self) -> None:
        _release_request.clear()
        request = asyncio.create_task(self._invoke(self._connect()))
        await self._wait_for(_request_started.is_set)

        start = asyncio.get_running_loop().time()
        await asyncio.wait_for(shutdown_udf_app(timeout=0.1), 3)
        elapsed = asyncio.get_running_loop().time() - start
        self.assertGreaterEqual(elapsed, 0.1)
        self.assertLess(elapsed, 1)
        try:
            status, _ = await asyncio.wait_for(request, 3)
        except (
            http.client.RemoteDisconnected,
            http.client.IncompleteRead,
            ConnectionResetError,
        ):
            pass
        else:
            self.assertGreaterEqual(status, 500)
        await self._wait_for(_request_finished.is_set)
        server = self.server
        assert server is not None
        await self._wait_for(lambda: not server.server_state.connections)

    async def test_cancelled_waiter_does_not_cancel_drain(self) -> None:
        _release_request.clear()
        request = asyncio.create_task(self._invoke(self._connect()))
        await self._wait_for(_request_started.is_set)

        first = asyncio.create_task(shutdown_udf_app(timeout=2))
        server = self.server
        assert server is not None
        await self._wait_for(lambda: server.should_exit)
        first.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await first
        assert self.server_task is not None
        self.assertFalse(self.server_task.done())

        second = asyncio.create_task(shutdown_udf_app(timeout=0))
        await asyncio.sleep(0)
        self.assertEqual(server.config.timeout_graceful_shutdown, 2)
        _release_request.set()
        status, _ = await asyncio.wait_for(request, 3)
        self.assertEqual(status, 200)
        await asyncio.wait_for(second, 3)

    async def test_can_start_again_after_shutdown(self) -> None:
        await shutdown_udf_app(timeout=1)
        await run_udf_app(kill_existing_app_server=False)
        server = _python_udfs._running_server
        assert server is not None
        self.assertIsNot(server, self.server)
        self.port = server.servers[0].sockets[0].getsockname()[1]
        status, _ = await self._invoke(self._connect())
        self.assertEqual(status, 200)

    async def test_shutdown_while_starting_does_not_leave_listener_open(self) -> None:
        await shutdown_udf_app(timeout=1)
        startup = asyncio.create_task(run_udf_app(kill_existing_app_server=False))
        await asyncio.sleep(0)
        server = _python_udfs._running_server
        assert server is not None
        self.assertFalse(server.started)
        try:
            await asyncio.wait_for(shutdown_udf_app(timeout=0), 3)
            await startup
            self.assertTrue(all(not listener.is_serving() for listener in server.servers))
        finally:
            for listener in server.servers:
                listener.close()

    async def test_rejects_invalid_budget_without_stopping_server(self) -> None:
        for timeout in (-1, float('inf'), float('-inf'), float('nan')):
            with self.subTest(timeout=timeout):
                with self.assertRaises(ValueError):
                    await shutdown_udf_app(timeout=timeout)
        assert self.server is not None
        self.assertFalse(self.server.should_exit)
        status, _ = await self._invoke(self._connect())
        self.assertEqual(status, 200)


if __name__ == '__main__':
    unittest.main()
