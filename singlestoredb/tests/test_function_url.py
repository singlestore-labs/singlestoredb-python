# type: ignore
import sys
import unittest
from collections.abc import Iterator
from typing import Any
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple
from unittest.mock import MagicMock
from unittest.mock import patch

from singlestoredb.apps._config import AppConfig
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

        async def shutdown() -> None:
            order.append('shutdown')

        existing.shutdown.side_effect = shutdown
        _python_udfs._running_server = existing
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


if __name__ == '__main__':
    unittest.main()
