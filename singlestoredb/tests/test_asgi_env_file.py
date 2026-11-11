"""Tests for loading a zipped environment file through ``asgi.main()``."""
import os
import sys
import tempfile
import textwrap
import unittest
import uuid
import zipfile
from types import ModuleType
from typing import Optional
from unittest.mock import MagicMock
from unittest.mock import patch

from ..functions.ext import asgi


GOOD_MODULE = textwrap.dedent('''
    from singlestoredb.functions import udf

    @udf
    def env_file_double(x: int) -> int:
        return x * 2
''')

FAILING_MODULE = textwrap.dedent('''
    raise RuntimeError('environment init failed')
''')


class TestEnvironmentFile(unittest.TestCase):
    """``main()`` imports the module inside a zip passed as the function source.

    Python 3.15 removed ``zipimporter.load_module()``, which this path used
    to rely on. uvicorn is mocked out, so no server is started.
    """

    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        # A unique name per test, so no module cached in sys.modules from
        # an earlier test can satisfy the import
        self.modname = f'envfile_{uuid.uuid4().hex}'
        self.addCleanup(sys.modules.pop, self.modname, None)
        self.cached_module: Optional[ModuleType] = None

    def _make_env(self, source: str) -> str:
        path = os.path.join(self.tmpdir.name, f'{self.modname}.zip')
        with zipfile.ZipFile(path, 'w') as zf:
            zf.writestr(f'{self.modname}/__init__.py', source)
        return path

    def _run_main(self, env_path: str) -> MagicMock:
        """Run ``main()`` on the environment file and return the mocked uvicorn.

        What ``sys.modules`` holds for the environment module is saved to
        ``self.cached_module`` before ``patch.dict`` exits, since restoring
        ``sys.modules`` would otherwise drop the entry. It is saved even when
        ``main()`` raises.

        """
        uvicorn = MagicMock()
        with patch.dict(sys.modules, {'uvicorn': uvicorn}):
            try:
                # An empty --db skips registering the functions with a database
                asgi.main(['--db', '', env_path])
            finally:
                self.cached_module = sys.modules.get(self.modname)
        return uvicorn

    def test_module_is_loaded_and_functions_found(self) -> None:
        uvicorn = self._run_main(self._make_env(GOOD_MODULE))

        uvicorn.run.assert_called_once()
        app = uvicorn.run.call_args.args[0]
        names = [info['signature']['name'] for _, info in app.endpoints.values()]
        self.assertEqual(names, ['env_file_double'])

        mod = self.cached_module
        assert mod is not None
        self.assertEqual(mod.env_file_double(21), 42)
        self.assertTrue(str(mod.__file__).startswith(self.tmpdir.name))

    def test_failed_import_is_not_cached(self) -> None:
        with self.assertRaisesRegex(RuntimeError, 'environment init failed'):
            self._run_main(self._make_env(FAILING_MODULE))

        self.assertIsNone(self.cached_module)


if __name__ == '__main__':
    unittest.main()
