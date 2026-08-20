#!/usr/bin/env python
# type: ignore
"""
Structural tests for the management API's version split.

These are the only versioning tests worth keeping now that the cross-version
bridge is gone: that the version-module importer reports failures usefully,
that the ``manage_*`` factories route to the right version package, and that
``management/v1/`` and ``management/v2/`` do not import each other -- the
invariant that makes deleting either one an ``rm -rf``.
"""
import ast
import contextlib
import importlib
import os
import sys
import unittest
import warnings
from unittest.mock import patch

from singlestoredb.exceptions import ManagementError
from singlestoredb.management._version_import import _import_versioned_module


FAKE_TOKEN = 'test-token-12345'
FAKE_BASE_URL = 'https://api.example.com'


@contextlib.contextmanager
def management_version(value):
    """Set the ``management.version`` option, restoring the exact original.

    ``conftest.py``'s ``protect_singlestoredb_url`` does not cover this
    option, and restoring with ``original or 'v1'`` would silently rewrite a
    ``None``/``''`` original into ``'v1'``.
    """
    from singlestoredb import config
    original = config.get_option('management.version')
    try:
        config.set_option('management.version', value)
        yield
    finally:
        config.set_option('management.version', original)


class TestImportVersionedModule(unittest.TestCase):
    """Test dynamic module import."""

    def test_import_v1_workspace(self):
        mod = _import_versioned_module('v1', 'workspace')
        self.assertTrue(hasattr(mod, 'Workspace'))
        self.assertTrue(hasattr(mod, 'WorkspaceManager'))

    def test_import_v2_cluster(self):
        """v2 has clusters, not workspaces."""
        mod = _import_versioned_module('v2', 'cluster')
        self.assertTrue(hasattr(mod, 'Cluster'))
        self.assertTrue(hasattr(mod, 'ClusterManager'))

    def test_v2_has_no_workspace_module(self):
        with self.assertRaises(ManagementError) as ctx:
            _import_versioned_module('v2', 'workspace')
        msg = str(ctx.exception)
        self.assertIn('workspace', msg)
        self.assertIn('v2', msg)

    def test_import_nonexistent_version_raises(self):
        with self.assertRaises(ManagementError) as ctx:
            _import_versioned_module('v99', 'workspace')
        self.assertIn('v99', str(ctx.exception))

    def test_import_nonexistent_module_raises(self):
        with self.assertRaises(ManagementError) as ctx:
            _import_versioned_module('v1', 'nonexistent_module')
        msg = str(ctx.exception)
        # Should NOT claim the version is unsupported when the version
        # package itself imports cleanly; should name the missing module.
        self.assertNotIn('Unsupported API version', msg)
        self.assertIn('nonexistent_module', msg)
        self.assertIn('v1', msg)


class TestConfigOption(unittest.TestCase):
    """Test that management.version config option exists and works."""

    def test_config_option_exists(self):
        from singlestoredb import config
        val = config.get_option('management.version')
        self.assertIn(val, ('v1', 'v2', None, ''))

    @patch('singlestoredb.management.manager.get_token', return_value=FAKE_TOKEN)
    def test_config_option_routes_manage_regions(self, _mock_token):
        """Setting management.version to v2 routes to v2."""
        from singlestoredb.management.region import manage_regions
        from singlestoredb.management.v2.region import RegionManager as V2RM

        with management_version('v2'):
            mgr = manage_regions(
                access_token=FAKE_TOKEN,
                base_url=FAKE_BASE_URL,
            )
            self.assertIsInstance(mgr, V2RM)

    @patch('singlestoredb.management.manager.get_token', return_value=FAKE_TOKEN)
    def test_config_option_does_not_reach_manage_workspaces(self, _mock_token):
        """
        A global preference for v2 must not break the v1-only workspace
        factory. Workspaces do not exist at v2, so the option has nothing to
        say about them; only an explicit ``version=`` is an error.
        """
        from singlestoredb.management.workspace import manage_workspaces
        from singlestoredb.management.v1.workspace import (
            WorkspaceManager as V1WM,
        )

        with management_version('v2'):
            mgr = manage_workspaces(
                access_token=FAKE_TOKEN,
                base_url=FAKE_BASE_URL,
            )
            self.assertIsInstance(mgr, V1WM)
            self.assertIn('/v1/', mgr._base_url)
            with self.assertRaises(ManagementError) as ctx:
                manage_workspaces(
                    access_token=FAKE_TOKEN,
                    base_url=FAKE_BASE_URL,
                    version='v2',
                )
            self.assertIn('manage_clusters', str(ctx.exception))

    def test_v1_manager_default_version_ignores_config(self):
        """
        ``default_version`` must not be frozen from the config option at
        import time -- that let a v1 class declare itself to be v2.
        """
        from singlestoredb.management.manager import Manager
        from singlestoredb.management.v1.workspace import WorkspaceManager
        from singlestoredb.management.files import FilesManager
        for cls in (Manager, WorkspaceManager, FilesManager):
            self.assertEqual(cls.default_version, 'v1', cls.__name__)


class TestManageRoutingForAllFactories(unittest.TestCase):
    """
    ``manage_*`` factories must route to the correct version module:
    ``version='v2'`` returns a v2 manager, default returns a v1 manager.
    """

    @patch('singlestoredb.management.manager.get_token', return_value=FAKE_TOKEN)
    def test_manage_workspaces(self, _mock_token):
        """Workspaces are v1-only; v2 callers are redirected to clusters."""
        from singlestoredb.management.workspace import manage_workspaces
        from singlestoredb.management.v1.workspace import (
            WorkspaceManager as V1WM,
        )

        with self.assertRaises(ManagementError):
            manage_workspaces(
                access_token=FAKE_TOKEN, base_url=FAKE_BASE_URL, version='v2',
            )
        v1 = manage_workspaces(
            access_token=FAKE_TOKEN, base_url=FAKE_BASE_URL, version='v1',
        )
        self.assertIsInstance(v1, V1WM)
        # default (no explicit version) falls back to v1 unless config overrides
        with management_version('v1'):
            default = manage_workspaces(
                access_token=FAKE_TOKEN, base_url=FAKE_BASE_URL,
            )
            self.assertIsInstance(default, V1WM)

    @patch('singlestoredb.management.manager.get_token', return_value=FAKE_TOKEN)
    def test_manage_clusters(self, _mock_token):
        """Clusters are v2-only, so ``manage_clusters`` defaults to v2."""
        from singlestoredb.management.cluster import manage_clusters
        from singlestoredb.management.v2.cluster import ClusterManager as V2CM

        v2 = manage_clusters(
            access_token=FAKE_TOKEN, base_url=FAKE_BASE_URL, version='v2',
        )
        self.assertIsInstance(v2, V2CM)
        default = manage_clusters(
            access_token=FAKE_TOKEN, base_url=FAKE_BASE_URL,
        )
        self.assertIsInstance(default, V2CM)
        self.assertIn('/v2/', default._base_url)
        with self.assertRaises(ManagementError):
            manage_clusters(
                access_token=FAKE_TOKEN, base_url=FAKE_BASE_URL, version='v1',
            )

    @patch('singlestoredb.management.manager.get_token', return_value=FAKE_TOKEN)
    def test_manage_regions(self, _mock_token):
        from singlestoredb.management.region import manage_regions
        from singlestoredb.management.v1.region import RegionManager as V1RM
        from singlestoredb.management.v2.region import RegionManager as V2RM

        self.assertIsInstance(
            manage_regions(
                access_token=FAKE_TOKEN, base_url=FAKE_BASE_URL, version='v2',
            ),
            V2RM,
        )
        self.assertIsInstance(
            manage_regions(
                access_token=FAKE_TOKEN, base_url=FAKE_BASE_URL, version='v1',
            ),
            V1RM,
        )

    @patch('singlestoredb.management.manager.get_token', return_value=FAKE_TOKEN)
    def test_manage_files(self, _mock_token):
        from singlestoredb.management.files import manage_files

        # The Files API is unchanged at v2, so both versions share one
        # ``FilesManager`` class; the version shows up only in the base URL.
        for ver in ('v1', 'v2'):
            mgr = manage_files(
                access_token=FAKE_TOKEN, base_url=FAKE_BASE_URL, version=ver,
            )
            self.assertTrue(
                mgr._base_url.endswith(f'/{ver}/'),
                f'expected base URL to end with /{ver}/, got {mgr._base_url}',
            )


class TestManageWorkspacesDeprecation(unittest.TestCase):
    """
    ``manage_workspaces()`` warns, but the internal v1-only path does not.

    Fusion, the UDF ``stage://`` handling and the AI helpers are v1-only by
    design, so they go through ``_manage_workspaces_v1`` -- warning there would
    be noise the caller can do nothing about.
    """

    @patch('singlestoredb.management.manager.get_token', return_value=FAKE_TOKEN)
    def test_public_factory_warns(self, _mock_token):
        from singlestoredb.management.workspace import manage_workspaces
        with self.assertWarns(DeprecationWarning) as ctx:
            manage_workspaces(
                access_token=FAKE_TOKEN, base_url=FAKE_BASE_URL,
            )
        self.assertIn('manage_clusters', str(ctx.warning))

    @patch('singlestoredb.management.manager.get_token', return_value=FAKE_TOKEN)
    def test_internal_path_is_silent(self, _mock_token):
        from singlestoredb.management.workspace import _manage_workspaces_v1
        with warnings.catch_warnings():
            warnings.simplefilter('error', DeprecationWarning)
            _manage_workspaces_v1(
                access_token=FAKE_TOKEN, base_url=FAKE_BASE_URL,
            )


class TestFactoriesAreNotDuplicated(unittest.TestCase):
    """
    The ``manage_*`` factories must live in exactly one place.

    They are version-neutral -- they take ``version`` as an argument and
    dispatch -- so duplicating them into ``v1/`` (as an earlier layout did)
    both invites the two copies to drift and makes ``v1/`` un-deletable.
    """

    def test_factories_defined_only_at_top_level(self):
        factories = {
            'manage_files': 'files',
            'manage_regions': 'region',
            'manage_workspaces': 'workspace',
            'manage_clusters': 'cluster',
        }
        for func, mod_name in factories.items():
            shared = importlib.import_module(f'singlestoredb.management.{mod_name}')
            self.assertTrue(
                callable(getattr(shared, func, None)),
                f'{func} should be defined in management/{mod_name}.py',
            )
            for ver in ('v1', 'v2'):
                try:
                    mod = importlib.import_module(
                        f'singlestoredb.management.{ver}.{mod_name}',
                    )
                except ModuleNotFoundError:
                    # Not every resource exists at every version; e.g. there
                    # is no v2 ``workspace`` module.
                    continue
                self.assertNotIn(
                    func, vars(mod),
                    f'{func} must not be duplicated into '
                    f'management/{ver}/{mod_name}.py',
                )


class TestVersionPackagesAreIndependent(unittest.TestCase):
    """
    Guard the invariant that makes either version package removable.

    The v1 endpoints will eventually be abandoned, at which point
    ``management/v1/`` should be deletable by ``rm -rf`` plus removal of the
    back-compat shims. That only holds while ``management/v1/`` and
    ``management/v2/`` do not import each other, in either direction:
    version-neutral code belongs in the shared top-level ``management/``
    modules, which both version packages import sideways.

    If this test fails, the fix is to move the shared code up to
    ``management/`` -- not to add a cross-version import.
    """

    def test_version_packages_extend_the_shared_base(self):
        """
        Inheritance runs shared base -> version subclass, never v1 -> v2.

        ``RegionManager`` is the representative case: the base carries the v2
        behavior and ``v1/`` holds the backward override, so ``v2/`` is a
        plain re-export.
        """
        from singlestoredb.management.region import RegionManager as Base
        from singlestoredb.management.v1.region import RegionManager as V1
        from singlestoredb.management.v2.region import RegionManager as V2
        self.assertTrue(issubclass(V1, Base))
        self.assertIs(V2, Base)
        self.assertFalse(issubclass(V2, V1))

    def _module_paths(self, version):
        pkg = importlib.import_module(f'singlestoredb.management.{version}')
        pkg_dir = os.path.dirname(pkg.__file__)
        return sorted(
            os.path.join(pkg_dir, f)
            for f in os.listdir(pkg_dir)
            if f.endswith('.py')
        )

    def _cross_version_imports(self, version, other):
        """Return every import of ``other`` found in ``version``'s modules."""
        offenders = []
        for path in self._module_paths(version):
            with open(path) as f:
                tree = ast.parse(f.read(), filename=path)
            for node in ast.walk(tree):
                # Relative ``from ..v2.x import y`` shows up as level=2 with
                # module='v2.x'; absolute imports show up with the full path.
                if isinstance(node, ast.ImportFrom):
                    mod = node.module or ''
                    if mod == other or mod.startswith(f'{other}.') or \
                            f'management.{other}' in mod:
                        offenders.append(
                            f'{os.path.basename(path)}:{node.lineno}: '
                            f'from {"." * node.level}{mod}',
                        )
                elif isinstance(node, ast.Import):
                    for alias in node.names:
                        if f'management.{other}' in alias.name:
                            offenders.append(
                                f'{os.path.basename(path)}:{node.lineno}: '
                                f'import {alias.name}',
                            )
        return offenders

    def test_no_v2_module_imports_from_v1(self):
        """No module under management/v2/ may import from management/v1/."""
        offenders = self._cross_version_imports('v2', 'v1')
        self.assertEqual(
            offenders, [],
            'management/v2/ must not import from management/v1/; move the '
            'shared code up to management/ instead:\n  ' +
            '\n  '.join(offenders),
        )

    def test_no_v1_module_imports_from_v2(self):
        """No module under management/v1/ may import from management/v2/."""
        offenders = self._cross_version_imports('v1', 'v2')
        self.assertEqual(
            offenders, [],
            'management/v1/ must not import from management/v2/; move the '
            'shared code up to management/ instead:\n  ' +
            '\n  '.join(offenders),
        )

    def _assert_imports_survive_removal(self, version, other):
        """Import every module of ``version`` with ``other`` blocked."""
        names = [
            f'singlestoredb.management.{version}.' + os.path.basename(p)[:-3]
            for p in self._module_paths(version)
            if not os.path.basename(p).startswith('__')
        ]
        blocked_prefix = f'singlestoredb.management.{other}'

        # Drop anything already imported so the blocker actually gets
        # consulted, then forbid the other version package outright.
        saved = {
            k: v for k, v in sys.modules.items()
            if k.startswith(blocked_prefix) or k in names
        }
        for k in saved:
            del sys.modules[k]

        class _Blocker:
            def find_module(self, fullname, path=None):
                return self.find_spec(fullname, path)

            def find_spec(self, fullname, path=None, target=None):
                if fullname.startswith(blocked_prefix):
                    raise AssertionError(
                        f'{version} import chain reached {fullname}; '
                        f'{other} is supposed to be removable',
                    )
                return None

        blocker = _Blocker()
        sys.meta_path.insert(0, blocker)
        try:
            for name in names:
                importlib.import_module(name)
        finally:
            sys.meta_path.remove(blocker)
            sys.modules.update(saved)

    def test_v2_imports_survive_v1_removal(self):
        """Importing every v2 module works with management.v1 blocked."""
        self._assert_imports_survive_removal('v2', 'v1')

    def test_v1_imports_survive_v2_removal(self):
        """Importing every v1 module works with management.v2 blocked."""
        self._assert_imports_survive_removal('v1', 'v2')


if __name__ == '__main__':
    unittest.main()
