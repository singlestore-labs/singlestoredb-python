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
import subprocess
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
    def test_the_option_does_not_reach_manage_workspaces(self, _mock_token):
        """
        Neither workspace factory consults the option -- v1 keeps working.

        Workspaces exist only at v1, so there is nothing for the option to
        select between. Flipping the default to v2 must not turn a bare
        ``manage_workspaces()`` into an error: that would be v1 ceasing to work
        rather than v1 being deprecated. The deprecation warning is what steers
        callers to clusters.
        """
        from singlestoredb.management.workspace import manage_workspaces
        from singlestoredb.management.workspace import _manage_workspaces_v1
        from singlestoredb.management.v1.workspace import (
            WorkspaceManager as V1WM,
        )

        for option in ('v1', 'v2', None):
            with self.subTest(option=option), management_version(option):
                for label, factory in (
                    ('public', manage_workspaces),
                    ('internal', _manage_workspaces_v1),
                ):
                    with self.subTest(factory=label):
                        mgr = factory(
                            access_token=FAKE_TOKEN,
                            base_url=FAKE_BASE_URL,
                        )
                        self.assertIsInstance(mgr, V1WM)
                        self.assertIn('/v1/', mgr._base_url)

    @patch('singlestoredb.management.manager.get_token', return_value=FAKE_TOKEN)
    def test_manage_workspaces_still_rejects_an_explicit_other_version(
        self, _mock_token,
    ):
        """Pinning to v1 is not the same as ignoring the argument."""
        from singlestoredb.management.workspace import manage_workspaces
        with self.assertRaises(ManagementError) as ctx:
            manage_workspaces(
                access_token=FAKE_TOKEN, base_url=FAKE_BASE_URL, version='v2',
            )
        self.assertIn('manage_clusters', str(ctx.exception))

    def test_default_version_is_a_literal_not_the_config_option(self):
        """
        ``default_version`` must not be frozen from the config option at
        import time -- that let a v1 class declare itself to be v2.

        ``Manager`` takes the shared ``DEFAULT_VERSION`` and ``FilesManager``
        inherits it; ``WorkspaceManager`` is a v1 class and pins itself.
        Setting the option must move none of them.
        """
        from singlestoredb.management.manager import Manager
        from singlestoredb.management.v1.workspace import WorkspaceManager
        from singlestoredb.management.files import FilesManager
        expected = {Manager: 'v2', FilesManager: 'v2', WorkspaceManager: 'v1'}
        for value in ('v1', 'v2', None):
            with management_version(value):
                for cls, want in expected.items():
                    self.assertEqual(cls.default_version, want, cls.__name__)

    def test_default_version_ignores_the_environment_variable(self):
        """
        The same guard for ``SINGLESTOREDB_MANAGEMENT_VERSION``, which the
        in-process check above cannot reach: the option's *registered default*
        absorbs the environment variable at import
        (``utils/config.py``, ``Option.__init__``), so resolving
        ``default_version`` through ``config.get_default()`` would hand a v2
        class a v1 URL whenever the variable was set. A fresh interpreter is
        the only way to see it.
        """
        script = (
            'from singlestoredb.management.manager import Manager;'
            'from singlestoredb.management.files import FilesManager;'
            'from singlestoredb.management import _version_import as vi;'
            'from singlestoredb import config;'
            'print(Manager.default_version, FilesManager.default_version,'
            ' vi.DEFAULT_VERSION, config.get_option("management.version"))'
        )
        env = dict(os.environ, SINGLESTOREDB_MANAGEMENT_VERSION='v1')
        out = subprocess.run(
            [sys.executable, '-c', script],
            env=env, capture_output=True, text=True, check=True,
        ).stdout.split()
        # The option follows the variable; the class attributes do not.
        self.assertEqual(out, ['v2', 'v2', 'v2', 'v1'])


class TestManageRoutingForAllFactories(unittest.TestCase):
    """
    ``manage_*`` factories must route to the correct version module:
    ``version='v2'`` returns a v2 manager, default returns a v1 manager.
    """

    @patch('singlestoredb.management.manager.get_token', return_value=FAKE_TOKEN)
    def test_manage_workspaces(self, _mock_token):
        """Workspaces are v1-only; an explicit v2 is refused, v1 still works."""
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
        # A bare call is pinned to v1 rather than resolved through the option,
        # so flipping the default to v2 left it working. Covered in full by
        # TestConfigOption.test_the_option_does_not_reach_manage_workspaces.
        for option in ('v1', None):
            with management_version(option):
                self.assertIsInstance(
                    manage_workspaces(
                        access_token=FAKE_TOKEN, base_url=FAKE_BASE_URL,
                    ),
                    V1WM,
                )

    @patch('singlestoredb.management.manager.get_token', return_value=FAKE_TOKEN)
    def test_manage_clusters(self, _mock_token):
        """
        ``manage_clusters`` follows ``management.version``.

        Clusters are v2-only, so a resolved ``v1`` raises whether it came from
        the caller or from the option. The option defaults to ``v2`` now; the
        live v2 suites still pass ``version='v2'`` explicitly so that they do
        not start testing v1 if the option is ever pointed back.
        """
        from singlestoredb.management.cluster import manage_clusters
        from singlestoredb.management.cluster import DEFAULT_CLUSTER_VERSION
        from singlestoredb.management.v2.cluster import ClusterManager as V2CM

        v2 = manage_clusters(
            access_token=FAKE_TOKEN, base_url=FAKE_BASE_URL, version='v2',
        )
        self.assertIsInstance(v2, V2CM)
        self.assertIn('/v2/', v2._base_url)

        # The option is followed, and beats DEFAULT_CLUSTER_VERSION.
        with management_version('v2'):
            default = manage_clusters(
                access_token=FAKE_TOKEN, base_url=FAKE_BASE_URL,
            )
            self.assertIsInstance(default, V2CM)
            self.assertIn('/v2/', default._base_url)

        # Unset option: DEFAULT_CLUSTER_VERSION is the fallback.
        self.assertEqual(DEFAULT_CLUSTER_VERSION, 'v2')
        with management_version(None):
            self.assertIsInstance(
                manage_clusters(
                    access_token=FAKE_TOKEN, base_url=FAKE_BASE_URL,
                ),
                V2CM,
            )

        # v1 raises, whether asked for outright...
        with self.assertRaises(ManagementError):
            manage_clusters(
                access_token=FAKE_TOKEN, base_url=FAKE_BASE_URL, version='v1',
            )
        # ...or inherited from the option.
        with management_version('v1'):
            with self.assertRaises(ManagementError):
                manage_clusters(
                    access_token=FAKE_TOKEN, base_url=FAKE_BASE_URL,
                )
            # An explicit v2 still overrides it.
            self.assertIsInstance(
                manage_clusters(
                    access_token=FAKE_TOKEN, base_url=FAKE_BASE_URL,
                    version='v2',
                ),
                V2CM,
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


class TestVersionNeutralHelpers(unittest.TestCase):
    """
    ``get_organization``/``get_secret``/``get_stage`` exported from
    ``singlestoredb.management`` follow ``management.version`` like the
    factories do, instead of being the v1 implementations under a neutral name.
    The version-locked ones remain reachable through the shim modules.
    """

    def test_top_level_names_are_the_neutral_ones(self):
        import singlestoredb.management as m
        self.assertEqual(
            m.get_organization.__module__,
            'singlestoredb.management.organization',
        )
        self.assertEqual(
            m.get_secret.__module__,
            'singlestoredb.management.organization',
        )
        self.assertEqual(
            m.get_stage.__module__, 'singlestoredb.management.stage',
        )

    def test_shims_still_expose_their_own_version(self):
        from singlestoredb.management import cluster, workspace
        for name in ('get_organization', 'get_secret', 'get_stage'):
            self.assertEqual(
                getattr(workspace, name).__module__,
                'singlestoredb.management.v1.workspace', name,
            )
            self.assertEqual(
                getattr(cluster, name).__module__,
                'singlestoredb.management.v2.cluster', name,
            )

    def test_helpers_dispatch_on_the_option(self):
        from singlestoredb.management import get_organization
        from singlestoredb.management import get_secret
        from singlestoredb.management import get_stage
        calls = []
        for ver in ('v1', 'v2'):
            with management_version(ver):
                for name, call, expected in (
                    ('get_organization', lambda: get_organization(), ()),
                    ('get_secret', lambda: get_secret('s'), ('s',)),
                    ('get_stage', lambda: get_stage('d'), ('d',)),
                ):
                    target = f'singlestoredb.management.{ver}.{name}'

                    def record(*args, _n=name, _v=ver):
                        calls.append((_v, _n, args))
                        return 'ok'

                    with patch(target, record):
                        self.assertEqual(call(), 'ok')
                    self.assertEqual(calls[-1], (ver, name, expected))
        self.assertEqual(len(calls), 6)

    def test_explicit_version_beats_the_option(self):
        from singlestoredb.management import get_organization
        with management_version('v1'):
            with patch(
                'singlestoredb.management.v2.get_organization',
                lambda: 'from-v2',
            ):
                self.assertEqual(get_organization(version='v2'), 'from-v2')

    def test_unknown_version_raises(self):
        from singlestoredb.management import get_organization
        with self.assertRaises(ManagementError) as ctx:
            get_organization(version='v99')
        self.assertIn('v99', str(ctx.exception))

    def test_version_without_the_helper_raises(self):
        from singlestoredb.management._version_import import _versioned_attr
        with self.assertRaises(ManagementError) as ctx:
            _versioned_attr('get_nothing', 'v1')
        msg = str(ctx.exception)
        self.assertIn('get_nothing', msg)
        self.assertIn('v1', msg)


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
            # Pinned so the assertion is about the warning rather than about
            # whatever version the ambient option happens to name.
            manage_workspaces(
                access_token=FAKE_TOKEN, base_url=FAKE_BASE_URL, version='v1',
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


class TestDeprecatedVersionWarning(unittest.TestCase):
    """
    Every public version-neutral entry point warns when it resolves to v1.

    v1 is being wound down, so a caller who lands on it -- whether by passing
    ``version='v1'`` or by inheriting it from the ``management.version``
    option -- has to be told. The warning fires after resolution rather than in
    ``_resolve_version``, so both routes are covered and the internal v1-only
    paths stay silent (see :class:`TestManageWorkspacesDeprecation`).
    """

    # (label, callable taking a version kwarg). Each is a public entry point
    # that can resolve to v1; ``manage_clusters`` is absent because v1 has no
    # clusters and it raises instead, and ``manage_workspaces`` because it
    # raises its own more specific warning, asserted separately below.
    def _entry_points(self):
        import singlestoredb as s2
        from singlestoredb.management import get_organization
        from singlestoredb.management import get_secret
        from singlestoredb.management import get_stage
        return [
            (
                'manage_files', lambda **kw: s2.manage_files(
                    access_token=FAKE_TOKEN, base_url=FAKE_BASE_URL, **kw,
                ),
            ),
            (
                'manage_regions', lambda **kw: s2.manage_regions(
                    access_token=FAKE_TOKEN, base_url=FAKE_BASE_URL, **kw,
                ),
            ),
            # The three helpers dispatch through _versioned_attr, so they are
            # patched out: the assertion is about the warning, not the route.
            ('get_organization', lambda **kw: get_organization(**kw)),
            ('get_secret', lambda **kw: get_secret('s', **kw)),
            ('get_stage', lambda **kw: get_stage('d', **kw)),
        ]

    @contextlib.contextmanager
    def _stubbed_helpers(self):
        """Stub the three version-package helpers at both versions."""
        with contextlib.ExitStack() as stack:
            for ver in ('v1', 'v2'):
                for name in ('get_organization', 'get_secret', 'get_stage'):
                    stack.enter_context(
                        patch(
                            f'singlestoredb.management.{ver}.{name}',
                            lambda *a: 'ok',
                            create=True,
                        ),
                    )
            yield

    @patch('singlestoredb.management.manager.get_token', return_value=FAKE_TOKEN)
    def test_explicit_v1_warns(self, _mock_token):
        with self._stubbed_helpers():
            for label, call in self._entry_points():
                with self.subTest(entry_point=label):
                    with self.assertWarns(DeprecationWarning) as ctx:
                        call(version='v1')
                    msg = str(ctx.warning)
                    self.assertIn('v1', msg)
                    self.assertIn('deprecated', msg)

    @patch('singlestoredb.management.manager.get_token', return_value=FAKE_TOKEN)
    def test_v1_inherited_from_the_option_warns(self, _mock_token):
        """A caller who never names a version still gets told."""
        with self._stubbed_helpers(), management_version('v1'):
            for label, call in self._entry_points():
                with self.subTest(entry_point=label):
                    with self.assertWarns(DeprecationWarning) as ctx:
                        call()
                    self.assertIn('deprecated', str(ctx.warning))

    @patch('singlestoredb.management.manager.get_token', return_value=FAKE_TOKEN)
    def test_v2_is_silent(self, _mock_token):
        """The default version must not warn -- otherwise nobody reads any of them."""
        with self._stubbed_helpers(), management_version('v2'):
            for label, call in self._entry_points() + [
                (
                    'manage_clusters', lambda **kw: __import__(
                        'singlestoredb',
                    ).manage_clusters(
                        access_token=FAKE_TOKEN, base_url=FAKE_BASE_URL, **kw,
                    ),
                ),
            ]:
                with self.subTest(entry_point=label):
                    with warnings.catch_warnings():
                        warnings.simplefilter('error', DeprecationWarning)
                        call()

    @patch('singlestoredb.management.manager.get_token', return_value=FAKE_TOKEN)
    def test_v1_still_works(self, _mock_token):
        """
        Deprecated must not mean broken. This is the point of the whole set.

        v2 is the default, but v1 is still a supported version: every entry
        point must return a working v1 object, and none may raise merely
        because the default moved. Warnings are the only consequence.
        """
        import singlestoredb as s2
        from singlestoredb.management.workspace import manage_workspaces
        with self._stubbed_helpers(), warnings.catch_warnings():
            warnings.simplefilter('ignore', DeprecationWarning)
            for label, call in self._entry_points():
                with self.subTest(entry_point=label):
                    self.assertIsNotNone(call(version='v1'))
            # The v1 routes really are v1 routes, not v2 ones relabelled.
            for label, factory in (
                ('manage_files', s2.manage_files),
                ('manage_regions', s2.manage_regions),
                ('manage_workspaces', manage_workspaces),
            ):
                with self.subTest(factory=label):
                    mgr = factory(
                        access_token=FAKE_TOKEN,
                        base_url=FAKE_BASE_URL,
                        version='v1',
                    )
                    self.assertIn('/v1/', mgr._base_url)

    def test_the_deprecated_version_is_not_the_default(self):
        """Guards the pair: whatever DEPRECATED_VERSION names cannot be the default."""
        from singlestoredb import config
        from singlestoredb.management import _version_import as vi
        self.assertNotEqual(vi.DEPRECATED_VERSION, vi.DEFAULT_VERSION)
        self.assertEqual(vi.DEFAULT_VERSION, 'v2')
        self.assertNotEqual(
            config.get_default('management.version'), vi.DEPRECATED_VERSION,
        )

    @patch('singlestoredb.management.manager.get_token', return_value=FAKE_TOKEN)
    def test_manage_workspaces_warns_once_not_twice(self, _mock_token):
        """
        ``manage_workspaces()`` is the one v1 entry point with its own message.

        It reaches v1 through ``_manage_workspaces_v1``, which is deliberately
        silent, so the caller gets exactly one warning -- the specific one
        naming ``manage_clusters`` -- rather than that plus the generic
        "v1 is deprecated".
        """
        from singlestoredb.management.workspace import manage_workspaces
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            manage_workspaces(
                access_token=FAKE_TOKEN, base_url=FAKE_BASE_URL, version='v1',
            )
        deprecations = [
            w for w in caught if issubclass(w.category, DeprecationWarning)
        ]
        self.assertEqual(len(deprecations), 1, [str(w.message) for w in deprecations])
        self.assertIn('manage_clusters', str(deprecations[0].message))


class TestV1IsDocumentedAsDeprecated(unittest.TestCase):
    """
    Every module under ``management/v1/`` carries a deprecation note.

    A docstring check rather than a runtime one because most of these are
    classes built by ``from_dict`` deep in the library, where a warning would
    be noise the caller cannot act on. The note is what a reader of the API
    docs and of an IDE tooltip actually sees.
    """

    #: ``inference/*`` has no v2 counterpart, so there is nowhere to send
    #: callers and deprecating it would be a lie. Its docstring says so
    #: explicitly, which the test below checks instead.
    NOT_DEPRECATED = {'inference_api'}

    def _v1_modules(self):
        import singlestoredb.management.v1 as v1
        directory = os.path.dirname(v1.__file__)
        return sorted(
            name[:-3] for name in os.listdir(directory)
            if name.endswith('.py') and name != '__init__.py'
        )

    def test_every_v1_module_says_it_is_deprecated(self):
        modules = self._v1_modules()
        self.assertTrue(modules, 'found no modules under management/v1/')
        for name in modules:
            if name in self.NOT_DEPRECATED:
                continue
            with self.subTest(module=name):
                mod = importlib.import_module(f'singlestoredb.management.v1.{name}')
                self.assertIsNotNone(mod.__doc__, f'v1/{name}.py has no docstring')
                self.assertIn('deprecated', mod.__doc__.lower())

    def test_the_v1_package_itself_says_it_is_deprecated(self):
        import singlestoredb.management.v1 as v1
        self.assertIn('deprecated', v1.__doc__.lower())

    def test_the_workspace_shim_says_it_is_deprecated(self):
        from singlestoredb.management import workspace
        self.assertIn('deprecated', workspace.__doc__.lower())

    def test_the_inference_api_explains_why_it_is_exempt(self):
        """The exemption must be justified in the module, not just in this test."""
        from singlestoredb.management.v1 import inference_api
        doc = inference_api.__doc__.lower()
        self.assertIn('not** deprecated', doc)
        # The reason: these routes are served nowhere else, so there is no
        # replacement to send callers to.
        self.assertIn('nowhere else', doc)

    def test_v1_only_classes_name_their_v2_replacement(self):
        """
        The v1 classes that v2 genuinely replaced carry their own note.

        Restricted to classes actually defined under ``v1/``: the modules that
        only re-export a shared implementation (``files``, ``region``,
        ``billing_usage``) must *not* grow a class-level note, because that
        note would show up on the v2 class too.
        """
        from singlestoredb.management.v1 import export
        from singlestoredb.management.v1 import job
        from singlestoredb.management.v1 import organization
        from singlestoredb.management.v1 import stage
        from singlestoredb.management.v1 import workspace
        expected = [
            (workspace.Workspace, 'cluster.Cluster'),
            (workspace.WorkspaceGroup, 'cluster.Cluster'),
            (workspace.StarterWorkspace, 'cluster.StarterCluster'),
            (workspace.WorkspaceManager, 'cluster.ClusterManager'),
            (stage.Stage, 'management.stage.Stage'),
            (job.JobsManager, 'management.job.JobsManager'),
            (organization.Organization, 'management.organization.Organization'),
            (organization.Organizations, 'management.organization.Organizations'),
            (export.ExportService, 'management.export.ExportService'),
            (export.ExportStatus, 'management.export.ExportStatus'),
        ]
        for cls, replacement in expected:
            with self.subTest(cls=cls.__name__):
                doc = cls.__doc__ or ''
                self.assertIn('.. deprecated::', doc)
                self.assertIn(replacement, doc)

    def test_shared_classes_are_not_marked_deprecated(self):
        """
        ``v1/files.py`` and friends re-export the shared classes.

        Marking those classes deprecated would tell v2 users their own classes
        are going away, so only the v1 *module path* carries the note.
        """
        from singlestoredb.management.v1 import billing_usage as v1_billing
        from singlestoredb.management.v1 import files as v1_files
        from singlestoredb.management.v1 import region as v1_region
        for mod, names in (
            (v1_files, ('FilesManager', 'FilesObject')),
            (v1_region, ('Region', 'RegionManager')),
            (v1_billing, ('BillingUsageItem', 'UsageItem')),
        ):
            for name in names:
                with self.subTest(cls=f'{mod.__name__}.{name}'):
                    cls = getattr(mod, name)
                    self.assertNotIn('.. deprecated::', cls.__doc__ or '')
                    # ...and it really is the shared class, not a v1 subclass.
                    self.assertFalse(
                        cls.__module__.startswith('singlestoredb.management.v1'),
                        f'{name} is defined under v1/, so the note above '
                        'would be correct and this test is wrong',
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

        ``Organization`` is the representative case: the base carries the v2
        behavior and ``v1/`` holds the backward override -- repointing the job
        and inference sub-managers -- so ``v2/`` is a plain re-export.

        ``RegionManager`` used to play this role, but no longer can: once
        ``regions/sharedtier`` was found to answer at both versions the v1
        override collapsed into a re-export, making ``V1 is V2 is Base`` and
        the ``issubclass(V2, V1)`` assertion vacuously wrong.
        """
        from singlestoredb.management.organization import (
            Organization as Base,
        )
        from singlestoredb.management.v1.organization import (
            Organization as V1,
        )
        from singlestoredb.management.v2.organization import (
            Organization as V2,
        )
        self.assertTrue(issubclass(V1, Base))
        self.assertIsNot(V1, Base)
        self.assertIs(V2, Base)
        self.assertFalse(issubclass(V2, V1))

    def test_a_version_package_that_only_re_exports_shares_the_base(self):
        """
        A version with no behavioral difference re-exports, not subclasses.

        Both ``region`` modules are now pure re-exports, so all three names
        are the same object. Asserted explicitly so that reintroducing a
        subclass on one side has to be a deliberate edit to this test.
        """
        from singlestoredb.management.region import RegionManager as Base
        from singlestoredb.management.v1.region import RegionManager as V1
        from singlestoredb.management.v2.region import RegionManager as V2
        self.assertIs(V1, Base)
        self.assertIs(V2, Base)

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
