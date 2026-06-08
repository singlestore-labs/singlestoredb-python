#!/usr/bin/env python
# type: ignore
"""Tests for versioned management API wrappers (ADR 0001)."""
import datetime
import unittest
from unittest.mock import MagicMock
from unittest.mock import patch
from unittest.mock import PropertyMock

from singlestoredb.exceptions import ManagementError
from singlestoredb.management.versioned import _import_versioned_module
from singlestoredb.management.versioned import VersionedMixin


FAKE_TOKEN = 'test-token-12345'
FAKE_BASE_URL = 'https://api.example.com'
FAKE_ORG_ID = 'org-12345'


def _make_workspace_manager(version='v1', organization_id=FAKE_ORG_ID):
    """Construct a v1 WorkspaceManager with patched token resolver."""
    from singlestoredb.management.v1.workspace import WorkspaceManager
    with patch(
        'singlestoredb.management.manager.get_token',
        return_value=FAKE_TOKEN,
    ):
        return WorkspaceManager(
            access_token=FAKE_TOKEN,
            base_url=FAKE_BASE_URL,
            version=version,
            organization_id=organization_id,
        )


def _patch_no_network_regions():
    """Patch the WorkspaceManager.regions property on both v1 and v2 to []."""
    from singlestoredb.management.v1.workspace import (
        WorkspaceManager as V1WM,
    )
    from singlestoredb.management.v2.workspace import (
        WorkspaceManager as V2WM,
    )
    return [
        patch.object(V1WM, 'regions', new_callable=PropertyMock, return_value=[]),
        patch.object(V2WM, 'regions', new_callable=PropertyMock, return_value=[]),
    ]


class _MultiPatch:
    """Stack multiple context managers."""

    def __init__(self, patches):
        self._patches = patches

    def __enter__(self):
        for p in self._patches:
            p.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb):
        for p in reversed(self._patches):
            p.__exit__(exc_type, exc, tb)


def _make_workspace_group(manager=None, group_id='wsg-456', extra_obj=None):
    """Build a v1 WorkspaceGroup from a fake API response.

    ``WorkspaceGroup.from_dict`` calls ``manager.regions`` to resolve the
    region; we stub it so no network call is made.
    """
    from singlestoredb.management.v1.workspace import WorkspaceGroup
    mgr = manager or _make_workspace_manager()
    obj = {
        'name': 'test-group',
        'workspaceGroupID': group_id,
        'createdAt': '2024-01-01T00:00:00Z',
        'regionID': 'region-789',
        'firewallRanges': ['0.0.0.0/0'],
    }
    if extra_obj:
        obj.update(extra_obj)
    with _MultiPatch(_patch_no_network_regions()):
        wg = WorkspaceGroup.from_dict(obj, mgr)
    return wg, mgr, obj


class TestVersionedMixin(unittest.TestCase):
    """Test VersionedMixin behavior per ADR 0001."""

    def test_getattr_matches_version_pattern(self):
        """__getattr__ intercepts v1, v2, v99 etc."""
        mixin = VersionedMixin()
        mixin._get_versioned = MagicMock(return_value='versioned_obj')
        result = mixin.v1
        mixin._get_versioned.assert_called_once_with('v1')
        self.assertEqual(result, 'versioned_obj')

    def test_getattr_does_not_match_non_version(self):
        """__getattr__ raises AttributeError for non-version attrs."""
        mixin = VersionedMixin()
        with self.assertRaises(AttributeError):
            _ = mixin.foo
        with self.assertRaises(AttributeError):
            _ = mixin.version1
        with self.assertRaises(AttributeError):
            _ = mixin.va1

    def test_version_access_is_cached(self):
        """Repeated access to .v1 returns the same object."""
        mixin = VersionedMixin()
        sentinel = object()
        mixin._get_versioned = MagicMock(return_value=sentinel)
        first = mixin.v1
        second = mixin.v1
        self.assertIs(first, second)
        mixin._get_versioned.assert_called_once_with('v1')

    def test_different_versions_cached_independently(self):
        """v1 and v2 are cached separately."""
        mixin = VersionedMixin()
        call_count = [0]

        def fake_get_versioned(ver):
            call_count[0] += 1
            return f'obj_{ver}'

        mixin._get_versioned = fake_get_versioned
        self.assertEqual(mixin.v1, 'obj_v1')
        self.assertEqual(mixin.v2, 'obj_v2')
        self.assertEqual(call_count[0], 2)


class TestImportVersionedModule(unittest.TestCase):
    """Test dynamic module import."""

    def test_import_v1_workspace(self):
        mod = _import_versioned_module('v1', 'workspace')
        self.assertTrue(hasattr(mod, 'Workspace'))
        self.assertTrue(hasattr(mod, 'WorkspaceManager'))

    def test_import_v2_workspace(self):
        mod = _import_versioned_module('v2', 'workspace')
        self.assertTrue(hasattr(mod, 'Workspace'))
        self.assertTrue(hasattr(mod, 'WorkspaceManager'))

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


class TestManagerVersionSwitching(unittest.TestCase):
    """Test Manager credential storage and version cloning."""

    def _make_manager(self, cls=None):
        from singlestoredb.management.v1.workspace import WorkspaceManager
        cls = cls or WorkspaceManager
        with patch('singlestoredb.management.manager.get_token', return_value=FAKE_TOKEN):
            mgr = cls(
                access_token=FAKE_TOKEN,
                base_url=FAKE_BASE_URL,
                version='v1',
                organization_id=FAKE_ORG_ID,
            )
        return mgr

    def test_credentials_stored(self):
        """Manager stores _access_token, _base_url_root, _organization_id."""
        mgr = self._make_manager()
        self.assertEqual(mgr._access_token, FAKE_TOKEN)
        self.assertEqual(mgr._base_url_root, FAKE_BASE_URL)
        self.assertEqual(mgr._organization_id, FAKE_ORG_ID)

    def test_base_url_includes_version(self):
        """_base_url is built from _base_url_root + api_version."""
        mgr = self._make_manager()
        self.assertIn('/v1/', mgr._base_url)

    def test_default_version_class_attribute(self):
        """Manager has default_version class attribute defaulting to 'v1'."""
        from singlestoredb.management.manager import Manager
        self.assertEqual(Manager.default_version, 'v1')

    def test_version_switch_creates_new_manager(self):
        """mgr.v2 returns a WorkspaceManager from the v2 module."""
        mgr = self._make_manager()
        with patch('singlestoredb.management.manager.get_token', return_value=FAKE_TOKEN):
            v2_mgr = mgr.v2
        from singlestoredb.management.v2.workspace import WorkspaceManager as V2WM
        self.assertIsInstance(v2_mgr, V2WM)

    def test_version_switch_preserves_credentials(self):
        """Versioned manager clone has same credentials."""
        mgr = self._make_manager()
        with patch('singlestoredb.management.manager.get_token', return_value=FAKE_TOKEN):
            v2_mgr = mgr.v2
        self.assertEqual(v2_mgr._access_token, FAKE_TOKEN)
        self.assertEqual(v2_mgr._base_url_root, FAKE_BASE_URL)
        self.assertEqual(v2_mgr._organization_id, FAKE_ORG_ID)

    def test_version_switch_is_cached(self):
        """mgr.v2 returns the same object on repeated access."""
        mgr = self._make_manager()
        with patch('singlestoredb.management.manager.get_token', return_value=FAKE_TOKEN):
            first = mgr.v2
            second = mgr.v2
        self.assertIs(first, second)


class TestEntityVersionSwitching(unittest.TestCase):
    """Test entity version switching via from_dict + versioned manager."""

    def _make_workspace(self):
        from singlestoredb.management.v1.workspace import Workspace
        from singlestoredb.management.v1.workspace import WorkspaceManager

        with patch('singlestoredb.management.manager.get_token', return_value=FAKE_TOKEN):
            mgr = WorkspaceManager(
                access_token=FAKE_TOKEN,
                base_url=FAKE_BASE_URL,
                version='v1',
                organization_id=FAKE_ORG_ID,
            )

        obj = {
            'name': 'test-ws',
            'workspaceID': 'ws-123',
            'workspaceGroupID': 'wsg-456',
            'size': 'S-00',
            'state': 'Active',
            'createdAt': '2024-01-01T00:00:00Z',
        }
        ws = Workspace.from_dict(obj, mgr)
        return ws, mgr, obj

    def test_entity_stores_response(self):
        """from_dict stores raw response as _response."""
        ws, _, obj = self._make_workspace()
        self.assertIs(ws._response, obj)

    def test_entity_stores_manager(self):
        """from_dict stores manager reference."""
        ws, mgr, _ = self._make_workspace()
        self.assertIs(ws._manager, mgr)

    def test_entity_version_switch(self):
        """ws.v2 constructs target class via from_dict with versioned manager."""
        ws, _, obj = self._make_workspace()
        with patch('singlestoredb.management.manager.get_token', return_value=FAKE_TOKEN):
            v2_ws = ws.v2
        from singlestoredb.management.v2.workspace import Workspace as V2Workspace
        self.assertIsInstance(v2_ws, V2Workspace)
        self.assertEqual(v2_ws.name, 'test-ws')
        self.assertEqual(v2_ws.id, 'ws-123')

    def test_entity_version_switch_cached(self):
        """Repeated entity.v2 access returns same object."""
        ws, _, _ = self._make_workspace()
        with patch('singlestoredb.management.manager.get_token', return_value=FAKE_TOKEN):
            first = ws.v2
            second = ws.v2
        self.assertIs(first, second)

    def test_entity_version_switch_uses_versioned_manager(self):
        """The v2 entity's manager should be the v2 versioned manager."""
        ws, mgr, _ = self._make_workspace()
        with patch('singlestoredb.management.manager.get_token', return_value=FAKE_TOKEN):
            v2_ws = ws.v2
        self.assertIn('/v2/', v2_ws._manager._base_url)


class TestTopLevelShims(unittest.TestCase):
    """Test that top-level modules are thin re-export shims."""

    def test_workspace_shim_exports_v1_classes(self):
        """Top-level workspace module re-exports from v1."""
        from singlestoredb.management import workspace as ws_shim
        from singlestoredb.management.v1 import workspace as v1_ws
        self.assertIs(ws_shim.Workspace, v1_ws.Workspace)
        self.assertIs(ws_shim.WorkspaceGroup, v1_ws.WorkspaceGroup)
        self.assertIs(ws_shim.WorkspaceManager, v1_ws.WorkspaceManager)

    def test_region_shim_exports_v1_classes(self):
        """Top-level region module re-exports from v1."""
        from singlestoredb.management import region as rg_shim
        from singlestoredb.management.v1 import region as v1_rg
        self.assertIs(rg_shim.Region, v1_rg.Region)
        self.assertIs(rg_shim.RegionManager, v1_rg.RegionManager)

    @patch('singlestoredb.management.manager.get_token', return_value=FAKE_TOKEN)
    def test_manage_workspaces_respects_version_param(self, _mock_token):
        """manage_workspaces(version='v2') returns a v2 WorkspaceManager."""
        from singlestoredb.management.workspace import manage_workspaces
        mgr = manage_workspaces(
            access_token=FAKE_TOKEN,
            version='v2',
            base_url=FAKE_BASE_URL,
        )
        from singlestoredb.management.v2.workspace import WorkspaceManager as V2WM
        self.assertIsInstance(mgr, V2WM)

    @patch('singlestoredb.management.manager.get_token', return_value=FAKE_TOKEN)
    def test_manage_workspaces_default_is_v1(self, _mock_token):
        """manage_workspaces() defaults to v1."""
        from singlestoredb.management.workspace import manage_workspaces
        mgr = manage_workspaces(
            access_token=FAKE_TOKEN,
            base_url=FAKE_BASE_URL,
        )
        from singlestoredb.management.v1.workspace import WorkspaceManager as V1WM
        self.assertIsInstance(mgr, V1WM)


class TestV2InheritanceModel(unittest.TestCase):
    """Test that v2 classes properly inherit from v1."""

    def test_v2_workspace_is_v1_workspace(self):
        """v2 Workspace is the same as (or subclass of) v1 Workspace."""
        from singlestoredb.management.v1.workspace import Workspace as V1
        from singlestoredb.management.v2.workspace import Workspace as V2
        self.assertTrue(issubclass(V2, V1))

    def test_v2_workspace_group_is_v1_workspace_group(self):
        from singlestoredb.management.v1.workspace import WorkspaceGroup as V1
        from singlestoredb.management.v2.workspace import WorkspaceGroup as V2
        self.assertTrue(issubclass(V2, V1))

    def test_v2_region_is_v1_region(self):
        from singlestoredb.management.v1.region import Region as V1
        from singlestoredb.management.v2.region import Region as V2
        self.assertTrue(issubclass(V2, V1))

    def test_v2_job_is_v1_job(self):
        from singlestoredb.management.v1.job import Job as V1
        from singlestoredb.management.v2.job import Job as V2
        self.assertTrue(issubclass(V2, V1))


class TestNoSilentFallback(unittest.TestCase):
    """ADR: no cross-version fallback — missing class raises error."""

    def test_nonexistent_class_in_version_raises(self):
        """Requesting a class that doesn't exist in a version raises."""

        class NonExistentClass(VersionedMixin):
            __module__ = 'singlestoredb.management.v1.workspace'

            def __init__(self):
                pass

        instance = NonExistentClass()
        instance._access_token = FAKE_TOKEN
        instance._base_url_root = FAKE_BASE_URL
        instance._organization_id = FAKE_ORG_ID

        with self.assertRaises(ManagementError) as ctx:
            instance._get_versioned('v1')
        self.assertIn('NonExistentClass', str(ctx.exception))
        self.assertIn('not available', str(ctx.exception))


class TestConfigOption(unittest.TestCase):
    """Test that management.version config option exists and works."""

    def test_config_option_exists(self):
        from singlestoredb import config
        val = config.get_option('management.version')
        self.assertIn(val, ('v1', 'v2', None, ''))

    @patch('singlestoredb.management.manager.get_token', return_value=FAKE_TOKEN)
    def test_config_option_routes_manage_workspaces(self, _mock_token):
        """Setting management.version to v2 routes to v2."""
        from singlestoredb import config
        from singlestoredb.management.workspace import manage_workspaces
        from singlestoredb.management.v2.workspace import WorkspaceManager as V2WM

        original = config.get_option('management.version')
        try:
            config.set_option('management.version', 'v2')
            mgr = manage_workspaces(
                access_token=FAKE_TOKEN,
                base_url=FAKE_BASE_URL,
            )
            self.assertIsInstance(mgr, V2WM)
        finally:
            config.set_option('management.version', original or 'v1')


class TestModuleNameConvention(unittest.TestCase):
    """Test convention-based module lookup per ADR."""

    def test_module_name_derived_from_class_module(self):
        """_module_name returns the last component of __module__."""
        from singlestoredb.management.v1.workspace import Workspace
        ws = Workspace.__new__(Workspace)
        self.assertEqual(ws._module_name, 'workspace')

    def test_module_name_for_region(self):
        from singlestoredb.management.v1.region import Region
        rg = Region.__new__(Region)
        self.assertEqual(rg._module_name, 'region')


class TestWrapperManagerVersionSwitching(unittest.TestCase):
    """Test version switching on wrapper managers (JobsManager, InferenceAPIManager)."""

    def _make_workspace_manager(self):
        from singlestoredb.management.v1.workspace import WorkspaceManager
        with patch('singlestoredb.management.manager.get_token', return_value=FAKE_TOKEN):
            mgr = WorkspaceManager(
                access_token=FAKE_TOKEN,
                base_url=FAKE_BASE_URL,
                version='v1',
                organization_id=FAKE_ORG_ID,
            )
        return mgr

    def test_jobs_manager_version_switch(self):
        """JobsManager.v2 returns a v2 JobsManager with a versioned parent."""
        from singlestoredb.management.v1.job import JobsManager

        parent = self._make_workspace_manager()
        jobs_mgr = JobsManager(parent)

        with patch('singlestoredb.management.manager.get_token', return_value=FAKE_TOKEN):
            v2_jobs = jobs_mgr.v2

        from singlestoredb.management.v2.job import JobsManager as V2JobsManager
        self.assertIsInstance(v2_jobs, V2JobsManager)
        self.assertIn('/v2/', v2_jobs._manager._base_url)

    def test_inference_api_manager_version_switch(self):
        """InferenceAPIManager.v2 returns a v2 InferenceAPIManager."""
        from singlestoredb.management.v1.inference_api import InferenceAPIManager

        parent = self._make_workspace_manager()
        inf_mgr = InferenceAPIManager(parent)

        with patch('singlestoredb.management.manager.get_token', return_value=FAKE_TOKEN):
            v2_inf = inf_mgr.v2

        from singlestoredb.management.v2.inference_api import (
            InferenceAPIManager as V2InfMgr,
        )
        self.assertIsInstance(v2_inf, V2InfMgr)

    def test_wrapper_manager_version_switch_is_cached(self):
        """Repeated .v2 on wrapper manager returns same object."""
        from singlestoredb.management.v1.job import JobsManager

        parent = self._make_workspace_manager()
        jobs_mgr = JobsManager(parent)

        with patch('singlestoredb.management.manager.get_token', return_value=FAKE_TOKEN):
            first = jobs_mgr.v2
            second = jobs_mgr.v2
        self.assertIs(first, second)


class TestTokenStorageFix(unittest.TestCase):
    """Test that Manager stores the resolved token, not the passed-in value."""

    @patch('singlestoredb.management.manager.is_jwt', return_value=False)
    @patch('singlestoredb.management.manager.get_token', return_value=FAKE_TOKEN)
    def test_none_token_resolves_and_stores(self, _mock_token, _mock_jwt):
        """When access_token=None, _access_token stores the resolved token."""
        from singlestoredb.management.v1.workspace import WorkspaceManager
        mgr = WorkspaceManager(
            access_token=None,
            base_url=FAKE_BASE_URL,
            version='v1',
        )
        self.assertEqual(mgr._access_token, FAKE_TOKEN)

    @patch('singlestoredb.management.manager.get_token', return_value=FAKE_TOKEN)
    def test_explicit_token_stored_as_is(self, _mock_token):
        """When access_token is provided, it's stored directly."""
        from singlestoredb.management.v1.workspace import WorkspaceManager
        mgr = WorkspaceManager(
            access_token='my-explicit-token',
            base_url=FAKE_BASE_URL,
            version='v1',
        )
        self.assertEqual(mgr._access_token, 'my-explicit-token')


class TestLocationManagerRebind(unittest.TestCase):
    """
    Regression test for commit 0cc6024f: when an entity that has a
    ``_location`` child manager is version-switched, the rebound
    ``_location._manager`` must point at the v2 versioned manager, and
    ``region`` must be preserved.
    """

    def test_location_manager_rebound_to_versioned_clone(self):
        from singlestoredb.management.v1.region import Region

        ws_mgr = _make_workspace_manager()
        wg, _, _ = _make_workspace_group(manager=ws_mgr)

        # Simulate a child location manager that points at the v1 manager.
        class _FakeLocation:
            pass
        loc = _FakeLocation()
        loc._manager = ws_mgr
        wg._location = loc
        wg.region = Region('reg-name', 'aws', 'region-789')

        with patch(
            'singlestoredb.management.manager.get_token',
            return_value=FAKE_TOKEN,
        ), _MultiPatch(_patch_no_network_regions()):
            v2_wg = wg.v2
            v2_mgr = ws_mgr.v2
        self.assertIs(v2_wg._location._manager, v2_mgr)
        # region must be preserved across version switch
        self.assertIs(v2_wg.region, wg.region)
        # Original entity's location is untouched (copy.copy was used)
        self.assertIs(loc._manager, ws_mgr)


class TestJWTRefreshInClones(unittest.TestCase):
    """
    Regression test for commit d52e8e40: a v2-cloned manager whose
    parent had ``_is_jwt=True`` must call ``get_token()`` again on each
    request and rotate the Authorization header.
    """

    def test_jwt_refresh_uses_latest_token_on_clone(self):
        from singlestoredb.management.v1.workspace import WorkspaceManager

        # Build a manager and force JWT mode on
        with patch(
            'singlestoredb.management.manager.get_token',
            return_value='initial-jwt',
        ):
            mgr = WorkspaceManager(
                access_token='initial-jwt',
                base_url=FAKE_BASE_URL,
                version='v1',
                organization_id=FAKE_ORG_ID,
            )
        mgr._is_jwt = True

        # Clone via .v2; the clone should also be in JWT mode
        with patch(
            'singlestoredb.management.manager.get_token',
            return_value='ignored-during-clone',
        ):
            v2_mgr = mgr.v2
        self.assertTrue(v2_mgr._is_jwt)

        # Now drive a request through the clone with a NEW token
        # returned by get_token(). The Authorization header must reflect
        # the new token, not the one set up at construction time.
        v2_mgr._sess = MagicMock()
        fake_response = MagicMock()
        v2_mgr._sess.get.return_value = fake_response

        with patch(
            'singlestoredb.management.manager.get_token',
            return_value='rotated-jwt',
        ):
            v2_mgr._doit('get', 'foo')

        # _doit should have updated session headers with the rotated token
        v2_mgr._sess.headers.update.assert_called_with(
            {'Authorization': 'Bearer rotated-jwt'},
        )


class TestDateTimeParsingFixes(unittest.TestCase):
    """
    Regression test for commit 85faf724: ISO8601-Z timestamp parsing
    on entities that go through ``to_datetime``.
    """

    def test_workspace_created_at_parsed(self):
        from singlestoredb.management.v1.workspace import Workspace
        mgr = _make_workspace_manager()
        obj = {
            'name': 'test-ws',
            'workspaceID': 'ws-1',
            'workspaceGroupID': 'wsg-1',
            'size': 'S-00',
            'state': 'Active',
            'createdAt': '2024-03-15T12:30:45Z',
            'lastResumedAt': '2024-03-16T08:00:00.123Z',
        }
        ws = Workspace.from_dict(obj, mgr)
        self.assertIsInstance(ws.created_at, datetime.datetime)
        self.assertEqual(ws.created_at.year, 2024)
        self.assertEqual(ws.created_at.month, 3)
        self.assertEqual(ws.created_at.day, 15)
        self.assertEqual(ws.created_at.hour, 12)
        self.assertIsInstance(ws.last_resumed_at, datetime.datetime)

    def test_workspace_group_expires_at_parsed(self):
        wg, _, _ = _make_workspace_group(
            extra_obj={'expiresAt': '2025-06-30T23:59:59Z'},
        )
        self.assertIsInstance(wg.expires_at, datetime.datetime)
        self.assertEqual(wg.expires_at.year, 2025)

    def test_workspace_group_terminated_at_zero_returns_none(self):
        """The sentinel 0001-01-01 timestamp must round-trip to None."""
        wg, _, _ = _make_workspace_group(
            extra_obj={'terminatedAt': '0001-01-01T00:00:00Z'},
        )
        self.assertIsNone(wg.terminated_at)


class TestEntityRoundTripFidelity(unittest.TestCase):
    """``entity.v2.v1`` should produce an equivalent entity."""

    def test_workspace_round_trip(self):
        from singlestoredb.management.v1.workspace import Workspace as V1Workspace
        mgr = _make_workspace_manager()
        obj = {
            'name': 'test-ws',
            'workspaceID': 'ws-123',
            'workspaceGroupID': 'wsg-456',
            'size': 'S-00',
            'state': 'Active',
            'createdAt': '2024-01-01T00:00:00Z',
        }
        ws = V1Workspace.from_dict(obj, mgr)
        with patch(
            'singlestoredb.management.manager.get_token',
            return_value=FAKE_TOKEN,
        ):
            round_tripped = ws.v2.v1
        self.assertIsInstance(round_tripped, V1Workspace)
        self.assertEqual(round_tripped.name, ws.name)
        self.assertEqual(round_tripped.id, ws.id)
        self.assertEqual(round_tripped.group_id, ws.group_id)
        # Same _response payload (object identity preserved through chain)
        self.assertIs(round_tripped._response, obj)

    def test_workspace_group_round_trip(self):
        from singlestoredb.management.v1.workspace import WorkspaceGroup as V1WG
        wg, _, obj = _make_workspace_group()
        with patch(
            'singlestoredb.management.manager.get_token',
            return_value=FAKE_TOKEN,
        ), _MultiPatch(_patch_no_network_regions()):
            round_tripped = wg.v2.v1
        self.assertIsInstance(round_tripped, V1WG)
        self.assertEqual(round_tripped.id, wg.id)
        self.assertIs(round_tripped._response, obj)


class TestWorkspaceFromDictNewFields(unittest.TestCase):
    """
    Coverage for the staged additions in ``v1/workspace.py``:
    ``auto_scale``, ``kai_enabled``, ``scale_factor``, plus the widened
    ``cache_config`` (now float).
    """

    def _base_obj(self):
        return {
            'name': 'test-ws',
            'workspaceID': 'ws-1',
            'workspaceGroupID': 'wsg-1',
            'size': 'S-00',
            'state': 'Active',
            'createdAt': '2024-01-01T00:00:00Z',
        }

    def test_new_fields_present(self):
        from singlestoredb.management.v1.workspace import Workspace
        mgr = _make_workspace_manager()
        obj = self._base_obj()
        obj.update({
            'autoScale': {
                'sensitivity': 'HIGH',
                'maxScaleFactor': 4.0,
                'changedAt': '2024-01-01T00:00:00Z',
                'lastAutoScaledAt': '2024-01-02T00:00:00Z',
            },
            'kaiEnabled': True,
            'scaleFactor': 2.5,
            'cacheConfig': 1.5,
        })
        ws = Workspace.from_dict(obj, mgr)
        # auto_scale keys are camel_to_snake_dict-converted
        self.assertEqual(ws.auto_scale['sensitivity'], 'HIGH')
        self.assertEqual(ws.auto_scale['max_scale_factor'], 4.0)
        self.assertEqual(ws.auto_scale['changed_at'], '2024-01-01T00:00:00Z')
        self.assertEqual(
            ws.auto_scale['last_auto_scaled_at'], '2024-01-02T00:00:00Z',
        )
        self.assertNotIn('maxScaleFactor', ws.auto_scale)
        self.assertIs(ws.kai_enabled, True)
        self.assertEqual(ws.scale_factor, 2.5)
        self.assertEqual(ws.cache_config, 1.5)

    def test_new_fields_default_to_none(self):
        from singlestoredb.management.v1.workspace import Workspace
        mgr = _make_workspace_manager()
        ws = Workspace.from_dict(self._base_obj(), mgr)
        self.assertIsNone(ws.auto_scale)
        self.assertIsNone(ws.kai_enabled)
        self.assertIsNone(ws.scale_factor)


class TestWorkspaceUpdatePosting(unittest.TestCase):
    """``Workspace.update`` must include the new fields in the PATCH body."""

    def _make_workspace(self, mgr):
        from singlestoredb.management.v1.workspace import Workspace
        obj = {
            'name': 'test-ws',
            'workspaceID': 'ws-1',
            'workspaceGroupID': 'wsg-1',
            'size': 'S-00',
            'state': 'Active',
            'createdAt': '2024-01-01T00:00:00Z',
        }
        return Workspace.from_dict(obj, mgr)

    def test_update_posts_new_fields_only_when_set(self):
        mgr = _make_workspace_manager()
        mgr._patch = MagicMock()
        ws = self._make_workspace(mgr)
        ws.refresh = MagicMock()

        ws.update(
            auto_scale={'sensitivity': 'HIGH'},
            enable_kai=True,
            scale_factor=2.0,
            cache_config=1.5,
        )

        mgr._patch.assert_called_once()
        args, kwargs = mgr._patch.call_args
        self.assertEqual(args[0], 'workspaces/ws-1')
        body = kwargs['json']
        self.assertEqual(body['autoScale'], {'sensitivity': 'HIGH'})
        self.assertIs(body['enableKai'], True)
        self.assertEqual(body['scaleFactor'], 2.0)
        self.assertEqual(body['cacheConfig'], 1.5)

    def test_update_omits_keys_when_param_none(self):
        mgr = _make_workspace_manager()
        mgr._patch = MagicMock()
        ws = self._make_workspace(mgr)
        ws.refresh = MagicMock()

        ws.update(size='S-1')

        body = mgr._patch.call_args.kwargs['json']
        self.assertEqual(body, {'size': 'S-1'})
        self.assertNotIn('autoScale', body)
        self.assertNotIn('enableKai', body)
        self.assertNotIn('scaleFactor', body)


class TestWorkspaceGroupNewFields(unittest.TestCase):
    """Coverage for the new staged fields on ``WorkspaceGroup.from_dict``."""

    def _obj_with_new_fields(self):
        return {
            'name': 'test-group',
            'workspaceGroupID': 'wsg-1',
            'createdAt': '2024-01-01T00:00:00Z',
            'regionID': 'region-789',
            'firewallRanges': ['0.0.0.0/0'],
            'allowAllTraffic': True,
            'deploymentType': 'PRODUCTION',
            'expiresAt': '2025-06-30T23:59:59Z',
            'highAvailabilityTwoZones': True,
            'optInPreviewFeature': False,
            'outboundAllowList': '203.0.113.0/24',
            'projectID': 'proj-1',
            'projectName': 'my-project',
            'smartDRStatus': 'ACTIVE',
            'state': 'ACTIVE',
            'updateWindow': {'day': 0, 'hour': 4},
            'provider': 'aws',
            'regionName': 'us-east-1',
        }

    def test_all_new_fields_mapped(self):
        from singlestoredb.management.v1.workspace import WorkspaceGroup
        mgr = _make_workspace_manager()
        with patch.object(
            type(mgr), 'regions',
            new_callable=PropertyMock, return_value=[],
        ):
            wg = WorkspaceGroup.from_dict(self._obj_with_new_fields(), mgr)
        self.assertEqual(wg.deployment_type, 'PRODUCTION')
        self.assertIsInstance(wg.expires_at, datetime.datetime)
        self.assertIs(wg.high_availability_two_zones, True)
        self.assertIs(wg.opt_in_preview_feature, False)
        self.assertEqual(wg.outbound_allow_list, '203.0.113.0/24')
        self.assertEqual(wg.project_id, 'proj-1')
        self.assertEqual(wg.project_name, 'my-project')
        self.assertEqual(wg.smart_dr_status, 'ACTIVE')
        self.assertEqual(wg.state, 'ACTIVE')
        # update_window stays a raw dict (not snake-cased)
        self.assertEqual(wg.update_window, {'day': 0, 'hour': 4})
        self.assertEqual(wg.provider, 'aws')
        self.assertEqual(wg.region_name, 'us-east-1')

    def test_new_fields_default_to_none(self):
        wg, _, _ = _make_workspace_group()
        self.assertIsNone(wg.deployment_type)
        self.assertIsNone(wg.expires_at)
        self.assertIsNone(wg.high_availability_two_zones)
        self.assertIsNone(wg.opt_in_preview_feature)
        self.assertIsNone(wg.outbound_allow_list)
        self.assertIsNone(wg.project_id)
        self.assertIsNone(wg.project_name)
        self.assertIsNone(wg.smart_dr_status)
        self.assertIsNone(wg.state)
        self.assertIsNone(wg.update_window)
        self.assertIsNone(wg.provider)
        self.assertIsNone(wg.region_name)


class TestWorkspaceGroupCreateUpdatePosting(unittest.TestCase):
    """Body coverage for create_workspace_group / WorkspaceGroup.update."""

    def test_create_workspace_group_posts_new_fields(self):
        mgr = _make_workspace_manager()
        # Make get_workspace_group a no-op; we only inspect the POST body.
        post_response = MagicMock()
        post_response.json.return_value = {'workspaceGroupID': 'wsg-new'}
        mgr._post = MagicMock(return_value=post_response)
        mgr.get_workspace_group = MagicMock(return_value='sentinel')

        result = mgr.create_workspace_group(
            name='wg-1',
            region='region-789',
            firewall_ranges=['0.0.0.0/0'],
            provider='aws',
            region_name='us-east-1',
            deployment_type='PRODUCTION',
            high_availability_two_zones=True,
            opt_in_preview_feature=False,
            project_id='proj-1',
        )

        self.assertEqual(result, 'sentinel')
        body = mgr._post.call_args.kwargs['json']
        self.assertEqual(body['provider'], 'aws')
        self.assertEqual(body['regionName'], 'us-east-1')
        self.assertEqual(body['deploymentType'], 'PRODUCTION')
        self.assertIs(body['highAvailabilityTwoZones'], True)
        self.assertIs(body['optInPreviewFeature'], False)
        self.assertEqual(body['projectID'], 'proj-1')

    def test_workspace_group_update_includes_deployment_type(self):
        wg, mgr, _ = _make_workspace_group()
        mgr._patch = MagicMock()
        wg.refresh = MagicMock()

        wg.update(deployment_type='NON-PRODUCTION', name='renamed')

        body = mgr._patch.call_args.kwargs['json']
        self.assertEqual(body['deploymentType'], 'NON-PRODUCTION')
        self.assertEqual(body['name'], 'renamed')

    def test_workspace_group_update_omits_unset_fields(self):
        wg, mgr, _ = _make_workspace_group()
        mgr._patch = MagicMock()
        wg.refresh = MagicMock()

        wg.update(name='renamed')

        body = mgr._patch.call_args.kwargs['json']
        self.assertNotIn('deploymentType', body)


class TestJobsManagerScheduleDuration(unittest.TestCase):
    """
    Coverage for the staged ``max_allowed_execution_duration_in_minutes``
    parameter on ``JobsManager.schedule``.
    """

    def _patch_post(self, mgr, response_obj):
        post_response = MagicMock()
        post_response.json.return_value = response_obj
        mgr._post = MagicMock(return_value=post_response)
        return post_response

    def _fake_job_response(self):
        return {
            'jobID': 'job-1',
            'name': 'j',
            'description': None,
            'enqueuedBy': 'me',
            'createdAt': '2024-01-01T00:00:00Z',
            'completedExecutionsCount': 0,
            'jobMetadata': [],
            'terminatedAt': None,
            'executionConfig': {
                'createSnapshot': True,
                'notebookPath': '/x.ipynb',
            },
            'schedule': {'mode': 'Once'},
            'targetConfig': None,
        }

    def test_duration_present_when_set(self):
        from singlestoredb.management.v1.job import JobsManager
        from singlestoredb.management.v1.job import Mode

        ws_mgr = _make_workspace_manager()
        jobs = JobsManager(ws_mgr)
        self._patch_post(ws_mgr, self._fake_job_response())

        with patch(
            'singlestoredb.management.v1.job.Job.from_dict',
            return_value='sentinel',
        ):
            jobs.schedule(
                notebook_path='/x.ipynb',
                mode=Mode.ONCE,
                create_snapshot=True,
                max_allowed_execution_duration_in_minutes=42,
            )

        body = ws_mgr._post.call_args.kwargs['json']
        self.assertEqual(
            body['executionConfig']['maxAllowedExecutionDurationInMinutes'],
            42,
        )

    def test_duration_absent_when_unset(self):
        from singlestoredb.management.v1.job import JobsManager
        from singlestoredb.management.v1.job import Mode

        ws_mgr = _make_workspace_manager()
        jobs = JobsManager(ws_mgr)
        self._patch_post(ws_mgr, self._fake_job_response())

        with patch(
            'singlestoredb.management.v1.job.Job.from_dict',
            return_value='sentinel',
        ):
            jobs.schedule(
                notebook_path='/x.ipynb',
                mode=Mode.ONCE,
                create_snapshot=True,
            )

        body = ws_mgr._post.call_args.kwargs['json']
        self.assertNotIn(
            'maxAllowedExecutionDurationInMinutes',
            body['executionConfig'],
        )


class TestSecretFromDictTimestamps(unittest.TestCase):
    """
    Coverage for the staged ``v1/organization.py`` change that runs
    Secret timestamp fields through ``to_datetime``.
    """

    def test_timestamps_parsed_to_datetime(self):
        from singlestoredb.management.v1.organization import Secret

        obj = {
            'secretID': 'sec-1',
            'name': 'my-secret',
            'createdBy': 'user-a',
            'createdAt': '2024-01-01T00:00:00Z',
            'lastUpdatedBy': 'user-b',
            'lastUpdatedAt': '2024-02-15T12:34:56Z',
            'value': 'shh',
            'deletedBy': None,
            'deletedAt': None,
        }
        sec = Secret.from_dict(obj)
        self.assertIsInstance(sec.created_at, datetime.datetime)
        self.assertEqual(sec.created_at.year, 2024)
        self.assertIsInstance(sec.last_updated_at, datetime.datetime)
        self.assertEqual(sec.last_updated_at.minute, 34)
        self.assertIsNone(sec.deleted_at)

    def test_missing_timestamps_become_none(self):
        from singlestoredb.management.v1.organization import Secret

        obj = {
            'secretID': 'sec-1',
            'name': 'my-secret',
            'createdBy': 'user-a',
            'lastUpdatedBy': 'user-b',
        }
        sec = Secret.from_dict(obj)
        self.assertIsNone(sec.created_at)
        self.assertIsNone(sec.last_updated_at)
        self.assertIsNone(sec.deleted_at)


class TestV2RegionBehavior(unittest.TestCase):
    """
    Coverage for the staged ``v2/region.py`` override:
    ``list_regions`` hits ``/v2/regions``.
    """

    def _make_v2_region_manager(self):
        from singlestoredb.management.v2.region import RegionManager
        with patch(
            'singlestoredb.management.manager.get_token',
            return_value=FAKE_TOKEN,
        ):
            return RegionManager(
                access_token=FAKE_TOKEN,
                base_url=FAKE_BASE_URL,
                version='v2',
            )

    def test_list_regions_uses_v2_endpoint(self):
        mgr = self._make_v2_region_manager()
        get_response = MagicMock()
        get_response.json.return_value = [
            {'provider': 'aws', 'region': 'us-east-1', 'regionName': 'US East 1'},
            {'provider': 'gcp', 'region': 'us-west-2', 'regionName': 'US West 2'},
        ]
        mgr._get = MagicMock(return_value=get_response)

        regions = mgr.list_regions()
        mgr._get.assert_called_once_with('regions')
        self.assertEqual(len(regions), 2)
        # v2 region entries have id=None (no regionID in the v2 response)
        for r in regions:
            self.assertIsNone(r.id)

    def test_v2_region_manager_inherits_v1(self):
        from singlestoredb.management.v1.region import RegionManager as V1
        from singlestoredb.management.v2.region import RegionManager as V2
        self.assertTrue(issubclass(V2, V1))


class TestWorkspaceGroupRegionResolution(unittest.TestCase):
    """``WorkspaceGroup.from_dict`` must resolve regions from v2 managers,
    where ``Region.id`` is ``None`` and only ``(region_name, provider)``
    identify a region."""

    def _v2_region(self, name, provider, region_name):
        from singlestoredb.management.v1.region import Region
        return Region(
            name=name, provider=provider, id=None, region_name=region_name,
        )

    def _wg_payload(self, **overrides):
        obj = {
            'name': 'test-group',
            'workspaceGroupID': 'wsg-1',
            'createdAt': '2024-01-01T00:00:00Z',
            'regionID': 'region-uuid-1',
            'regionName': 'us-west1',
            'provider': 'GCP',
        }
        obj.update(overrides)
        return obj

    def test_v2_resolves_by_region_name_and_provider(self):
        from singlestoredb.management.v1.workspace import (
            WorkspaceGroup, WorkspaceManager,
        )
        mgr = MagicMock(spec=WorkspaceManager)
        mgr.regions = [
            self._v2_region('us-west1', 'GCP', 'us-west1'),
            self._v2_region('eu-central-1', 'AWS', 'eu-central-1'),
        ]
        wg = WorkspaceGroup.from_dict(self._wg_payload(), mgr)
        self.assertEqual(wg.region.name, 'us-west1')
        self.assertEqual(wg.region.provider, 'GCP')
        self.assertEqual(wg.region.region_name, 'us-west1')

    def test_v1_match_by_id_still_wins(self):
        from singlestoredb.management.v1.region import Region
        from singlestoredb.management.v1.workspace import (
            WorkspaceGroup, WorkspaceManager,
        )
        mgr = MagicMock(spec=WorkspaceManager)
        mgr.regions = [
            Region(
                name='us-west1', provider='GCP',
                id='region-uuid-1', region_name='us-west1',
            ),
        ]
        wg = WorkspaceGroup.from_dict(self._wg_payload(), mgr)
        self.assertEqual(wg.region.id, 'region-uuid-1')
        self.assertEqual(wg.region.name, 'us-west1')

    def test_no_match_falls_back_to_payload_fields(self):
        from singlestoredb.management.v1.workspace import (
            WorkspaceGroup, WorkspaceManager,
        )
        mgr = MagicMock(spec=WorkspaceManager)
        mgr.regions = []
        wg = WorkspaceGroup.from_dict(self._wg_payload(), mgr)
        self.assertEqual(wg.region.name, 'us-west1')
        self.assertEqual(wg.region.provider, 'GCP')
        self.assertEqual(wg.region.id, 'region-uuid-1')
        self.assertEqual(wg.region.region_name, 'us-west1')

    def test_no_match_no_payload_fields_uses_unknown(self):
        from singlestoredb.management.v1.workspace import (
            WorkspaceGroup, WorkspaceManager,
        )
        mgr = MagicMock(spec=WorkspaceManager)
        mgr.regions = []
        obj = {
            'name': 'test-group',
            'workspaceGroupID': 'wsg-1',
            'createdAt': '2024-01-01T00:00:00Z',
        }
        wg = WorkspaceGroup.from_dict(obj, mgr)
        self.assertEqual(wg.region.name, '<unknown>')
        self.assertEqual(wg.region.provider, '<unknown>')
        self.assertIsNone(wg.region.id)


class TestV2WorkspaceGroupGetMetrics(unittest.TestCase):
    """Coverage for ``v2/workspace.py:WorkspaceGroup.get_metrics``."""

    def _make_v2_wg_with_org(self, organization_id=FAKE_ORG_ID, params=None):
        ws_mgr = _make_workspace_manager(organization_id=organization_id)
        if params is not None:
            ws_mgr._params = params
        wg, _, _ = _make_workspace_group(manager=ws_mgr)
        with patch(
            'singlestoredb.management.manager.get_token',
            return_value=FAKE_TOKEN,
        ), _MultiPatch(_patch_no_network_regions()):
            v2_wg = wg.v2
        return v2_wg, v2_wg._manager

    def test_uses_organization_id_from_manager(self):
        v2_wg, v2_mgr = self._make_v2_wg_with_org()
        get_response = MagicMock()
        get_response.text = 'metric_a 1\nmetric_b 2\n'
        v2_mgr._get = MagicMock(return_value=get_response)

        result = v2_wg.get_metrics()

        self.assertEqual(result, 'metric_a 1\nmetric_b 2\n')
        args, kwargs = v2_mgr._get.call_args
        self.assertEqual(
            args[0],
            f'organizations/{FAKE_ORG_ID}/workspaceGroups/wsg-456/metrics',
        )
        self.assertEqual(kwargs['headers'], {'Accept': 'text/plain'})

    def test_falls_back_to_params_organization_id(self):
        v2_wg, v2_mgr = self._make_v2_wg_with_org(
            organization_id=None, params={'organizationID': 'org-from-params'},
        )
        # Force fallback by clearing _organization_id on the clone too
        v2_mgr._organization_id = None
        v2_mgr._params = {'organizationID': 'org-from-params'}

        get_response = MagicMock()
        get_response.text = ''
        v2_mgr._get = MagicMock(return_value=get_response)

        v2_wg.get_metrics()

        args, _ = v2_mgr._get.call_args
        self.assertIn('org-from-params', args[0])

    def test_falls_back_to_manager_organization(self):
        """Fallback resolves org ID via the v1 clone (per OpenAPI spec).

        v2 has no ``organizations/current`` endpoint, so the metrics method
        must drop to ``self._manager.v1.organization.id``.
        """
        v2_wg, v2_mgr = self._make_v2_wg_with_org(organization_id=None)
        v2_mgr._organization_id = None
        v2_mgr._params = {}

        # Build a fake v1 clone whose `.organization.id` returns the value
        # we want to see in the eventual metrics URL.
        fake_org = MagicMock()
        fake_org.id = 'org-from-current'
        fake_v1 = MagicMock()
        fake_v1.organization = fake_org

        get_response = MagicMock()
        get_response.text = ''
        v2_mgr._get = MagicMock(return_value=get_response)

        # Inject the v1 clone into VersionedMixin's cache so attribute
        # access for `.v1` returns it without spinning up a real manager.
        v2_mgr._version_cache = {'v1': fake_v1}
        v2_wg.get_metrics()

        args, _ = v2_mgr._get.call_args
        self.assertIn('org-from-current', args[0])
        # The metrics request itself must still go to the v2-cloned manager.
        self.assertEqual(
            args[0],
            'organizations/org-from-current'
            '/workspaceGroups/wsg-456/metrics',
        )

    def test_raises_when_manager_is_none(self):
        from singlestoredb.management.v2.workspace import WorkspaceGroup as V2WG
        # Build a v2 group entirely detached from any manager
        wg = V2WG.__new__(V2WG)
        wg._manager = None
        wg._response = {}
        wg.id = 'wsg-x'
        with self.assertRaises(ManagementError) as ctx:
            wg.get_metrics()
        self.assertIn('No workspace manager', str(ctx.exception))

    def test_raises_when_org_id_unresolvable(self):
        v2_wg, v2_mgr = self._make_v2_wg_with_org(organization_id=None)
        v2_mgr._organization_id = None
        v2_mgr._params = {}
        # Stub the v1 clone's organization to return one whose id is empty
        fake_org = MagicMock()
        fake_org.id = ''
        fake_v1 = MagicMock()
        fake_v1.organization = fake_org
        v2_mgr._version_cache = {'v1': fake_v1}
        with self.assertRaises(ManagementError) as ctx:
            v2_wg.get_metrics()
        self.assertIn('organization ID', str(ctx.exception))


class TestManageRoutingForAllFactories(unittest.TestCase):
    """
    ``manage_*`` factories must route to the correct version module:
    ``version='v2'`` returns a v2 manager, default returns a v1 manager.
    """

    @patch('singlestoredb.management.manager.get_token', return_value=FAKE_TOKEN)
    def test_manage_workspaces(self, _mock_token):
        from singlestoredb.management.workspace import manage_workspaces
        from singlestoredb.management.v1.workspace import (
            WorkspaceManager as V1WM,
        )
        from singlestoredb.management.v2.workspace import (
            WorkspaceManager as V2WM,
        )

        v2 = manage_workspaces(
            access_token=FAKE_TOKEN, base_url=FAKE_BASE_URL, version='v2',
        )
        self.assertIsInstance(v2, V2WM)
        v1 = manage_workspaces(
            access_token=FAKE_TOKEN, base_url=FAKE_BASE_URL, version='v1',
        )
        self.assertIsInstance(v1, V1WM)
        # default (no explicit version) falls back to v1 unless config overrides
        from singlestoredb import config
        original = config.get_option('management.version')
        try:
            config.set_option('management.version', 'v1')
            default = manage_workspaces(
                access_token=FAKE_TOKEN, base_url=FAKE_BASE_URL,
            )
            self.assertIsInstance(default, V1WM)
        finally:
            config.set_option('management.version', original or 'v1')

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
        from singlestoredb.management.v1.files import FilesManager as V1FM
        from singlestoredb.management.v2.files import FilesManager as V2FM

        self.assertIsInstance(
            manage_files(
                access_token=FAKE_TOKEN, base_url=FAKE_BASE_URL, version='v2',
            ),
            V2FM,
        )
        self.assertIsInstance(
            manage_files(
                access_token=FAKE_TOKEN, base_url=FAKE_BASE_URL, version='v1',
            ),
            V1FM,
        )


class TestV1FactoryRoutesByVersion(unittest.TestCase):
    """The duplicate ``manage_*`` factories in ``v1/*.py`` must route by
    ``version`` the same way the top-level shims do, so callers using
    ``from singlestoredb.management.v1.region import manage_regions`` with
    ``version='v2'`` still get a v2 manager."""

    @patch('singlestoredb.management.manager.get_token', return_value=FAKE_TOKEN)
    def test_v1_namespace_manage_regions_routes_v2(self, _mock_token):
        from singlestoredb.management.v1.region import manage_regions
        from singlestoredb.management.v2.region import RegionManager as V2RM
        mgr = manage_regions(
            access_token=FAKE_TOKEN, base_url=FAKE_BASE_URL, version='v2',
        )
        self.assertIsInstance(mgr, V2RM)

    @patch('singlestoredb.management.manager.get_token', return_value=FAKE_TOKEN)
    def test_v1_namespace_manage_workspaces_routes_v2(self, _mock_token):
        from singlestoredb.management.v1.workspace import manage_workspaces
        from singlestoredb.management.v2.workspace import (
            WorkspaceManager as V2WM,
        )
        mgr = manage_workspaces(
            access_token=FAKE_TOKEN, base_url=FAKE_BASE_URL, version='v2',
        )
        self.assertIsInstance(mgr, V2WM)

    @patch('singlestoredb.management.manager.get_token', return_value=FAKE_TOKEN)
    def test_v1_namespace_manage_files_routes_v2(self, _mock_token):
        from singlestoredb.management.v1.files import manage_files
        from singlestoredb.management.v2.files import FilesManager as V2FM
        mgr = manage_files(
            access_token=FAKE_TOKEN, base_url=FAKE_BASE_URL, version='v2',
        )
        self.assertIsInstance(mgr, V2FM)


class TestRecursiveDownloadPathTraversal(unittest.TestCase):
    """Recursive download helpers must refuse to write outside ``local_path``
    when the remote listing contains traversal segments (``..``)."""

    def _make_file_location(self):
        # FileSpace is a concrete FileLocation subclass; instantiate via
        # __new__ to skip its constructor (which expects a real FilesManager).
        from singlestoredb.management.v1.files import FileSpace
        loc = FileSpace.__new__(FileSpace)
        loc._manager = MagicMock()
        return loc

    def _make_files_object(self, path, type_='file'):
        from singlestoredb.management.v1.files import FilesObject
        return FilesObject(
            name=path.rsplit('/', 1)[-1],
            path=path,
            size=0,
            type=type_,
            format='',
            mimetype='',
            created=None,
            last_modified=None,
            writable=True,
        )

    def test_files_download_folder_rejects_traversal(self):
        import tempfile
        loc = self._make_file_location()
        # Listing returns an entry whose path escapes via '..'
        loc.listdir = MagicMock(
            return_value=[self._make_files_object('../escape.txt')],
        )
        loc._download_file = MagicMock()
        with tempfile.TemporaryDirectory() as tmp:
            target = f'{tmp}/dest'
            import os
            os.makedirs(target)
            with self.assertRaises(ManagementError) as ctx:
                loc.download_folder('remote', target, overwrite=True)
            self.assertIn('outside destination', str(ctx.exception))
            loc._download_file.assert_not_called()

    def test_files_download_folder_rejects_traversal_directory(self):
        import tempfile
        loc = self._make_file_location()
        # Directory entry that escapes
        loc.listdir = MagicMock(
            return_value=[self._make_files_object('../evil', type_='directory')],
        )
        with tempfile.TemporaryDirectory() as tmp:
            target = f'{tmp}/dest'
            import os
            os.makedirs(target)
            with self.assertRaises(ManagementError) as ctx:
                loc.download_folder('remote', target, overwrite=True)
            self.assertIn('outside destination', str(ctx.exception))

    def test_stage_download_folder_rejects_traversal(self):
        import tempfile
        from singlestoredb.management.v1.workspace import Stage
        stage = Stage.__new__(Stage)
        stage.listdir = MagicMock(return_value=['../escape.txt'])
        # is_dir(stage_path) must return True (it's a directory); each
        # listing entry returns False (so it's treated as a file).
        stage.is_dir = MagicMock(side_effect=lambda p: p == 'remote')
        stage.download_file = MagicMock()
        with tempfile.TemporaryDirectory() as tmp:
            target = f'{tmp}/dest'
            import os
            os.makedirs(target)
            with self.assertRaises(ManagementError) as ctx:
                stage.download_folder('remote', target, overwrite=True)
            self.assertIn('outside destination', str(ctx.exception))
            stage.download_file.assert_not_called()


if __name__ == '__main__':
    unittest.main()
