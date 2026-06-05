#!/usr/bin/env python
# type: ignore
"""Tests for versioned management API wrappers (ADR 0001)."""
import unittest
from unittest.mock import MagicMock
from unittest.mock import patch

from singlestoredb.exceptions import ManagementError
from singlestoredb.management.versioned import _import_versioned_module
from singlestoredb.management.versioned import VersionedMixin


FAKE_TOKEN = 'test-token-12345'
FAKE_BASE_URL = 'https://api.example.com'
FAKE_ORG_ID = 'org-12345'


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
        with self.assertRaises(ManagementError):
            _import_versioned_module('v1', 'nonexistent_module')


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


if __name__ == '__main__':
    unittest.main()
