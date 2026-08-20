#!/usr/bin/env python
# type: ignore
"""Tests for versioned management API wrappers (ADR 0001)."""
import ast
import datetime
import importlib
import os
import sys
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
    """Patch the ``regions`` property on the v1 and v2 managers to []."""
    from singlestoredb.management.v1.workspace import (
        WorkspaceManager as V1WM,
    )
    from singlestoredb.management.v2.cluster import (
        ClusterManager as V2CM,
    )
    return [
        patch.object(V1WM, 'regions', new_callable=PropertyMock, return_value=[]),
        patch.object(V2CM, 'regions', new_callable=PropertyMock, return_value=[]),
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
        """mgr.v2 returns a ClusterManager from the v2 cluster module."""
        mgr = self._make_manager()
        with patch('singlestoredb.management.manager.get_token', return_value=FAKE_TOKEN):
            v2_mgr = mgr.v2
        from singlestoredb.management.v2.cluster import ClusterManager as V2CM
        self.assertIsInstance(v2_mgr, V2CM)

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
        """ws.v2 constructs the v2 Cluster via from_dict with a v2 manager."""
        ws, _, obj = self._make_workspace()
        with patch('singlestoredb.management.manager.get_token', return_value=FAKE_TOKEN):
            v2_ws = ws.v2
        from singlestoredb.management.v2.cluster import Cluster as V2Cluster
        self.assertIsInstance(v2_ws, V2Cluster)
        self.assertEqual(v2_ws.name, 'test-ws')
        self.assertEqual(v2_ws.id, 'ws-123')
        self.assertEqual(v2_ws.group_id, 'wsg-456')

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
    def test_manage_workspaces_rejects_v2(self, _mock_token):
        """manage_workspaces(version='v2') points the caller at clusters."""
        from singlestoredb.management.workspace import manage_workspaces
        with self.assertRaises(ManagementError) as ctx:
            manage_workspaces(
                access_token=FAKE_TOKEN,
                version='v2',
                base_url=FAKE_BASE_URL,
            )
        self.assertIn('manage_clusters', str(ctx.exception))

    @patch('singlestoredb.management.manager.get_token', return_value=FAKE_TOKEN)
    def test_manage_clusters_returns_v2_manager(self, _mock_token):
        """manage_clusters() defaults to a v2 ClusterManager."""
        from singlestoredb.management.cluster import manage_clusters
        from singlestoredb.management.v2.cluster import ClusterManager as V2CM
        mgr = manage_clusters(
            access_token=FAKE_TOKEN,
            base_url=FAKE_BASE_URL,
        )
        self.assertIsInstance(mgr, V2CM)
        self.assertIn('/v2/', mgr._base_url)

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
    """Test that v2 classes properly inherit from v1 where they share a shape."""

    def test_v2_cluster_does_not_inherit_from_v1(self):
        """Clusters are a fresh v2 resource, not a subclass of Workspace."""
        from singlestoredb.management.v1.workspace import Workspace as V1
        from singlestoredb.management.v2.cluster import Cluster as V2
        self.assertFalse(issubclass(V2, V1))

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
    def test_config_option_routes_manage_regions(self, _mock_token):
        """Setting management.version to v2 routes to v2."""
        from singlestoredb import config
        from singlestoredb.management.region import manage_regions
        from singlestoredb.management.v2.region import RegionManager as V2RM

        original = config.get_option('management.version')
        try:
            config.set_option('management.version', 'v2')
            mgr = manage_regions(
                access_token=FAKE_TOKEN,
                base_url=FAKE_BASE_URL,
            )
            self.assertIsInstance(mgr, V2RM)
        finally:
            config.set_option('management.version', original or 'v1')

    @patch('singlestoredb.management.manager.get_token', return_value=FAKE_TOKEN)
    def test_config_option_cannot_force_workspaces_to_v2(self, _mock_token):
        """management.version='v2' makes manage_workspaces() an error, not a v2 call."""
        from singlestoredb import config
        from singlestoredb.management.workspace import manage_workspaces

        original = config.get_option('management.version')
        try:
            config.set_option('management.version', 'v2')
            with self.assertRaises(ManagementError) as ctx:
                manage_workspaces(
                    access_token=FAKE_TOKEN,
                    base_url=FAKE_BASE_URL,
                )
            self.assertIn('manage_clusters', str(ctx.exception))
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
    ``_location._manager`` must point at the versioned manager, and the
    original entity's location must be left untouched.
    """

    def test_location_manager_rebound_to_versioned_clone(self):
        from singlestoredb.management.v1.workspace import Workspace

        ws_mgr = _make_workspace_manager()
        ws = Workspace.from_dict(
            {
                'name': 'test-ws',
                'workspaceID': 'ws-123',
                'workspaceGroupID': 'wsg-456',
                'size': 'S-00',
                'state': 'Active',
                'createdAt': '2024-01-01T00:00:00Z',
            },
            ws_mgr,
        )

        # Simulate a child location manager that points at the v1 manager.
        class _FakeLocation:
            pass
        loc = _FakeLocation()
        loc._manager = ws_mgr
        ws._location = loc

        with patch(
            'singlestoredb.management.manager.get_token',
            return_value=FAKE_TOKEN,
        ), _MultiPatch(_patch_no_network_regions()):
            v2_ws = ws.v2
            v2_mgr = ws_mgr.v2
        self.assertIs(v2_ws._location._manager, v2_mgr)
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
        # Each hop re-keys the payload into a fresh dict, so identity is not
        # preserved -- but the field names and values must survive intact.
        self.assertIsNot(round_tripped._response, obj)
        self.assertEqual(round_tripped._response, obj)

    def test_workspace_group_has_no_v2_counterpart(self):
        """Workspace groups were dissolved into clusters; ``wg.v2`` must fail."""
        wg, _, _ = _make_workspace_group()
        with patch(
            'singlestoredb.management.manager.get_token',
            return_value=FAKE_TOKEN,
        ), _MultiPatch(_patch_no_network_regions()):
            with self.assertRaises(ManagementError) as ctx:
                wg.v2
        self.assertIn('WorkspaceGroup', str(ctx.exception))
        self.assertIn('v2', str(ctx.exception))


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
        stage.listdir = MagicMock(
            return_value=[self._make_files_object('../escape.txt')],
        )
        # is_dir(stage_path) must return True (it's a directory); the entry
        # type in the listing marks each entry as a file.
        stage.is_dir = MagicMock(side_effect=lambda p: p == 'remote')
        stage._download_file = MagicMock()
        with tempfile.TemporaryDirectory() as tmp:
            target = f'{tmp}/dest'
            import os
            os.makedirs(target)
            with self.assertRaises(ManagementError) as ctx:
                stage.download_folder('remote', target, overwrite=True)
            self.assertIn('outside destination', str(ctx.exception))
            stage._download_file.assert_not_called()


class TestFolderTransferPaths(unittest.TestCase):
    """Folder helpers must address remote objects with the full remote path
    and resolve ``ignore`` globs relative to the local folder."""

    def _make_stage(self):
        from singlestoredb.management.v1.workspace import Stage
        stage = Stage.__new__(Stage)
        stage._manager = MagicMock()
        return stage

    def _make_file_space(self):
        from singlestoredb.management.v1.files import FileSpace
        space = FileSpace.__new__(FileSpace)
        space._manager = MagicMock()
        return space

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

    def _make_local_tree(self, tmp):
        """Create ``<tmp>/src/keep.py`` and ``<tmp>/src/sub/skip.pyc``."""
        import os
        root = os.path.join(tmp, 'src')
        os.makedirs(os.path.join(root, 'sub'))
        keep = os.path.join(root, 'keep.py')
        skip = os.path.join(root, 'sub', 'skip.pyc')
        for path in (keep, skip):
            with open(path, 'w') as f:
                f.write('x')
        return root, keep, skip

    def test_stage_download_folder_prefixes_remote_paths(self):
        import tempfile
        stage = self._make_stage()
        # listdir strips the stage_path prefix from its results
        stage.listdir = MagicMock(
            return_value=[
                self._make_files_object('a.txt'),
                self._make_files_object('sub/b.txt'),
            ],
        )
        stage.is_dir = MagicMock(side_effect=lambda p: p == 'remote/folder')
        stage._download_file = MagicMock()
        with tempfile.TemporaryDirectory() as tmp:
            stage.download_folder('remote/folder', tmp, overwrite=True)
        requested = [call.args[0] for call in stage._download_file.call_args_list]
        self.assertEqual(
            requested, ['remote/folder/a.txt', 'remote/folder/sub/b.txt'],
        )

    def test_stage_download_folder_normalizes_prefix(self):
        import tempfile
        stage = self._make_stage()
        stage.listdir = MagicMock(
            return_value=[self._make_files_object('a.txt')],
        )
        # download_folder normalizes './remote/folder/' before probing.
        stage.is_dir = MagicMock(side_effect=lambda p: p == 'remote/folder')
        stage._download_file = MagicMock()
        with tempfile.TemporaryDirectory() as tmp:
            stage.download_folder('./remote/folder/', tmp, overwrite=True)
        self.assertEqual(
            stage._download_file.call_args_list[0].args[0],
            'remote/folder/a.txt',
        )

    def test_stage_download_folder_uses_listing_type_not_is_dir(self):
        """The entry type comes from the listing, so no per-entry is_dir
        call is made, and empty remote folders are still created locally."""
        import os
        import tempfile
        stage = self._make_stage()
        stage.listdir = MagicMock(
            return_value=[
                self._make_files_object('empty', type_='directory'),
                self._make_files_object('a.txt'),
            ],
        )
        is_dir_calls = []

        def is_dir(p):
            is_dir_calls.append(p)
            return p == 'remote'

        stage.is_dir = is_dir
        stage._download_file = MagicMock()
        with tempfile.TemporaryDirectory() as tmp:
            dest = os.path.join(tmp, 'dest')
            stage.download_folder('remote', dest, overwrite=True)
            # Only the top-level folder check, nothing per entry
            self.assertEqual(is_dir_calls, ['remote'])
            self.assertTrue(os.path.isdir(os.path.join(dest, 'empty')))
        requested = [call.args[0] for call in stage._download_file.call_args_list]
        self.assertEqual(requested, ['remote/a.txt'])

    def test_stage_upload_folder_ignores_folder_patterns(self):
        import os
        import tempfile
        stage = self._make_stage()
        stage.exists = MagicMock(return_value=False)
        stage.upload_file = MagicMock()
        stage.info = MagicMock()
        with tempfile.TemporaryDirectory() as tmp:
            root = os.path.join(tmp, 'src')
            os.makedirs(os.path.join(root, '__pycache__'))
            keep = os.path.join(root, 'keep.py')
            for path in (keep, os.path.join(root, '__pycache__', 'a.pyc')):
                with open(path, 'w') as f:
                    f.write('x')
            stage.upload_folder(root, 'dest', ignore='**/__pycache__')
            uploaded = [
                call.args[0] for call in stage.upload_file.call_args_list
            ]
            self.assertEqual(uploaded, [keep])

    def test_file_space_upload_folder_ignores_folder_patterns(self):
        import os
        import tempfile
        space = self._make_file_space()
        space.upload_file = MagicMock()
        space.info = MagicMock()
        with tempfile.TemporaryDirectory() as tmp:
            root = os.path.join(tmp, 'src')
            os.makedirs(os.path.join(root, '__pycache__'))
            keep = os.path.join(root, 'keep.py')
            for path in (keep, os.path.join(root, '__pycache__', 'a.pyc')):
                with open(path, 'w') as f:
                    f.write('x')
            space.upload_folder(root, 'dest', ignore='**/__pycache__')
            uploaded = [
                call.kwargs['local_path']
                for call in space.upload_file.call_args_list
            ]
            self.assertEqual(uploaded, [keep])

    def test_download_folder_defaults_to_remote_folder_name(self):
        """With no local_path, the destination is the remote folder's name
        in the current directory."""
        import os
        import tempfile
        cwd = os.getcwd()
        for name, obj, attr in (
            ('Stage', self._make_stage(), '_download_file'),
            ('FileSpace', self._make_file_space(), '_download_file'),
        ):
            obj.listdir = MagicMock(
                return_value=[self._make_files_object('a.txt')],
            )
            obj.is_dir = MagicMock(return_value=True)
            setattr(obj, attr, MagicMock())
            with tempfile.TemporaryDirectory() as tmp:
                try:
                    os.chdir(tmp)
                    obj.download_folder('remote/folder')
                finally:
                    os.chdir(cwd)
                target = getattr(obj, attr).call_args_list[0].args[1]
                self.assertEqual(
                    os.path.normpath(target),
                    os.path.join('folder', 'a.txt'),
                    f'{name} wrote to {target}',
                )

    def test_download_folder_root_without_local_path_raises(self):
        for obj in (self._make_stage(), self._make_file_space()):
            obj.listdir = MagicMock(return_value=[])
            obj.is_dir = MagicMock(return_value=True)
            with self.assertRaises(ValueError) as ctx:
                obj.download_folder('/')
            self.assertIn('local_path must be specified', str(ctx.exception))

    def test_download_folder_explicit_local_path_unchanged(self):
        """Explicit local_path keeps writing directly into that directory."""
        import os
        import tempfile
        for obj in (self._make_stage(), self._make_file_space()):
            obj.listdir = MagicMock(
                return_value=[self._make_files_object('a.txt')],
            )
            obj.is_dir = MagicMock(return_value=True)
            obj._download_file = MagicMock()
            with tempfile.TemporaryDirectory() as tmp:
                dest = os.path.join(tmp, 'dest')
                obj.download_folder('remote/folder', dest, overwrite=True)
                self.assertEqual(
                    obj._download_file.call_args_list[0].args[1],
                    os.path.join(dest, 'a.txt'),
                )

    def test_upload_folder_builds_slash_separated_remote_paths(self):
        """Remote paths must use '/' even when the local platform uses '\\'."""
        import tempfile
        stage = self._make_stage()
        stage.exists = MagicMock(return_value=False)
        stage.upload_file = MagicMock()
        stage.info = MagicMock()
        with tempfile.TemporaryDirectory() as tmp:
            root, _, _ = self._make_local_tree(tmp)
            stage.upload_folder(root, 'dest/')
            targets = sorted(
                call.args[1] for call in stage.upload_file.call_args_list
            )
            self.assertEqual(targets, ['dest/keep.py', 'dest/sub/skip.pyc'])
            for target in targets:
                self.assertNotIn('\\', target)

    def test_stage_upload_folder_applies_ignore_globs(self):
        import tempfile
        stage = self._make_stage()
        stage.exists = MagicMock(return_value=False)
        stage.upload_file = MagicMock()
        stage.info = MagicMock()
        with tempfile.TemporaryDirectory() as tmp:
            root, keep, _ = self._make_local_tree(tmp)
            stage.upload_folder(root, 'dest', ignore='**/*.pyc')
            uploaded = [
                call.args[0] for call in stage.upload_file.call_args_list
            ]
            self.assertEqual(uploaded, [keep])

    def test_stage_upload_folder_applies_ignore_globs_to_cwd(self):
        import os
        import tempfile
        stage = self._make_stage()
        stage.exists = MagicMock(return_value=False)
        stage.upload_file = MagicMock()
        stage.info = MagicMock()
        cwd = os.getcwd()
        with tempfile.TemporaryDirectory() as tmp:
            root, _, _ = self._make_local_tree(tmp)
            try:
                os.chdir(root)
                stage.upload_folder('.', 'dest', ignore='**/*.pyc')
            finally:
                os.chdir(cwd)
            uploaded = [
                call.args[0] for call in stage.upload_file.call_args_list
            ]
            self.assertEqual(uploaded, ['keep.py'])

    def test_file_space_upload_folder_applies_ignore_globs(self):
        import tempfile
        space = self._make_file_space()
        space.upload_file = MagicMock()
        space.info = MagicMock()
        with tempfile.TemporaryDirectory() as tmp:
            root, keep, _ = self._make_local_tree(tmp)
            space.upload_folder(root, 'dest', ignore='**/*.pyc')
            uploaded = [
                call.kwargs['local_path']
                for call in space.upload_file.call_args_list
            ]
            self.assertEqual(uploaded, [keep])

    def test_file_space_upload_folder_applies_ignore_globs_to_cwd(self):
        import os
        import tempfile
        space = self._make_file_space()
        space.upload_file = MagicMock()
        space.info = MagicMock()
        cwd = os.getcwd()
        with tempfile.TemporaryDirectory() as tmp:
            root, _, _ = self._make_local_tree(tmp)
            try:
                os.chdir(root)
                space.upload_folder('.', 'dest', ignore='**/*.pyc')
            finally:
                os.chdir(cwd)
            uploaded = [
                call.kwargs['local_path']
                for call in space.upload_file.call_args_list
            ]
            self.assertEqual(uploaded, ['keep.py'])


class TestV1IsDeletable(unittest.TestCase):
    """
    Guard the invariant that makes v1 removable.

    The v1 endpoints will eventually be abandoned, at which point
    ``management/v1/`` should be deletable by ``rm -rf`` plus removal of the
    back-compat shims. That only holds while nothing under ``management/v2/``
    imports from ``management/v1/``: version-neutral code belongs in the
    shared top-level ``management/`` modules, which both version packages
    import sideways.

    If this test fails, the fix is to move the shared code up to
    ``management/`` -- not to add a v1 import to v2.
    """

    def _v2_module_paths(self):
        from singlestoredb.management import v2
        v2_dir = os.path.dirname(v2.__file__)
        return sorted(
            os.path.join(v2_dir, f)
            for f in os.listdir(v2_dir)
            if f.endswith('.py')
        )

    def test_no_v2_module_imports_from_v1(self):
        """No module under management/v2/ may import from management/v1/."""
        offenders = []
        for path in self._v2_module_paths():
            with open(path) as f:
                tree = ast.parse(f.read(), filename=path)
            for node in ast.walk(tree):
                # Relative ``from ..v1.x import y`` shows up as level=2 with
                # module='v1.x'; absolute imports show up with the full path.
                if isinstance(node, ast.ImportFrom):
                    mod = node.module or ''
                    if mod == 'v1' or mod.startswith('v1.') or \
                            'management.v1' in mod:
                        offenders.append(
                            f'{os.path.basename(path)}:{node.lineno}: '
                            f'from {"." * node.level}{mod}',
                        )
                elif isinstance(node, ast.Import):
                    for alias in node.names:
                        if 'management.v1' in alias.name:
                            offenders.append(
                                f'{os.path.basename(path)}:{node.lineno}: '
                                f'import {alias.name}',
                            )

        self.assertEqual(
            offenders, [],
            'management/v2/ must not import from management/v1/; move the '
            'shared code up to management/ instead:\n  ' +
            '\n  '.join(offenders),
        )

    def test_v2_imports_survive_v1_removal(self):
        """Importing every v2 module works with management.v1 blocked."""
        v2_names = [
            'singlestoredb.management.v2.' + os.path.basename(p)[:-3]
            for p in self._v2_module_paths()
            if not os.path.basename(p).startswith('__')
        ]

        # Drop anything already imported so the blocker actually gets
        # consulted, then forbid the v1 package outright.
        saved = {
            k: v for k, v in sys.modules.items()
            if k.startswith('singlestoredb.management.v1')
            or k in v2_names
        }
        for k in saved:
            del sys.modules[k]

        class _BlockV1:
            def find_module(self, fullname, path=None):
                return self.find_spec(fullname, path)

            def find_spec(self, fullname, path=None, target=None):
                if fullname.startswith('singlestoredb.management.v1'):
                    raise AssertionError(
                        f'v2 import chain reached {fullname}; v1 is supposed '
                        'to be removable',
                    )
                return None

        blocker = _BlockV1()
        sys.meta_path.insert(0, blocker)
        try:
            for name in v2_names:
                importlib.import_module(name)
        finally:
            sys.meta_path.remove(blocker)
            sys.modules.update(saved)


if __name__ == '__main__':
    unittest.main()
