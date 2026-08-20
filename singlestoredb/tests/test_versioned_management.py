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
from singlestoredb.management._version_import import _import_versioned_module


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


def _make_workspace_group(manager=None, group_id='wsg-456', extra_obj=None):
    """Build a v1 WorkspaceGroup from a fake API response.

    ``WorkspaceGroup.from_dict`` calls ``manager.regions`` to resolve the
    region; we stub it so no network call is made.
    """
    from singlestoredb.management.v1.workspace import WorkspaceGroup
    from singlestoredb.management.v1.workspace import WorkspaceManager
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
    with patch.object(
        WorkspaceManager, 'regions',
        new_callable=PropertyMock, return_value=[],
    ):
        wg = WorkspaceGroup.from_dict(obj, mgr)
    return wg, mgr, obj


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
    def test_config_option_does_not_reach_manage_workspaces(self, _mock_token):
        """
        A global preference for v2 must not break the v1-only workspace
        factory. Workspaces do not exist at v2, so the option has nothing to
        say about them; only an explicit ``version=`` is an error.
        """
        from singlestoredb import config
        from singlestoredb.management.workspace import manage_workspaces
        from singlestoredb.management.v1.workspace import (
            WorkspaceManager as V1WM,
        )

        original = config.get_option('management.version')
        try:
            config.set_option('management.version', 'v2')
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
        finally:
            config.set_option('management.version', original or 'v1')

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


class TestTokenStorageFix(unittest.TestCase):
    """Test that Manager authenticates with the resolved token."""

    @patch('singlestoredb.management.manager.is_jwt', return_value=False)
    @patch('singlestoredb.management.manager.get_token', return_value=FAKE_TOKEN)
    def test_none_token_resolves(self, _mock_token, _mock_jwt):
        """When access_token=None, the resolved token is used."""
        from singlestoredb.management.v1.workspace import WorkspaceManager
        mgr = WorkspaceManager(
            access_token=None,
            base_url=FAKE_BASE_URL,
            version='v1',
        )
        self.assertEqual(
            mgr._sess.headers['Authorization'], f'Bearer {FAKE_TOKEN}',
        )

    @patch('singlestoredb.management.manager.get_token', return_value=FAKE_TOKEN)
    def test_explicit_token_used_as_is(self, _mock_token):
        """When access_token is provided, it's used directly."""
        from singlestoredb.management.v1.workspace import WorkspaceManager
        mgr = WorkspaceManager(
            access_token='my-explicit-token',
            base_url=FAKE_BASE_URL,
            version='v1',
        )
        self.assertEqual(
            mgr._sess.headers['Authorization'], 'Bearer my-explicit-token',
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

    def test_v1_region_manager_extends_the_shared_base(self):
        """v1 subclasses the level-set base, not the other way around."""
        from singlestoredb.management.region import RegionManager as Base
        from singlestoredb.management.v1.region import RegionManager as V1
        from singlestoredb.management.v2.region import RegionManager as V2
        self.assertTrue(issubclass(V1, Base))
        self.assertIs(V2, Base)
        self.assertFalse(issubclass(V2, V1))

    def test_shared_tier_regions_raises_at_v2(self):
        mgr = self._make_v2_region_manager()
        with self.assertRaises(ManagementError):
            mgr.list_shared_tier_regions()


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
