#!/usr/bin/env python
# type: ignore
"""
SingleStoreDB v2 Management API testing.

Everything here targets management API v2 -- the flat ``Cluster`` resource and
the starter clusters, stages, secrets, jobs and regions hanging off it. No test
in this file may branch on version; the v1 equivalents live in
``test_management_v1.py``, the version-neutral helper units in
``test_management_utils.py``, and the structural cross-version invariants in
``test_management_versioning.py``.

.. warning:: The ``@pytest.mark.management`` suites below have not been run
   against a live v2 organization. They were written by translating the v1
   suites resource by resource, so every assertion that rests on a v2 response
   or request *shape* rather than on SDK-internal behavior is marked with an
   ``UNVERIFIED`` comment. Treat a failure in one of those as "check the API",
   not automatically as "fix the test".
"""
import os
import random
import re
import secrets
import unittest
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

import singlestoredb as s2
from singlestoredb.exceptions import ManagementError
from singlestoredb.management.job import Status
from singlestoredb.management.job import TargetType
from singlestoredb.management.region import Region
from singlestoredb.management.utils import NamedList


TEST_DIR = os.path.dirname(__file__)

FAKE_TOKEN = 'test-token-12345'
FAKE_BASE_URL = 'https://api.example.com'

# Fake project IDs. These have to be UUID-shaped: a project can be named by
# either its name or its ID, and the wrapper tells the two apart by shape, so a
# stand-in such as 'pr-1' would be read as a name and send the manager off to
# list the organization's projects.
FAKE_PROJECT_ID = '11111111-1111-4111-8111-111111111111'
FAKE_SHARED_PROJECT_ID = '22222222-2222-4222-8222-222222222222'
FAKE_STANDARD_PROJECT_ID = '33333333-3333-4333-8333-333333333333'


def clean_name(s):
    """
    Return ``s`` as a valid v2 cluster name.

    Verified against the live API: a cluster name has to match
    ``[a-z0-9]([a-z0-9-]*[a-z0-9])?`` and be 1-32 characters. Lowercase letters,
    digits and hyphens only -- an uppercase letter, an underscore, a dot, a
    space, or a leading or trailing hyphen all draw
    ``400 name: must be in a valid format``. Repeated hyphens are fine.
    """
    out = re.sub(r'[^\w]', r'-', s).replace('_', '-').lower().strip('-')
    return out or 'x'


def shared_database_name(s):
    """Return a shared database name. Cannot contain special characters except -"""
    return re.sub(r'[^\w]', '', s).replace('-', '_').lower()


def _us_regions(manager):
    """Return the US regions a v2 manager reports, or skip the test."""
    out = [x for x in manager.regions if 'US' in x.name or 'us-' in x.name]
    if not out:
        raise unittest.SkipTest('No US regions reported by the v2 API')
    return out


def _project_id(manager):
    """
    Return the project ID the live v2 suites deploy into, or skip the test.

    ``POST /v2/clusters`` requires ``projectID``, so a project has to be chosen
    before anything can be created. ``SINGLESTOREDB_PROJECT`` wins if it is set;
    otherwise the STANDARD-edition project is used, which is where every
    workspace group the v1 suites create already lands.
    """
    from_env = os.environ.get('SINGLESTOREDB_PROJECT')
    if from_env:
        return from_env

    standard = [x for x in manager.projects if x.edition == 'STANDARD']
    if not standard:
        raise unittest.SkipTest(
            'No STANDARD project in this organization; set '
            'SINGLESTOREDB_PROJECT to the project to deploy into',
        )
    return standard[0].id


#
# Unit tests. These need no token and no deployment.
#

class TestV2RegionBehavior(unittest.TestCase):
    """
    ``RegionManager`` at v2: ``list_regions`` hits ``/v2/regions`` and
    ``list_shared_tier_regions`` hits ``/v2/regions/sharedtier``, which
    answers with the same shape.
    """

    def _make_region_manager(self):
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

    def test_list_regions_uses_regions_endpoint(self):
        mgr = self._make_region_manager()
        get_response = MagicMock()
        # Live shape (2026-08-24): ``region`` is the display name and
        # ``regionName`` the provider slug -- not the other way round.
        get_response.json.return_value = [
            {
                'provider': 'AWS',
                'region': 'US East 1 (N. Virginia)',
                'regionName': 'us-east-1',
            },
            {
                'provider': 'GCP',
                'region': 'US West 2 (Oregon)',
                'regionName': 'us-west2',
            },
        ]
        mgr._get = MagicMock(return_value=get_response)

        regions = mgr.list_regions()
        mgr._get.assert_called_once_with('regions')
        self.assertEqual(len(regions), 2)
        # v2 region entries have id=None -- there is no regionID in the
        # response, so a region is identified by (provider, region_name).
        for r in regions:
            self.assertIsNone(r.id)
        self.assertEqual(regions[0].name, 'US East 1 (N. Virginia)')
        self.assertEqual(regions[0].region_name, 'us-east-1')

    def test_list_shared_tier_regions_uses_sharedtier_endpoint(self):
        mgr = self._make_region_manager()
        get_response = MagicMock()
        # Live shape (2026-08-24): ``GET /v2/regions/sharedtier`` returns 200
        # with exactly the same keys as ``GET /v2/regions``.
        get_response.json.return_value = [
            {
                'provider': 'AWS',
                'region': 'US East 1 (N. Virginia)',
                'regionName': 'us-east-1',
            },
        ]
        mgr._get = MagicMock(return_value=get_response)

        regions = mgr.list_shared_tier_regions()
        mgr._get.assert_called_once_with('regions/sharedtier')
        self.assertEqual(len(regions), 1)
        self.assertEqual(regions[0].name, 'US East 1 (N. Virginia)')
        self.assertEqual(regions[0].region_name, 'us-east-1')
        self.assertIsNone(regions[0].id)


class TestClusterManagerPosting(unittest.TestCase):
    """
    Request bodies the ``ClusterManager`` sends.

    .. warning:: UNVERIFIED. Every field name asserted here comes from the
       wrapper, not from a recorded v2 response, so these tests pin the
       wrapper's current behavior rather than confirming the API accepts it.
       ``create_cluster``'s POST body in particular -- the nested ``size``
       object and the ``provider``/``region`` pair replacing v1's
       ``regionID`` -- needs checking against a live v2 organization.
    """

    def _make_cluster_manager(self):
        from singlestoredb.management.v2.cluster import ClusterManager
        with patch(
            'singlestoredb.management.manager.get_token',
            return_value=FAKE_TOKEN,
        ):
            return ClusterManager(
                access_token=FAKE_TOKEN,
                base_url=FAKE_BASE_URL,
                version='v2',
            )

    def test_create_cluster_body(self):
        mgr = self._make_cluster_manager()
        post_response = MagicMock()
        post_response.json.return_value = {'clusterID': 'cl-1'}
        mgr._post = MagicMock(return_value=post_response)
        sentinel = MagicMock()
        mgr.get_cluster = MagicMock(return_value=sentinel)

        out = mgr.create_cluster(
            'my-cluster',
            provider='AWS',
            region_name='us-east-1',
            size='S-00',
            scale_factor=1.0,
            firewall_ranges=['0.0.0.0/0'],
            admin_password='hunter2',
            update_window={'day': 3, 'hour': 4},
            project_id=FAKE_PROJECT_ID,
        )

        self.assertIs(out, sentinel)
        mgr.get_cluster.assert_called_once_with('cl-1')
        path, kwargs = mgr._post.call_args[0][0], mgr._post.call_args[1]
        self.assertEqual(path, 'clusters')
        body = kwargs['json']
        self.assertEqual(body['name'], 'my-cluster')
        self.assertEqual(body['provider'], 'AWS')
        # v2 names the region by its provider region name; there is no
        # regionID to send.
        self.assertEqual(body['region'], 'us-east-1')
        self.assertNotIn('regionID', body)
        # Size and scale factor are nested in one object.
        self.assertEqual(body['size'], {'size': 'S-00', 'scaleFactor': 1.0})
        self.assertEqual(body['firewallRanges'], ['0.0.0.0/0'])
        self.assertEqual(body['adminPassword'], 'hunter2')
        self.assertEqual(body['updateWindow'], {'day': 3, 'hour': 4})
        # The API rejects a create without projectID.
        self.assertEqual(body['projectID'], FAKE_PROJECT_ID)
        # Unset options are dropped rather than sent as null.
        self.assertNotIn('kai', body)
        self.assertNotIn('autoSuspend', body)

    def test_create_cluster_accepts_a_region_object(self):
        mgr = self._make_cluster_manager()
        post_response = MagicMock()
        post_response.json.return_value = {'clusterID': 'cl-1'}
        mgr._post = MagicMock(return_value=post_response)
        mgr.get_cluster = MagicMock()

        mgr.create_cluster(
            'my-cluster',
            region=Region(
                name='us-east-1', provider='AWS',
                id=None, region_name='us-east-1',
            ),
            project_id=FAKE_PROJECT_ID,
        )
        body = mgr._post.call_args[1]['json']
        self.assertEqual(body['provider'], 'AWS')
        self.assertEqual(body['region'], 'us-east-1')

    def test_create_starter_cluster_body(self):
        mgr = self._make_cluster_manager()
        post_response = MagicMock()
        # UNVERIFIED: the starter-cluster create response is expected to name
        # the new deployment ``virtualClusterID``.
        post_response.json.return_value = {'virtualClusterID': 'vc-1'}
        mgr._post = MagicMock(return_value=post_response)
        mgr.get_starter_cluster = MagicMock(return_value='sentinel')

        out = mgr.create_starter_cluster(
            'my-starter', database_name='db1',
            provider='AWS', region_name='us-east-1',
        )
        self.assertEqual(out, 'sentinel')
        mgr.get_starter_cluster.assert_called_once_with('vc-1')
        self.assertEqual(
            mgr._post.call_args[1]['json'], {
                'name': 'my-starter',
                'databaseName': 'db1',
                'provider': 'AWS',
                'regionName': 'us-east-1',
            },
        )

    def test_create_cluster_returns_the_generated_admin_password(self):
        """
        The generated password is carried off the create response.

        Verified live: ``POST /v2/clusters`` generates the admin password no
        matter what ``adminPassword`` is sent, returns it in the create
        response, and reports it nowhere else -- ``GET /v2/clusters/{id}`` has
        no such field. Losing it means losing access to the cluster.
        """
        from singlestoredb.management.v2.cluster import Cluster
        mgr = self._make_cluster_manager()
        post_response = MagicMock()
        post_response.json.return_value = {
            'clusterID': 'cl-1', 'adminPassword': 'generated-not-hunter2',
        }
        mgr._post = MagicMock(return_value=post_response)
        cluster = Cluster(name='my-cluster', id='cl-1', state='PENDING')
        mgr.get_cluster = MagicMock(return_value=cluster)

        out = mgr.create_cluster(
            'my-cluster', provider='AWS', region_name='us-east-1',
            admin_password='hunter2', project_id=FAKE_PROJECT_ID,
        )
        self.assertEqual(out.admin_password, 'generated-not-hunter2')
        # A cluster that did not come from a create has no password to report.
        self.assertIsNone(Cluster(name='x', id='cl-2', state='ACTIVE').admin_password)
        # And it must not leak into the string representations.
        self.assertNotIn('generated-not-hunter2', str(out))
        self.assertNotIn('generated-not-hunter2', repr(out))

    def test_create_starter_cluster_upper_cases_the_provider(self):
        """
        The shared-tier route accepts only AWS | AZURE | GCP verbatim.

        Verified live: 'Azure' -- the spelling ``GET /v2/regions`` itself
        reports -- fails with ``500 Unspecified is not a valid
        CloudServiceProvider``, so a region's ``provider`` cannot be passed
        through as-is. ``POST /v2/clusters`` has no such restriction.
        """
        mgr = self._make_cluster_manager()
        post_response = MagicMock()
        post_response.json.return_value = {'virtualClusterID': 'vc-1'}
        mgr._post = MagicMock(return_value=post_response)
        mgr.get_starter_cluster = MagicMock()

        mgr.create_starter_cluster(
            'my-starter', database_name='db1',
            provider='Azure', region_name='southcentralus',
        )
        self.assertEqual(mgr._post.call_args[1]['json']['provider'], 'AZURE')

    def test_create_starter_cluster_without_an_id_raises(self):
        mgr = self._make_cluster_manager()
        post_response = MagicMock()
        post_response.json.return_value = {}
        mgr._post = MagicMock(return_value=post_response)
        with self.assertRaises(ManagementError):
            mgr.create_starter_cluster(
                'my-starter', database_name='db1',
                provider='AWS', region_name='us-east-1',
            )

    def test_shared_tier_regions_uses_sharedtier_endpoint(self):
        mgr = self._make_cluster_manager()
        get_response = MagicMock()
        get_response.json.return_value = [
            {
                'provider': 'AWS',
                'region': 'US East 1 (N. Virginia)',
                'regionName': 'us-east-1',
            },
        ]
        mgr._get = MagicMock(return_value=get_response)

        regions = mgr.shared_tier_regions
        mgr._get.assert_called_once_with('regions/sharedtier')
        self.assertEqual(len(regions), 1)
        self.assertEqual(regions[0].name, 'US East 1 (N. Virginia)')
        self.assertEqual(regions[0].region_name, 'us-east-1')


class TestClusterFirewallWaiting(unittest.TestCase):
    """
    Waiting for the asynchronously-applied firewall.

    Verified live: ``POST /v2/clusters`` and ``PATCH /v2/clusters/{id}`` apply
    ``firewallRanges`` outside the state machine. The cluster reaches ACTIVE
    with a resolvable endpoint while ``GET /v2/clusters/{id}`` still reports
    ``firewallRanges: []`` and ``allowAllTraffic: null``, which denies all
    inbound traffic, so a connect attempt in that window times out at the TCP
    level.

    Also verified live: a requested ``firewallRanges: ['0.0.0.0/0']`` is stored
    as ``allowAllTraffic: True`` with ``firewallRanges: []`` -- and that
    cluster does accept connections (port 3306 open) -- so "reachable" is
    either one, not non-empty ranges.
    """

    def _make_cluster_manager(self):
        from singlestoredb.management.v2.cluster import ClusterManager
        with patch(
            'singlestoredb.management.manager.get_token',
            return_value=FAKE_TOKEN,
        ):
            return ClusterManager(
                access_token=FAKE_TOKEN,
                base_url=FAKE_BASE_URL,
                version='v2',
            )

    def _cluster(
        self, firewall_ranges=None, state='ACTIVE', manager=None,
        allow_all_traffic=None,
    ):
        from singlestoredb.management.v2.cluster import Cluster
        out = Cluster(
            name='my-cluster', id='cl-1', state=state,
            endpoint='svc.singlestore.com',
            firewall_ranges=firewall_ranges,
            allow_all_traffic=allow_all_traffic,
        )
        out._manager = manager
        return out

    def test_wait_on_firewall_polls_until_non_empty(self):
        mgr = self._make_cluster_manager()
        pending = self._cluster(firewall_ranges=[])
        applied = self._cluster(firewall_ranges=['0.0.0.0/0'])
        mgr.get_cluster = MagicMock(
            side_effect=[self._cluster(firewall_ranges=[]), pending, applied],
        )

        with patch('singlestoredb.management.v2.cluster.time.sleep'):
            out = mgr._wait_on_firewall(
                self._cluster(firewall_ranges=[]), interval=1,
            )

        self.assertIs(out, applied)
        self.assertEqual(mgr.get_cluster.call_count, 3)

    def test_wait_on_firewall_times_out(self):
        mgr = self._make_cluster_manager()
        mgr.get_cluster = MagicMock(return_value=self._cluster(firewall_ranges=[]))

        with patch('singlestoredb.management.v2.cluster.time.sleep'):
            with self.assertRaises(ManagementError) as cm:
                mgr._wait_on_firewall(
                    self._cluster(firewall_ranges=[]), interval=1, timeout=3,
                )
        assert 'cl-1' in cm.exception.msg, cm.exception.msg
        assert 'refuses all inbound' in cm.exception.msg, cm.exception.msg

    def test_wait_on_firewall_expected_waits_for_the_new_ranges(self):
        """
        On an existing cluster, non-empty says nothing -- the pre-PATCH ranges
        are already non-empty -- so the update path waits for the ranges asked
        for.
        """
        mgr = self._make_cluster_manager()
        new = self._cluster(firewall_ranges=['192.168.0.0/16'])
        mgr.get_cluster = MagicMock(
            side_effect=[self._cluster(firewall_ranges=['0.0.0.0/0']), new],
        )

        with patch('singlestoredb.management.v2.cluster.time.sleep'):
            out = mgr._wait_on_firewall(
                self._cluster(firewall_ranges=['0.0.0.0/0']),
                interval=1, expected=['192.168.0.0/16'],
            )

        self.assertIs(out, new)
        self.assertEqual(mgr.get_cluster.call_count, 2)

    def _create(self, mgr, **kwargs):
        post_response = MagicMock()
        post_response.json.return_value = {'clusterID': 'cl-1'}
        mgr._post = MagicMock(return_value=post_response)
        with patch('singlestoredb.management.v2.cluster.time.sleep'), \
                patch('singlestoredb.management.manager.time.sleep'):
            return mgr.create_cluster(
                'my-cluster', provider='AWS', region_name='us-east-1',
                project_id=FAKE_PROJECT_ID, wait_interval=1, **kwargs,
            )

    def test_create_cluster_waits_on_the_firewall(self):
        mgr = self._make_cluster_manager()
        applied = self._cluster(firewall_ranges=['0.0.0.0/0'])
        mgr.get_cluster = MagicMock(
            side_effect=[
                self._cluster(firewall_ranges=[]),
                self._cluster(firewall_ranges=[]),
                applied,
            ],
        )

        out = self._create(
            mgr, firewall_ranges=['0.0.0.0/0'], wait_on_active=True,
        )
        self.assertIs(out, applied)
        self.assertEqual(mgr.get_cluster.call_count, 3)

    def test_create_cluster_waits_on_the_firewall_for_allow_all_traffic(self):
        mgr = self._make_cluster_manager()
        applied = self._cluster(firewall_ranges=[], allow_all_traffic=True)
        mgr.get_cluster = MagicMock(
            side_effect=[self._cluster(firewall_ranges=[]), applied],
        )

        out = self._create(mgr, allow_all_traffic=True, wait_on_active=True)
        self.assertIs(out, applied)
        self.assertEqual(mgr.get_cluster.call_count, 2)

    def test_create_cluster_accepts_allow_all_traffic_as_the_applied_form(self):
        """
        ``firewall_ranges=['0.0.0.0/0']`` comes back as ``allowAllTraffic``.

        Verified live: the API stores it that way and leaves ``firewallRanges``
        empty, and the endpoint accepts connections. Waiting for non-empty
        ranges here would hang for the full timeout on a cluster that is
        already reachable.
        """
        mgr = self._make_cluster_manager()
        applied = self._cluster(firewall_ranges=[], allow_all_traffic=True)
        mgr.get_cluster = MagicMock(
            side_effect=[self._cluster(firewall_ranges=[]), applied],
        )

        out = self._create(
            mgr, firewall_ranges=['0.0.0.0/0'], wait_on_active=True,
        )
        self.assertIs(out, applied)
        self.assertEqual(mgr.get_cluster.call_count, 2)

    def test_wait_on_firewall_expected_accepts_allow_all_traffic(self):
        """A requested 0.0.0.0/0 is satisfied by allow_all_traffic."""
        mgr = self._make_cluster_manager()
        applied = self._cluster(firewall_ranges=[], allow_all_traffic=True)
        mgr.get_cluster = MagicMock(side_effect=[applied])

        with patch('singlestoredb.management.v2.cluster.time.sleep'):
            out = mgr._wait_on_firewall(
                self._cluster(firewall_ranges=['10.0.0.0/8']),
                interval=1, expected=['0.0.0.0/0'],
            )
        self.assertIs(out, applied)

        # ...but a narrower range is not.
        mgr.get_cluster = MagicMock(
            return_value=self._cluster(
                firewall_ranges=[], allow_all_traffic=True,
            ),
        )
        with patch('singlestoredb.management.v2.cluster.time.sleep'):
            with self.assertRaises(ManagementError):
                mgr._wait_on_firewall(
                    self._cluster(firewall_ranges=['10.0.0.0/8']),
                    interval=1, timeout=3, expected=['192.168.0.0/16'],
                )

    def test_create_cluster_does_not_wait_without_a_firewall_request(self):
        """
        ``firewall_ranges=[]`` is a legitimate deny-all request -- the field
        must be present and an empty list disallows all inbound traffic -- so
        it must not hang waiting for a non-empty value that never comes.
        """
        for ranges in ([], None):
            with self.subTest(firewall_ranges=ranges):
                mgr = self._make_cluster_manager()
                created = self._cluster(firewall_ranges=ranges)
                mgr.get_cluster = MagicMock(return_value=created)

                out = self._create(
                    mgr, firewall_ranges=ranges, wait_on_active=True,
                )
                self.assertIs(out, created)
                self.assertEqual(mgr.get_cluster.call_count, 1)

    def test_create_cluster_does_not_wait_without_wait_on_active(self):
        mgr = self._make_cluster_manager()
        created = self._cluster(firewall_ranges=[])
        mgr.get_cluster = MagicMock(return_value=created)

        out = self._create(mgr, firewall_ranges=['0.0.0.0/0'])
        self.assertIs(out, created)
        self.assertEqual(mgr.get_cluster.call_count, 1)

    def test_update_waits_only_when_asked(self):
        mgr = self._make_cluster_manager()
        mgr._patch = MagicMock()
        cluster = self._cluster(firewall_ranges=['0.0.0.0/0'], manager=mgr)

        # Without wait_on_active, only the trailing refresh() re-fetches, and
        # it reports the pre-PATCH ranges.
        stale = self._cluster(firewall_ranges=['0.0.0.0/0'], manager=mgr)
        mgr.get_cluster = MagicMock(return_value=stale)
        cluster.update(firewall_ranges=['192.168.0.0/16'])
        self.assertEqual(mgr.get_cluster.call_count, 1)
        self.assertEqual(cluster.firewall_ranges, ['0.0.0.0/0'])

        # With it, the new ranges are polled for.
        mgr.get_cluster = MagicMock(
            side_effect=[
                self._cluster(firewall_ranges=['0.0.0.0/0'], manager=mgr),
                self._cluster(firewall_ranges=['192.168.0.0/16'], manager=mgr),
                self._cluster(firewall_ranges=['192.168.0.0/16'], manager=mgr),
            ],
        )
        with patch('singlestoredb.management.v2.cluster.time.sleep'), \
                patch('singlestoredb.management.manager.time.sleep'):
            cluster.update(
                firewall_ranges=['192.168.0.0/16'],
                wait_on_active=True, wait_interval=1,
            )
        self.assertEqual(mgr.get_cluster.call_count, 3)
        self.assertEqual(cluster.firewall_ranges, ['192.168.0.0/16'])


class TestProjects(unittest.TestCase):
    """
    Projects and the project ID ``create_cluster`` sends.

    ``POST /v2/clusters`` rejects a body without ``projectID`` -- verified
    against a live v2 organization -- where ``POST /v1/workspaceGroups``
    assigned one implicitly. So a v2 create has to resolve a project first.
    """

    #: A ``GET /v2/projects`` response, as returned by the live API.
    PROJECTS = [
        {
            'createdAt': '2025-10-15T11:22:33.454592Z',
            'edition': 'SHARED',
            'name': 'Shared Project',
            'projectID': FAKE_SHARED_PROJECT_ID,
        },
        {
            'createdAt': '2025-10-15T11:22:33.454592Z',
            'edition': 'STANDARD',
            'name': 'Standard Project',
            'projectID': FAKE_STANDARD_PROJECT_ID,
        },
    ]

    def _make_cluster_manager(self, projects=None):
        from singlestoredb.management.v2.cluster import ClusterManager
        with patch(
            'singlestoredb.management.manager.get_token',
            return_value=FAKE_TOKEN,
        ):
            mgr = ClusterManager(
                access_token=FAKE_TOKEN,
                base_url=FAKE_BASE_URL,
                version='v2',
            )
        if projects is not None:
            get_response = MagicMock()
            get_response.json.return_value = projects
            mgr._get = MagicMock(return_value=get_response)
        return mgr

    def _without_env(self):
        """Patch the environment with SINGLESTOREDB_PROJECT removed."""
        ctx = patch.dict(os.environ)
        ctx.start()
        os.environ.pop('SINGLESTOREDB_PROJECT', None)
        self.addCleanup(ctx.stop)

    def test_projects_lists_from_the_projects_endpoint(self):
        mgr = self._make_cluster_manager(self.PROJECTS)
        projects = mgr.projects
        mgr._get.assert_called_once_with('projects')
        self.assertIsInstance(projects, NamedList)
        self.assertEqual(
            [x.id for x in projects],
            [FAKE_SHARED_PROJECT_ID, FAKE_STANDARD_PROJECT_ID],
        )
        self.assertEqual([x.edition for x in projects], ['SHARED', 'STANDARD'])
        # NamedList lookup works by name and by ID.
        self.assertEqual(projects['Standard Project'].id, FAKE_STANDARD_PROJECT_ID)
        self.assertEqual(projects[FAKE_SHARED_PROJECT_ID].name, 'Shared Project')
        self.assertEqual(projects[0].created_at.year, 2025)

    def test_get_project(self):
        mgr = self._make_cluster_manager(self.PROJECTS[1])
        project = mgr.get_project(FAKE_STANDARD_PROJECT_ID)
        mgr._get.assert_called_once_with(f'projects/{FAKE_STANDARD_PROJECT_ID}')
        self.assertEqual(project.name, 'Standard Project')

    def test_explicit_project_id_wins_over_the_environment(self):
        mgr = self._make_cluster_manager()
        with patch.dict(
            os.environ, {'SINGLESTOREDB_PROJECT': FAKE_STANDARD_PROJECT_ID},
        ):
            self.assertEqual(
                mgr._resolve_project_id(FAKE_PROJECT_ID), FAKE_PROJECT_ID,
            )

    def test_environment_used_when_no_project_id_is_passed(self):
        mgr = self._make_cluster_manager(self.PROJECTS)
        with patch.dict(
            os.environ, {'SINGLESTOREDB_PROJECT': FAKE_STANDARD_PROJECT_ID},
        ):
            self.assertEqual(
                mgr._resolve_project_id(), FAKE_STANDARD_PROJECT_ID,
            )
        # An ID answers without listing projects.
        mgr._get.assert_not_called()

    def test_a_project_may_be_named_instead_of_identified(self):
        mgr = self._make_cluster_manager(self.PROJECTS)
        self.assertEqual(
            mgr._resolve_project_id('Standard Project'),
            FAKE_STANDARD_PROJECT_ID,
        )
        mgr._get.assert_called_once_with('projects')

    def test_the_environment_may_name_a_project(self):
        mgr = self._make_cluster_manager(self.PROJECTS)
        with patch.dict(
            os.environ, {'SINGLESTOREDB_PROJECT': 'Shared Project'},
        ):
            self.assertEqual(
                mgr._resolve_project_id(), FAKE_SHARED_PROJECT_ID,
            )

    def test_an_unknown_project_name_raises_and_lists_the_projects(self):
        mgr = self._make_cluster_manager(self.PROJECTS)
        with self.assertRaises(ManagementError) as cm:
            mgr._resolve_project_id('Nonexistent Project')
        msg = str(cm.exception)
        self.assertIn('Nonexistent Project', msg)
        self.assertIn('Standard Project', msg)
        self.assertIn(FAKE_SHARED_PROJECT_ID, msg)

    def test_an_ambiguous_project_name_raises(self):
        # The API does not promise unique names, so two projects may share one.
        twins = [
            dict(self.PROJECTS[0], name='Twin'),
            dict(self.PROJECTS[1], name='Twin'),
        ]
        mgr = self._make_cluster_manager(twins)
        with self.assertRaises(ManagementError) as cm:
            mgr._resolve_project_id('Twin')
        msg = str(cm.exception)
        self.assertIn(FAKE_SHARED_PROJECT_ID, msg)
        self.assertIn(FAKE_STANDARD_PROJECT_ID, msg)

    def test_create_starter_cluster_resolves_a_project_name(self):
        mgr = self._make_cluster_manager(self.PROJECTS)
        post_response = MagicMock()
        post_response.json.return_value = {'virtualClusterID': 'vc-1'}
        mgr._post = MagicMock(return_value=post_response)
        mgr.get_starter_cluster = MagicMock()

        mgr.create_starter_cluster(
            'my-starter', database_name='db1', provider='AWS',
            region_name='us-east-1', project_id='Standard Project',
        )
        self.assertEqual(
            mgr._post.call_args[1]['json']['projectID'],
            FAKE_STANDARD_PROJECT_ID,
        )

    def test_a_sole_project_is_the_default(self):
        self._without_env()
        mgr = self._make_cluster_manager(self.PROJECTS[:1])
        self.assertEqual(mgr._resolve_project_id(), FAKE_SHARED_PROJECT_ID)

    def test_more_than_one_project_raises_and_names_them(self):
        self._without_env()
        mgr = self._make_cluster_manager(self.PROJECTS)
        with self.assertRaises(ManagementError) as cm:
            mgr._resolve_project_id()
        msg = str(cm.exception)
        self.assertIn(FAKE_SHARED_PROJECT_ID, msg)
        self.assertIn('Standard Project', msg)
        self.assertIn('SINGLESTOREDB_PROJECT', msg)

    def test_no_projects_raises(self):
        self._without_env()
        mgr = self._make_cluster_manager([])
        with self.assertRaises(ManagementError):
            mgr._resolve_project_id()

    def test_create_cluster_resolves_the_project(self):
        self._without_env()
        mgr = self._make_cluster_manager(self.PROJECTS[:1])
        post_response = MagicMock()
        post_response.json.return_value = {'clusterID': 'cl-1'}
        mgr._post = MagicMock(return_value=post_response)
        mgr.get_cluster = MagicMock()

        mgr.create_cluster('my-cluster', provider='AWS', region_name='us-east-1')
        self.assertEqual(
            mgr._post.call_args[1]['json']['projectID'], FAKE_SHARED_PROJECT_ID,
        )


class TestClusterFromDict(unittest.TestCase):
    """
    ``Cluster.from_dict`` against a v2 payload.

    .. warning:: UNVERIFIED response shape -- the keys below are the ones the
       wrapper reads, not keys observed on the wire.
    """

    def _payload(self, **overrides):
        obj = {
            'name': 'my-cluster',
            'clusterID': 'cl-1',
            'state': 'ACTIVE',
            # Size is reported as an object, not a bare string.
            'size': {'size': 'S-00', 'scaleFactor': 1.0},
            'createdAt': '2024-03-15T12:30:45Z',
            'endpoint': 'svc.example.com',
            'provider': 'AWS',
            'region': 'us-east-1',
            'firewallRanges': ['0.0.0.0/0'],
        }
        obj.update(overrides)
        return obj

    def test_fields_and_timestamps(self):
        from singlestoredb.management.v2.cluster import Cluster
        mgr = MagicMock()
        c = Cluster.from_dict(self._payload(), mgr)
        self.assertEqual(c.id, 'cl-1')
        self.assertEqual(c.name, 'my-cluster')
        self.assertEqual(c.state, 'ACTIVE')
        self.assertEqual(c.provider, 'AWS')
        self.assertEqual(c.size, 'S-00')
        self.assertEqual(c.scale_factor, 1.0)
        self.assertEqual(c.region_name, 'us-east-1')
        self.assertEqual(c.created_at.year, 2024)
        self.assertEqual(c.created_at.month, 3)
        self.assertEqual(c.firewall_ranges, ['0.0.0.0/0'])

    def test_no_manager_raises(self):
        from singlestoredb.management.v2.cluster import Cluster
        mgr = MagicMock()
        c = Cluster.from_dict(self._payload(), mgr)
        c._manager = None
        with self.assertRaises(ManagementError) as cm:
            c.refresh()
        self.assertIn('cluster manager', cm.exception.msg)
        with self.assertRaises(ManagementError):
            c.terminate()

    def test_missing_endpoint_blocks_connect(self):
        from singlestoredb.management.v2.cluster import Cluster
        c = Cluster.from_dict(self._payload(endpoint=None), MagicMock())
        with self.assertRaises(ManagementError) as cm:
            c.connect(user='admin', password='x')
        self.assertIn('endpoint', cm.exception.msg)

    def test_stage_is_nested_under_the_cluster(self):
        from singlestoredb.management.v2.cluster import Cluster
        c = Cluster.from_dict(self._payload(), MagicMock())
        self.assertEqual(
            c.stage._fs_path('a.sql'), 'clusters/cl-1/stage/fs/a.sql',
        )


#
# Live suites. These need SINGLESTOREDB_MANAGEMENT_TOKEN and an organization
# with v2 access, and they create and destroy real deployments.
#

@pytest.mark.management
class TestCluster(unittest.TestCase):

    manager = None
    cluster = None
    password = None

    @classmethod
    def setUpClass(cls):
        cls.manager = s2.manage_clusters(version='v2')

        us_regions = _us_regions(cls.manager)

        name = clean_name(secrets.token_urlsafe(20)[:20])
        region = random.choice(us_regions)

        # v2 has no workspace group: the cluster is created in one call, with
        # the firewall settings passed alongside the compute settings.
        cls.cluster = cls.manager.create_cluster(
            f'cl-test-{name}',
            provider=region.provider,
            region_name=region.region_name or region.name,
            size='S-00',
            firewall_ranges=['0.0.0.0/0'],
            project_id=_project_id(cls.manager),
            wait_on_active=True,
        )

        # v2 generates the admin password and reports it only in the create
        # response; anything passed as admin_password= is ignored. So the
        # password has to be read back rather than chosen here.
        cls.password = cls.cluster.admin_password

        # The firewall is applied asynchronously, after the cluster is already
        # ACTIVE with a resolvable endpoint; until it lands the cluster admits
        # nothing and refuses every inbound connection, so test_connect would
        # time out at the TCP level. wait_on_active covers that, and this
        # asserts it did -- no polling needed here.
        #
        # Verified live: a requested firewall_ranges=['0.0.0.0/0'] is stored as
        # allow_all_traffic=True with firewall_ranges == [], so either one
        # means reachable.
        assert cls.cluster.allow_all_traffic or cls.cluster.firewall_ranges, (
            'create_cluster(wait_on_active=True) returned a cluster whose '
            'firewall still admits nothing; every inbound connection would be '
            'refused'
        )

    @classmethod
    def tearDownClass(cls):
        if cls.cluster is not None:
            cls.cluster.terminate(force=True)
        cls.cluster = None
        cls.manager = None
        cls.password = None

    def test_str(self):
        assert self.cluster.name in str(self.cluster)

    def test_repr(self):
        assert repr(self.cluster) == str(self.cluster)

    def test_regions(self):
        out = self.manager.regions
        providers = {x.provider for x in out}
        assert any(
            p in providers for p in ('Azure', 'GCP', 'AWS', 'azure', 'gcp', 'aws')
        ), providers
        # v2 regions carry no ID, so they are addressable by name only.
        for region in out:
            assert region.id is None, region

    def test_clusters(self):
        clusters = self.manager.clusters
        ids = [x.id for x in clusters]
        names = [x.name for x in clusters]
        assert self.cluster.id in ids
        assert self.cluster.name in names

        assert clusters.ids() == ids
        assert clusters.names() == names

        objs = {}
        for item in clusters:
            objs[item.id] = item
            objs[item.name] = item

        name = random.choice(names)
        assert clusters[name] == objs[name]
        id = random.choice(ids)
        assert clusters[id] == objs[id]

    def test_get_cluster(self):
        cluster = self.manager.get_cluster(self.cluster.id)
        assert cluster.id == self.cluster.id, cluster.id

        with self.assertRaises(s2.ManagementError):
            self.manager.get_cluster('bad id')

    def test_update(self):
        """
        Update the firewall, and show that ``name`` is not updatable.

        Both halves live in one test because each ``PATCH /v2/clusters/{id}``
        cycles the cluster back through PENDING, and a second test issuing its
        own PATCH while that is in flight is asking for trouble.

        On the name: verified live that v2 clusters cannot be renamed, unlike
        v1 workspace groups. ``name`` is a *known* field on the PATCH route --
        an unknown field draws ``400 request body contains an unknown field``
        and ``name`` does not -- and the request succeeds, even cycling the
        cluster through PENDING, but the name never changes in either
        ``GET /v2/clusters/{id}`` or ``GET /v2/clusters`` (polled for two
        minutes). Pinned here so the API growing real rename support is
        noticed rather than assumed.
        """
        # setUpClass asked for ['0.0.0.0/0'], which the API may store either
        # verbatim or as allow_all_traffic with the ranges left empty.
        opened = self.cluster.allow_all_traffic \
            or self.cluster.firewall_ranges == ['0.0.0.0/0']
        assert opened, (
            self.cluster.allow_all_traffic, self.cluster.firewall_ranges,
        )

        # The PATCH is applied asynchronously: without wait_on_active the
        # refresh() inside update() still reports the old ranges.
        self.cluster.update(
            firewall_ranges=['192.168.0.0/16'], wait_on_active=True,
        )

        cluster = self.cluster
        assert cluster.firewall_ranges == ['192.168.0.0/16'], \
            cluster.firewall_ranges

        name = cluster.name.replace('cl-test-', 'cl-foo-')
        assert name != cluster.name
        cluster.update(name=name)

        assert cluster.name != name, cluster.name
        assert self.manager.get_cluster(cluster.id).name != name

    def test_no_manager(self):
        cluster = self.manager.get_cluster(self.cluster.id)
        cluster._manager = None

        with self.assertRaises(s2.ManagementError) as cm:
            cluster.refresh()
        assert 'cluster manager' in cm.exception.msg, cm.exception.msg

        with self.assertRaises(s2.ManagementError) as cm:
            cluster.terminate()
        assert 'cluster manager' in cm.exception.msg, cm.exception.msg

    def test_connect(self):
        with self.cluster.connect(user='admin', password=self.password) as conn:
            with conn.cursor() as cur:
                cur.execute('show databases')
                assert 'cluster' in [x[0] for x in list(cur)]

        # Test missing endpoint
        cluster = self.manager.get_cluster(self.cluster.id)
        cluster.endpoint = None

        with self.assertRaises(s2.ManagementError) as cm:
            cluster.connect(user='admin', password=self.password)
        assert 'endpoint' in cm.exception.msg, cm.exception.msg


@pytest.mark.management
class TestStarterCluster(unittest.TestCase):

    manager = None
    starter_cluster = None

    @classmethod
    def setUpClass(cls):
        cls.manager = s2.manage_clusters(version='v2')

        # Starter regions come from GET /v2/regions/sharedtier, which answers
        # at v2 with the same shape as GET /v2/regions. Only regions on that
        # list work -- anything else gets a 500 'no shared tier region found
        # for provider X and region Y' out of POST /v2/sharedtier/
        # virtualClusters -- so discover rather than sampling all regions.
        regions = list(cls.manager.shared_tier_regions)
        if not regions:
            raise unittest.SkipTest(
                'no shared-tier capable region is available to this '
                'organization',
            )

        cls.starter_username = 'starter_user'
        cls.password = secrets.token_urlsafe(20)

        name = shared_database_name(secrets.token_urlsafe(20)[:20])
        cls.database_name = f'starter_db_{name}'

        region = random.choice(regions)

        cls.starter_cluster = cls.manager.create_starter_cluster(
            f'starter-cl-test-{name}',
            database_name=cls.database_name,
            provider=region.provider,
            region_name=region.region_name or region.name,
        )

        cls.starter_cluster.create_user(
            username=cls.starter_username,
            password=cls.password,
        )

    @classmethod
    def tearDownClass(cls):
        if cls.starter_cluster is not None:
            cls.starter_cluster.terminate()
        cls.starter_cluster = None
        cls.manager = None
        cls.password = None

    def test_str(self):
        assert self.starter_cluster.name in str(self.starter_cluster)

    def test_repr(self):
        assert repr(self.starter_cluster) == str(self.starter_cluster)

    def test_get_starter_cluster(self):
        cluster = self.manager.get_starter_cluster(self.starter_cluster.id)
        assert cluster.id == self.starter_cluster.id, cluster.id

        with self.assertRaises(s2.ManagementError):
            self.manager.get_starter_cluster('bad id')

    def test_starter_clusters(self):
        clusters = self.manager.starter_clusters
        ids = [x.id for x in clusters]
        names = [x.name for x in clusters]
        assert self.starter_cluster.id in ids
        assert self.starter_cluster.name in names

        objs = {}
        for item in clusters:
            objs[item.id] = item
            objs[item.name] = item

        name = random.choice(names)
        assert clusters[name] == objs[name]
        id = random.choice(ids)
        assert clusters[id] == objs[id]

    def test_no_manager(self):
        cluster = self.manager.get_starter_cluster(self.starter_cluster.id)
        cluster._manager = None

        with self.assertRaises(s2.ManagementError) as cm:
            cluster.refresh()
        assert 'cluster manager' in cm.exception.msg, cm.exception.msg

        with self.assertRaises(s2.ManagementError) as cm:
            cluster.terminate()
        assert 'cluster manager' in cm.exception.msg, cm.exception.msg

    def test_connect(self):
        with self.starter_cluster.connect(
            user=self.starter_username,
            password=self.password,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute('show databases')
                assert self.database_name in [x[0] for x in list(cur)]

        # Test missing endpoint
        cluster = self.manager.get_starter_cluster(self.starter_cluster.id)
        cluster.endpoint = None

        with self.assertRaises(s2.ManagementError) as cm:
            cluster.connect(user=self.starter_username, password=self.password)
        assert 'endpoint' in cm.exception.msg, cm.exception.msg


@pytest.mark.management
class TestStage(unittest.TestCase):
    """
    Stage at v2 hangs off the cluster (``clusters/{id}/stage/fs/``) rather
    than being a top-level resource keyed by workspace group.
    """

    manager = None
    cluster = None
    password = None

    @classmethod
    def setUpClass(cls):
        cls.manager = s2.manage_clusters(version='v2')

        us_regions = _us_regions(cls.manager)

        name = clean_name(secrets.token_urlsafe(20)[:20])
        region = random.choice(us_regions)

        # UNVERIFIED: v1 could reach a stage from a workspace group without
        # ever starting a workspace. At v2 there is no group, so a cluster has
        # to exist for its stage to be addressable.
        cls.cluster = cls.manager.create_cluster(
            f'cl-test-{name}',
            provider=region.provider,
            region_name=region.region_name or region.name,
            size='S-00',
            firewall_ranges=['0.0.0.0/0'],
            project_id=_project_id(cls.manager),
            wait_on_active=True,
        )

        # v2 generates the admin password; see TestCluster.setUpClass.
        cls.password = cls.cluster.admin_password

    @classmethod
    def tearDownClass(cls):
        if cls.cluster is not None:
            cls.cluster.terminate(force=True)
        cls.cluster = None
        cls.manager = None
        cls.password = None

    def test_root_info(self):
        st = self.cluster.stage
        root = st.info('/')
        assert str(root.path) == '/'
        assert root.type == 'directory'

    def test_upload_file(self):
        st = self.cluster.stage

        upload_test_sql = f'upload_test_{id(self)}.sql'
        upload_test2_sql = f'upload_test2_{id(self)}.sql'

        f = st.upload_file(f'{TEST_DIR}/test.sql', upload_test_sql)
        assert str(f.path) == upload_test_sql
        assert f.type == 'file'

        txt = f.download(encoding='utf-8')
        assert txt == open(f'{TEST_DIR}/test.sql').read()

        # No silent overwrite
        with self.assertRaises(OSError):
            st.upload_file(f'{TEST_DIR}/test.sql', upload_test_sql)

        f = st.upload_file(
            open(f'{TEST_DIR}/test2.sql', 'r'),
            upload_test_sql,
            overwrite=True,
        )
        txt = f.download(encoding='utf-8')
        assert txt == open(f'{TEST_DIR}/test2.sql').read()

        with self.assertRaises(IsADirectoryError):
            st.upload_file(TEST_DIR, 'test3.sql')

        lib = st.mkdir(f'/lib_{id(self)}/')
        assert lib.type == 'directory'

        with self.assertRaises(IsADirectoryError):
            st.upload_file(f'{TEST_DIR}/test2.sql', lib.path, overwrite=True)

        f = st.upload_file(
            f'{TEST_DIR}/test2.sql',
            os.path.join(lib.path, upload_test2_sql),
        )
        assert str(f.path) == f'{lib.path}{upload_test2_sql}'
        assert f.type == 'file'

    def test_open(self):
        st = self.cluster.stage
        open_test_sql = f'open_test_{id(self)}.sql'

        with st.open(open_test_sql, 'w') as f:
            f.write('create table foo (id int);')

        with st.open(open_test_sql, 'r') as f:
            assert f.read() == 'create table foo (id int);'

        # Reading a missing object fails. Note that this raises
        # ManagementError rather than the FileNotFoundError the rest of
        # Stage.open's builtin-open emulation would suggest -- the 404 from
        # the download comes straight back out. Verified live; asserted here
        # so a change to it is deliberate rather than accidental.
        with self.assertRaises(s2.ManagementError) as cm:
            st.open(f'missing_{id(self)}.sql', 'r')
        assert cm.exception.errno == 404, cm.exception.errno

    def test_listdir_and_remove(self):
        st = self.cluster.stage
        name = f'listdir_test_{id(self)}.sql'

        st.upload_file(f'{TEST_DIR}/test.sql', name)
        assert name in [str(x) for x in st.listdir('/')]
        assert st.exists(name)
        assert st.is_file(name)
        assert not st.is_dir(name)

        st.remove(name)
        assert not st.exists(name)

    def test_rename(self):
        st = self.cluster.stage
        src = f'rename_src_{id(self)}.sql'
        dst = f'rename_dst_{id(self)}.sql'

        st.upload_file(f'{TEST_DIR}/test.sql', src)
        st.rename(src, dst)
        assert not st.exists(src)
        assert st.exists(dst)
        st.remove(dst)

    def test_mkdir_and_rmdir(self):
        st = self.cluster.stage
        d = f'dir_{id(self)}'

        # mkdir() and rmdir() append the trailing slash themselves, but
        # exists()/is_dir()/info() do not: without it the metadata GET 404s
        # and is_dir() reports False. The v1 suite passes the slash
        # explicitly for the same reason.
        st.mkdir(d)
        assert st.is_dir(f'{d}/')
        assert not st.is_file(f'{d}/')
        st.rmdir(d)
        assert not st.exists(f'{d}/')


@pytest.mark.management
class TestSecrets(unittest.TestCase):
    """
    Secrets are organization-scoped, so unlike v1 this needs no deployment.
    """

    manager = None

    @classmethod
    def setUpClass(cls):
        cls.manager = s2.manage_clusters(version='v2')

    @classmethod
    def tearDownClass(cls):
        cls.manager = None

    def test_get_secret(self):
        name = f'secret_{id(self)}'

        # Clear a leftover secret from a previous run
        try:
            secret = self.manager.organizations.current.get_secret(name)
            self.manager._delete(f'secrets/{secret.id}')
        except s2.ManagementError:
            pass

        self.manager._post(
            'secrets',
            json=dict(name=name, value='secret_value'),
        )
        try:
            secret = self.manager.organizations.current.get_secret(name)
            assert secret.name == name
            assert secret.value == 'secret_value'
        finally:
            self.manager._delete(f'secrets/{secret.id}')


@pytest.mark.management
class TestJob(unittest.TestCase):
    """
    Scheduled notebook jobs at v2.

    The one v2-visible difference is the ``targetType`` the SDK sends for a
    deployment: v1 called it ``Workspace``, v2 calls it ``Cluster``.
    """

    manager = None
    cluster = None
    password = None
    job_ids = []

    @classmethod
    def setUpClass(cls):
        cls.manager = s2.manage_clusters(version='v2')

        us_regions = _us_regions(cls.manager)

        name = clean_name(secrets.token_urlsafe(20)[:20])
        region = random.choice(us_regions)

        cls.cluster = cls.manager.create_cluster(
            f'cl-test-{name}',
            provider=region.provider,
            region_name=region.region_name or region.name,
            size='S-00',
            firewall_ranges=['0.0.0.0/0'],
            project_id=_project_id(cls.manager),
            wait_on_active=True,
        )

        # v2 generates the admin password; see TestCluster.setUpClass.
        cls.password = cls.cluster.admin_password

    @classmethod
    def tearDownClass(cls):
        for job_id in cls.job_ids:
            try:
                cls.manager.organizations.current.jobs.delete(job_id)
            except Exception:
                pass
        if cls.cluster is not None:
            cls.cluster.terminate(force=True)
        cls.cluster = None
        cls.manager = None
        cls.password = None
        os.environ.pop('SINGLESTOREDB_WORKSPACE', None)
        os.environ.pop('SINGLESTOREDB_DEFAULT_DATABASE', None)

    def test_job_without_database_target(self):
        os.environ.pop('SINGLESTOREDB_WORKSPACE', None)
        os.environ.pop('SINGLESTOREDB_DEFAULT_DATABASE', None)

        job_manager = self.manager.organizations.current.jobs
        job = job_manager.run(
            'Scheduling Test.ipynb',
            'notebooks-cpu-small',
            {'strParam': 'string', 'intParam': 1, 'floatParam': 1.0, 'boolParam': True},
        )
        self.job_ids.append(job.job_id)
        assert job.execution_config.notebook_path == 'Scheduling Test.ipynb'
        assert job.schedule.mode == job_manager.modes().ONCE
        assert not job.execution_config.create_snapshot
        assert job.completed_executions_count == 0
        assert job.target_config is None
        job.wait()
        job = job_manager.get(job.job_id)
        assert job.completed_executions_count == 1
        assert len(job.job_metadata) == 1
        assert job.job_metadata[0].status == Status.COMPLETED
        assert job.target_config is None
        assert job.delete()
        job = job_manager.get(job.job_id)
        assert job.terminated_at is not None

    def test_job_with_database_target(self):
        os.environ['SINGLESTOREDB_DEFAULT_DATABASE'] = 'information_schema'
        os.environ['SINGLESTOREDB_WORKSPACE'] = self.cluster.id

        job_manager = self.manager.organizations.current.jobs
        job = job_manager.run(
            'Scheduling Test.ipynb',
            'notebooks-cpu-small',
            {'strParam': 'string', 'intParam': 1, 'floatParam': 1.0, 'boolParam': True},
        )
        self.job_ids.append(job.job_id)
        assert job.target_config is not None
        assert job.target_config.database_name == 'information_schema'
        assert job.target_config.target_id == self.cluster.id
        # The v2 name for a deployment target.
        assert job.target_config.target_type == TargetType.CLUSTER
        assert not job.target_config.resume_target
        job.wait()
        job = job_manager.get(job.job_id)
        assert job.completed_executions_count == 1
        assert job.job_metadata[0].status == Status.COMPLETED
        assert job.target_config.target_type == TargetType.CLUSTER
        assert job.delete()
        job = job_manager.get(job.job_id)
        assert job.terminated_at is not None


@pytest.mark.management
class TestRegions(unittest.TestCase):
    """Region listing through the standalone region manager."""

    manager = None

    @classmethod
    def setUpClass(cls):
        cls.manager = s2.manage_regions(version='v2')

    @classmethod
    def tearDownClass(cls):
        cls.manager = None

    def test_list_regions(self):
        regions = self.manager.list_regions()
        assert isinstance(regions, NamedList)
        assert len(regions) > 0

        region = regions[0]
        assert isinstance(region, Region)
        # v2 responses carry no regionID.
        assert region.id is None
        assert region.name
        assert region.provider
        # ``region`` is the display name, ``regionName`` the provider slug.
        assert region.region_name

    def test_list_shared_tier_regions(self):
        regions = self.manager.list_shared_tier_regions()
        assert isinstance(regions, NamedList)
        assert len(regions) > 0

        region = regions[0]
        assert isinstance(region, Region)
        assert region.id is None
        assert region.name
        assert region.provider
        assert region.region_name

    def test_str_repr(self):
        regions = self.manager.list_regions()
        if not regions:
            self.skipTest('No regions available for testing')

        region = regions[0]
        s = str(region)
        assert region.name in s
        assert region.provider in s
        assert repr(region) == s


if __name__ == '__main__':
    unittest.main()
