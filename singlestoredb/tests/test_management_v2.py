#!/usr/bin/env python
# type: ignore
"""
SingleStoreDB v2 Management API testing.

Everything here targets management API v2 -- the flat ``Cluster`` resource and
the starter clusters, stages, secrets, jobs and regions hanging off it. No test
in this file may branch on version; the v1 equivalents live in
``test_management.py``, the version-neutral helper units in
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


def clean_name(s):
    """Change all non-word characters to -."""
    return re.sub(r'[^\w]', r'-', s).replace('_', '-').lower()


def shared_database_name(s):
    """Return a shared database name. Cannot contain special characters except -"""
    return re.sub(r'[^\w]', '', s).replace('-', '_').lower()


def _us_regions(manager):
    """Return the US regions a v2 manager reports, or skip the test."""
    out = [x for x in manager.regions if 'US' in x.name or 'us-' in x.name]
    if not out:
        raise unittest.SkipTest('No US regions reported by the v2 API')
    return out


#
# Unit tests. These need no token and no deployment.
#

class TestV2RegionBehavior(unittest.TestCase):
    """
    ``RegionManager`` at v2: ``list_regions`` hits ``/v2/regions``, and the
    shared-tier listing has no v2 equivalent.
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
        # UNVERIFIED: v2 region payload shape.
        get_response.json.return_value = [
            {'provider': 'aws', 'region': 'us-east-1', 'regionName': 'US East 1'},
            {'provider': 'gcp', 'region': 'us-west-2', 'regionName': 'US West 2'},
        ]
        mgr._get = MagicMock(return_value=get_response)

        regions = mgr.list_regions()
        mgr._get.assert_called_once_with('regions')
        self.assertEqual(len(regions), 2)
        # v2 region entries have id=None -- there is no regionID in the
        # response, so a region is identified by (provider, region_name).
        for r in regions:
            self.assertIsNone(r.id)

    def test_shared_tier_regions_raises(self):
        mgr = self._make_region_manager()
        with self.assertRaises(ManagementError):
            mgr.list_shared_tier_regions()


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
        mgr.get_cluster = MagicMock(return_value='sentinel')

        out = mgr.create_cluster(
            'my-cluster',
            provider='AWS',
            region_name='us-east-1',
            size='S-00',
            scale_factor=1.0,
            firewall_ranges=['0.0.0.0/0'],
            admin_password='hunter2',
            update_window={'day': 3, 'hour': 4},
        )

        self.assertEqual(out, 'sentinel')
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

    def test_shared_tier_regions_raises(self):
        mgr = self._make_cluster_manager()
        with self.assertRaises(ManagementError):
            mgr.shared_tier_regions


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
        cls.manager = s2.manage_clusters()

        us_regions = _us_regions(cls.manager)
        cls.password = secrets.token_urlsafe(20) + '-x&$'

        name = clean_name(secrets.token_urlsafe(20)[:20])
        region = random.choice(us_regions)

        # v2 has no workspace group: the cluster is created in one call, with
        # the firewall settings passed alongside the compute settings.
        cls.cluster = cls.manager.create_cluster(
            f'cl-test-{name}',
            provider=region.provider,
            region_name=region.region_name or region.name,
            size='S-00',
            admin_password=cls.password,
            firewall_ranges=['0.0.0.0/0'],
            wait_on_active=True,
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
        assert self.cluster.name.startswith('cl-test-')

        name = self.cluster.name.replace('cl-test-', 'cl-foo-')
        self.cluster.update(name=name)

        cluster = self.manager.get_cluster(self.cluster.id)
        assert cluster.name == name, cluster.name

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
        cls.manager = s2.manage_clusters()

        # v1 discovered starter regions through GET /regions/sharedtier, which
        # has no v2 equivalent; the full region list is all there is.
        # UNVERIFIED: that every region in that list accepts a starter cluster.
        regions = _us_regions(cls.manager)

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
        cls.manager = s2.manage_clusters()

        us_regions = _us_regions(cls.manager)
        cls.password = secrets.token_urlsafe(20) + '-x&$'

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
            admin_password=cls.password,
            firewall_ranges=['0.0.0.0/0'],
            wait_on_active=True,
        )

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

        # Reading a missing object fails
        with self.assertRaises(OSError):
            st.open(f'missing_{id(self)}.sql', 'r')

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

        st.mkdir(d)
        assert st.is_dir(d)
        st.rmdir(d)
        assert not st.exists(d)


@pytest.mark.management
class TestSecrets(unittest.TestCase):
    """
    Secrets are organization-scoped, so unlike v1 this needs no deployment.
    """

    manager = None

    @classmethod
    def setUpClass(cls):
        cls.manager = s2.manage_clusters()

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
        cls.manager = s2.manage_clusters()

        us_regions = _us_regions(cls.manager)
        cls.password = secrets.token_urlsafe(20) + '-x&$'

        name = clean_name(secrets.token_urlsafe(20)[:20])
        region = random.choice(us_regions)

        cls.cluster = cls.manager.create_cluster(
            f'cl-test-{name}',
            provider=region.provider,
            region_name=region.region_name or region.name,
            size='S-00',
            admin_password=cls.password,
            firewall_ranges=['0.0.0.0/0'],
            wait_on_active=True,
        )

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

    def test_list_shared_tier_regions_is_gone(self):
        with self.assertRaises(ManagementError):
            self.manager.list_shared_tier_regions()

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
