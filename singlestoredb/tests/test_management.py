#!/usr/bin/env python
# type: ignore
"""SingleStoreDB HTTP connection testing."""
import os
import random
import re
import secrets
import unittest

import pytest

import singlestoredb as s2


def clean_name(s):
    """Change all non-word characters to -."""
    return re.sub(r'[^\w]', r'-', s).replace('_', '-').lower()


@pytest.mark.management
class TestCluster(unittest.TestCase):

    manager = None
    cluster = None
    password = None

    @classmethod
    def setUpClass(cls):
        cls.manager = s2.manage_cluster()

        us_regions = [x for x in cls.manager.regions if 'US' in x.name]
        cls.password = secrets.token_urlsafe(20)

        cls.cluster = cls.manager.create_cluster(
            clean_name('cm-test-{}'.format(secrets.token_urlsafe(20)[:20])),
            region=random.choice(us_regions).id,
            admin_password=cls.password,
            firewall_ranges=['0.0.0.0/0'],
            expires_at='1h',
            size='S-00',
            wait_on_active=True,
        )

    @classmethod
    def tearDownClass(cls):
        if cls.cluster is not None:
            cls.cluster.terminate()
        cls.cluster = None
        cls.manager = None
        cls.password = None

    def test_str(self):
        assert self.cluster.name in str(self.cluster.name)

    def test_repr(self):
        assert repr(self.cluster) == str(self.cluster)

    def test_region_str(self):
        s = str(self.cluster.region)
        assert 'Azure' in s or 'GCP' in s or 'AWS' in s, s

    def test_region_repr(self):
        assert repr(self.cluster.region) == str(self.cluster.region)

    def test_regions(self):
        out = self.manager.regions
        providers = {x.provider for x in out}
        assert 'Azure' in providers, providers
        assert 'GCP' in providers, providers
        assert 'AWS' in providers, providers

    def test_clusters(self):
        clusters = self.manager.clusters
        ids = [x.id for x in clusters]
        assert self.cluster.id in ids, ids

    def test_get_cluster(self):
        clus = self.manager.get_cluster(self.cluster.id)
        assert clus.id == self.cluster.id, clus.id

        with self.assertRaises(s2.ManagementError) as cm:
            clus = self.manager.get_cluster('bad id')

        assert 'UUID' in cm.exception.msg, cm.exception.msg

    def test_update(self):
        assert self.cluster.name.startswith('cm-test-')

        name = self.cluster.name.replace('cm-test-', 'cm-foo-')
        self.cluster.update(name=name)

        clus = self.manager.get_cluster(self.cluster.id)
        assert clus.name == name, clus.name

    def test_suspend_resume(self):
        trues = ['1', 'on', 'true']
        do_test = os.environ.get('SINGLESTOREDB_TEST_SUSPEND', '0').lower() in trues

        if not do_test:
            self.skipTest(
                'Suspend / resume tests skipped by default due to '
                'being time consuming; set SINGLESTOREDB_TEST_SUSPEND=1 '
                'to enable',
            )

        assert self.cluster.state != 'Suspended', self.cluster.state

        self.cluster.suspend(wait_on_suspended=True)
        assert self.cluster.state == 'Suspended', self.cluster.state

        self.cluster.resume(wait_on_resumed=True)
        assert self.cluster.state == 'Active', self.cluster.state

    def test_no_manager(self):
        clus = self.manager.get_cluster(self.cluster.id)
        clus._manager = None

        with self.assertRaises(s2.ManagementError) as cm:
            clus.refresh()

        assert 'No cluster manager' in cm.exception.msg, cm.exception.msg

        with self.assertRaises(s2.ManagementError) as cm:
            clus.update()

        assert 'No cluster manager' in cm.exception.msg, cm.exception.msg

        with self.assertRaises(s2.ManagementError) as cm:
            clus.suspend()

        assert 'No cluster manager' in cm.exception.msg, cm.exception.msg

        with self.assertRaises(s2.ManagementError) as cm:
            clus.resume()

        assert 'No cluster manager' in cm.exception.msg, cm.exception.msg

        with self.assertRaises(s2.ManagementError) as cm:
            clus.terminate()

        assert 'No cluster manager' in cm.exception.msg, cm.exception.msg

    def test_connect(self):
        trues = ['1', 'on', 'true']
        pure_python = os.environ.get('SINGLESTOREDB_PURE_PYTHON', '0').lower() in trues

        if pure_python:
            self.skipTest('Connections through managed service are disabled')

        try:
            with self.cluster.connect(user='admin', password=self.password) as conn:
                with conn.cursor() as cur:
                    cur.execute('show databases')
                    assert 'cluster' in [x[0] for x in list(cur)]
        except s2.ManagementError as exc:
            if 'endpoint has not been set' not in str(exc):
                self.skipTest('No endpoint in response. Skipping connection test.')

        # Test missing endpoint
        clus = self.manager.get_cluster(self.cluster.id)
        clus.endpoint = None

        with self.assertRaises(s2.ManagementError) as cm:
            clus.connect(user='admin', password=self.password)

        assert 'endpoint' in cm.exception.msg, cm.exception.msg


@pytest.mark.management
class TestWorkspace(unittest.TestCase):

    manager = None
    workspace_group = None
    workspace = None
    password = None

    @classmethod
    def setUpClass(cls):
        cls.manager = s2.manage_workspaces()

        us_regions = [x for x in cls.manager.regions if 'US' in x.name]
        cls.password = secrets.token_urlsafe(20)

        name = clean_name(secrets.token_urlsafe(20)[:20])

        cls.workspace_group = cls.manager.create_workspace_group(
            f'wg-test-{name}',
            region=random.choice(us_regions).id,
            admin_password=cls.password,
            firewall_ranges=['0.0.0.0/0'],
        )

        try:
            cls.workspace = cls.workspace_group.create_workspace(
                f'ws-test-{name}-x',
                wait_on_active=True,
            )
        except Exception:
            cls.workspace_group.terminate(force=True)
            raise

    @classmethod
    def tearDownClass(cls):
        if cls.workspace_group is not None:
            cls.workspace_group.terminate(force=True)
        cls.workspace_group = None
        cls.workspace = None
        cls.manager = None
        cls.password = None

    def test_str(self):
        assert self.workspace.name in str(self.workspace.name)
        assert self.workspace_group.name in str(self.workspace_group.name)

    def test_repr(self):
        assert repr(self.workspace) == str(self.workspace)
        assert repr(self.workspace_group) == str(self.workspace_group)

    def test_region_str(self):
        s = str(self.workspace_group.region)
        assert 'Azure' in s or 'GCP' in s or 'AWS' in s, s

    def test_region_repr(self):
        assert repr(self.workspace_group.region) == str(self.workspace_group.region)

    def test_regions(self):
        out = self.manager.regions
        providers = {x.provider for x in out}
        assert 'Azure' in providers, providers
        assert 'GCP' in providers, providers
        assert 'AWS' in providers, providers

    def test_workspace_groups(self):
        workspace_groups = self.manager.workspace_groups
        ids = [x.id for x in workspace_groups]
        assert self.workspace_group.id in ids, ids

    def test_workspaces(self):
        spaces = self.workspace_group.workspaces
        ids = [x.id for x in spaces]
        assert self.workspace.id in ids, ids

    def test_get_workspace_group(self):
        group = self.manager.get_workspace_group(self.workspace_group.id)
        assert group.id == self.workspace_group.id, group.id

        with self.assertRaises(s2.ManagementError) as cm:
            group = self.manager.get_workspace_group('bad id')

        assert 'UUID' in cm.exception.msg, cm.exception.msg

    def test_get_workspace(self):
        space = self.manager.get_workspace(self.workspace.id)
        assert space.id == self.workspace.id, space.id

        with self.assertRaises(s2.ManagementError) as cm:
            space = self.manager.get_workspace('bad id')

        assert 'UUID' in cm.exception.msg, cm.exception.msg

    def test_update(self):
        assert self.workspace_group.name.startswith('wg-test-')

        name = self.workspace_group.name.replace('wg-test-', 'wg-foo-')
        self.workspace_group.update(name=name)

        group = self.manager.get_workspace_group(self.workspace_group.id)
        assert group.name == name, group.name

    def test_no_manager(self):
        space = self.manager.get_workspace(self.workspace.id)
        space._manager = None

        with self.assertRaises(s2.ManagementError) as cm:
            space.refresh()

        assert 'No workspace manager' in cm.exception.msg, cm.exception.msg

        with self.assertRaises(s2.ManagementError) as cm:
            space.terminate()

        assert 'No workspace manager' in cm.exception.msg, cm.exception.msg

    def test_connect(self):
        with self.workspace.connect(user='admin', password=self.password) as conn:
            with conn.cursor() as cur:
                cur.execute('show databases')
                assert 'cluster' in [x[0] for x in list(cur)]

        # Test missing endpoint
        space = self.manager.get_workspace(self.workspace.id)
        space.endpoint = None

        with self.assertRaises(s2.ManagementError) as cm:
            space.connect(user='admin', password=self.password)

        assert 'endpoint' in cm.exception.msg, cm.exception.msg


if __name__ == '__main__':
    import nose2
    nose2.main()
