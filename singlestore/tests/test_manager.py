#!/usr/bin/env python
# type: ignore
"""SingleStore HTTP connection testing."""
from __future__ import annotations

import os
import random
import secrets
import unittest

import singlestore as s2


class TestManager(unittest.TestCase):

    manager = None
    cluster = None
    password = None

    @classmethod
    def setUpClass(cls):
        cls.manager = s2.manage_cluster()

        us_regions = [x for x in cls.manager.regions if 'US' in x.region]
        cls.password = secrets.token_urlsafe(20)

        cls.cluster = cls.manager.create_cluster(
            'CM-TEST-{}'.format(secrets.token_urlsafe(20)),
            region_id=random.choice(us_regions).id,
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

        with self.assertRaises(s2.ClusterManagerError) as cm:
            clus = self.manager.get_cluster('bad id')

        assert 'UUID' in cm.exception.msg, cm.exception.msg

    def test_update(self):
        assert self.cluster.name.startswith('CM-TEST-')

        name = self.cluster.name.replace('CM-TEST-', 'CM-FOO')
        self.cluster.update(name=name)

        clus = self.manager.get_cluster(self.cluster.id)
        assert clus.name == name, clus.name

    def test_suspend_resume(self):
        trues = ['1', 'on', 'true']
        do_test = os.environ.get('SINGLESTORE_TEST_SUSPEND', '0').lower() in trues

        if not do_test:
            self.skipTest(
                'Suspend / resume tests skipped by default due to '
                'being time consuming; set SINGLESTORE_TEST_SUSPEND=1 '
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

        with self.assertRaises(s2.ClusterManagerError) as cm:
            clus.refresh()

        assert 'No cluster manager' in cm.exception.msg, cm.exception.msg

        with self.assertRaises(s2.ClusterManagerError) as cm:
            clus.update()

        assert 'No cluster manager' in cm.exception.msg, cm.exception.msg

        with self.assertRaises(s2.ClusterManagerError) as cm:
            clus.suspend()

        assert 'No cluster manager' in cm.exception.msg, cm.exception.msg

        with self.assertRaises(s2.ClusterManagerError) as cm:
            clus.resume()

        assert 'No cluster manager' in cm.exception.msg, cm.exception.msg

        with self.assertRaises(s2.ClusterManagerError) as cm:
            clus.terminate()

        assert 'No cluster manager' in cm.exception.msg, cm.exception.msg

    def test_connect(self):
        with self.cluster.connect(user='admin', password=self.password) as conn:
            with conn.cursor() as cur:
                cur.execute('show databases')
                assert 'cluster' in [x[0] for x in list(cur)]

        # Test missing endpoint
        clus = self.manager.get_cluster(self.cluster.id)
        clus.endpoint = None

        with self.assertRaises(s2.ClusterManagerError) as cm:
            clus.connect(user='admin', password=self.password)

        assert 'endpoint' in cm.exception.msg, cm.exception.msg


if __name__ == '__main__':
    import nose2
    nose2.main()
