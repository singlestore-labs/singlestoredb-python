#!/usr/bin/env python
# type: ignore
"""SingleStoreDB Management API testing."""
import os
import pathlib
import random
import re
import secrets
import unittest

import pytest

import singlestoredb as s2
from singlestoredb.management.job import Status
from singlestoredb.management.job import TargetType
from singlestoredb.management.region import Region
from singlestoredb.management.utils import NamedList


TEST_DIR = pathlib.Path(os.path.dirname(__file__))


def clean_name(s):
    """Change all non-word characters to -."""
    return re.sub(r'[^\w]', r'-', s).replace('_', '-').lower()


def shared_database_name(s):
    """Return a shared database name. Cannot contain special characters except -"""
    return re.sub(r'[^\w]', '', s).replace('-', '_').lower()


@pytest.mark.management
class TestCluster(unittest.TestCase):

    manager = None
    cluster = None
    password = None

    @classmethod
    def setUpClass(cls):
        cls.manager = s2.manage_cluster()

        us_regions = [x for x in cls.manager.regions if 'US' in x.name]
        cls.password = secrets.token_urlsafe(20) + '-x&$'

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
        names = [x.name for x in out]
        assert 'Azure' in providers, providers
        assert 'GCP' in providers, providers
        assert 'AWS' in providers, providers

        objs = {}
        ids = []
        for item in out:
            ids.append(item.id)
            objs[item.id] = item
            if item.name not in objs:
                objs[item.name] = item

        name = random.choice(names)
        assert out[name] == objs[name]
        id = random.choice(ids)
        assert out[id] == objs[id]

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

        self.skipTest('Connection test is disable due to flakey server')

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
        cls.password = secrets.token_urlsafe(20) + '-x&$'

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
        names = [x.name for x in out]
        assert 'Azure' in providers, providers
        assert 'GCP' in providers, providers
        assert 'AWS' in providers, providers

        objs = {}
        ids = []
        for item in out:
            ids.append(item.id)
            objs[item.id] = item
            if item.name not in objs:
                objs[item.name] = item

        name = random.choice(names)
        assert out[name] == objs[name]
        id = random.choice(ids)
        assert out[id] == objs[id]

    def test_workspace_groups(self):
        workspace_groups = self.manager.workspace_groups
        ids = [x.id for x in workspace_groups]
        names = [x.name for x in workspace_groups]
        assert self.workspace_group.id in ids
        assert self.workspace_group.name in names

        assert workspace_groups.ids() == ids
        assert workspace_groups.names() == names

        objs = {}
        for item in workspace_groups:
            objs[item.id] = item
            objs[item.name] = item

        name = random.choice(names)
        assert workspace_groups[name] == objs[name]
        id = random.choice(ids)
        assert workspace_groups[id] == objs[id]

    def test_workspaces(self):
        spaces = self.workspace_group.workspaces
        ids = [x.id for x in spaces]
        names = [x.name for x in spaces]
        assert self.workspace.id in ids
        assert self.workspace.name in names

        assert spaces.ids() == ids
        assert spaces.names() == names

        objs = {}
        for item in spaces:
            objs[item.id] = item
            objs[item.name] = item

        name = random.choice(names)
        assert spaces[name] == objs[name]
        id = random.choice(ids)
        assert spaces[id] == objs[id]

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


@pytest.mark.management
class TestStarterWorkspace(unittest.TestCase):

    manager = None
    starter_workspace = None

    @classmethod
    def setUpClass(cls):
        cls.manager = s2.manage_workspaces()

        shared_tier_regions: NamedList[Region] = [
            x for x in cls.manager.shared_tier_regions if 'US' in x.name
        ]
        cls.starter_username = 'starter_user'
        cls.password = secrets.token_urlsafe(20)

        name = shared_database_name(secrets.token_urlsafe(20)[:20])

        cls.database_name = f'starter_db_{name}'

        shared_tier_region: Region = random.choice(shared_tier_regions)

        if not shared_tier_region:
            raise ValueError('No shared tier regions found')

        cls.starter_workspace = cls.manager.create_starter_workspace(
            f'starter-ws-test-{name}',
            database_name=cls.database_name,
            provider=shared_tier_region.provider,
            region_name=shared_tier_region.region_name,
        )

        cls.starter_workspace.create_user(
            username=cls.starter_username,
            password=cls.password,
        )

    @classmethod
    def tearDownClass(cls):
        if cls.starter_workspace is not None:
            cls.starter_workspace.terminate()
        cls.manager = None
        cls.password = None

    def test_str(self):
        assert self.starter_workspace.name in str(self.starter_workspace.name)

    def test_repr(self):
        assert repr(self.starter_workspace) == str(self.starter_workspace)

    def test_get_starter_workspace(self):
        workspace = self.manager.get_starter_workspace(self.starter_workspace.id)
        assert workspace.id == self.starter_workspace.id, workspace.id

        with self.assertRaises(s2.ManagementError) as cm:
            workspace = self.manager.get_starter_workspace('bad id')

        assert 'UUID' in cm.exception.msg, cm.exception.msg

    def test_starter_workspaces(self):
        workspaces = self.manager.starter_workspaces
        ids = [x.id for x in workspaces]
        names = [x.name for x in workspaces]
        assert self.starter_workspace.id in ids
        assert self.starter_workspace.name in names

        objs = {}
        for item in workspaces:
            objs[item.id] = item
            objs[item.name] = item

        name = random.choice(names)
        assert workspaces[name] == objs[name]
        id = random.choice(ids)
        assert workspaces[id] == objs[id]

    def test_no_manager(self):
        workspace = self.manager.get_starter_workspace(self.starter_workspace.id)
        workspace._manager = None

        with self.assertRaises(s2.ManagementError) as cm:
            workspace.refresh()

        assert 'No workspace manager' in cm.exception.msg, cm.exception.msg

        with self.assertRaises(s2.ManagementError) as cm:
            workspace.terminate()

        assert 'No workspace manager' in cm.exception.msg, cm.exception.msg

    def test_connect(self):
        with self.starter_workspace.connect(
            user=self.starter_username,
            password=self.password,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute('show databases')
                assert self.database_name in [x[0] for x in list(cur)]

        # Test missing endpoint
        workspace = self.manager.get_starter_workspace(self.starter_workspace.id)
        workspace.endpoint = None

        with self.assertRaises(s2.ManagementError) as cm:
            workspace.connect(user=self.starter_username, password=self.password)

        assert 'endpoint' in cm.exception.msg, cm.exception.msg


@pytest.mark.management
class TestStage(unittest.TestCase):

    manager = None
    wg = None
    password = None

    @classmethod
    def setUpClass(cls):
        cls.manager = s2.manage_workspaces()

        us_regions = [x for x in cls.manager.regions if 'US' in x.name]
        cls.password = secrets.token_urlsafe(20) + '-x&$'

        name = clean_name(secrets.token_urlsafe(20)[:20])

        cls.wg = cls.manager.create_workspace_group(
            f'wg-test-{name}',
            region=random.choice(us_regions).id,
            admin_password=cls.password,
            firewall_ranges=['0.0.0.0/0'],
        )

    @classmethod
    def tearDownClass(cls):
        if cls.wg is not None:
            cls.wg.terminate(force=True)
        cls.wg = None
        cls.manager = None
        cls.password = None

    def test_upload_file(self):
        st = self.wg.stage

        upload_test_sql = f'upload_test_{id(self)}.sql'
        upload_test2_sql = f'upload_test2_{id(self)}.sql'

        root = st.info('/')
        assert str(root.path) == '/'
        assert root.type == 'directory'

        # Upload file
        f = st.upload_file(TEST_DIR / 'test.sql', upload_test_sql)
        assert str(f.path) == upload_test_sql
        assert f.type == 'file'

        # Download and compare to original
        txt = f.download(encoding='utf-8')
        assert txt == open(TEST_DIR / 'test.sql').read()

        # Make sure we can't overwrite
        with self.assertRaises(OSError):
            st.upload_file(TEST_DIR / 'test.sql', upload_test_sql)

        # Force overwrite with new content; use file object this time
        f = st.upload_file(
            open(TEST_DIR / 'test2.sql', 'r'),
            upload_test_sql,
            overwrite=True,
        )
        assert str(f.path) == upload_test_sql
        assert f.type == 'file'

        # Verify new content
        txt = f.download(encoding='utf-8')
        assert txt == open(TEST_DIR / 'test2.sql').read()

        # Try to upload folder
        with self.assertRaises(IsADirectoryError):
            st.upload_file(TEST_DIR, 'test3.sql')

        lib = st.mkdir('/lib/')
        assert str(lib.path) == 'lib/'
        assert lib.type == 'directory'

        # Try to overwrite stage folder with file
        with self.assertRaises(IsADirectoryError):
            st.upload_file(TEST_DIR / 'test2.sql', lib.path, overwrite=True)

        # Write file into folder
        f = st.upload_file(
            TEST_DIR / 'test2.sql',
            os.path.join(lib.path, upload_test2_sql),
        )
        assert str(f.path) == 'lib/' + upload_test2_sql
        assert f.type == 'file'

    def test_open(self):
        st = self.wg.stage

        open_test_sql = f'open_test_{id(self)}.sql'

        # See if error is raised for non-existent file
        with self.assertRaises(s2.ManagementError):
            st.open(open_test_sql, 'r')

        # Load test file
        st.upload_file(TEST_DIR / 'test.sql', open_test_sql)

        # Read file using `open`
        with st.open(open_test_sql, 'r') as rfile:
            assert rfile.read() == open(TEST_DIR / 'test.sql').read()

        # Read file using `open` with 'rt' mode
        with st.open(open_test_sql, 'rt') as rfile:
            assert rfile.read() == open(TEST_DIR / 'test.sql').read()

        # Read file using `open` with 'rb' mode
        with st.open(open_test_sql, 'rb') as rfile:
            assert rfile.read() == open(TEST_DIR / 'test.sql', 'rb').read()

        # Read file using `open` with 'rb' mode
        with self.assertRaises(ValueError):
            with st.open(open_test_sql, 'b') as rfile:
                pass

        # Attempt overwrite file using `open` with mode 'x'
        with self.assertRaises(OSError):
            with st.open(open_test_sql, 'x') as wfile:
                pass

        # Attempt overwrite file using `open` with mode 'w'
        with st.open(open_test_sql, 'w') as wfile:
            wfile.write(open(TEST_DIR / 'test2.sql').read())

        txt = st.download_file(open_test_sql, encoding='utf-8')

        assert txt == open(TEST_DIR / 'test2.sql').read()

        open_raw_test_sql = f'open_raw_test_{id(self)}.sql'

        # Test writer without context manager
        wfile = st.open(open_raw_test_sql, 'w')
        for line in open(TEST_DIR / 'test.sql'):
            wfile.write(line)
        wfile.close()

        txt = st.download_file(open_raw_test_sql, encoding='utf-8')

        assert txt == open(TEST_DIR / 'test.sql').read()

        # Test reader without context manager
        rfile = st.open(open_raw_test_sql, 'r')
        txt = ''
        for line in rfile:
            txt += line
        rfile.close()

        assert txt == open(TEST_DIR / 'test.sql').read()

    def test_obj_open(self):
        st = self.wg.stage

        obj_open_test_sql = f'obj_open_test_{id(self)}.sql'
        obj_open_dir = f'obj_open_dir_{id(self)}'

        # Load test file
        f = st.upload_file(TEST_DIR / 'test.sql', obj_open_test_sql)

        # Read file using `open`
        with f.open() as rfile:
            assert rfile.read() == open(TEST_DIR / 'test.sql').read()

        # Make sure directories error out
        d = st.mkdir(obj_open_dir)
        with self.assertRaises(IsADirectoryError):
            d.open()

        # Write file using `open`
        with f.open('w', encoding='utf-8') as wfile:
            wfile.write(open(TEST_DIR / 'test2.sql').read())

        assert f.download(encoding='utf-8') == open(TEST_DIR / 'test2.sql').read()

        # Test writer without context manager
        wfile = f.open('w')
        for line in open(TEST_DIR / 'test.sql'):
            wfile.write(line)
        wfile.close()

        txt = st.download_file(f.path, encoding='utf-8')

        assert txt == open(TEST_DIR / 'test.sql').read()

        # Test reader without context manager
        rfile = f.open('r')
        txt = ''
        for line in rfile:
            txt += line
        rfile.close()

        assert txt == open(TEST_DIR / 'test.sql').read()

    def test_os_directories(self):
        st = self.wg.stage

        # mkdir
        st.mkdir('mkdir_test_1')
        st.mkdir('mkdir_test_2')
        with self.assertRaises(s2.ManagementError):
            st.mkdir('mkdir_test_2/nest_1/nest_2')
        st.mkdir('mkdir_test_2/nest_1')
        st.mkdir('mkdir_test_2/nest_1/nest_2')
        st.mkdir('mkdir_test_3')

        assert st.exists('mkdir_test_1/')
        assert st.exists('mkdir_test_2/')
        assert st.exists('mkdir_test_2/nest_1/')
        assert st.exists('mkdir_test_2/nest_1/nest_2/')
        assert not st.exists('foo/')
        assert not st.exists('foo/bar/')

        assert st.is_dir('mkdir_test_1/')
        assert st.is_dir('mkdir_test_2/')
        assert st.is_dir('mkdir_test_2/nest_1/')
        assert st.is_dir('mkdir_test_2/nest_1/nest_2/')

        assert not st.is_file('mkdir_test_1/')
        assert not st.is_file('mkdir_test_2/')
        assert not st.is_file('mkdir_test_2/nest_1/')
        assert not st.is_file('mkdir_test_2/nest_1/nest_2/')

        out = st.listdir('/')
        assert 'mkdir_test_1/' in out
        assert 'mkdir_test_2/' in out
        assert 'mkdir_test_2/nest_1/nest_2/' not in out

        out = st.listdir('/', recursive=True)
        assert 'mkdir_test_1/' in out
        assert 'mkdir_test_2/' in out
        assert 'mkdir_test_2/nest_1/nest_2/' in out

        out = st.listdir('mkdir_test_2')
        assert 'mkdir_test_1/' not in out
        assert 'nest_1/' in out
        assert 'nest_2/' not in out
        assert 'nest_1/nest_2/' not in out

        out = st.listdir('mkdir_test_2', recursive=True)
        assert 'mkdir_test_1/' not in out
        assert 'nest_1/' in out
        assert 'nest_2/' not in out
        assert 'nest_1/nest_2/' in out

        # rmdir
        before = st.listdir('/', recursive=True)
        st.rmdir('mkdir_test_1/')
        after = st.listdir('/', recursive=True)
        assert 'mkdir_test_1/' in before
        assert 'mkdir_test_1/' not in after
        assert list(sorted(before)) == list(sorted(after + ['mkdir_test_1/']))

        with self.assertRaises(OSError):
            st.rmdir('mkdir_test_2/')

        st.upload_file(TEST_DIR / 'test.sql', 'mkdir_test.sql')

        with self.assertRaises(NotADirectoryError):
            st.rmdir('mkdir_test.sql')

        # removedirs
        before = st.listdir('/')
        st.removedirs('mkdir_test_2/')
        after = st.listdir('/')
        assert 'mkdir_test_2/' in before
        assert 'mkdir_test_2/' not in after
        assert list(sorted(before)) == list(sorted(after + ['mkdir_test_2/']))

        with self.assertRaises(s2.ManagementError):
            st.removedirs('mkdir_test.sql')

    def test_os_files(self):
        st = self.wg.stage

        st.mkdir('files_test_1')
        st.mkdir('files_test_1/nest_1')

        st.upload_file(TEST_DIR / 'test.sql', 'files_test.sql')
        st.upload_file(TEST_DIR / 'test.sql', 'files_test_1/nest_1/nested_files_test.sql')
        st.upload_file(
            TEST_DIR / 'test.sql',
            'files_test_1/nest_1/nested_files_test_2.sql',
        )

        # remove
        with self.assertRaises(IsADirectoryError):
            st.remove('files_test_1/')

        before = st.listdir('/')
        st.remove('files_test.sql')
        after = st.listdir('/')
        assert 'files_test.sql' in before
        assert 'files_test.sql' not in after
        assert list(sorted(before)) == list(sorted(after + ['files_test.sql']))

        before = st.listdir('files_test_1/nest_1/')
        st.remove('files_test_1/nest_1/nested_files_test.sql')
        after = st.listdir('files_test_1/nest_1/')
        assert 'nested_files_test.sql' in before
        assert 'nested_files_test.sql' not in after
        assert st.is_dir('files_test_1/nest_1/')

        # Removing the last file does not remove empty directories
        st.remove('files_test_1/nest_1/nested_files_test_2.sql')
        assert not st.is_file('files_test_1/nest_1/nested_files_test_2.sql')
        assert st.is_dir('files_test_1/nest_1/')
        assert st.is_dir('files_test_1/')

        st.removedirs('files_test_1')
        assert not st.is_dir('files_test_1/nest_1/')
        assert not st.is_dir('files_test_1/')

    def test_os_rename(self):
        st = self.wg.stage

        st.upload_file(TEST_DIR / 'test.sql', 'rename_test.sql')

        with self.assertRaises(s2.ManagementError):
            st.upload_file(
                TEST_DIR / 'test.sql',
                'rename_test_1/nest_1/nested_rename_test.sql',
            )

        st.mkdir('rename_test_1')
        st.mkdir('rename_test_1/nest_1')

        assert st.exists('/rename_test_1/nest_1/')

        st.upload_file(
            TEST_DIR / 'test.sql',
            'rename_test_1/nest_1/nested_rename_test.sql',
        )

        st.upload_file(
            TEST_DIR / 'test.sql',
            'rename_test_1/nest_1/nested_rename_test_2.sql',
        )

        # rename file
        assert 'rename_test.sql' in st.listdir('/')
        assert 'rename_test_2.sql' not in st.listdir('/')
        st.rename('rename_test.sql', 'rename_test_2.sql')
        assert 'rename_test.sql' not in st.listdir('/')
        assert 'rename_test_2.sql' in st.listdir('/')

        # rename directory
        assert 'rename_test_1/' in st.listdir('/')
        assert 'rename_test_2/' not in st.listdir('/')
        st.rename('rename_test_1/', 'rename_test_2/')
        assert 'rename_test_1/' not in st.listdir('/')
        assert 'rename_test_2/' in st.listdir('/')
        assert st.is_file('rename_test_2/nest_1/nested_rename_test.sql')
        assert st.is_file('rename_test_2/nest_1/nested_rename_test_2.sql')

        # rename nested
        assert 'rename_test_2/nest_1/nested_rename_test.sql' in st.listdir(
            '/', recursive=True,
        )
        assert 'rename_test_2/nest_1/nested_rename_test_3.sql' not in st.listdir(
            '/', recursive=True,
        )
        st.rename(
            'rename_test_2/nest_1/nested_rename_test.sql',
            'rename_test_2/nest_1/nested_rename_test_3.sql',
        )
        assert 'rename_test_2/nest_1/nested_rename_test.sql' not in st.listdir(
            '/', recursive=True,
        )
        assert 'rename_test_2/nest_1/nested_rename_test_3.sql' in st.listdir(
            '/', recursive=True,
        )
        assert not st.is_file('rename_test_2/nest_1/nested_rename_test.sql')
        assert st.is_file('rename_test_2/nest_1/nested_rename_test_2.sql')
        assert st.is_file('rename_test_2/nest_1/nested_rename_test_3.sql')

        # non-existent file
        with self.assertRaises(OSError):
            st.rename('rename_foo.sql', 'rename_foo_2.sql')

        # overwrite
        with self.assertRaises(OSError):
            st.rename(
                'rename_test_2.sql',
                'rename_test_2/nest_1/nested_rename_test_3.sql',
            )

        st.rename(
            'rename_test_2.sql',
            'rename_test_2/nest_1/nested_rename_test_3.sql', overwrite=True,
        )

    def test_file_object(self):
        st = self.wg.stage

        st.mkdir('obj_test')
        st.mkdir('obj_test/nest_1')

        f1 = st.upload_file(TEST_DIR / 'test.sql', 'obj_test.sql')
        f2 = st.upload_file(TEST_DIR / 'test.sql', 'obj_test/nest_1/obj_test.sql')
        d2 = st.info('obj_test/nest_1/')

        # is_file / is_dir
        assert not f1.is_dir()
        assert f1.is_file()
        assert not f2.is_dir()
        assert f2.is_file()
        assert d2.is_dir()
        assert not d2.is_file()

        # abspath / basename / dirname / exists
        assert f1.abspath() == 'obj_test.sql'
        assert f1.basename() == 'obj_test.sql'
        assert f1.dirname() == '/'
        assert f1.exists()
        assert f2.abspath() == 'obj_test/nest_1/obj_test.sql'
        assert f2.basename() == 'obj_test.sql'
        assert f2.dirname() == 'obj_test/nest_1/'
        assert f2.exists()
        assert d2.abspath() == 'obj_test/nest_1/'
        assert d2.basename() == 'nest_1'
        assert d2.dirname() == 'obj_test/'
        assert d2.exists()

        # download
        assert f1.download(encoding='utf-8') == open(TEST_DIR / 'test.sql', 'r').read()
        assert f1.download() == open(TEST_DIR / 'test.sql', 'rb').read()

        # remove
        with self.assertRaises(IsADirectoryError):
            d2.remove()

        assert st.is_file('obj_test.sql')
        f1.remove()
        assert not st.is_file('obj_test.sql')

        # removedirs
        with self.assertRaises(NotADirectoryError):
            f2.removedirs()

        assert st.exists(d2.path)
        d2.removedirs()
        assert not st.exists(d2.path)

        # rmdir
        f1 = st.upload_file(TEST_DIR / 'test.sql', 'obj_test.sql')
        d2 = st.mkdir('obj_test/nest_1')

        assert st.exists(f1.path)
        assert st.exists(d2.path)

        with self.assertRaises(NotADirectoryError):
            f1.rmdir()

        assert st.exists(f1.path)
        assert st.exists(d2.path)

        d2.rmdir()

        assert not st.exists('obj_test/nest_1/')
        assert not st.exists('obj_test')

        # mtime / ctime
        assert f1.getmtime() > 0
        assert f1.getctime() > 0

        # rename
        assert st.exists('obj_test.sql')
        assert not st.exists('obj_test_2.sql')
        f1.rename('obj_test_2.sql')
        assert not st.exists('obj_test.sql')
        assert st.exists('obj_test_2.sql')
        assert f1.abspath() == 'obj_test_2.sql'


@pytest.mark.management
class TestSecrets(unittest.TestCase):

    manager = None
    wg = None
    password = None

    @classmethod
    def setUpClass(cls):
        cls.manager = s2.manage_workspaces()

        us_regions = [x for x in cls.manager.regions if 'US' in x.name]
        cls.password = secrets.token_urlsafe(20) + '-x&$'

        name = clean_name(secrets.token_urlsafe(20)[:20])

        cls.wg = cls.manager.create_workspace_group(
            f'wg-test-{name}',
            region=random.choice(us_regions).id,
            admin_password=cls.password,
            firewall_ranges=['0.0.0.0/0'],
        )

    @classmethod
    def tearDownClass(cls):
        if cls.wg is not None:
            cls.wg.terminate(force=True)
        cls.wg = None
        cls.manager = None
        cls.password = None

    def test_get_secret(self):
        # manually create secret and then get secret
        # try to delete the secret if it exists
        try:
            secret = self.manager.organizations.current.get_secret('secret_name')

            secret_id = secret.id

            self.manager._delete(f'secrets/{secret_id}')
        except s2.ManagementError:
            pass

        self.manager._post(
            'secrets',
            json=dict(
                name='secret_name',
                value='secret_value',
            ),
        )

        secret = self.manager.organizations.current.get_secret('secret_name')

        assert secret.name == 'secret_name'
        assert secret.value == 'secret_value'


@pytest.mark.management
class TestJob(unittest.TestCase):

    manager = None
    workspace_group = None
    workspace = None
    password = None
    job_ids = []

    @classmethod
    def setUpClass(cls):
        cls.manager = s2.manage_workspaces()

        us_regions = [x for x in cls.manager.regions if 'US' in x.name]
        cls.password = secrets.token_urlsafe(20) + '-x&$'

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
        for job_id in cls.job_ids:
            try:
                cls.manager.organizations.current.jobs.delete(job_id)
            except Exception:
                pass
        if cls.workspace_group is not None:
            cls.workspace_group.terminate(force=True)
        cls.workspace_group = None
        cls.workspace = None
        cls.manager = None
        cls.password = None
        if os.environ.get('SINGLESTOREDB_WORKSPACE', None) is not None:
            del os.environ['SINGLESTOREDB_WORKSPACE']
        if os.environ.get('SINGLESTOREDB_DEFAULT_DATABASE', None) is not None:
            del os.environ['SINGLESTOREDB_DEFAULT_DATABASE']

    def test_job_without_database_target(self):
        """
        Creates job without target database on a specific runtime
        Waits for job to finish
        Gets the job
        Deletes the job
        """
        if os.environ.get('SINGLESTOREDB_WORKSPACE', None) is not None:
            del os.environ['SINGLESTOREDB_WORKSPACE']
        if os.environ.get('SINGLESTOREDB_DEFAULT_DATABASE', None) is not None:
            del os.environ['SINGLESTOREDB_DEFAULT_DATABASE']

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
        assert job.name is None
        assert job.description is None
        assert job.job_metadata == []
        assert job.terminated_at is None
        assert job.target_config is None
        job.wait()
        job = job_manager.get(job.job_id)
        assert job.execution_config.notebook_path == 'Scheduling Test.ipynb'
        assert job.schedule.mode == job_manager.modes().ONCE
        assert not job.execution_config.create_snapshot
        assert job.completed_executions_count == 1
        assert job.name is None
        assert job.description is None
        assert job.job_metadata != []
        assert len(job.job_metadata) == 1
        assert job.job_metadata[0].count == 1
        assert job.job_metadata[0].status == Status.COMPLETED
        assert job.terminated_at is None
        assert job.target_config is None
        deleted = job.delete()
        assert deleted
        job = job_manager.get(job.job_id)
        assert job.terminated_at is not None

    def test_job_with_database_target(self):
        """
        Creates job with target database on a specific runtime
        Waits for job to finish
        Gets the job
        Deletes the job
        """
        os.environ['SINGLESTOREDB_DEFAULT_DATABASE'] = 'information_schema'
        os.environ['SINGLESTOREDB_WORKSPACE'] = self.workspace.id

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
        assert job.name is None
        assert job.description is None
        assert job.job_metadata == []
        assert job.terminated_at is None
        assert job.target_config is not None
        assert job.target_config.database_name == 'information_schema'
        assert job.target_config.target_id == self.workspace.id
        assert job.target_config.target_type == TargetType.WORKSPACE
        assert not job.target_config.resume_target
        job.wait()
        job = job_manager.get(job.job_id)
        assert job.execution_config.notebook_path == 'Scheduling Test.ipynb'
        assert job.schedule.mode == job_manager.modes().ONCE
        assert not job.execution_config.create_snapshot
        assert job.completed_executions_count == 1
        assert job.name is None
        assert job.description is None
        assert job.job_metadata != []
        assert len(job.job_metadata) == 1
        assert job.job_metadata[0].count == 1
        assert job.job_metadata[0].status == Status.COMPLETED
        assert job.terminated_at is None
        assert job.target_config is not None
        assert job.target_config.database_name == 'information_schema'
        assert job.target_config.target_id == self.workspace.id
        assert job.target_config.target_type == TargetType.WORKSPACE
        assert not job.target_config.resume_target
        deleted = job.delete()
        assert deleted
        job = job_manager.get(job.job_id)
        assert job.terminated_at is not None


@pytest.mark.management
class TestFileSpaces(unittest.TestCase):

    manager = None
    personal_space = None
    shared_space = None

    @classmethod
    def setUpClass(cls):
        cls.manager = s2.manage_files()
        cls.personal_space = cls.manager.personal_space
        cls.shared_space = cls.manager.shared_space

    @classmethod
    def tearDownClass(cls):
        cls.manager = None
        cls.personal_space = None
        cls.shared_space = None

    def test_upload_file(self):
        upload_test_ipynb = f'upload_test_{id(self)}.ipynb'

        for space in [self.personal_space, self.shared_space]:
            root = space.info('/')
            assert str(root.path) == '/'
            assert root.type == 'directory'

            # Upload files
            f = space.upload_file(
                TEST_DIR / 'test.ipynb',
                upload_test_ipynb,
            )
            assert str(f.path) == upload_test_ipynb
            assert f.type == 'notebook'

            # Download and compare to original
            txt = f.download(encoding='utf-8')
            assert txt == open(TEST_DIR / 'test.ipynb').read()

            # Make sure we can't overwrite
            with self.assertRaises(OSError):
                space.upload_file(
                    TEST_DIR / 'test.ipynb',
                    upload_test_ipynb,
                )

            # Force overwrite with new content
            f = space.upload_file(
                TEST_DIR / 'test2.ipynb',
                upload_test_ipynb, overwrite=True,
            )
            assert str(f.path) == upload_test_ipynb
            assert f.type == 'notebook'

            # Verify new content
            txt = f.download(encoding='utf-8')
            assert txt == open(TEST_DIR / 'test2.ipynb').read()

            # Make sure we can't upload a folder
            with self.assertRaises(s2.ManagementError):
                space.upload_folder(TEST_DIR, 'test')

            # Cleanup
            space.remove(upload_test_ipynb)

    def test_upload_file_io(self):
        upload_test_ipynb = f'upload_test_{id(self)}.ipynb'

        for space in [self.personal_space, self.shared_space]:
            root = space.info('/')
            assert str(root.path) == '/'
            assert root.type == 'directory'

            # Upload files
            f = space.upload_file(
                open(TEST_DIR / 'test.ipynb', 'r'),
                upload_test_ipynb,
            )
            assert str(f.path) == upload_test_ipynb
            assert f.type == 'notebook'

            # Download and compare to original
            txt = f.download(encoding='utf-8')
            assert txt == open(TEST_DIR / 'test.ipynb').read()

            # Make sure we can't overwrite
            with self.assertRaises(OSError):
                space.upload_file(
                    open(TEST_DIR / 'test.ipynb', 'r'),
                    upload_test_ipynb,
                )

            # Force overwrite with new content
            f = space.upload_file(
                open(TEST_DIR / 'test2.ipynb', 'r'),
                upload_test_ipynb, overwrite=True,
            )
            assert str(f.path) == upload_test_ipynb
            assert f.type == 'notebook'

            # Verify new content
            txt = f.download(encoding='utf-8')
            assert txt == open(TEST_DIR / 'test2.ipynb').read()

            # Make sure we can't upload a folder
            with self.assertRaises(s2.ManagementError):
                space.upload_folder(TEST_DIR, 'test')

            # Cleanup
            space.remove(upload_test_ipynb)

    def test_open(self):
        for space in [self.personal_space, self.shared_space]:
            open_test_ipynb = f'open_test_ipynb_{id(self)}.ipynb'

            # See if error is raised for non-existent file
            with self.assertRaises(s2.ManagementError):
                space.open(open_test_ipynb, 'r')

            # Load test file
            space.upload_file(TEST_DIR / 'test.ipynb', open_test_ipynb)

            # Read file using `open`
            with space.open(open_test_ipynb, 'r') as rfile:
                assert rfile.read() == open(TEST_DIR / 'test.ipynb').read()

            # Read file using `open` with 'rt' mode
            with space.open(open_test_ipynb, 'rt') as rfile:
                assert rfile.read() == open(TEST_DIR / 'test.ipynb').read()

            # Read file using `open` with 'rb' mode
            with space.open(open_test_ipynb, 'rb') as rfile:
                assert rfile.read() == open(TEST_DIR / 'test.ipynb', 'rb').read()

            # Read file using `open` with 'rb' mode
            with self.assertRaises(ValueError):
                with space.open(open_test_ipynb, 'b') as rfile:
                    pass

            # Attempt overwrite file using `open` with mode 'x'
            with self.assertRaises(OSError):
                with space.open(open_test_ipynb, 'x') as wfile:
                    pass

            # Attempt overwrite file using `open` with mode 'w'
            with space.open(open_test_ipynb, 'w') as wfile:
                wfile.write(open(TEST_DIR / 'test2.ipynb').read())

            txt = space.download_file(open_test_ipynb, encoding='utf-8')

            assert txt == open(TEST_DIR / 'test2.ipynb').read()

            open_raw_test_ipynb = f'open_raw_test_{id(self)}.ipynb'

            # Test writer without context manager
            wfile = space.open(open_raw_test_ipynb, 'w')
            for line in open(TEST_DIR / 'test.ipynb'):
                wfile.write(line)
            wfile.close()

            txt = space.download_file(
                open_raw_test_ipynb,
                encoding='utf-8',
            )

            assert txt == open(TEST_DIR / 'test.ipynb').read()

            # Test reader without context manager
            rfile = space.open(open_raw_test_ipynb, 'r')
            txt = ''
            for line in rfile:
                txt += line
            rfile.close()

            assert txt == open(TEST_DIR / 'test.ipynb').read()

            # Cleanup
            space.remove(open_test_ipynb)
            space.remove(open_raw_test_ipynb)

    def test_obj_open(self):
        for space in [self.personal_space, self.shared_space]:
            obj_open_test_ipynb = f'obj_open_test_{id(self)}.ipynb'
            obj_open_dir = f'obj_open_dir_{id(self)}'

            # Load test file
            f = space.upload_file(
                TEST_DIR / 'test.ipynb',
                obj_open_test_ipynb,
            )

            # Read file using `open`
            with f.open() as rfile:
                assert rfile.read() == open(TEST_DIR / 'test.ipynb').read()

            # Make sure directories error out
            with self.assertRaises(s2.ManagementError):
                space.mkdir(obj_open_dir)

            # Write file using `open`
            with f.open('w', encoding='utf-8') as wfile:
                wfile.write(open(TEST_DIR / 'test2.ipynb').read())

            assert f.download(encoding='utf-8') == open(TEST_DIR / 'test2.ipynb').read()

            # Test writer without context manager
            wfile = f.open('w')
            for line in open(TEST_DIR / 'test.ipynb'):
                wfile.write(line)
            wfile.close()

            txt = space.download_file(f.path, encoding='utf-8')

            assert txt == open(TEST_DIR / 'test.ipynb').read()

            # Test reader without context manager
            rfile = f.open('r')
            txt = ''
            for line in rfile:
                txt += line
            rfile.close()

            assert txt == open(TEST_DIR / 'test.ipynb').read()

            # Cleanup
            space.remove(obj_open_test_ipynb)

    def test_os_directories(self):
        for space in [self.personal_space, self.shared_space]:
            # Make sure directories error out
            with self.assertRaises(s2.ManagementError):
                space.mkdir('mkdir_test_1')

            with self.assertRaises(s2.ManagementError):
                space.exists('mkdir_test_1/')

            out = space.listdir('/')
            assert 'mkdir_test_1/' not in out

            with self.assertRaises(s2.ManagementError):
                space.rmdir('mkdir_test_1/')

    def test_os_rename(self):
        for space in [self.personal_space, self.shared_space]:
            space.upload_file(
                TEST_DIR / 'test.ipynb',
                'rename_test.ipynb',
            )
            assert 'rename_test.ipynb' in space.listdir('/')
            assert 'rename_test_2.ipynb' not in space.listdir('/')

            space.rename(
                'rename_test.ipynb',
                'rename_test_2.ipynb',
            )
            assert 'rename_test.ipynb' not in space.listdir('/')
            assert 'rename_test_2.ipynb' in space.listdir('/')

            # non-existent file
            with self.assertRaises(OSError):
                space.rename('rename_foo.ipynb', 'rename_foo_2.ipynb')

            space.upload_file(
                TEST_DIR / 'test.ipynb',
                'rename_test_3.ipynb',
            )

            # overwrite
            with self.assertRaises(OSError):
                space.rename(
                    'rename_test_2.ipynb',
                    'rename_test_3.ipynb',
                )

            space.rename(
                'rename_test_2.ipynb',
                'rename_test_3.ipynb', overwrite=True,
            )

            # Cleanup
            space.remove('rename_test_3.ipynb')

    def test_file_object(self):
        for space in [self.personal_space, self.shared_space]:
            f = space.upload_file(
                TEST_DIR / 'test.ipynb',
                'obj_test.ipynb',
            )

            assert not f.is_dir()
            assert f.is_file()

            # abspath / basename / dirname / exists
            assert f.abspath() == 'obj_test.ipynb'
            assert f.basename() == 'obj_test.ipynb'
            assert f.dirname() == '/'
            assert f.exists()

            # download
            assert f.download(encoding='utf-8') == \
                open(TEST_DIR / 'test.ipynb', 'r').read()
            assert f.download() == open(TEST_DIR / 'test.ipynb', 'rb').read()

            assert space.is_file('obj_test.ipynb')
            f.remove()
            assert not space.is_file('obj_test.ipynb')

            # mtime / ctime
            assert f.getmtime() > 0
            assert f.getctime() > 0

            # rename
            f = space.upload_file(
                TEST_DIR / 'test.ipynb',
                'obj_test.ipynb',
            )
            assert space.exists('obj_test.ipynb')
            assert not space.exists('obj_test_2.ipynb')
            f.rename('obj_test_2.ipynb')
            assert not space.exists('obj_test.ipynb')
            assert space.exists('obj_test_2.ipynb')
            assert f.abspath() == 'obj_test_2.ipynb'

            # Cleanup
            space.remove('obj_test_2.ipynb')


@pytest.mark.management
class TestRegions(unittest.TestCase):
    """Test cases for region management."""

    manager = None

    @classmethod
    def setUpClass(cls):
        """Set up the test environment."""
        cls.manager = s2.manage_regions()

    @classmethod
    def tearDownClass(cls):
        """Clean up the test environment."""
        cls.manager = None

    def test_list_regions(self):
        """Test listing all regions."""
        regions = self.manager.list_regions()

        # Verify we get a NamedList
        assert isinstance(regions, NamedList)

        # Verify we have at least one region
        assert len(regions) > 0

        # Verify region properties
        region = regions[0]
        assert isinstance(region, Region)
        assert hasattr(region, 'id')
        assert hasattr(region, 'name')
        assert hasattr(region, 'provider')

        # Verify provider values
        providers = {x.provider for x in regions}
        assert 'Azure' in providers or 'GCP' in providers or 'AWS' in providers

    def test_list_shared_tier_regions(self):
        """Test listing shared tier regions."""
        regions = self.manager.list_shared_tier_regions()

        # Verify we get a NamedList
        assert isinstance(regions, NamedList)

        # Verify region properties if we have any shared tier regions
        if regions:
            region = regions[0]
            assert isinstance(region, Region)
            assert hasattr(region, 'name')
            assert hasattr(region, 'provider')
            assert hasattr(region, 'region_name')

            # Verify provider values
            providers = {x.provider for x in regions}
            assert any(p in providers for p in ['Azure', 'GCP', 'AWS'])

    def test_str_repr(self):
        """Test string representation of regions."""
        regions = self.manager.list_regions()
        if not regions:
            self.skipTest('No regions available for testing')

        region = regions[0]

        # Test __str__
        s = str(region)
        assert region.id in s
        assert region.name in s
        assert region.provider in s

        # Test __repr__
        assert repr(region) == str(region)
