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


TEST_DIR = pathlib.Path(os.path.dirname(__file__))


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
        cls.password = secrets.token_urlsafe(20) + '-x&'

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
class TestStage(unittest.TestCase):

    manager = None
    wg = None
    password = None

    @classmethod
    def setUpClass(cls):
        cls.manager = s2.manage_workspaces()

        us_regions = [x for x in cls.manager.regions if 'US' in x.name]
        cls.password = secrets.token_urlsafe(20)

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

        root = st.info('/')
        assert str(root.path) == '/'
        assert root.type == 'directory'

        # Upload file
        f = st.upload_file(TEST_DIR / 'test.sql', 'upload_test.sql')
        assert str(f.path) == 'upload_test.sql'
        assert f.type == 'file'

        # Download and compare to original
        txt = f.download(encoding='utf-8')
        assert txt == open(TEST_DIR / 'test.sql').read()

        # Make sure we can't overwrite
        with self.assertRaises(OSError):
            st.upload_file(TEST_DIR / 'test.sql', 'upload_test.sql')

        # Force overwrite with new content
        f = st.upload_file(TEST_DIR / 'test2.sql', 'upload_test.sql', overwrite=True)
        assert str(f.path) == 'upload_test.sql'
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
            os.path.join(lib.path, 'upload_test2.sql'),
        )
        assert str(f.path) == 'lib/upload_test2.sql'
        assert f.type == 'file'

    def test_open(self):
        st = self.wg.stage

        # See if error is raised for non-existent file
        with self.assertRaises(s2.ManagementError):
            st.open('open_test.sql', 'r')

        # Load test file
        st.upload_file(TEST_DIR / 'test.sql', 'open_test.sql')

        # Read file using `open`
        with st.open('open_test.sql', 'r') as rfile:
            assert rfile.read() == open(TEST_DIR / 'test.sql').read()

        # Read file using `open` with 'rt' mode
        with st.open('open_test.sql', 'rt') as rfile:
            assert rfile.read() == open(TEST_DIR / 'test.sql').read()

        # Read file using `open` with 'rb' mode
        with st.open('open_test.sql', 'rb') as rfile:
            assert rfile.read() == open(TEST_DIR / 'test.sql', 'rb').read()

        # Read file using `open` with 'rb' mode
        with self.assertRaises(ValueError):
            with st.open('open_test.sql', 'b') as rfile:
                pass

        # Attempt overwrite file using `open` with mode 'x'
        with self.assertRaises(OSError):
            with st.open('open_test.sql', 'x') as wfile:
                pass

        # Attempt overwrite file using `open` with mode 'w'
        with st.open('open_test.sql', 'w') as wfile:
            wfile.write(open(TEST_DIR / 'test2.sql').read())

        txt = st.download_file('open_test.sql', encoding='utf-8')

        assert txt == open(TEST_DIR / 'test2.sql').read()

        # Test writer without context manager
        wfile = st.open('open_raw_test.sql', 'w')
        for line in open(TEST_DIR / 'test.sql'):
            wfile.write(line)
        wfile.close()

        txt = st.download_file('open_raw_test.sql', encoding='utf-8')

        assert txt == open(TEST_DIR / 'test.sql').read()

        # Test reader without context manager
        rfile = st.open('open_raw_test.sql', 'r')
        txt = ''
        for line in rfile:
            txt += line
        rfile.close()

        assert txt == open(TEST_DIR / 'test.sql').read()

    def test_obj_open(self):
        st = self.wg.stage

        # Load test file
        f = st.upload_file(TEST_DIR / 'test.sql', 'obj_open_test.sql')

        # Read file using `open`
        with f.open() as rfile:
            assert rfile.read() == open(TEST_DIR / 'test.sql').read()

        # Make sure directories error out
        d = st.mkdir('obj_open_dir')
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

    def test_stage_object(self):
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
        cls.password = secrets.token_urlsafe(20)

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
