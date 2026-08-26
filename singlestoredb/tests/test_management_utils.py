#!/usr/bin/env python
# type: ignore
"""
Version-neutral unit tests for the management API helpers.

Nothing here touches a version-specific module or needs a management token or
a container. These were originally written alongside the versioned wrappers
only because that is where the bugs were found.
"""
import datetime
import os
import pathlib
import unittest
from unittest.mock import MagicMock
from unittest.mock import patch

from singlestoredb.exceptions import ManagementError
from singlestoredb.management.utils import normalize_remote_path


TEST_DIR = pathlib.Path(os.path.dirname(__file__))


class TestFolderTransferPaths(unittest.TestCase):
    """Folder helpers must address remote objects with the full remote path
    and resolve ``ignore`` globs relative to the local folder."""

    def _make_stage(self):
        from singlestoredb.management.stage import Stage
        stage = Stage.__new__(Stage)
        stage._manager = MagicMock()
        return stage

    def _make_file_space(self):
        from singlestoredb.management.files import FileSpace
        space = FileSpace.__new__(FileSpace)
        space._manager = MagicMock()
        return space

    def _make_files_object(self, path, type_='file'):
        from singlestoredb.management.files import FilesObject
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


class TestRecursiveDownloadPathTraversal(unittest.TestCase):
    """Recursive download helpers must refuse to write outside ``local_path``
    when the remote listing contains traversal segments (``..``)."""

    def _make_file_location(self):
        # FileSpace is a concrete FileLocation subclass; instantiate via
        # __new__ to skip its constructor (which expects a real FilesManager).
        from singlestoredb.management.files import FileSpace
        loc = FileSpace.__new__(FileSpace)
        loc._manager = MagicMock()
        return loc

    def _make_files_object(self, path, type_='file'):
        from singlestoredb.management.files import FilesObject
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
        from singlestoredb.management.stage import Stage
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


class TestRemotePathUtils(unittest.TestCase):
    """Test cases for remote path normalization (no server required)."""

    def test_local_separators_converted(self):
        # A prefix built with os.path.join on Windows keeps a trailing '\'
        assert normalize_remote_path('llama3\\') == 'llama3'
        assert normalize_remote_path('a\\b\\c.txt') == 'a/b/c.txt'
        assert normalize_remote_path(pathlib.PurePosixPath('a/b')) == 'a/b'

    def test_duplicate_and_trailing_separators_collapsed(self):
        assert normalize_remote_path('a//b/') == 'a/b'
        assert normalize_remote_path('a/b///') == 'a/b'
        assert normalize_remote_path('a\\\\b\\') == 'a/b'

    def test_strip_leading(self):
        assert normalize_remote_path('./a/b', strip_leading=True) == 'a/b'
        assert normalize_remote_path('/a/b', strip_leading=True) == 'a/b'
        assert normalize_remote_path('.\\a\\b', strip_leading=True) == 'a/b'
        assert normalize_remote_path('/', strip_leading=True) == ''
        assert normalize_remote_path('', strip_leading=True) == ''

    def test_strip_leading_off_by_default(self):
        assert normalize_remote_path('/a/b') == '/a/b'

    def test_joining_produces_valid_remote_path(self):
        # Regression: 'llama3\/file' was produced before normalization
        prefix = normalize_remote_path('llama3\\')
        assert f'{prefix}/file' == 'llama3/file'

    def test_listdir_style_suffix(self):
        # The listdir call sites append '/' after normalizing
        assert normalize_remote_path('llama3\\', strip_leading=True) + '/' \
            == 'llama3/'
        assert normalize_remote_path('/', strip_leading=True) + '/' == '/'


class TestSecretFromDictTimestamps(unittest.TestCase):
    """
    Coverage for ``Secret.from_dict`` running its timestamp fields
    through ``to_datetime``.
    """

    def test_timestamps_parsed_to_datetime(self):
        from singlestoredb.management.organization import Secret

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
        from singlestoredb.management.organization import Secret

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


class TestTTLProperty(unittest.TestCase):
    """A ttl_property caches per instance, not per class."""

    @staticmethod
    def _counter_class():
        from singlestoredb.management.utils import ttl_property

        class Counter:
            def __init__(self):
                self.calls = 0

            @ttl_property(datetime.timedelta(hours=1))
            def value(self):
                self.calls += 1
                return self.calls

        return Counter

    def test_repeated_reads_are_served_from_the_cache(self):
        obj = self._counter_class()()
        self.assertEqual(obj.value, 1)
        self.assertEqual(obj.value, 1)
        self.assertEqual(obj.calls, 1)

    def test_each_instance_caches_its_own_value(self):
        # Two managers may hold different tokens, so one must never be served
        # the other's copy.
        cls = self._counter_class()
        first, second = cls(), cls()
        self.assertEqual(first.value, 1)
        self.assertEqual(second.value, 1)
        self.assertEqual(first.calls, 1)
        self.assertEqual(second.calls, 1)

    def test_an_expired_value_is_refetched(self):
        cls = self._counter_class()
        obj = cls()
        self.assertEqual(obj.value, 1)
        type(obj).__dict__['value'].ttl = datetime.timedelta(0)
        self.assertEqual(obj.value, 2)

    def test_reset_discards_the_cached_value(self):
        cls = self._counter_class()
        obj = cls()
        self.assertEqual(obj.value, 1)
        type(obj).__dict__['value'].reset(obj)
        self.assertEqual(obj.value, 2)


class TestManagerTransport(unittest.TestCase):
    """
    Retries and timeouts on the session every manager shares.

    The long ``wait_on_active`` loops poll for twenty minutes, and a
    keep-alive connection the far end closed while the client slept surfaces
    as ``RemoteDisconnected`` on the next poll -- which used to fail the whole
    operation, leaving a live cluster behind.
    """

    def _manager(self):
        from singlestoredb.management.manager import Manager
        return Manager(access_token='fake-token', base_url='https://example.com')

    def test_retries_are_mounted_for_both_schemes(self):
        mgr = self._manager()
        for prefix in ('http://', 'https://'):
            retries = mgr._sess.get_adapter(prefix + 'x').max_retries
            self.assertGreater(retries.total, 0)

    def test_post_is_not_replayed(self):
        # A dropped connection does not say whether the server acted on the
        # request, and a replayed POST /clusters deploys twice.
        retries = self._manager()._sess.get_adapter('https://x').max_retries
        self.assertNotIn('POST', retries.allowed_methods)
        self.assertIn('GET', retries.allowed_methods)
        self.assertIn('DELETE', retries.allowed_methods)

    def test_transient_statuses_are_retried(self):
        retries = self._manager()._sess.get_adapter('https://x').max_retries
        for status in (429, 502, 503, 504):
            self.assertIn(status, retries.status_forcelist)
        self.assertNotIn(404, retries.status_forcelist)
        # _check has to be the one to raise, so it can quote the body.
        self.assertFalse(retries.raise_on_status)

    def test_a_default_timeout_is_applied(self):
        mgr = self._manager()
        mgr._sess.get = MagicMock()
        mgr._doit('get', 'clusters')
        self.assertEqual(
            mgr._sess.get.call_args[1]['timeout'],
            (10.0, 180.0),
        )

    def test_an_explicit_timeout_wins(self):
        mgr = self._manager()
        mgr._sess.get = MagicMock()
        mgr._doit('get', 'clusters', timeout=1)
        self.assertEqual(mgr._sess.get.call_args[1]['timeout'], 1)

    def test_a_transport_failure_names_the_route(self):
        import requests

        mgr = self._manager()
        mgr._sess.get = MagicMock(
            side_effect=requests.exceptions.ConnectionError(
                'Connection aborted.',
            ),
        )
        with self.assertRaises(ManagementError) as cm:
            mgr._doit('get', 'clusters/abc')

        msg = str(cm.exception)
        self.assertIn('ConnectionError', msg)
        self.assertIn('GET', msg)
        self.assertIn('clusters/abc', msg)


class TestDeploymentTracking(unittest.TestCase):
    """
    The sweeper in ``tests/utils.py`` that keeps test runs from leaking
    billable deployments.
    """

    def setUp(self):
        from singlestoredb.tests import utils
        self.utils = utils
        self.saved = list(utils._tracked)
        utils._tracked.clear()
        self.addCleanup(self._restore)
        self.owner = utils.get_owner()
        self.addCleanup(lambda: utils.set_owner(self.owner))

    def _restore(self):
        self.utils._tracked.clear()
        self.utils._tracked.extend(self.saved)

    def _deployment(self, name, terminated_at=None, state='ACTIVE'):
        """A stand-in that is not a Mock, so tracking does not skip it."""
        class Deployment:
            def __init__(self):
                self.name = name
                self.id = name
                self.terminated_at = terminated_at
                self.state = state
                self.terminated_with = None
                self._manager = object()

            def refresh(self):
                return self

            def terminate(self, force=False):
                self.terminated_with = force

        return Deployment()

    def test_mocked_deployments_are_not_tracked(self):
        # The unit tests create objects from patched _post calls; sweeping
        # those would be a round trip and a warning per fake object.
        self.utils.track(MagicMock())
        self.assertEqual(self.utils._tracked, [])

    def test_a_tracked_deployment_is_terminated_with_force(self):
        obj = self._deployment('wg-1')
        self.utils.track(obj)
        self.assertEqual(len(self.utils.cleanup_tracked()), 1)
        self.assertTrue(obj.terminated_with)
        self.assertEqual(self.utils._tracked, [])

    def test_an_already_terminated_deployment_is_left_alone(self):
        obj = self._deployment('wg-1', terminated_at='2026-01-01T00:00:00Z')
        self.utils.track(obj)
        self.assertEqual(self.utils.cleanup_tracked(), [])
        self.assertIsNone(obj.terminated_with)

    def test_a_deployment_that_no_longer_exists_is_left_alone(self):
        obj = self._deployment('wg-1')
        obj.refresh = MagicMock(side_effect=KeyError('gone'))
        self.utils.track(obj)
        self.assertEqual(self.utils.cleanup_tracked(), [])
        self.assertIsNone(obj.terminated_with)

    def test_children_are_terminated_before_their_parents(self):
        group = self._deployment('wg-1')
        space = self._deployment('ws-1')
        order = []
        for obj in (group, space):
            obj.terminate = lambda force=False, obj=obj: order.append(obj.name)
        self.utils.track(group)
        self.utils.track(space)
        self.utils.cleanup_tracked()
        self.assertEqual(order, ['ws-1', 'wg-1'])

    def test_a_sweep_is_limited_to_one_owner(self):
        self.utils.set_owner('mod.ClassA')
        first = self.utils.track(self._deployment('a'))
        self.utils.set_owner('mod.ClassB')
        second = self.utils.track(self._deployment('b'))

        self.assertEqual(self.utils.cleanup_tracked('mod.ClassA'), ["Deployment 'a'"])
        self.assertTrue(first.terminated_with)
        self.assertIsNone(second.terminated_with)

        # ... and the rest still goes at the end of the session.
        self.assertEqual(len(self.utils.cleanup_tracked()), 1)
        self.assertTrue(second.terminated_with)

    def test_a_failed_termination_does_not_stop_the_sweep(self):
        first = self._deployment('a')
        first.terminate = MagicMock(side_effect=RuntimeError('boom'))
        second = self._deployment('b')
        self.utils.track(first)
        self.utils.track(second)

        # Nothing raises: this runs outside any test, where an exception is
        # reported against whatever happens to run next.
        self.assertEqual(self.utils.cleanup_tracked(), ["Deployment 'b'"])
        self.assertTrue(second.terminated_with)

    def test_untrack_drops_a_deployment(self):
        obj = self.utils.track(self._deployment('a'))
        self.utils.untrack(obj)
        self.assertEqual(self.utils.cleanup_tracked(), [])
        self.assertIsNone(obj.terminated_with)

    def test_every_creation_method_is_wrapped(self):
        # A rename that silently stops tracking is how a cluster leaks.
        import importlib

        self.utils.install_deployment_tracking()
        for module_name, class_name, method_name in self.utils._CREATORS:
            klass = getattr(importlib.import_module(module_name), class_name)
            method = getattr(klass, method_name, None)
            self.assertIsNotNone(
                method, f'{class_name}.{method_name} no longer exists',
            )
            self.assertTrue(
                hasattr(method, '__wrapped__'),
                f'{class_name}.{method_name} is not tracked',
            )


class TestLeftoverDeploymentPatterns(unittest.TestCase):
    """
    The maintenance sweep runs against a real organization, so it must match
    the names the suite generates and nothing else.
    """

    def setUp(self):
        from singlestoredb.tests import cleanup_deployments
        self.mod = cleanup_deployments

    def test_generated_names_match(self):
        for name in (
            'wg-test-abcDEF_12',
            'ws-test-abcDEF-x',
            'cl-test-abcDEF',
            'starter-ws-test-abcDEF',
            'starter-cl-test-abcDEF',
            'A Fusion Testing deadbeefdeadbeef',
            'C Fusion Testing deadbeef',
            'd-fusion-cluster-deadbeef',
            'jobs-fusion-deadbeef',
            'stage-fusion-2-deadbeef',
        ):
            self.assertTrue(self.mod.is_test_deployment(name), name)

    def test_names_a_person_chose_do_not_match(self):
        for name in (
            None,
            '',
            'my-production-cluster',
            'wg-test',
            'prod wg-test-x',
            'analytics-fusion-cluster',
            'Fusion Testing',
            'a-fusion-cluster-deadbeef-prod',
        ):
            self.assertFalse(self.mod.is_test_deployment(name), name)

    def _cluster(self, name, hours=None, naive=False):
        # A naive created_at is what the API sends when it omits the zone: the
        # instant is still UTC, the tzinfo is just missing.
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        if naive:
            now = now.replace(tzinfo=None)

        class Cluster:
            def __init__(self):
                self.name = name
                self.id = name
                self.terminated_at = None
                self.created_at = (
                    None if hours is None
                    else now - datetime.timedelta(hours=hours)
                )

        return Cluster()

    def _find(self, clusters, **kwargs):
        """Run find_leftovers against a fixed cluster list."""
        import singlestoredb as s2

        manager = MagicMock()
        manager.clusters = clusters
        manager.starter_clusters = []
        with patch.object(
            s2, 'manage_clusters', return_value=manager,
        ), patch.object(
            s2, 'manage_workspaces', side_effect=RuntimeError('no v1'),
        ):
            found, spared = self.mod.find_leftovers(**kwargs)
        return [x[1].name for x in found], spared

    def test_the_age_filter_spares_a_deployment_a_live_run_may_own(self):
        # A parallel run's fixtures are named exactly like stranded ones, so
        # age is the only thing keeping this from killing them mid-test.
        old = self._cluster('cl-test-old', hours=5)
        new = self._cluster('cl-test-new', hours=0.5)
        terminated = self._cluster('cl-test-gone', hours=5)
        terminated.terminated_at = 'yes'

        names, spared = self._find([old, new, terminated], older_than=2)

        self.assertEqual(names, ['cl-test-old'])
        self.assertEqual(len(spared), 1)
        self.assertIn('cl-test-new', spared[0])

    def test_the_default_spares_anything_a_run_could_still_own(self):
        # Not zero: a default that swept every match would make running this
        # during a test run destructive.
        self.assertGreaterEqual(self.mod.DEFAULT_MIN_AGE_HOURS, 1)
        names, spared = self._find([
            self._cluster('cl-test-mid-run', hours=1),
        ])
        self.assertEqual(names, [])
        self.assertEqual(len(spared), 1)

    def test_an_unreported_creation_time_is_spared_by_default(self):
        names, spared = self._find([self._cluster('cl-test-ageless')])
        self.assertEqual(names, [])
        self.assertIn('cl-test-ageless', spared[0])

        names, _ = self._find(
            [self._cluster('cl-test-ageless')], include_unknown_age=True,
        )
        self.assertEqual(names, ['cl-test-ageless'])

    def test_a_naive_timestamp_is_read_as_utc(self):
        # Reading it as local time would overstate the age east of UTC and
        # sweep a deployment a live run owns.
        obj = self._cluster('cl-test-naive', hours=1, naive=True)
        self.assertAlmostEqual(self.mod._age_hours(obj), 1, delta=0.1)

    def test_zero_sweeps_everything_matched(self):
        names, spared = self._find(
            [self._cluster('cl-test-brand-new', hours=0)], older_than=0,
        )
        self.assertEqual(names, ['cl-test-brand-new'])
        self.assertEqual(spared, [])


if __name__ == '__main__':
    unittest.main()
