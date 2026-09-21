#!/usr/bin/env python
# type: ignore
"""
Version-neutral unit tests for the management API helpers.

Nothing here touches a version-specific module or needs a management token or
a container. These were originally written alongside the versioned wrappers
only because that is where the bugs were found.
"""
import datetime
import json
import os
import pathlib
import shutil
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock
from unittest.mock import patch

from singlestoredb.exceptions import ManagementError
from singlestoredb.management.utils import _normalize_datetime
from singlestoredb.management.utils import normalize_remote_path
from singlestoredb.management.utils import to_datetime
from singlestoredb.management.utils import to_datetime_strict
from singlestoredb.tests.utils import counting_file_space
from singlestoredb.tests.utils import counting_stage


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

    def test_stage_upload_folder_strips_leading_prefix_segments(self):
        """A '/foo' or './foo' prefix must not survive into the remote path.

        ``listdir`` and ``download_folder`` already pass
        ``strip_leading=True``, and the stage routes interpolate the path into
        a URL, so a leading '/' would produce a doubled slash.
        """
        import tempfile
        for prefix in ('/dest', './dest'):
            stage = self._make_stage()
            stage.exists = MagicMock(return_value=False)
            stage.upload_file = MagicMock()
            stage.info = MagicMock()
            with tempfile.TemporaryDirectory() as tmp:
                root, _, _ = self._make_local_tree(tmp)
                stage.upload_folder(root, prefix)
            targets = sorted(
                call.args[1] for call in stage.upload_file.call_args_list
            )
            self.assertEqual(
                targets, ['dest/keep.py', 'dest/sub/skip.pyc'],
                f'prefix {prefix!r} produced {targets}',
            )

    def test_file_space_upload_folder_strips_leading_prefix_segments(self):
        """``FileSpace._upload`` builds ``files/fs/{location}/{path}``, so a
        leading '/' would request ``files/fs/<loc>//dest/...``."""
        import tempfile
        for prefix in ('/dest', './dest'):
            space = self._make_file_space()
            space.upload_file = MagicMock()
            space.info = MagicMock()
            with tempfile.TemporaryDirectory() as tmp:
                root, _, _ = self._make_local_tree(tmp)
                space.upload_folder(root, prefix)
            targets = sorted(
                call.kwargs['path']
                for call in space.upload_file.call_args_list
            )
            self.assertEqual(
                targets, ['dest/keep.py', 'dest/sub/skip.pyc'],
                f'prefix {prefix!r} produced {targets}',
            )

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


class TestCustomModelUploadPaths(unittest.TestCase):
    """``UPLOAD CUSTOM MODEL`` must not replay the local directory tree into
    the models space. The handler is hidden (``_enabled = False``) and so has
    no live coverage."""

    def _run(self, local_path):
        from singlestoredb.fusion.handlers.models import UploadCustomModelHandler
        handler = UploadCustomModelHandler.__new__(UploadCustomModelHandler)
        space = MagicMock()
        with patch(
            'singlestoredb.fusion.handlers.models.get_file_space',
            return_value=space,
        ):
            handler.run(
                dict(
                    model_name='mymodel',
                    local_path=local_path,
                    overwrite=False,
                ),
            )
        return space

    def test_single_file_uploads_under_the_model_name(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            local = os.path.join(tmp, 'nested', 'weights.bin')
            os.makedirs(os.path.dirname(local))
            with open(local, 'w') as f:
                f.write('x')
            space = self._run(local)
        space.upload_folder.assert_not_called()
        self.assertEqual(
            space._upload_local_file.call_args.kwargs['path'],
            'mymodel/weights.bin',
        )

    def test_a_directory_still_goes_through_upload_folder(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            space = self._run(tmp)
        space._upload_local_file.assert_not_called()
        self.assertEqual(
            space.upload_folder.call_args.kwargs['path'], 'mymodel',
        )


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


class TestUploadRoundTrips(unittest.TestCase):
    """An upload must not repeat work it has already done.

    The counts pinned here are the Stage / file space half of what
    ``UPLOAD FILE TO STAGE`` costs; the two that resolve ``IN '<name>'`` are
    made before a ``Stage`` exists and so cannot be seen from here. For the
    whole-statement count, add two.
    """

    def _local_file(self, tmp, content='contents'):
        local = os.path.join(tmp, 'local.csv')
        with open(local, 'w') as f:
            f.write(content)
        return local

    def test_a_fresh_upload_costs_one_check_and_one_write(self):
        with tempfile.TemporaryDirectory() as tmp:
            local = self._local_file(tmp)
            stage, manager = counting_stage()
            obj = stage.upload_file(local, 'remote.csv')
        # Was four: upload_file and _upload each checked exists()
        self.assertEqual(
            manager.calls, [
                ('GET', 'remote.csv'),   # exists()
                ('PUT', 'remote.csv'),   # the upload
                ('GET', 'remote.csv'),   # info() for the return value
            ],
        )
        # The public contract still hands back a populated object
        self.assertEqual(obj.name, 'remote.csv')
        self.assertEqual(obj.path, 'remote.csv')
        self.assertEqual(obj.type, 'file')
        self.assertEqual(obj.size, 8)
        self.assertTrue(obj.writable)

    def test_an_overwrite_costs_one_check_and_one_delete(self):
        with tempfile.TemporaryDirectory() as tmp:
            local = self._local_file(tmp)
            stage, manager = counting_stage(existing=['remote.csv'])
            stage.upload_file(local, 'remote.csv', overwrite=True)
        # Was six: the duplicated exists() dragged a second remove() check in,
        # and then the remaining exists()/is_dir() pair was the same GET twice
        self.assertEqual(
            manager.calls, [
                ('GET', 'remote.csv'),      # the one metadata fetch
                ('DELETE', 'remote.csv'),
                ('PUT', 'remote.csv'),
                ('GET', 'remote.csv'),      # info() for the return value
            ],
        )

    def test_an_overwrite_of_a_folder_raises_on_the_one_check(self):
        # The IsADirectoryError remove() used to raise through _upload is
        # raised by _upload itself now, with the same message.
        with tempfile.TemporaryDirectory() as tmp:
            local = self._local_file(tmp)
            stage, manager = counting_stage(existing=['remote.csv/'])
            with self.assertRaises(IsADirectoryError) as ctx:
                stage.upload_file(local, 'remote.csv', overwrite=True)
        self.assertIn('use rmdir or removedirs', str(ctx.exception))
        self.assertEqual(manager.calls, [('GET', 'remote.csv')])

    def test_a_conflict_still_raises_and_closes_the_local_file(self):
        opened = []
        real_open = open

        def recording_open(*args, **kwargs):
            handle = real_open(*args, **kwargs)
            opened.append(handle)
            return handle

        with tempfile.TemporaryDirectory() as tmp:
            local = self._local_file(tmp)
            stage, manager = counting_stage(existing=['remote.csv'])
            with patch('builtins.open', recording_open):
                with self.assertRaises(OSError) as ctx:
                    stage.upload_file(local, 'remote.csv')
        self.assertIn('stage path already exists', str(ctx.exception))
        self.assertEqual(manager.calls, [('GET', 'remote.csv')])
        # The conflict is now detected inside _upload, which is after the
        # local file has been opened, so that handle has to close on the way
        # out rather than wait for the collector
        self.assertTrue(opened)
        self.assertTrue(all(handle.closed for handle in opened))

    def test_a_local_directory_is_rejected_before_any_request(self):
        with tempfile.TemporaryDirectory() as tmp:
            stage, manager = counting_stage()
            with self.assertRaises(IsADirectoryError):
                stage.upload_file(tmp, 'remote.csv')
        self.assertEqual(manager.calls, [])

    def test_the_fusion_path_skips_the_metadata_request(self):
        with tempfile.TemporaryDirectory() as tmp:
            local = self._local_file(tmp)
            stage, manager = counting_stage()
            out = stage._upload_local_file(local, 'remote.csv', fetch_info=False)
        self.assertIsNone(out)
        self.assertEqual(
            manager.calls, [('GET', 'remote.csv'), ('PUT', 'remote.csv')],
        )

    def test_the_fusion_handler_takes_that_path(self):
        from singlestoredb.fusion.handlers.stage import UploadStageFileHandler
        handler = UploadStageFileHandler.__new__(UploadStageFileHandler)
        with tempfile.TemporaryDirectory() as tmp:
            local = self._local_file(tmp)
            stage, manager = counting_stage()
            with patch(
                'singlestoredb.fusion.handlers.stage.get_deployment',
                return_value=SimpleNamespace(stage=stage),
            ):
                handler.run(
                    dict(
                        local_path=local,
                        stage_path='remote.csv',
                        overwrite=False,
                    ),
                )
        self.assertEqual(
            manager.calls, [('GET', 'remote.csv'), ('PUT', 'remote.csv')],
        )

    def test_a_file_space_upload_costs_the_same(self):
        with tempfile.TemporaryDirectory() as tmp:
            local = self._local_file(tmp)
            space, manager = counting_file_space()
            obj = space.upload_file(local, 'remote.csv')
            fresh = list(manager.calls)

            manager.calls.clear()
            out = space._upload_local_file(
                local, 'other.csv', fetch_info=False,
            )
        self.assertEqual(
            fresh, [
                ('GET', 'remote.csv'),
                ('PUT', 'remote.csv'),
                ('GET', 'remote.csv'),
            ],
        )
        self.assertEqual(obj.type, 'file')
        self.assertIsNone(out)
        self.assertEqual(
            manager.calls, [('GET', 'other.csv'), ('PUT', 'other.csv')],
        )

    def test_a_file_space_conflict_names_the_file_space(self):
        with tempfile.TemporaryDirectory() as tmp:
            local = self._local_file(tmp)
            space, _ = counting_file_space(existing=['remote.csv'])
            with self.assertRaises(OSError) as ctx:
                space.upload_file(local, 'remote.csv')
        self.assertIn('file path already exists', str(ctx.exception))

    def test_a_file_space_overwrite_also_checks_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            local = self._local_file(tmp)
            space, manager = counting_file_space(existing=['remote.csv'])
            space.upload_file(local, 'remote.csv', overwrite=True)
        self.assertEqual(
            manager.calls, [
                ('GET', 'remote.csv'),
                ('DELETE', 'remote.csv'),
                ('PUT', 'remote.csv'),
                ('GET', 'remote.csv'),
            ],
        )

    def test_a_file_space_overwrite_of_a_folder_raises(self):
        with tempfile.TemporaryDirectory() as tmp:
            local = self._local_file(tmp)
            space, manager = counting_file_space(existing=['remote.csv/'])
            with self.assertRaises(IsADirectoryError) as ctx:
                space.upload_file(local, 'remote.csv', overwrite=True)
        self.assertIn('file path is a directory', str(ctx.exception))
        self.assertEqual(manager.calls, [('GET', 'remote.csv')])

    def test_a_folder_upload_pays_the_saving_per_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = os.path.join(tmp, 'src')
            os.makedirs(root)
            for name in ('a.csv', 'b.csv'):
                with open(os.path.join(root, name), 'w') as f:
                    f.write('x')
            stage, manager = counting_stage(existing=['dest/'])
            stage.upload_folder(root, 'dest')
        # Two files: one exists() + one PUT + one info() each, plus the
        # exists() / is_dir() on the destination and the closing info().
        # Was eleven -- one duplicated exists() per file.
        self.assertEqual(len(manager.calls), 9)
        self.assertEqual(manager.counts()['PUT'], 2)


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


class TestWaitOnEndpoint(unittest.TestCase):
    """
    ``Manager._wait_on_endpoint`` polls a new deployment by connecting to it.

    It only runs inside the notebook environment, which is why the loop having
    no exit on success went unnoticed: a successful connect fell out of the
    ``try`` and straight back into ``while True``, so the only ways out were an
    access-denied error or the timeout.
    """

    def _manager(self):
        from singlestoredb.management.manager import Manager
        mgr = Manager(access_token='fake-token', base_url='https://example.com')
        mgr.obj_type = 'cluster'
        return mgr

    def test_a_successful_connect_ends_the_wait(self):
        mgr = self._manager()
        out = MagicMock()

        with patch.dict(
            os.environ, {'SINGLESTOREDB_WORKLOAD_TYPE': 'notebook'},
        ):
            with patch('singlestoredb.management.timing.time.sleep'):
                result = mgr._wait_on_endpoint(out, interval=1, timeout=10)

        self.assertIs(result, out)
        # Once, not until the timeout ran out.
        self.assertEqual(out.connect.call_count, 1)

    def test_nothing_is_waited_on_outside_the_notebook_environment(self):
        mgr = self._manager()
        out = MagicMock()

        with patch.dict(os.environ, {'SINGLESTOREDB_WORKLOAD_TYPE': ''}):
            result = mgr._wait_on_endpoint(out, interval=1, timeout=10)

        self.assertIs(result, out)
        out.connect.assert_not_called()

    def test_a_refused_connection_is_retried_until_the_timeout(self):
        mgr = self._manager()
        out = MagicMock()
        out.connect = MagicMock(side_effect=OSError('connection refused'))

        with patch.dict(
            os.environ, {'SINGLESTOREDB_WORKLOAD_TYPE': 'notebook'},
        ):
            with patch('singlestoredb.management.timing.time.sleep'):
                with self.assertRaises(ManagementError) as cm:
                    mgr._wait_on_endpoint(out, interval=10, timeout=30)

        self.assertIn('endpoint', str(cm.exception))
        self.assertEqual(out.connect.call_count, 4)


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
        self.saved_in_flight = list(utils._in_flight)
        utils._in_flight.clear()
        self.addCleanup(self._restore)
        self.owner = utils.get_owner()
        self.addCleanup(lambda: utils.set_owner(self.owner))

    def _restore(self):
        self.utils._tracked.clear()
        self.utils._tracked.extend(self.saved)
        self.utils._in_flight.clear()
        self.utils._in_flight.extend(self.saved_in_flight)

    def _deployment(
        self, name, terminated_at=None, state='ACTIVE', classname=None,
    ):
        """
        A stand-in that is not a Mock, so tracking does not skip it.

        ``classname`` renames the class, which is how the ledger decides a
        kind (``utils._KIND_BY_CLASS`` is keyed by class name). The default
        ``Deployment`` is deliberately *not* a ledger kind, so the tests that
        only care about tracking write no ledger records even when one is
        configured.
        """
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

        if classname:
            Deployment.__name__ = classname
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
        obj.refresh = MagicMock(
            side_effect=ManagementError(errno=404, msg='not found'),
        )
        self.utils.track(obj)
        self.assertEqual(self.utils.cleanup_tracked(), [])
        self.assertIsNone(obj.terminated_with)
        self.assertEqual(self.utils._tracked, [])

    def test_a_refresh_that_fails_transiently_is_still_terminated(self):
        """Only a 404 means gone. Guessing "gone" on a 503 would skip the
        termination and leave the deployment running and billing."""
        for exc in (
            ManagementError(errno=503, msg='service unavailable'),
            KeyError('connection dropped'),
        ):
            obj = self._deployment('wg-1')
            obj.refresh = MagicMock(side_effect=exc)
            self.utils.track(obj)
            self.assertEqual(
                self.utils.cleanup_tracked(), ["Deployment 'wg-1'"],
                f'{exc!r} was taken as already gone',
            )
            self.assertTrue(obj.terminated_with)

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

        # 'a' stays tracked so the end-of-session sweep retries it. Dropping it
        # here is how one transient error used to leak a cluster for good.
        self.assertEqual(self.utils.tracked_labels(), ["Deployment 'a'"])
        first.terminate = MagicMock()
        self.assertEqual(self.utils.cleanup_tracked(), ["Deployment 'a'"])
        self.assertEqual(self.utils.tracked_labels(), [])

    def test_a_create_that_fails_while_waiting_still_tracks_the_orphan(self):
        """The headline leak: a creator makes the deployment and only then
        waits for it, so a wait that times out raises after the server has a
        live cluster. Tracking wraps the return value, so without recovery
        nothing registers it -- silently, with no summary line."""
        orphan = self._deployment('cl-test-shared-0-abc')
        receiver = SimpleNamespace(clusters=[orphan])

        def create_then_fail_waiting(recv, name, **kwargs):
            # What create_cluster does: the cluster exists by now, and the
            # wait is what raises.
            raise ManagementError(msg=f'Exceeded waiting time for {name}')

        wrapped = self.utils._tracking_wrapper(
            create_then_fail_waiting, 'cluster', lambda recv: recv.clusters,
        )
        with self.assertRaises(ManagementError):
            wrapped(receiver, 'cl-test-shared-0-abc', wait_on_active=True)

        self.assertEqual(
            self.utils.tracked_labels(),
            [
                "Deployment 'cl-test-shared-0-abc' (left behind by a failed "
                'create)',
            ],
        )
        self.assertEqual(len(self.utils.cleanup_tracked()), 1)
        self.assertTrue(orphan.terminated_with)

    def test_an_interrupt_during_the_wait_also_recovers_the_orphan(self):
        """Ctrl-C during wait_on_active leaves the same live cluster a timeout
        does, so the wrapper catches BaseException rather than Exception."""
        orphan = self._deployment('cl-1')
        receiver = SimpleNamespace(clusters=[orphan])

        def interrupted(recv, name, **kwargs):
            raise KeyboardInterrupt

        wrapped = self.utils._tracking_wrapper(
            interrupted, 'cluster', lambda recv: recv.clusters,
        )
        with self.assertRaises(KeyboardInterrupt):
            wrapped(receiver, 'cl-1')
        self.assertEqual(len(self.utils._tracked), 1)

    def test_a_mocked_receiver_does_not_track_what_it_returns(self):
        """A unit test's stubbed ``get_cluster`` hands back a real Cluster
        whose ``_manager`` is None, or a bare sentinel. ``track()`` calls
        anything it cannot place real -- rightly, since guessing "fake" leaks
        a billable cluster -- so it would register both, and the end-of-session
        summary would report phantom live deployments. The receiver's verdict
        is what decides."""
        mgr = SimpleNamespace(
            _get=MagicMock(), _post=MagicMock(), _delete=MagicMock(),
        )
        returned = self._deployment('my-cluster')
        returned._manager = None

        for value in (returned, 'sentinel'):
            wrapped = self.utils._tracking_wrapper(
                lambda recv, name, value=value, **kwargs: value,
                'cluster', lambda recv: [],
            )
            self.assertIs(wrapped(mgr, 'my-cluster'), value)

        self.assertEqual(self.utils.tracked_labels(), [])

    def test_a_real_receiver_still_tracks_what_it_returns(self):
        """The other side of the check above: an unrecognisable return value
        from a real manager is still swept, because a cluster left running
        costs money and a redundant terminate costs one round trip."""
        mgr = SimpleNamespace(_get=object(), _post=object(), _delete=object())
        returned = self._deployment('cl-1')
        returned._manager = None

        wrapped = self.utils._tracking_wrapper(
            lambda recv, name, **kwargs: returned, 'cluster', lambda recv: [],
        )
        wrapped(mgr, 'cl-1')
        self.assertEqual(self.utils.tracked_labels(), ["Deployment 'cl-1'"])

    def test_a_mocked_receiver_is_not_searched_for_orphans(self):
        """The unit tests drive these creators with patched transports; a
        failure there names nothing real to recover."""
        def boom(recv, name, **kwargs):
            raise ManagementError(msg='boom')

        wrapped = self.utils._tracking_wrapper(
            boom, 'cluster', lambda recv: recv.clusters,
        )
        with self.assertRaises(ManagementError):
            wrapped(MagicMock(), 'cl-1')
        self.assertEqual(self.utils._tracked, [])

    def test_a_real_manager_with_a_patched_post_is_not_searched_either(self):
        """The receiver of these creators is the manager itself, which has no
        ``_manager``. Checking for one made a real manager with a patched
        ``_post`` -- what the unit tests drive -- read as live, so the recovery
        fired an actual management API GET from a unit test."""
        receiver = SimpleNamespace(_get=object(), _post=MagicMock())

        def boom(recv, name, **kwargs):
            raise ManagementError(msg='boom')

        def finder(recv):
            raise AssertionError('recovery called the live API')

        wrapped = self.utils._tracking_wrapper(boom, 'cluster', finder)
        with self.assertRaises(ManagementError):
            wrapped(receiver, 'cl-1')
        self.assertEqual(self.utils._tracked, [])
        self.assertEqual(self.utils._in_flight, [])

    def test_a_create_killed_mid_wait_is_recovered_by_the_sweep(self):
        """The second half of the same leak: a create that has POSTed and is
        blocked in ``wait_on_active`` is not tracked yet, and the wrapper's
        ``except`` never runs if the process is killed. So the shutdown sweep
        recovers whatever is in flight before it walks ``_tracked``."""
        orphan = self._deployment('cl-1')
        receiver = SimpleNamespace(clusters=[orphan])

        def create_then_wait(recv, name, **kwargs):
            # Stands in for the sweep firing from SIGTERM/atexit while the
            # wait is still blocked.
            self.assertEqual(len(self.utils._in_flight), 1)
            self.utils.recover_in_flight()
            raise AssertionError('the process would have been killed here')

        wrapped = self.utils._tracking_wrapper(
            create_then_wait, 'cluster', lambda recv: recv.clusters,
        )
        with self.assertRaises(AssertionError):
            wrapped(receiver, 'cl-1', wait_on_active=True)

        # Recovered once, not twice: the entry is popped as it is drained, so
        # the wrapper's own except finds nothing left to recover.
        self.assertEqual(
            self.utils.tracked_labels(),
            ["Deployment 'cl-1' (left behind by a failed create)"],
        )
        self.assertEqual(self.utils._in_flight, [])

    def test_a_finished_create_leaves_nothing_in_flight(self):
        receiver = SimpleNamespace(clusters=[])

        def finder(recv):
            return recv.clusters

        made = self._deployment('cl-1')
        wrapped = self.utils._tracking_wrapper(
            lambda recv, name, **kwargs: made, 'cluster', finder,
        )
        self.assertIs(wrapped(receiver, 'cl-1'), made)
        self.assertEqual(self.utils._in_flight, [])
        self.assertEqual(len(self.utils._tracked), 1)

        def boom(recv, name, **kwargs):
            raise ManagementError(msg='boom')

        receiver.clusters = [self._deployment('cl-2')]
        with self.assertRaises(ManagementError):
            self.utils._tracking_wrapper(
                boom, 'cluster', finder,
            )(receiver, 'cl-2')
        self.assertEqual(self.utils._in_flight, [])
        # The orphan was recovered once, not once per code path.
        self.assertEqual(len(self.utils._tracked), 2)

    def test_a_mocked_receiver_never_enters_the_in_flight_list(self):
        def create(recv, name, **kwargs):
            raise AssertionError(str(self.utils._in_flight))

        wrapped = self.utils._tracking_wrapper(
            create, 'cluster', lambda recv: recv.clusters,
        )
        with self.assertRaises(AssertionError) as raised:
            wrapped(MagicMock(), 'cl-1')
        self.assertEqual(str(raised.exception), '[]')
        self.assertEqual(self.utils._in_flight, [])

    def test_recovering_nothing_in_flight_is_a_no_op(self):
        self.utils.recover_in_flight()
        self.assertEqual(self.utils._tracked, [])

    def test_orphan_recovery_matches_on_the_name_keyword_too(self):
        orphan = self._deployment('cl-1')
        receiver = SimpleNamespace(clusters=[self._deployment('other'), orphan])
        self.utils._recover_orphan(
            receiver, lambda recv: recv.clusters, (), {'name': 'cl-1'},
        )
        self.assertEqual(len(self.utils._tracked), 1)
        self.assertIs(self.utils._tracked[0][2], orphan)

    def test_orphan_recovery_never_raises(self):
        """It runs while the caller's exception is propagating, so a failure
        here must not replace the real error."""
        receiver = SimpleNamespace()
        self.utils._recover_orphan(
            receiver,
            lambda recv: recv.clusters,  # AttributeError
            ('cl-1',),
            {},
        )
        self.assertEqual(self.utils._tracked, [])

    def test_untrack_drops_a_deployment(self):
        obj = self.utils.track(self._deployment('a'))
        self.utils.untrack(obj)
        self.assertEqual(self.utils.cleanup_tracked(), [])
        self.assertIsNone(obj.terminated_with)

    def test_a_mocked_creation_is_recognised_by_its_manager(self):
        mgr = MagicMock()
        self.assertTrue(self.utils._creator_is_mocked(mgr))

        real = SimpleNamespace(_get=object(), _post=object(), _delete=object())
        self.assertFalse(self.utils._creator_is_mocked(real))

        # A created object reaches its manager through _manager.
        self.assertTrue(
            self.utils._creator_is_mocked(SimpleNamespace(_manager=mgr)),
        )
        self.assertFalse(
            self.utils._creator_is_mocked(SimpleNamespace(_manager=real)),
        )

    def test_a_deployment_without_a_manager_is_still_tracked(self):
        # The fail-safe bias: an unrecognisable object counts as real. A fake
        # deployment swept is a round trip and a warning, whereas a real one
        # skipped is a cluster left running and billing.
        obj = self._deployment('a')
        obj._manager = None
        self.utils.track(obj)
        self.assertEqual(len(self.utils._tracked), 1)

        self.assertFalse(self.utils._is_mocked(obj))
        self.assertFalse(self.utils._is_mocked(SimpleNamespace(name='b')))

    def test_every_creation_method_is_wrapped(self):
        # A rename that silently stops tracking is how a cluster leaks.
        import importlib

        self.utils.install_deployment_tracking()
        for module_name, class_name, method_name, _, _ in self.utils._CREATORS:
            klass = getattr(importlib.import_module(module_name), class_name)
            method = getattr(klass, method_name, None)
            self.assertIsNotNone(
                method, f'{class_name}.{method_name} no longer exists',
            )
            self.assertTrue(
                hasattr(method, '__wrapped__'),
                f'{class_name}.{method_name} is not tracked',
            )

    def test_every_creator_takes_name_first_and_has_a_finder(self):
        """``_recover_orphan`` reads the name from the first argument and
        searches the collection the finder returns, so both have to hold."""
        import importlib
        import inspect

        from singlestoredb.tests import cleanup_deployments

        for module_name, class_name, method_name, kind, finder in \
                self.utils._CREATORS:
            klass = getattr(importlib.import_module(module_name), class_name)
            method = getattr(klass, method_name)
            params = list(
                inspect.signature(
                    getattr(method, '__wrapped__', method),
                ).parameters,
            )
            self.assertEqual(
                params[:2], ['self', 'name'],
                f'{class_name}.{method_name} no longer takes name first, so '
                'a failed create would not be recoverable',
            )
            self.assertTrue(callable(finder))
            # The ledger's `pending` record carries this kind, and the reaper
            # resolves it through cleanup_deployments.LEDGER_KINDS. A kind
            # neither side knows would make a cancelled create unreapable,
            # which is the whole point of the ledger.
            self.assertIn(
                kind, set(self.utils._KIND_BY_CLASS.values()),
                f'{class_name}.{method_name} has an unknown ledger kind',
            )
            self.assertIn(kind, cleanup_deployments.LEDGER_KINDS)


class TestDeploymentLedger(TestDeploymentTracking):
    """
    The on-disk ledger that makes a killed run's deployments reapable.

    Inherits ``TestDeploymentTracking``'s fixtures for the module globals and
    the non-Mock deployment stand-in. It re-runs that class's tests with a
    ledger configured, which is worth having: those tests all use the default
    ``Deployment`` classname, so they also pin that a ledger being configured
    changes nothing about the in-memory behaviour.
    """

    def setUp(self):
        super().setUp()
        self.dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.dir, True)
        self.ledger = os.path.join(self.dir, 'deployments.jsonl')
        patcher = patch.dict(
            os.environ, {self.utils.LEDGER_ENV_VAR: self.ledger},
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def records(self):
        """Every record in the ledger, in the order it was written."""
        if not os.path.exists(self.ledger):
            return []
        with open(self.ledger) as file:
            return [json.loads(x) for x in file if x.strip()]

    def events(self):
        return [(x['event'], x.get('kind'), x.get('name')) for x in
                self.records()]

    #
    # Writing
    #

    def test_no_ledger_is_written_without_the_environment_variable(self):
        """Opt-in is the whole contract: a local run must behave exactly as it
        did before, with no file appearing anywhere."""
        with patch.dict(os.environ, {}, clear=False):
            del os.environ[self.utils.LEDGER_ENV_VAR]
            self.utils.track(self._deployment('cl-1', classname='Cluster'))
        self.assertFalse(os.path.exists(self.ledger))

    def test_a_mocked_creation_writes_nothing(self):
        """The unit tests drive the creators with patched transports. Recording
        those would have the reaper chasing ids that never existed, and -- worse
        -- exit non-zero on every one it could not resolve."""
        wrapped = self.utils._tracking_wrapper(
            lambda recv, name, **kwargs: MagicMock(),
            'cluster', lambda recv: [],
        )
        wrapped(MagicMock(), 'cl-1')
        self.utils.track(MagicMock())
        self.assertEqual(self.records(), [])

    def test_a_real_creation_writes_pending_then_live(self):
        """In that order, and with the pending written before the creator is
        even called: the window this closes is the one where the POST has landed
        and nothing in the process knows an id yet."""
        made = self._deployment('cl-1', classname='Cluster')
        seen = []

        def create(recv, name, **kwargs):
            # What the ledger holds *during* the wait, which is where the
            # cancelled job died.
            seen.extend(self.events())
            return made

        receiver = SimpleNamespace(
            _get=object(), _post=object(), _delete=object(),
        )
        wrapped = self.utils._tracking_wrapper(
            create, 'cluster', lambda recv: [],
        )
        wrapped(receiver, 'cl-1')

        self.assertEqual(seen, [('pending', 'cluster', 'cl-1')])
        self.assertEqual(
            self.events(), [
                ('pending', 'cluster', 'cl-1'),
                ('live', 'cluster', 'cl-1'),
            ],
        )
        self.assertEqual(self.records()[1]['id'], 'cl-1')

    def test_the_pending_name_comes_from_the_keyword_too(self):
        receiver = SimpleNamespace(
            _get=object(), _post=object(), _delete=object(),
        )
        self.utils._tracking_wrapper(
            lambda recv, name, **kwargs: None, 'workspace_group',
            lambda recv: [],
        )(receiver, name='wg-1')
        self.assertEqual(
            self.events(), [('pending', 'workspace_group', 'wg-1')],
        )

    def test_a_create_that_dies_mid_wait_leaves_pending_with_no_gone(self):
        """The reported failure, as the ledger sees it. The creator raises and
        the orphan is not in the listing yet, so nothing else is ever written --
        and that lone `pending` is what the reaper resolves by name."""
        receiver = SimpleNamespace(
            _get=object(), _post=object(), _delete=object(), clusters=[],
        )

        def create_then_fail_waiting(recv, name, **kwargs):
            raise ManagementError(msg=f'Exceeded waiting time for {name}')

        wrapped = self.utils._tracking_wrapper(
            create_then_fail_waiting, 'cluster', lambda recv: recv.clusters,
        )
        with self.assertRaises(ManagementError):
            wrapped(receiver, 'a-fusion-cluster-1f2e', wait_on_active=True)

        self.assertEqual(
            self.events(),
            [('pending', 'cluster', 'a-fusion-cluster-1f2e')],
        )

    def test_a_recovered_orphan_is_recorded_live(self):
        """``_recover_orphan`` goes through ``track()``, so the id it digs out
        of the listing reaches the ledger and the reaper can use the point
        lookup instead of searching by name."""
        orphan = self._deployment('cl-1', classname='Cluster')
        receiver = SimpleNamespace(
            _get=object(), _post=object(), _delete=object(),
            clusters=[orphan],
        )

        def boom(recv, name, **kwargs):
            raise ManagementError(msg='boom')

        with self.assertRaises(ManagementError):
            self.utils._tracking_wrapper(
                boom, 'cluster', lambda recv: recv.clusters,
            )(receiver, 'cl-1')

        self.assertEqual(
            self.events(), [
                ('pending', 'cluster', 'cl-1'),
                ('live', 'cluster', 'cl-1'),
            ],
        )

    def test_a_successful_sweep_appends_gone(self):
        obj = self._deployment('cl-1', classname='Cluster')
        self.utils.track(obj)
        self.assertEqual(len(self.utils.cleanup_tracked()), 1)
        self.assertEqual(
            self.events(), [
                ('live', 'cluster', 'cl-1'),
                ('gone', 'cluster', 'cl-1'),
            ],
        )

    def test_a_deployment_already_gone_is_recorded_gone(self):
        """A test that terminated in its own teardown: the sweep finds it gone
        rather than terminating it, and the record still has to be closed or
        the reaper spends a lookup on it and reports it unresolved."""
        obj = self._deployment(
            'cl-1', terminated_at='now', classname='Cluster',
        )
        self.utils.track(obj)
        self.assertEqual(self.utils.cleanup_tracked(), [])
        self.assertEqual(
            [x['event'] for x in self.records()], ['live', 'gone'],
        )

    def test_a_failed_terminate_writes_no_gone(self):
        """The deployment is still live and still billing, so the reaper must
        still see it."""
        obj = self._deployment('cl-1', classname='Cluster')

        def boom(force=False):
            raise ManagementError(errno=500, msg='boom')

        obj.terminate = boom
        self.utils.track(obj)
        self.assertEqual(self.utils.cleanup_tracked(), [])
        self.assertEqual([x['event'] for x in self.records()], ['live'])

    def test_untrack_records_gone_only_for_something_tracked(self):
        obj = self._deployment('cl-1', classname='Cluster')
        self.utils.untrack(obj)
        self.assertEqual(self.records(), [])

        self.utils.track(obj)
        self.utils.untrack(obj)
        self.assertEqual([x['event'] for x in self.records()], ['live', 'gone'])

    def test_a_write_failure_is_logged_and_not_raised(self):
        """This sits on the creation path of every management test: an
        unwritable ledger must cost a warning, not a failed test run."""
        with patch.dict(
            os.environ,
            {self.utils.LEDGER_ENV_VAR: os.path.join(self.dir, 'no', 'such')},
        ):
            with self.assertLogs(self.utils.logger, 'WARNING') as logs:
                self.utils.track(self._deployment('cl-1', classname='Cluster'))
        self.assertIn('deployment ledger', logs.output[0])

    #
    # Folding
    #

    def fold(self, *lines):
        from singlestoredb.tests import cleanup_deployments
        return cleanup_deployments.fold_ledger(lines)

    def test_folding_keeps_only_what_is_not_gone(self):
        kept = self.fold(
            json.dumps(dict(event='pending', kind='cluster', name='cl-1')),
            json.dumps(
                dict(event='live', kind='cluster', name='cl-1', id='id-1'),
            ),
            json.dumps(
                dict(event='gone', kind='cluster', name='cl-1', id='id-1'),
            ),
            # Created and never terminated.
            json.dumps(dict(event='pending', kind='cluster', name='cl-2')),
            json.dumps(
                dict(event='live', kind='cluster', name='cl-2', id='id-2'),
            ),
            # Interrupted before it returned: pending only.
            json.dumps(dict(event='pending', kind='cluster', name='cl-3')),
        )
        self.assertEqual(
            [(x['event'], x.get('id'), x['name']) for x in kept],
            [('live', 'id-2', 'cl-2'), ('pending', None, 'cl-3')],
        )

    def test_a_live_record_retires_its_pending(self):
        """Otherwise the reaper resolves the same cluster twice -- once by id
        and once by name -- and reports two."""
        kept = self.fold(
            json.dumps(dict(event='pending', kind='cluster', name='cl-1')),
            json.dumps(
                dict(event='live', kind='cluster', name='cl-1', id='id-1'),
            ),
        )
        self.assertEqual(len(kept), 1)
        self.assertEqual(kept[0]['id'], 'id-1')

    def test_gone_cancels_a_pending_that_never_went_live(self):
        kept = self.fold(
            json.dumps(dict(event='pending', kind='cluster', name='cl-1')),
            json.dumps(dict(event='gone', kind='cluster', name='cl-1')),
        )
        self.assertEqual(kept, [])

    def test_the_same_name_in_two_kinds_is_two_deployments(self):
        """`cl-test-abc` as a cluster and as a workspace are different things,
        and a `gone` for one must not clear the other."""
        kept = self.fold(
            json.dumps(dict(event='pending', kind='cluster', name='x')),
            json.dumps(dict(event='pending', kind='workspace', name='x')),
            json.dumps(dict(event='gone', kind='cluster', name='x')),
        )
        self.assertEqual([x['kind'] for x in kept], ['workspace'])

    def test_a_malformed_line_is_skipped_rather_than_fatal(self):
        """A truncated last line -- a process killed between the write and the
        fsync -- must not cost the reaper every other record."""
        kept = self.fold(
            json.dumps(dict(event='pending', kind='cluster', name='cl-1')),
            '{"event": "pending", "kin',
            '',
            '[]',
        )
        self.assertEqual([x['name'] for x in kept], ['cl-1'])

    def test_reading_reverses_into_newest_first(self):
        """A workspace has to be terminated before the group that holds it, the
        same ordering ``cleanup_tracked`` uses."""
        from singlestoredb.tests import cleanup_deployments
        with open(self.ledger, 'w') as file:
            for kind, name in (
                ('workspace_group', 'wg-1'), ('workspace', 'ws-1'),
            ):
                file.write(
                    json.dumps(dict(event='pending', kind=kind, name=name))
                    + '\n',
                )
        self.assertEqual(
            [x['name'] for x in cleanup_deployments.read_ledger(self.ledger)],
            ['ws-1', 'wg-1'],
        )

    #
    # Resolving, with the management API stubbed out
    #

    def stub_managers(self, **attrs):
        """Patch the reaper's manager lookup with a namespace."""
        from singlestoredb.tests import cleanup_deployments
        mgr = SimpleNamespace(**attrs)
        patcher = patch.object(
            cleanup_deployments, '_manager', lambda version: mgr,
        )
        patcher.start()
        self.addCleanup(patcher.stop)
        return cleanup_deployments, mgr

    def write_ledger(self, *records):
        with open(self.ledger, 'w') as file:
            for record in records:
                file.write(json.dumps(record) + '\n')

    def test_an_id_that_404s_is_treated_as_already_gone(self):
        """The common case by far: the ledger records every creation, and a run
        that ended normally terminated all of them. A clean sweep must exit 0
        and terminate nothing."""
        def get_cluster(ident):
            raise ManagementError(errno=404, msg='not found')

        mod, _ = self.stub_managers(get_cluster=get_cluster)
        self.write_ledger(
            dict(event='live', kind='cluster', name='cl-1', id='id-1'),
        )
        found, gone, unresolved = mod.find_ledger_leftovers(self.ledger)
        self.assertEqual(found, [])
        self.assertEqual(len(gone), 1)
        self.assertEqual(unresolved, [])
        self.assertEqual(mod.main(['--ledger', self.ledger, '--yes']), 0)

    def test_a_live_id_is_resolved_and_terminated(self):
        obj = self._deployment('cl-1', classname='Cluster')
        mod, _ = self.stub_managers(get_cluster=lambda ident: obj)
        self.write_ledger(
            dict(event='live', kind='cluster', name='cl-1', id='id-1'),
        )
        self.assertEqual(mod.main(['--ledger', self.ledger, '--yes']), 0)
        self.assertTrue(obj.terminated_with)

    def test_a_dry_run_terminates_nothing(self):
        obj = self._deployment('cl-1', classname='Cluster')
        mod, _ = self.stub_managers(get_cluster=lambda ident: obj)
        self.write_ledger(
            dict(event='live', kind='cluster', name='cl-1', id='id-1'),
        )
        self.assertEqual(mod.main(['--ledger', self.ledger]), 0)
        self.assertIsNone(obj.terminated_with)

    def test_a_pending_record_is_resolved_by_name_over_the_listing(self):
        """No id was ever returned, so the listing is the only handle -- the
        same match ``_recover_orphan`` makes, and the case a cancelled
        ``wait_on_active`` leaves."""
        wanted = self._deployment('a-fusion-cluster-1f2e', classname='Cluster')
        other = self._deployment('someone-elses', classname='Cluster')
        mod, _ = self.stub_managers(clusters=[other, wanted])
        self.write_ledger(
            dict(event='pending', kind='cluster', name='a-fusion-cluster-1f2e'),
        )
        self.assertEqual(mod.main(['--ledger', self.ledger, '--yes']), 0)
        self.assertTrue(wanted.terminated_with)
        self.assertIsNone(other.terminated_with)

    def test_a_pending_name_absent_from_the_listing_is_gone(self):
        """The POST never landed, so there is nothing to reap and nothing to
        complain about."""
        mod, _ = self.stub_managers(clusters=[])
        self.write_ledger(dict(event='pending', kind='cluster', name='cl-1'))
        found, gone, unresolved = mod.find_ledger_leftovers(self.ledger)
        self.assertEqual((found, len(gone), unresolved), ([], 1, []))

    def test_an_already_terminated_deployment_is_not_terminated_again(self):
        obj = self._deployment(
            'cl-1', terminated_at='now', classname='Cluster',
        )
        mod, _ = self.stub_managers(get_cluster=lambda ident: obj)
        self.write_ledger(
            dict(event='live', kind='cluster', name='cl-1', id='id-1'),
        )
        self.assertEqual(mod.main(['--ledger', self.ledger, '--yes']), 0)
        self.assertIsNone(obj.terminated_with)

    def test_a_lookup_failure_that_is_not_a_404_exits_non_zero(self):
        """"Could not tell" and "not there" must not read the same when the
        difference is a cluster billing."""
        def get_cluster(ident):
            raise ManagementError(errno=500, msg='gateway sulked')

        mod, _ = self.stub_managers(get_cluster=get_cluster)
        self.write_ledger(
            dict(event='live', kind='cluster', name='cl-1', id='id-1'),
        )
        found, gone, unresolved = mod.find_ledger_leftovers(self.ledger)
        self.assertEqual((found, gone), ([], []))
        self.assertEqual(len(unresolved), 1)
        self.assertEqual(mod.main(['--ledger', self.ledger, '--yes']), 1)

    def test_an_unknown_kind_is_reported_rather_than_skipped(self):
        mod, _ = self.stub_managers()
        self.write_ledger(dict(event='live', kind='mystery', name='x', id='1'))
        _, _, unresolved = mod.find_ledger_leftovers(self.ledger)
        self.assertEqual(len(unresolved), 1)
        self.assertIn('unknown kind', unresolved[0])

    def test_a_missing_ledger_is_not_an_error(self):
        """The variable is set for a whole job, including steps whose tests
        create nothing. Failing there would turn those runs red."""
        from singlestoredb.tests import cleanup_deployments
        missing = os.path.join(self.dir, 'never-written.jsonl')
        self.assertEqual(cleanup_deployments.read_ledger(missing), [])
        self.assertEqual(
            cleanup_deployments.main(['--ledger', missing, '--yes']), 0,
        )

    def test_the_sweep_waits_out_a_provision_rather_than_the_class_budget(self):
        """The whole point of the ledger is a job cancelled inside
        ``wait_on_active``, whose cluster is minutes from deletable. Borrowing
        ``utils.TERMINATE_RETRY_TIMEOUT`` -- short so the per-class sweep cannot
        stall the suite -- would exhaust the budget and leave it billing, and
        nothing runs after this to try again."""
        from singlestoredb.tests import utils
        obj = self._deployment('cl-1', classname='Cluster')
        mod, _ = self.stub_managers(get_cluster=lambda ident: obj)
        self.write_ledger(
            dict(event='live', kind='cluster', name='cl-1', id='id-1'),
        )

        calls = []
        patcher = patch.object(
            utils, 'terminate',
            lambda obj, **kwargs: calls.append(kwargs),
        )
        patcher.start()
        self.addCleanup(patcher.stop)

        self.assertEqual(mod.main(['--ledger', self.ledger, '--yes']), 0)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]['timeout'], mod.TERMINATE_TIMEOUT)
        self.assertGreater(calls[0]['timeout'], utils.TERMINATE_RETRY_TIMEOUT)

    def test_ledger_mode_refuses_the_guards_it_replaces(self):
        """Silently ignoring --older-than would read as a safety guard that is
        not there."""
        from singlestoredb.tests import cleanup_deployments
        for extra in (
            ['--older-than', '0'], ['--any-name'],
            ['--kind', 'cluster'], ['--show-unmatched'],
        ):
            with self.assertRaises(SystemExit):
                cleanup_deployments.main(
                    ['--ledger', self.ledger] + extra,
                )


class TestTerminateRetry(unittest.TestCase):
    """
    ``utils.terminate()``'s bounded retry for a deployment the API will not
    delete yet.

    A deployment killed mid-provision is PENDING/TRANSITIONING and the DELETE
    comes back 400 or 409. Nothing retried that: Manager.RETRY_STATUSES is
    {429, 500, 502, 503, 504}, so the per-class sweep warned, the session-end
    sweep tried once more and the cluster stayed up.
    """

    def setUp(self):
        from singlestoredb.tests import utils
        self.utils = utils
        self.slept = []
        # A fake clock, not just a stubbed sleep: the retry budget is measured
        # with time.monotonic(), so a sleep that does not advance it makes the
        # deadline unreachable and the loop only ends when the stub runs out of
        # refusals. That is the opposite of what the budget test asserts.
        self.now = 0.0

        def sleep(seconds):
            self.slept.append(seconds)
            self.now += seconds

        for name, value in (
            ('sleep', sleep), ('monotonic', lambda: self.now),
        ):
            patcher = patch(f'time.{name}', value)
            patcher.start()
            self.addCleanup(patcher.stop)

    def _refuser(self, *errnos):
        """A deployment whose terminate raises these in turn, then succeeds."""
        class Deployment:
            attempts = 0
            terminated_with = None

            def terminate(inner, force=False):
                inner.attempts += 1
                if inner.attempts <= len(errnos):
                    raise ManagementError(
                        errno=errnos[inner.attempts - 1],
                        msg='still provisioning',
                    )
                inner.terminated_with = force

        return Deployment()

    def test_a_400_is_retried_until_it_succeeds(self):
        obj = self._refuser(400, 409)
        self.utils.terminate(obj)
        self.assertEqual(obj.attempts, 3)
        self.assertTrue(obj.terminated_with)
        self.assertEqual(self.slept, [15.0, 15.0])

    def test_a_404_is_not_retried(self):
        """It is already gone; retrying would burn the whole budget waiting for
        something that is not coming back."""
        obj = self._refuser(404)
        with self.assertRaises(ManagementError):
            self.utils.terminate(obj)
        self.assertEqual(obj.attempts, 1)
        self.assertEqual(self.slept, [])

    def test_a_5xx_is_not_retried_here(self):
        """The transport already retried it; another round trip from this layer
        is not what fixes it."""
        obj = self._refuser(503)
        with self.assertRaises(ManagementError):
            self.utils.terminate(obj)
        self.assertEqual(obj.attempts, 1)

    def test_the_budget_is_bounded_and_the_error_is_re_raised(self):
        """Raising is what keeps the deployment in ``_tracked``, so the
        end-of-session sweep gets another go at it."""
        obj = self._refuser(*([409] * 100))
        with self.assertRaises(ManagementError):
            self.utils.terminate(obj, timeout=45.0, interval=15.0)
        self.assertEqual(obj.attempts, 3)
        self.assertEqual(self.slept, [15.0, 15.0])

    def test_a_starter_kind_is_terminated_without_force(self):
        """StarterWorkspace.terminate / StarterCluster.terminate take no
        arguments at all."""
        class Starter:
            called = False

            def terminate(inner):
                inner.called = True

        obj = Starter()
        self.utils.terminate(obj)
        self.assertTrue(obj.called)

    def test_force_is_passed_when_the_signature_accepts_it(self):
        """``force`` is what makes a workspace group with live workspaces in it
        go away, so this is not cosmetic."""
        seen = []

        class Group:
            def terminate(inner, force=False):
                seen.append(force)

        self.utils.terminate(Group())
        self.assertEqual(seen, [True])

    def test_a_type_error_from_inside_terminate_is_not_a_second_delete(self):
        """The signature is inspected rather than discovered by catching
        TypeError from the call. The old ``except TypeError`` also caught one
        raised *inside* a terminate that did accept force, and retried without
        it -- two DELETEs, the second unforced, which is exactly the shape that
        leaves a workspace group behind."""
        calls = []

        class Group:
            def terminate(inner, force=False):
                calls.append(force)
                raise TypeError('something inside went wrong')

        with self.assertRaises(TypeError):
            self.utils.terminate(Group())
        self.assertEqual(calls, [True])


class TestSharedClusterPool(unittest.TestCase):
    """
    The pool in ``tests/utils.py`` that keeps the Stage and Job suites from
    deploying a cluster apiece.
    """

    def setUp(self):
        from singlestoredb.tests import utils
        self.utils = utils

        # Redirected before anything can create a cluster: the stand-in
        # manager's create_cluster calls the real utils.track, which ledgers,
        # and _pool_id is the live one, so under CI these mocked units used to
        # append `id-of-cl-test-shared-N-<real pool id>` to the job's real
        # ledger. The cleanup step then could not resolve those ids and exited
        # non-zero on every run, burying any genuine unresolved record.
        tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmp, True)
        patcher = patch.dict(
            os.environ,
            {utils.LEDGER_ENV_VAR: os.path.join(tmp, 'deployments.jsonl')},
        )
        patcher.start()
        self.addCleanup(patcher.stop)

        self.saved_pool = list(utils._pool)
        self.saved_skip = utils._pool_skip
        self.saved_tracked = list(utils._tracked)
        self.saved_owner = utils.get_owner()
        utils._pool.clear()
        utils._pool_skip = None
        utils._tracked.clear()
        self.addCleanup(self._restore)

        self.created = []

    def _restore(self):
        self.utils._pool[:] = self.saved_pool
        self.utils._pool_skip = self.saved_skip
        self.utils._tracked[:] = self.saved_tracked
        self.utils.set_owner(self.saved_owner)

    def _manager(self, regions=('US East 1',), projects=('STANDARD',)):
        """
        A stand-in cluster manager.

        Not a ``Mock``: ``utils.track`` skips anything that came out of a
        mocked manager, and the owner a pool cluster is tracked under is the
        whole point of the pool. ``create_cluster`` calls ``track`` itself
        because that is what ``install_deployment_tracking`` does to the real
        method.
        """
        created = self.created
        utils = self.utils

        class Region:
            def __init__(self, name):
                self.name = name
                self.region_name = name

        class Project:
            def __init__(self, edition):
                self.edition = edition
                self.id = f'project-{edition}'

        class Cluster:
            def __init__(self, name, kwargs):
                self.name = name
                self.id = f'id-of-{name}'
                self.terminated_at = None
                self.state = 'ACTIVE'
                self.kwargs = kwargs
                self._manager = object()

            def refresh(self):
                return self

            def terminate(self, force=False):
                pass

        # Bound outside the class body: a comprehension there cannot see the
        # enclosing function's names.
        region_list = [Region(x) for x in regions]
        project_list = [Project(x) for x in projects]

        class Manager:
            regions = region_list
            projects = project_list

            def create_cluster(self, name, **kwargs):
                # The owner in force at creation time is what decides whether
                # the per-class sweep eats the pool.
                created.append((name, utils.get_owner(), kwargs))
                return utils.track(Cluster(name, kwargs))

        return Manager()

    def _patched(self, **kwargs):
        import singlestoredb as s2
        return patch.object(s2, 'manage_clusters', return_value=self._manager(**kwargs))

    def test_the_pool_is_built_once(self):
        with self._patched():
            first = self.utils.shared_clusters(2)
            second = self.utils.shared_clusters(2)

        self.assertEqual([x.id for x in first], [x.id for x in second])
        self.assertEqual(len(self.created), 2)

    def test_the_pool_grows_to_the_largest_request(self):
        with self._patched():
            one = self.utils.shared_clusters(1)
            two = self.utils.shared_clusters(2)

        # The second call adds a cluster rather than replacing the first.
        self.assertEqual(len(self.created), 2)
        self.assertEqual(two[0].id, one[0].id)
        self.assertEqual(len(two), 2)

    def test_pool_clusters_are_tracked_under_the_empty_owner(self):
        # A pool cluster tracked under the class that asked for it first would
        # be terminated by conftest's per-class sweep the moment the run moved
        # on -- so the pool would die after one consumer.
        self.utils.set_owner('mod.ClassA')
        with self._patched():
            self.utils.shared_clusters(2)

        self.assertEqual([x[1] for x in self.created], ['', ''])
        self.assertEqual([x[0] for x in self.utils._tracked], ['', ''])

        # The owner the caller was running under is put back...
        self.assertEqual(self.utils.get_owner(), 'mod.ClassA')
        # ... and a sweep of that class leaves the pool alone.
        self.assertEqual(self.utils.cleanup_tracked('mod.ClassA'), [])
        self.assertEqual(len(self.utils._tracked), 2)
        # Only the end-of-session sweep, which matches every owner, takes it.
        self.assertEqual(len(self.utils.cleanup_tracked()), 2)

    def test_pool_names_are_swept_by_the_maintenance_script(self):
        from singlestoredb.tests import cleanup_deployments

        with self._patched():
            self.utils.shared_clusters(1)

        self.assertTrue(
            cleanup_deployments.is_test_deployment(self.created[0][0]),
            self.created[0][0],
        )
        # POST /v2/clusters caps a name at 32 characters.
        self.assertLessEqual(len(self.created[0][0]), 32)

    def test_a_pool_cluster_is_deployed_where_its_consumers_deployed_theirs(self):
        with self._patched():
            self.utils.shared_clusters(1)

        _, _, kwargs = self.created[0]
        self.assertEqual(kwargs['size'], 'S-00')
        self.assertEqual(kwargs['project'], 'project-STANDARD')
        self.assertEqual(kwargs['firewall_ranges'], ['0.0.0.0/0'])
        self.assertTrue(kwargs['wait_on_active'])

    def test_no_us_region_skips_rather_than_failing(self):
        with self._patched(regions=('EU West 1',)):
            with self.assertRaises(unittest.SkipTest):
                self.utils.shared_clusters(1)

            # Cached: the next class to ask skips without repeating the
            # lookups, and nothing was deployed.
            with self.assertRaises(unittest.SkipTest):
                self.utils.shared_clusters(1)

        self.assertEqual(self.created, [])

    def test_no_standard_project_skips_rather_than_failing(self):
        with self._patched(projects=('SHARED',)):
            with self.assertRaises(unittest.SkipTest) as cm:
                self.utils.shared_clusters(1)
        self.assertIn('SINGLESTOREDB_TEST_PROJECT', str(cm.exception))
        self.assertEqual(self.created, [])

    def test_an_explicit_project_does_not_need_a_standard_one(self):
        with patch.dict(
            os.environ, {'SINGLESTOREDB_TEST_PROJECT': 'chosen-project'},
        ):
            with self._patched(projects=('SHARED',)):
                self.utils.shared_clusters(1)

        self.assertEqual(self.created[0][2]['project'], 'chosen-project')


class TestClearStage(unittest.TestCase):
    """
    Emptying a pooled deployment's stage, which is what lets a class that
    asserts exact stage listings borrow a cluster another class has used.
    """

    def setUp(self):
        from singlestoredb.tests import utils
        self.utils = utils

    def _deployment(self, entries, failing=()):
        removed = []

        class Obj:
            def __init__(self, path, type):
                self.path = path
                self.type = type

        class Stage:
            def listdir(self, path='/', *, recursive=False, return_objects=False):
                assert return_objects
                return [Obj(p, t) for p, t in entries]

            def remove(self, path):
                if path in failing:
                    raise OSError('nope')
                removed.append(('remove', path))

            def removedirs(self, path):
                if path in failing:
                    raise OSError('nope')
                removed.append(('removedirs', path))

        class Deployment:
            stage = Stage()

        return Deployment(), removed

    def test_files_are_removed_and_folders_go_recursively(self):
        deployment, removed = self._deployment(
            [('test.sql', 'file'), ('data/', 'directory')],
        )
        self.utils.clear_stage(deployment)
        self.assertEqual(
            removed, [('remove', 'test.sql'), ('removedirs', 'data/')],
        )

    def test_a_path_that_will_not_go_does_not_stop_the_rest(self):
        deployment, removed = self._deployment(
            [('stuck.sql', 'file'), ('test.sql', 'file')],
            failing=('stuck.sql',),
        )
        self.utils.clear_stage(deployment)
        self.assertEqual(removed, [('remove', 'test.sql')])


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
            'cl-test-shared-0-deadbeef',
            'starter-ws-test-abcDEF',
            'starter-cl-test-abcDEF',
            'A Fusion Testing deadbeefdeadbeef',
            'C Fusion Testing deadbeef',
            'd-fusion-cluster-deadbeef',
            'jobs-fusion-deadbeef',
            'stage-fusion-2-deadbeef',
            'Create WG Test deadbeefdeadbeef',
            # The decimal id(self) that test named it with before, so groups
            # stranded by older runs are still reachable
            'Create WG Test 140234981234',
        ):
            self.assertTrue(self.mod.is_test_deployment(name), name)

    def test_retired_names_still_match(self):
        # main still creates these, and it carries no sweep at all, so they
        # keep arriving. Stranded deployments are billed whichever revision
        # made them.
        for name in (
            'Stage Fusion Testing 1 f00e4647f2c664fb',
            'Stage Fusion Testing 2 f00e4647f2c664fb',
            'Files Fusion Testing 1beb5e18ba06e135',
            # Unattributed -- no revision here generates it -- but present in
            # the organization and swept on the owner's say-so
            'Group 3fed3756',
            'Group 3fed37563fed3756',
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
            # The 'Group <hex>' pattern must not reach a name a person or the
            # portal produced -- that is someone's live workspace group
            'Group 1',
            'Group 2',
            'Group deadbeef prod',
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
            found, spared, self.unmatched = self.mod.find_leftovers(**kwargs)
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
        # Nothing runs after this tool, so its terminate budget has to cover a
        # full provision (~460s for an S-00 cluster reaching ACTIVE) rather than
        # the per-class budget, which is short on purpose.
        from singlestoredb.tests import utils
        self.assertGreater(
            self.mod.TERMINATE_TIMEOUT, utils.TERMINATE_RETRY_TIMEOUT,
        )
        self.assertGreaterEqual(self.mod.TERMINATE_TIMEOUT, 460)
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

    def test_an_unrecognized_name_is_reported_not_swept(self):
        # The failure this guards against is silent accumulation: a test that
        # names a deployment outside PATTERNS leaves strays the sweep reports
        # as 'none found'.
        names, _ = self._find([
            self._cluster('cl-test-known', hours=10),
            self._cluster('some-persons-cluster', hours=10),
        ])
        self.assertEqual(names, ['cl-test-known'])
        self.assertEqual(len(self.unmatched), 1)
        self.assertIn('some-persons-cluster', self.unmatched[0])
        self.assertIn('10.0h old', self.unmatched[0])

    def test_a_terminated_deployment_is_not_reported_as_unrecognized(self):
        obj = self._cluster('some-persons-cluster', hours=10)
        obj.terminated_at = 'yes'
        names, _ = self._find([obj])
        self.assertEqual(names, [])
        self.assertEqual(self.unmatched, [])

    def test_zero_sweeps_everything_matched(self):
        names, spared = self._find(
            [self._cluster('cl-test-brand-new', hours=0)], older_than=0,
        )
        self.assertEqual(names, ['cl-test-brand-new'])
        self.assertEqual(spared, [])

    def test_since_inverts_the_age_filter(self):
        # --since is for clearing out a recent session, so it must select what
        # the age guard rejects and reject what the age guard selects.
        recent = self._cluster('cl-test-today', hours=2)
        stale = self._cluster('cl-test-last-week', hours=24 * 7)

        names, spared = self._find(
            [recent, stale],
            since=datetime.datetime.now(tz=datetime.timezone.utc)
            - datetime.timedelta(hours=30),
        )

        self.assertEqual(names, ['cl-test-today'])
        self.assertEqual(len(spared), 1)
        self.assertIn('cl-test-last-week', spared[0])

    def test_since_ignores_older_than(self):
        # Both guards applying would leave a window nothing falls into, so a
        # caller passing --since gets the calendar cutoff alone.
        names, _ = self._find(
            [self._cluster('cl-test-today', hours=1)],
            older_than=self.mod.DEFAULT_MIN_AGE_HOURS,
            since=datetime.datetime.now(tz=datetime.timezone.utc)
            - datetime.timedelta(hours=30),
        )
        self.assertEqual(names, ['cl-test-today'])

    def test_since_still_spares_an_unreported_creation_time(self):
        # An unknown creation time cannot be shown to fall inside the window.
        names, spared = self._find(
            [self._cluster('cl-test-ageless')],
            since=datetime.datetime.now(tz=datetime.timezone.utc),
        )
        self.assertEqual(names, [])
        self.assertIn('cl-test-ageless', spared[0])

    def test_any_name_drops_the_name_gate(self):
        names, _ = self._find(
            [self._cluster('some-persons-cluster', hours=10)],
            older_than=2, any_name=True,
        )
        self.assertEqual(names, ['some-persons-cluster'])
        # Nothing is unrecognized once every name counts.
        self.assertEqual(self.unmatched, [])

    def test_kind_keeps_the_sweep_off_the_other_apis(self):
        # This is the only guard left when --any-name and --since are both
        # given, so a kind that was not asked for must not even be listed.
        import singlestoredb as s2

        clusters = MagicMock()
        clusters.clusters = [self._cluster('anything', hours=10)]
        clusters.starter_clusters = []
        workspaces = MagicMock()
        workspaces.workspace_groups = [self._cluster('a group', hours=10)]
        workspaces.starter_workspaces = [self._cluster('a starter', hours=10)]

        with patch.object(
            s2, 'manage_clusters', return_value=clusters,
        ) as clusters_call, patch.object(
            s2, 'manage_workspaces', return_value=workspaces,
        ):
            found, _, _ = self.mod.find_leftovers(
                older_than=2, any_name=True, kinds=['workspace-group'],
            )

        self.assertEqual([x[1].name for x in found], ['a group'])
        clusters_call.assert_not_called()

    def test_since_reads_a_day_as_local_midnight(self):
        for text, expected in (
            ('today', datetime.date.today()),
            (
                'yesterday',
                datetime.date.today() - datetime.timedelta(days=1),
            ),
            ('2026-09-01', datetime.date(2026, 9, 1)),
        ):
            cutoff = self.mod.parse_since(text)
            self.assertEqual(cutoff.date(), expected, text)
            self.assertEqual(cutoff.hour, 0, text)
            # Aware, or comparing it with a created_at raises.
            self.assertIsNotNone(cutoff.tzinfo, text)

    def test_a_since_that_is_not_a_date_is_rejected(self):
        import argparse
        with self.assertRaises(argparse.ArgumentTypeError):
            self.mod.parse_since('last tuesday')


class TestToDatetime(unittest.TestCase):
    """
    ``to_datetime`` has to read both timestamp shapes the API returns.

    Most fields come back as RFC 3339, but ``GET /v2/clusters/{id}`` reports
    ``expiresAt`` as a Go ``time.Time.String()`` rendering -- verified live:
    ``2026-09-17 14:42:41.445984 +0000 UTC`` against a ``createdAt`` of
    ``2026-09-17T13:42:41.493848Z`` on the same cluster. The trailing zone name
    is not ISO 8601, and parsing it used to fail into ``None``, which reads as
    "this cluster never expires".
    """

    def test_rfc_3339(self):
        out = to_datetime('2026-09-17T13:42:41.493848Z')
        self.assertEqual(out, datetime.datetime(2026, 9, 17, 13, 42, 41, 493848))

    def test_go_time_string(self):
        out = to_datetime('2026-09-17 14:42:41.445984 +0000 UTC')
        self.assertEqual(out, datetime.datetime(2026, 9, 17, 14, 42, 41, 445984))

    def test_offset_is_normalized_to_include_a_colon(self):
        # Go writes +0000; datetime.fromisoformat only accepts that spelling on
        # 3.11 and later, so the normalizer has to insert the colon itself. This
        # asserts on the normalized string rather than on a parsed result
        # because the parsed result is only wrong on 3.9 and 3.10, which would
        # leave the failure invisible to anyone testing on a newer interpreter.
        self.assertEqual(
            _normalize_datetime('2026-09-17 14:42:41.445984 +0000 UTC'),
            '2026-09-17 14:42:41.445984+00:00',
        )
        self.assertEqual(
            _normalize_datetime('2026-09-17 09:42:41 +0530 IST'),
            '2026-09-17 09:42:41+05:30',
        )
        # An offset that already carries a colon is left as it is.
        self.assertEqual(
            _normalize_datetime('2026-09-17 09:42:41 +05:30 IST'),
            '2026-09-17 09:42:41+05:30',
        )

    def test_rfc_3339_fraction_is_padded(self):
        # The API trims trailing zeros here too: a job's createdAt came back as
        # '2026-09-18T12:39:20.43888Z'. Only 3.11 and later read a fraction that
        # is neither 3 nor 6 digits, so before Z was recognized as an offset this
        # value skipped the padding and to_datetime_strict raised on 3.10.
        self.assertEqual(
            _normalize_datetime('2026-09-18T12:39:20.43888Z'),
            '2026-09-18T12:39:20.438880+00:00',
        )
        self.assertEqual(
            to_datetime_strict('2026-09-18T12:39:20.43888Z'),
            datetime.datetime(2026, 9, 18, 12, 39, 20, 438880),
        )

    def test_rfc_3339_nanoseconds_are_truncated(self):
        # Nine digits does not fit a datetime; the extra ones are dropped.
        self.assertEqual(
            _normalize_datetime('2026-09-18T12:39:20.438880123Z'),
            '2026-09-18T12:39:20.438880+00:00',
        )

    def test_go_time_string_with_truncated_fraction(self):
        # Go trims trailing zeros, so the fraction is not always 6 digits.
        out = to_datetime('2026-09-17 14:42:41.4 +0000 UTC')
        self.assertEqual(out, datetime.datetime(2026, 9, 17, 14, 42, 41, 400000))

    def test_go_time_string_with_monotonic_reading(self):
        out = to_datetime(
            '2026-09-17 14:42:41.445984 +0000 UTC m=+0.000000001',
        )
        self.assertEqual(out, datetime.datetime(2026, 9, 17, 14, 42, 41, 445984))

    def test_offset_is_applied_and_dropped(self):
        # Shifted onto UTC and left naive, matching the RFC 3339 values, so two
        # timestamps read off one object can be compared.
        out = to_datetime('2026-09-17 09:42:41 -0500 EST')
        self.assertEqual(out, datetime.datetime(2026, 9, 17, 14, 42, 41))
        self.assertIsNone(out.tzinfo)

    def test_both_shapes_subtract(self):
        created = to_datetime('2026-09-17T13:42:41.493848Z')
        expires = to_datetime('2026-09-17 14:42:41.445984 +0000 UTC')
        self.assertAlmostEqual(
            (expires - created).total_seconds(), 3600, delta=1,
        )

    def test_date_only(self):
        out = to_datetime('2026-09-17')
        self.assertEqual(out, datetime.datetime(2026, 9, 17))

    def test_zero_sentinel_and_unparseable_are_none(self):
        self.assertIsNone(to_datetime('0001-01-01T00:00:00Z'))
        self.assertIsNone(to_datetime(None))
        self.assertIsNone(to_datetime(''))
        self.assertIsNone(to_datetime('not a date'))

    def test_the_go_spelling_of_the_zero_sentinel_is_none_too(self):
        # Go's zero time means "unset" -- an expiresAt on a resource that does
        # not expire -- and arrives in whichever shape the field uses. Reading
        # the Go spelling as a real timestamp reported year 1 as an expiry.
        self.assertIsNone(to_datetime('0001-01-01 00:00:00 +0000 UTC'))
        # Recognized from the parsed value, so the trimmings Go may add do not
        # each need their own literal.
        self.assertIsNone(
            to_datetime('0001-01-01 00:00:00 +0000 UTC m=+0.000000001'),
        )
        self.assertIsNone(to_datetime('0001-01-01 00:00:00 +0000 GMT'))
        self.assertIsNone(to_datetime('0001-01-01'))

    def test_datetime_passes_through(self):
        given = datetime.datetime(2026, 9, 17, 13, 42, 41)
        self.assertIs(to_datetime(given), given)

    def test_strict_reads_the_go_shape_too(self):
        out = to_datetime_strict('2026-09-17 14:42:41.445984 +0000 UTC')
        self.assertEqual(out, datetime.datetime(2026, 9, 17, 14, 42, 41, 445984))

    def test_strict_still_raises_on_nothing(self):
        with self.assertRaises(TypeError):
            to_datetime_strict(None)
        with self.assertRaises(ValueError):
            to_datetime_strict('0001-01-01T00:00:00Z')

    def test_strict_raises_on_the_go_spelling_of_the_sentinel(self):
        with self.assertRaises(ValueError):
            to_datetime_strict('0001-01-01 00:00:00 +0000 UTC')


if __name__ == '__main__':
    unittest.main()
