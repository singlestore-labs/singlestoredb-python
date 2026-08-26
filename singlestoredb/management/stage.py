#!/usr/bin/env python
"""
SingleStoreDB Stage management.

Stage is version-neutral apart from where it hangs off the API: at v1 the
filesystem lives under ``stage/{deployment_id}/fs/...``, at v2 it moved under
the cluster resource (``clusters/{cluster_id}/stage/fs/...``). Every request
this class makes routes through :meth:`Stage._fs_path`, so the version
difference is a one-line override in the v2 subclass rather than a copy of
every method.
"""
from __future__ import annotations

import io
import os
import re
from typing import Any
from typing import cast
from typing import List
from typing import Literal
from typing import Optional
from typing import overload
from typing import Union

from ..exceptions import ManagementError
from ._version_import import _versioned_attr
from .files import FileLocation
from .files import FilesObject
from .files import FilesObjectBytesReader
from .files import FilesObjectBytesWriter
from .files import FilesObjectTextReader
from .files import FilesObjectTextWriter
from .manager import Manager
from .utils import ensure_within
from .utils import normalize_remote_path
from .utils import PathLike
from .utils import resolve_ignore_files
from .utils import vars_to_str


def get_stage(
    deployment: Optional[Any] = None,
    version: Optional[str] = None,
) -> 'Stage':
    """
    Get the stage of a deployment.

    Parameters
    ----------
    deployment : Cluster or WorkspaceGroup or str, optional
        The deployment whose stage is wanted, or its name or ID. What counts
        as a deployment is version-specific: a cluster at v2, a workspace
        group at v1. If not given, the deployment named by the environment is
        used -- ``SINGLESTOREDB_WORKSPACE_GROUP`` at v1, and
        ``SINGLESTOREDB_WORKSPACE`` at v2.
    version : str, optional
        Version of the API to use. Defaults to the ``management.version``
        option (the ``SINGLESTOREDB_MANAGEMENT_VERSION`` environment
        variable).

    Returns
    -------
    :class:`Stage`

    """
    return _versioned_attr('get_stage', version)(deployment)


class Stage(FileLocation):
    """
    Stage manager.

    This object is not instantiated directly.
    It is returned by ``Cluster.stage`` or ``StarterCluster.stage``.

    """

    def __init__(self, deployment_id: str, manager: Manager):
        self._deployment_id = deployment_id
        self._manager = manager

    def _fs_path(self, path: PathLike = '') -> str:
        """
        Return the management API path for a Stage filesystem location.

        Overridden by the v1 ``Stage``, where Stage was a top-level resource
        rather than nested under the cluster. All Stage requests go through
        here so that the version difference is a one-line override rather
        than a copy of every method.

        Parameters
        ----------
        path : Path or str, optional
            Stage path, relative to the root of the deployment's Stage

        Returns
        -------
        str

        """
        return f'clusters/{self._deployment_id}/stage/fs/{path}'

    def open(
        self,
        stage_path: PathLike,
        mode: str = 'r',
        encoding: Optional[str] = None,
    ) -> Union[io.StringIO, io.BytesIO]:
        """
        Open a Stage path for reading or writing.

        Parameters
        ----------
        stage_path : Path or str
            The stage path to read / write
        mode : str, optional
            The read / write mode. The following modes are supported:
                * 'r' open for reading (default)
                * 'w' open for writing, truncating the file first
                * 'x' create a new file and open it for writing
            The data type can be specified by adding one of the following:
                * 'b' binary mode
                * 't' text mode (default)
        encoding : str, optional
            The string encoding to use for text

        Returns
        -------
        FilesObjectBytesReader - 'rb' or 'b' mode
        FilesObjectBytesWriter - 'wb' or 'xb' mode
        FilesObjectTextReader - 'r' or 'rt' mode
        FilesObjectTextWriter - 'w', 'x', 'wt' or 'xt' mode

        """
        if '+' in mode or 'a' in mode:
            raise ValueError('modifying an existing stage file is not supported')

        if 'w' in mode or 'x' in mode:
            exists = self.exists(stage_path)
            if exists:
                if 'x' in mode:
                    raise FileExistsError(f'stage path already exists: {stage_path}')
                self.remove(stage_path)
            if 'b' in mode:
                return FilesObjectBytesWriter(b'', self, stage_path)
            return FilesObjectTextWriter('', self, stage_path)

        if 'r' in mode:
            content = self.download_file(stage_path)
            if isinstance(content, bytes):
                if 'b' in mode:
                    return FilesObjectBytesReader(content)
                encoding = 'utf-8' if encoding is None else encoding
                return FilesObjectTextReader(content.decode(encoding))

            if isinstance(content, str):
                return FilesObjectTextReader(content)

            raise ValueError(f'unrecognized file content type: {type(content)}')

        raise ValueError(f'must have one of create/read/write mode specified: {mode}')

    def upload_file(
        self,
        local_path: Union[PathLike, io.IOBase],
        stage_path: PathLike,
        *,
        overwrite: bool = False,
    ) -> FilesObject:
        """
        Upload a local file.

        Parameters
        ----------
        local_path : Path or str or file-like
            Path to the local file or an open file object
        stage_path : Path or str
            Path to the stage file
        overwrite : bool, optional
            Should the ``stage_path`` be overwritten if it exists already?

        """
        if isinstance(local_path, io.IOBase):
            pass
        elif not os.path.isfile(local_path):
            raise IsADirectoryError(f'local path is not a file: {local_path}')

        if self.exists(stage_path):
            if not overwrite:
                raise OSError(f'stage path already exists: {stage_path}')

            self.remove(stage_path)

        if isinstance(local_path, io.IOBase):
            return self._upload(local_path, stage_path, overwrite=overwrite)

        return self._upload(open(local_path, 'rb'), stage_path, overwrite=overwrite)

    def upload_folder(
        self,
        local_path: PathLike,
        stage_path: PathLike,
        *,
        overwrite: bool = False,
        recursive: bool = True,
        include_root: bool = False,
        ignore: Optional[Union[PathLike, List[PathLike]]] = None,
    ) -> FilesObject:
        """
        Upload a folder recursively.

        Only the contents of the folder are uploaded. To include the
        folder name itself in the target path use ``include_root=True``.

        Parameters
        ----------
        local_path : Path or str
            Local directory to upload
        stage_path : Path or str
            Path of stage folder to upload to
        overwrite : bool, optional
            If a file already exists, should it be overwritten?
        recursive : bool, optional
            Should nested folders be uploaded?
        include_root : bool, optional
            Should the local root folder itself be uploaded as the top folder?
        ignore : Path or str or List[Path] or List[str], optional
            Glob patterns of files or folders to ignore, for example,
            ``**/*.pyc`` will ignore all ``*.pyc`` files in the directory
            tree, and ``**/__pycache__`` will ignore those folders entirely.
            Relative patterns are resolved against ``local_path``.

        """
        if not os.path.isdir(local_path):
            raise NotADirectoryError(f'local path is not a directory: {local_path}')

        stage_prefix = normalize_remote_path(stage_path)

        if self.exists(stage_prefix) and not self.is_dir(stage_prefix):
            raise NotADirectoryError(f'stage path is not a directory: {stage_path}')

        ignore_files = resolve_ignore_files(local_path, ignore)

        local_root = os.path.normpath(str(local_path))
        root_name = os.path.basename(local_root)

        for dir_path, dirs, files in os.walk(local_root):
            if ignore_files:
                # Prune ignored folders so their contents are skipped too
                dirs[:] = [
                    d for d in dirs
                    if os.path.normpath(os.path.join(dir_path, d))
                    not in ignore_files
                ]
            for fname in files:
                # Normalized so it compares equal to the normalized
                # glob results in ignore_files (e.g. local_path='.')
                local_file_path = os.path.normpath(os.path.join(dir_path, fname))
                if ignore_files and local_file_path in ignore_files:
                    continue
                rel = os.path.relpath(local_file_path, local_root)
                if include_root:
                    rel = os.path.join(root_name, rel)
                # Remote paths always use '/', whatever the local platform
                rel = rel.replace(os.sep, '/')
                target = f'{stage_prefix}/{rel}' if stage_prefix else rel
                self.upload_file(local_file_path, target, overwrite=overwrite)
            if not recursive:
                break

        return self.info(stage_prefix)

    def _upload(
        self,
        content: Union[str, bytes, io.IOBase],
        stage_path: PathLike,
        *,
        overwrite: bool = False,
    ) -> FilesObject:
        """
        Upload content to a stage file.

        Parameters
        ----------
        content : str or bytes or file-like
            Content to upload to stage
        stage_path : Path or str
            Path to the stage file
        overwrite : bool, optional
            Should the ``stage_path`` be overwritten if it exists already?

        """
        if self.exists(stage_path):
            if not overwrite:
                raise OSError(f'stage path already exists: {stage_path}')
            self.remove(stage_path)

        self._manager._put(
            self._fs_path(stage_path),
            files={'file': content},
            headers={'Content-Type': None},
        )

        return self.info(stage_path)

    def mkdir(self, stage_path: PathLike, overwrite: bool = False) -> FilesObject:
        """
        Make a directory in the stage.

        Parameters
        ----------
        stage_path : Path or str
            Path of the folder to create
        overwrite : bool, optional
            Should the stage path be overwritten if it exists already?

        Returns
        -------
        FilesObject

        """
        stage_path = re.sub(r'/*$', r'', str(stage_path)) + '/'

        if self.exists(stage_path):
            if not overwrite:
                return self.info(stage_path)

            self.remove(stage_path)

        self._manager._put(
            self._fs_path(stage_path) + '?isFile=false',
        )

        return self.info(stage_path)

    mkdirs = mkdir

    def rename(
        self,
        old_path: PathLike,
        new_path: PathLike,
        *,
        overwrite: bool = False,
    ) -> FilesObject:
        """
        Move the stage file to a new location.

        Paraemeters
        -----------
        old_path : Path or str
            Original location of the path
        new_path : Path or str
            New location of the path
        overwrite : bool, optional
            Should the ``new_path`` be overwritten if it exists already?

        """
        if not self.exists(old_path):
            raise OSError(f'stage path does not exist: {old_path}')

        if self.exists(new_path):
            if not overwrite:
                raise OSError(f'stage path already exists: {new_path}')

            if str(old_path).endswith('/') and not str(new_path).endswith('/'):
                raise OSError('original and new paths are not the same type')

            if str(new_path).endswith('/'):
                self.removedirs(new_path)
            else:
                self.remove(new_path)

        self._manager._patch(
            self._fs_path(old_path),
            json=dict(newPath=new_path),
        )

        return self.info(new_path)

    def info(self, stage_path: PathLike) -> FilesObject:
        """
        Return information about a stage location.

        Parameters
        ----------
        stage_path : Path or str
            Path to the stage location

        Returns
        -------
        FilesObject

        """
        res = self._manager._get(
            re.sub(r'/+$', r'/', self._fs_path(stage_path)),
            params=dict(metadata=1),
        ).json()

        return FilesObject.from_dict(res, self)

    def exists(self, stage_path: PathLike) -> bool:
        """
        Does the given stage path exist?

        Parameters
        ----------
        stage_path : Path or str
            Path to stage object

        Returns
        -------
        bool

        """
        try:
            self.info(stage_path)
            return True
        except ManagementError as exc:
            if exc.errno == 404:
                return False
            raise

    def is_dir(self, stage_path: PathLike) -> bool:
        """
        Is the given stage path a directory?

        Parameters
        ----------
        stage_path : Path or str
            Path to stage object

        Returns
        -------
        bool

        """
        try:
            return self.info(stage_path).type == 'directory'
        except ManagementError as exc:
            if exc.errno == 404:
                return False
            raise

    def is_file(self, stage_path: PathLike) -> bool:
        """
        Is the given stage path a file?

        Parameters
        ----------
        stage_path : Path or str
            Path to stage object

        Returns
        -------
        bool

        """
        try:
            return self.info(stage_path).type != 'directory'
        except ManagementError as exc:
            if exc.errno == 404:
                return False
            raise

    def _listdir(
        self, stage_path: PathLike, *,
        recursive: bool = False,
        return_objects: bool = False,
    ) -> List[Union[str, 'FilesObject']]:
        """
        Return the names (or FilesObject instances) of files in a directory.

        Parameters
        ----------
        stage_path : Path or str
            Path to the folder in Stage
        recursive : bool, optional
            Should folders be listed recursively?
        return_objects : bool, optional
            If True, return list of FilesObject instances. Otherwise just paths.

        """
        from .files import FilesObject
        res = self._manager._get(
            re.sub(r'/+$', r'/', self._fs_path(stage_path)),
        ).json()
        if recursive:
            out: List[Union[str, FilesObject]] = []
            for item in res['content'] or []:
                if return_objects:
                    out.append(FilesObject.from_dict(item, self))
                else:
                    out.append(item['path'])
                if item['type'] == 'directory':
                    out.extend(
                        self._listdir(
                            item['path'],
                            recursive=recursive,
                            return_objects=return_objects,
                        ),
                    )
            return out
        if return_objects:
            return [
                FilesObject.from_dict(x, self)
                for x in res['content'] or []
            ]
        return [x['path'] for x in res['content'] or []]

    @overload
    def listdir(
        self,
        stage_path: PathLike = '/',
        *,
        recursive: bool = False,
        return_objects: Literal[True],
    ) -> List['FilesObject']:
        ...

    @overload
    def listdir(
        self,
        stage_path: PathLike = '/',
        *,
        recursive: bool = False,
        return_objects: Literal[False] = False,
    ) -> List[str]:
        ...

    def listdir(
        self,
        stage_path: PathLike = '/',
        *,
        recursive: bool = False,
        return_objects: bool = False,
    ) -> Union[List[str], List['FilesObject']]:
        """
        List the files / folders at the given path.

        Parameters
        ----------
        stage_path : Path or str, optional
            Path to the stage location
        recursive : bool, optional
            If True, recursively list all files and folders
        return_objects : bool, optional
            If True, return list of FilesObject instances. Otherwise just paths.

        Returns
        -------
        List[str] or List[FilesObject]

        """
        from .files import FilesObject
        stage_path = normalize_remote_path(stage_path, strip_leading=True) + '/'

        if self.is_dir(stage_path):
            out = self._listdir(
                stage_path,
                recursive=recursive,
                return_objects=return_objects,
            )
            if stage_path != '/':
                stage_path_n = len(stage_path.split('/')) - 1
                if return_objects:
                    result: List[FilesObject] = []
                    for item in out:
                        if isinstance(item, FilesObject):
                            rel = '/'.join(item.path.split('/')[stage_path_n:])
                            item.path = rel
                            result.append(item)
                    return result
                out = ['/'.join(str(x).split('/')[stage_path_n:]) for x in out]
            if return_objects:
                return cast(List[FilesObject], out)
            return cast(List[str], out)

        raise NotADirectoryError(f'stage path is not a directory: {stage_path}')

    def download_file(
        self,
        stage_path: PathLike,
        local_path: Optional[PathLike] = None,
        *,
        overwrite: bool = False,
        encoding: Optional[str] = None,
    ) -> Optional[Union[bytes, str]]:
        """
        Download the content of a stage path.

        Parameters
        ----------
        stage_path : Path or str
            Path to the stage file
        local_path : Path or str
            Path to local file target location
        overwrite : bool, optional
            Should an existing file be overwritten if it exists?
        encoding : str, optional
            Encoding used to convert the resulting data

        Returns
        -------
        bytes or str - ``local_path`` is None
        None - ``local_path`` is a Path or str

        """
        return self._download_file(
            stage_path,
            local_path=local_path,
            overwrite=overwrite,
            encoding=encoding,
            _skip_dir_check=False,
        )

    def _download_file(
        self,
        stage_path: PathLike,
        local_path: Optional[PathLike] = None,
        *,
        overwrite: bool = False,
        encoding: Optional[str] = None,
        _skip_dir_check: bool = False,
    ) -> Optional[Union[bytes, str]]:
        """
        Internal method to download the content of a stage path.

        Parameters
        ----------
        stage_path : Path or str
            Path to the stage file
        local_path : Path or str
            Path to local file target location
        overwrite : bool, optional
            Should an existing file be overwritten if it exists?
        encoding : str, optional
            Encoding used to convert the resulting data
        _skip_dir_check : bool, optional
            Skip the remote directory check when the caller already knows
            ``stage_path`` refers to a file (e.g. from a directory listing)

        Returns
        -------
        bytes or str - ``local_path`` is None
        None - ``local_path`` is a Path or str

        """
        if local_path is not None and not overwrite and os.path.exists(local_path):
            raise OSError('target file already exists; use overwrite=True to replace')
        if not _skip_dir_check and self.is_dir(stage_path):
            raise IsADirectoryError(f'stage path is a directory: {stage_path}')

        out = self._manager._get(
            self._fs_path(stage_path),
        ).content

        if local_path is not None:
            with open(local_path, 'wb') as outfile:
                outfile.write(out)
            return None

        if encoding:
            return out.decode(encoding)

        return out

    def download_folder(
        self,
        stage_path: PathLike,
        local_path: Optional[PathLike] = None,
        *,
        overwrite: bool = False,
    ) -> None:
        """
        Download a Stage folder to a local directory.

        The contents of ``stage_path`` are written into ``local_path``,
        which is created as the destination folder.

        Parameters
        ----------
        stage_path : Path or str
            Path to the stage folder
        local_path : Path or str, optional
            Local directory to create and download into. Defaults to the
            name of the ``stage_path`` folder in the current directory.
        overwrite : bool, optional
            Should an existing directory / files be overwritten if they exist?

        """
        # ``listdir`` returns paths relative to ``stage_path``, so the folder
        # prefix has to be added back on before making any remote calls.
        stage_prefix = normalize_remote_path(stage_path, strip_leading=True)

        if local_path is None:
            local_path = os.path.basename(stage_prefix)
            if not local_path:
                raise ValueError(
                    'local_path must be specified when downloading '
                    'the root folder',
                )

        if not overwrite and os.path.exists(local_path):
            raise OSError(
                'target directory already exists; '
                'use overwrite=True to replace',
            )
        if not self.is_dir(stage_prefix):
            raise NotADirectoryError(f'stage path is not a directory: {stage_path}')

        # Request objects so the file / directory type comes from the listing
        # rather than an extra is_dir call per entry.
        for entry in self.listdir(stage_prefix, recursive=True, return_objects=True):
            rel_path = entry.path
            target = ensure_within(local_path, os.path.join(local_path, rel_path))
            if entry.type == 'directory':
                os.makedirs(target, exist_ok=True)
                continue
            remote_path = (
                f'{stage_prefix}/{rel_path}' if stage_prefix else rel_path
            )
            os.makedirs(os.path.dirname(target) or '.', exist_ok=True)
            self._download_file(
                remote_path, target,
                overwrite=overwrite, _skip_dir_check=True,
            )

    def remove(self, stage_path: PathLike) -> None:
        """
        Delete a stage location.

        Parameters
        ----------
        stage_path : Path or str
            Path to the stage location

        """
        if self.is_dir(stage_path):
            raise IsADirectoryError(
                'stage path is a directory, '
                f'use rmdir or removedirs: {stage_path}',
            )

        self._manager._delete(self._fs_path(stage_path))

    def removedirs(self, stage_path: PathLike) -> None:
        """
        Delete a stage folder recursively.

        Parameters
        ----------
        stage_path : Path or str
            Path to the stage location

        """
        stage_path = re.sub(r'/*$', r'', str(stage_path)) + '/'
        self._manager._delete(self._fs_path(stage_path))

    def rmdir(self, stage_path: PathLike) -> None:
        """
        Delete a stage folder.

        Parameters
        ----------
        stage_path : Path or str
            Path to the stage location

        """
        stage_path = re.sub(r'/*$', r'', str(stage_path)) + '/'

        if self.listdir(stage_path):
            raise OSError(f'stage folder is not empty, use removedirs: {stage_path}')

        self._manager._delete(self._fs_path(stage_path))

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)


StageObject = FilesObject  # alias for backward compatibility
