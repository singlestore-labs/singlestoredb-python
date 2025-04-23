#!/usr/bin/env python
"""SingleStoreDB Workspace Management."""
from __future__ import annotations

import datetime
import glob
import io
import os
import re
import time
from collections.abc import Mapping
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

from .. import config
from .. import connection
from ..exceptions import ManagementError
from .billing_usage import BillingUsageItem
from .files import FileLocation
from .files import FilesObject
from .files import FilesObjectBytesReader
from .files import FilesObjectBytesWriter
from .files import FilesObjectTextReader
from .files import FilesObjectTextWriter
from .manager import Manager
from .organization import Organization
from .region import Region
from .utils import camel_to_snake_dict
from .utils import from_datetime
from .utils import NamedList
from .utils import PathLike
from .utils import snake_to_camel
from .utils import snake_to_camel_dict
from .utils import to_datetime
from .utils import ttl_property
from .utils import vars_to_str


def get_organization() -> Organization:
    """Get the organization."""
    return manage_workspaces().organization


def get_secret(name: str) -> Optional[str]:
    """Get a secret from the organization."""
    return get_organization().get_secret(name).value


def get_workspace_group(
    workspace_group: Optional[Union[WorkspaceGroup, str]] = None,
) -> WorkspaceGroup:
    """Get the stage for the workspace group."""
    if isinstance(workspace_group, WorkspaceGroup):
        return workspace_group
    elif workspace_group:
        return manage_workspaces().workspace_groups[workspace_group]
    elif 'SINGLESTOREDB_WORKSPACE_GROUP' in os.environ:
        return manage_workspaces().workspace_groups[
            os.environ['SINGLESTOREDB_WORKSPACE_GROUP']
        ]
    raise RuntimeError('no workspace group specified')


def get_stage(
    workspace_group: Optional[Union[WorkspaceGroup, str]] = None,
) -> Stage:
    """Get the stage for the workspace group."""
    return get_workspace_group(workspace_group).stage


def get_workspace(
    workspace_group: Optional[Union[WorkspaceGroup, str]] = None,
    workspace: Optional[Union[Workspace, str]] = None,
) -> Workspace:
    """Get the workspaces for a workspace_group."""
    if isinstance(workspace, Workspace):
        return workspace
    wg = get_workspace_group(workspace_group)
    if workspace:
        return wg.workspaces[workspace]
    elif 'SINGLESTOREDB_WORKSPACE' in os.environ:
        return wg.workspaces[
            os.environ['SINGLESTOREDB_WORKSPACE']
        ]
    raise RuntimeError('no workspace group specified')


class Stage(FileLocation):
    """
    Stage manager.

    This object is not instantiated directly.
    It is returned by ``WorkspaceGroup.stage`` or ``StarterWorkspace.stage``.

    """

    def __init__(self, deployment_id: str, manager: WorkspaceManager):
        self._deployment_id = deployment_id
        self._manager = manager

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
            Glob patterns of files to ignore, for example, '**/*.pyc` will
            ignore all '*.pyc' files in the directory tree

        """
        if not os.path.isdir(local_path):
            raise NotADirectoryError(f'local path is not a directory: {local_path}')
        if self.exists(stage_path) and not self.is_dir(stage_path):
            raise NotADirectoryError(f'stage path is not a directory: {stage_path}')

        ignore_files = set()
        if ignore:
            if isinstance(ignore, list):
                for item in ignore:
                    ignore_files.update(glob.glob(str(item), recursive=recursive))
            else:
                ignore_files.update(glob.glob(str(ignore), recursive=recursive))

        parent_dir = os.path.basename(os.getcwd())

        files = glob.glob(os.path.join(local_path, '**'), recursive=recursive)

        for src in files:
            if ignore_files and src in ignore_files:
                continue
            target = os.path.join(parent_dir, src) if include_root else src
            self.upload_file(src, target, overwrite=overwrite)

        return self.info(stage_path)

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
            f'stage/{self._deployment_id}/fs/{stage_path}',
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
            f'stage/{self._deployment_id}/fs/{stage_path}?isFile=false',
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
            f'stage/{self._deployment_id}/fs/{old_path}',
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
            re.sub(r'/+$', r'/', f'stage/{self._deployment_id}/fs/{stage_path}'),
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

    def _listdir(self, stage_path: PathLike, *, recursive: bool = False) -> List[str]:
        """
        Return the names of files in a directory.

        Parameters
        ----------
        stage_path : Path or str
            Path to the folder in Stage
        recursive : bool, optional
            Should folders be listed recursively?

        """
        res = self._manager._get(
            f'stage/{self._deployment_id}/fs/{stage_path}',
        ).json()
        if recursive:
            out = []
            for item in res['content'] or []:
                out.append(item['path'])
                if item['type'] == 'directory':
                    out.extend(self._listdir(item['path'], recursive=recursive))
            return out
        return [x['path'] for x in res['content'] or []]

    def listdir(
        self,
        stage_path: PathLike = '/',
        *,
        recursive: bool = False,
    ) -> List[str]:
        """
        List the files / folders at the given path.

        Parameters
        ----------
        stage_path : Path or str, optional
            Path to the stage location

        Returns
        -------
        List[str]

        """
        stage_path = re.sub(r'^(\./|/)+', r'', str(stage_path))
        stage_path = re.sub(r'/+$', r'', stage_path) + '/'

        if self.is_dir(stage_path):
            out = self._listdir(stage_path, recursive=recursive)
            if stage_path != '/':
                stage_path_n = len(stage_path.split('/')) - 1
                out = ['/'.join(x.split('/')[stage_path_n:]) for x in out]
            return out

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
        if local_path is not None and not overwrite and os.path.exists(local_path):
            raise OSError('target file already exists; use overwrite=True to replace')
        if self.is_dir(stage_path):
            raise IsADirectoryError(f'stage path is a directory: {stage_path}')

        out = self._manager._get(
            f'stage/{self._deployment_id}/fs/{stage_path}',
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
        local_path: PathLike = '.',
        *,
        overwrite: bool = False,
    ) -> None:
        """
        Download a Stage folder to a local directory.

        Parameters
        ----------
        stage_path : Path or str
            Path to the stage file
        local_path : Path or str
            Path to local directory target location
        overwrite : bool, optional
            Should an existing directory / files be overwritten if they exist?

        """
        if local_path is not None and not overwrite and os.path.exists(local_path):
            raise OSError(
                'target directory already exists; '
                'use overwrite=True to replace',
            )
        if not self.is_dir(stage_path):
            raise NotADirectoryError(f'stage path is not a directory: {stage_path}')

        for f in self.listdir(stage_path, recursive=True):
            if self.is_dir(f):
                continue
            target = os.path.normpath(os.path.join(local_path, f))
            os.makedirs(os.path.dirname(target), exist_ok=True)
            self.download_file(f, target, overwrite=overwrite)

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

        self._manager._delete(f'stage/{self._deployment_id}/fs/{stage_path}')

    def removedirs(self, stage_path: PathLike) -> None:
        """
        Delete a stage folder recursively.

        Parameters
        ----------
        stage_path : Path or str
            Path to the stage location

        """
        stage_path = re.sub(r'/*$', r'', str(stage_path)) + '/'
        self._manager._delete(f'stage/{self._deployment_id}/fs/{stage_path}')

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

        self._manager._delete(f'stage/{self._deployment_id}/fs/{stage_path}')

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)


StageObject = FilesObject  # alias for backward compatibility


class Workspace(object):
    """
    SingleStoreDB workspace definition.

    This object is not instantiated directly. It is used in the results
    of API calls on the :class:`WorkspaceManager`. Workspaces are created using
    :meth:`WorkspaceManager.create_workspace`, or existing workspaces are
    accessed by either :attr:`WorkspaceManager.workspaces` or by calling
    :meth:`WorkspaceManager.get_workspace`.

    See Also
    --------
    :meth:`WorkspaceManager.create_workspace`
    :meth:`WorkspaceManager.get_workspace`
    :attr:`WorkspaceManager.workspaces`

    """

    name: str
    id: str
    group_id: str
    size: str
    state: str
    created_at: Optional[datetime.datetime]
    terminated_at: Optional[datetime.datetime]
    endpoint: Optional[str]
    auto_suspend: Optional[Dict[str, Any]]
    cache_config: Optional[int]
    deployment_type: Optional[str]
    resume_attachments: Optional[List[Dict[str, Any]]]
    scaling_progress: Optional[int]
    last_resumed_at: Optional[datetime.datetime]

    def __init__(
        self,
        name: str,
        workspace_id: str,
        workspace_group: Union[str, 'WorkspaceGroup'],
        size: str,
        state: str,
        created_at: Union[str, datetime.datetime],
        terminated_at: Optional[Union[str, datetime.datetime]] = None,
        endpoint: Optional[str] = None,
        auto_suspend: Optional[Dict[str, Any]] = None,
        cache_config: Optional[int] = None,
        deployment_type: Optional[str] = None,
        resume_attachments: Optional[List[Dict[str, Any]]] = None,
        scaling_progress: Optional[int] = None,
        last_resumed_at: Optional[Union[str, datetime.datetime]] = None,
    ):
        #: Name of the workspace
        self.name = name

        #: Unique ID of the workspace
        self.id = workspace_id

        #: Unique ID of the workspace group
        if isinstance(workspace_group, WorkspaceGroup):
            self.group_id = workspace_group.id
        else:
            self.group_id = workspace_group

        #: Size of the workspace in workspace size notation (S-00, S-1, etc.)
        self.size = size

        #: State of the workspace: PendingCreation, Transitioning, Active,
        #: Terminated, Suspended, Resuming, Failed
        self.state = state.strip()

        #: Timestamp of when the workspace was created
        self.created_at = to_datetime(created_at)

        #: Timestamp of when the workspace was terminated
        self.terminated_at = to_datetime(terminated_at)

        #: Hostname (or IP address) of the workspace database server
        self.endpoint = endpoint

        #: Current auto-suspend settings
        self.auto_suspend = camel_to_snake_dict(auto_suspend)

        #: Multiplier for the persistent cache
        self.cache_config = cache_config

        #: Deployment type of the workspace
        self.deployment_type = deployment_type

        #: Database attachments
        self.resume_attachments = [
            camel_to_snake_dict(x)  # type: ignore
            for x in resume_attachments or []
            if x is not None
        ]

        #: Current progress percentage for scaling the workspace
        self.scaling_progress = scaling_progress

        #: Timestamp when workspace was last resumed
        self.last_resumed_at = to_datetime(last_resumed_at)

        self._manager: Optional[WorkspaceManager] = None

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)

    @classmethod
    def from_dict(cls, obj: Dict[str, Any], manager: 'WorkspaceManager') -> 'Workspace':
        """
        Construct a Workspace from a dictionary of values.

        Parameters
        ----------
        obj : dict
            Dictionary of values
        manager : WorkspaceManager, optional
            The WorkspaceManager the Workspace belongs to

        Returns
        -------
        :class:`Workspace`

        """
        out = cls(
            name=obj['name'],
            workspace_id=obj['workspaceID'],
            workspace_group=obj['workspaceGroupID'],
            size=obj.get('size', 'Unknown'),
            state=obj['state'],
            created_at=obj['createdAt'],
            terminated_at=obj.get('terminatedAt'),
            endpoint=obj.get('endpoint'),
            auto_suspend=obj.get('autoSuspend'),
            cache_config=obj.get('cacheConfig'),
            deployment_type=obj.get('deploymentType'),
            last_resumed_at=obj.get('lastResumedAt'),
            resume_attachments=obj.get('resumeAttachments'),
            scaling_progress=obj.get('scalingProgress'),
        )
        out._manager = manager
        return out

    def update(
        self,
        auto_suspend: Optional[Dict[str, Any]] = None,
        cache_config: Optional[int] = None,
        deployment_type: Optional[str] = None,
        size: Optional[str] = None,
    ) -> None:
        """
        Update the workspace definition.

        Parameters
        ----------
        auto_suspend : Dict[str, Any], optional
            Auto-suspend mode for the workspace: IDLE, SCHEDULED, DISABLED
        cache_config : int, optional
            Specifies the multiplier for the persistent cache associated
            with the workspace. If specified, it enables the cache configuration
            multiplier. It can have one of the following values: 1, 2, or 4.
        deployment_type : str, optional
            The deployment type that will be applied to all the workspaces
            within the group
        size : str, optional
            Size of the workspace (in workspace size notation), such as "S-1".

        """
        if self._manager is None:
            raise ManagementError(
                msg='No workspace manager is associated with this object.',
            )
        data = {
            k: v for k, v in dict(
                autoSuspend=snake_to_camel_dict(auto_suspend),
                cacheConfig=cache_config,
                deploymentType=deployment_type,
                size=size,
            ).items() if v is not None
        }
        self._manager._patch(f'workspaces/{self.id}', json=data)
        self.refresh()

    def refresh(self) -> Workspace:
        """Update the object to the current state."""
        if self._manager is None:
            raise ManagementError(
                msg='No workspace manager is associated with this object.',
            )
        new_obj = self._manager.get_workspace(self.id)
        for name, value in vars(new_obj).items():
            if isinstance(value, Mapping):
                setattr(self, name, snake_to_camel_dict(value))
            else:
                setattr(self, name, value)
        return self

    def terminate(
        self,
        wait_on_terminated: bool = False,
        wait_interval: int = 10,
        wait_timeout: int = 600,
        force: bool = False,
    ) -> None:
        """
        Terminate the workspace.

        Parameters
        ----------
        wait_on_terminated : bool, optional
            Wait for the workspace to go into 'Terminated' mode before returning
        wait_interval : int, optional
            Number of seconds between each server check
        wait_timeout : int, optional
            Total number of seconds to check server before giving up
        force : bool, optional
            Should the workspace group be terminated even if it has workspaces?

        Raises
        ------
        ManagementError
            If timeout is reached

        """
        if self._manager is None:
            raise ManagementError(
                msg='No workspace manager is associated with this object.',
            )
        force_str = 'true' if force else 'false'
        self._manager._delete(f'workspaces/{self.id}?force={force_str}')
        if wait_on_terminated:
            self._manager._wait_on_state(
                self._manager.get_workspace(self.id),
                'Terminated', interval=wait_interval, timeout=wait_timeout,
            )
            self.refresh()

    def connect(self, **kwargs: Any) -> connection.Connection:
        """
        Create a connection to the database server for this workspace.

        Parameters
        ----------
        **kwargs : keyword-arguments, optional
            Parameters to the SingleStoreDB `connect` function except host
            and port which are supplied by the workspace object

        Returns
        -------
        :class:`Connection`

        """
        if not self.endpoint:
            raise ManagementError(
                msg='An endpoint has not been set in this workspace configuration',
            )
        kwargs['host'] = self.endpoint
        return connection.connect(**kwargs)

    def suspend(
        self,
        wait_on_suspended: bool = False,
        wait_interval: int = 20,
        wait_timeout: int = 600,
    ) -> None:
        """
        Suspend the workspace.

        Parameters
        ----------
        wait_on_suspended : bool, optional
            Wait for the workspace to go into 'Suspended' mode before returning
        wait_interval : int, optional
            Number of seconds between each server check
        wait_timeout : int, optional
            Total number of seconds to check server before giving up

        Raises
        ------
        ManagementError
            If timeout is reached

        """
        if self._manager is None:
            raise ManagementError(
                msg='No workspace manager is associated with this object.',
            )
        self._manager._post(f'workspaces/{self.id}/suspend')
        if wait_on_suspended:
            self._manager._wait_on_state(
                self._manager.get_workspace(self.id),
                'Suspended', interval=wait_interval, timeout=wait_timeout,
            )
            self.refresh()

    def resume(
        self,
        disable_auto_suspend: bool = False,
        wait_on_resumed: bool = False,
        wait_interval: int = 20,
        wait_timeout: int = 600,
    ) -> None:
        """
        Resume the workspace.

        Parameters
        ----------
        disable_auto_suspend : bool, optional
            Should auto-suspend be disabled?
        wait_on_resumed : bool, optional
            Wait for the workspace to go into 'Resumed' or 'Active' mode before returning
        wait_interval : int, optional
            Number of seconds between each server check
        wait_timeout : int, optional
            Total number of seconds to check server before giving up

        Raises
        ------
        ManagementError
            If timeout is reached

        """
        if self._manager is None:
            raise ManagementError(
                msg='No workspace manager is associated with this object.',
            )
        self._manager._post(
            f'workspaces/{self.id}/resume',
            json=dict(disableAutoSuspend=disable_auto_suspend),
        )
        if wait_on_resumed:
            self._manager._wait_on_state(
                self._manager.get_workspace(self.id),
                ['Resumed', 'Active'], interval=wait_interval, timeout=wait_timeout,
            )
            self.refresh()


class WorkspaceGroup(object):
    """
    SingleStoreDB workspace group definition.

    This object is not instantiated directly. It is used in the results
    of API calls on the :class:`WorkspaceManager`. Workspace groups are created using
    :meth:`WorkspaceManager.create_workspace_group`, or existing workspace groups are
    accessed by either :attr:`WorkspaceManager.workspace_groups` or by calling
    :meth:`WorkspaceManager.get_workspace_group`.

    See Also
    --------
    :meth:`WorkspaceManager.create_workspace_group`
    :meth:`WorkspaceManager.get_workspace_group`
    :attr:`WorkspaceManager.workspace_groups`

    """

    name: str
    id: str
    created_at: Optional[datetime.datetime]
    region: Optional[Region]
    firewall_ranges: List[str]
    terminated_at: Optional[datetime.datetime]
    allow_all_traffic: bool

    def __init__(
        self,
        name: str,
        id: str,
        created_at: Union[str, datetime.datetime],
        region: Optional[Region],
        firewall_ranges: List[str],
        terminated_at: Optional[Union[str, datetime.datetime]],
        allow_all_traffic: Optional[bool],
    ):
        #: Name of the workspace group
        self.name = name

        #: Unique ID of the workspace group
        self.id = id

        #: Timestamp of when the workspace group was created
        self.created_at = to_datetime(created_at)

        #: Region of the workspace group (see :class:`Region`)
        self.region = region

        #: List of allowed incoming IP addresses / ranges
        self.firewall_ranges = firewall_ranges

        #: Timestamp of when the workspace group was terminated
        self.terminated_at = to_datetime(terminated_at)

        #: Should all traffic be allowed?
        self.allow_all_traffic = allow_all_traffic or False

        self._manager: Optional[WorkspaceManager] = None

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)

    @classmethod
    def from_dict(
        cls, obj: Dict[str, Any], manager: 'WorkspaceManager',
    ) -> 'WorkspaceGroup':
        """
        Construct a WorkspaceGroup from a dictionary of values.

        Parameters
        ----------
        obj : dict
            Dictionary of values
        manager : WorkspaceManager, optional
            The WorkspaceManager the WorkspaceGroup belongs to

        Returns
        -------
        :class:`WorkspaceGroup`

        """
        try:
            region = [x for x in manager.regions if x.id == obj['regionID']][0]
        except IndexError:
            region = Region(obj.get('regionID', '<unknown>'), '<unknown>', '<unknown>')
        out = cls(
            name=obj['name'],
            id=obj['workspaceGroupID'],
            created_at=obj['createdAt'],
            region=region,
            firewall_ranges=obj.get('firewallRanges', []),
            terminated_at=obj.get('terminatedAt'),
            allow_all_traffic=obj.get('allowAllTraffic'),
        )
        out._manager = manager
        return out

    @property
    def organization(self) -> Organization:
        if self._manager is None:
            raise ManagementError(
                msg='No workspace manager is associated with this object.',
            )
        return self._manager.organization

    @property
    def stage(self) -> Stage:
        """Stage manager."""
        if self._manager is None:
            raise ManagementError(
                msg='No workspace manager is associated with this object.',
            )
        return Stage(self.id, self._manager)

    stages = stage

    def refresh(self) -> 'WorkspaceGroup':
        """Update the object to the current state."""
        if self._manager is None:
            raise ManagementError(
                msg='No workspace manager is associated with this object.',
            )
        new_obj = self._manager.get_workspace_group(self.id)
        for name, value in vars(new_obj).items():
            if isinstance(value, Mapping):
                setattr(self, name, camel_to_snake_dict(value))
            else:
                setattr(self, name, value)
        return self

    def update(
        self,
        name: Optional[str] = None,
        firewall_ranges: Optional[List[str]] = None,
        admin_password: Optional[str] = None,
        expires_at: Optional[str] = None,
        allow_all_traffic: Optional[bool] = None,
        update_window: Optional[Dict[str, int]] = None,
    ) -> None:
        """
        Update the workspace group definition.

        Parameters
        ----------
        name : str, optional
            Name of the workspace group
        firewall_ranges : list[str], optional
            List of allowed CIDR ranges. An empty list indicates that all
            inbound requests are allowed.
        admin_password : str, optional
            Admin password for the workspace group. If no password is supplied,
            a password will be generated and retured in the response.
        expires_at : str, optional
            The timestamp of when the workspace group will expire.
            If the expiration time is not specified,
            the workspace group will have no expiration time.
            At expiration, the workspace group is terminated and all the data is lost.
            Expiration time can be specified as a timestamp or duration.
            Example: "2021-01-02T15:04:05Z07:00", "2021-01-02", "3h30m"
        allow_all_traffic : bool, optional
            Allow all traffic to the workspace group
        update_window : Dict[str, int], optional
            Specify the day and hour of an update window: dict(day=0-6, hour=0-23)

        """
        if self._manager is None:
            raise ManagementError(
                msg='No workspace manager is associated with this object.',
            )
        data = {
            k: v for k, v in dict(
                name=name,
                firewallRanges=firewall_ranges,
                adminPassword=admin_password,
                expiresAt=expires_at,
                allowAllTraffic=allow_all_traffic,
                updateWindow=snake_to_camel_dict(update_window),
            ).items() if v is not None
        }
        self._manager._patch(f'workspaceGroups/{self.id}', json=data)
        self.refresh()

    def terminate(
        self, force: bool = False,
        wait_on_terminated: bool = False,
        wait_interval: int = 10,
        wait_timeout: int = 600,
    ) -> None:
        """
        Terminate the workspace group.

        Parameters
        ----------
        force : bool, optional
            Terminate a workspace group even if it has active workspaces
        wait_on_terminated : bool, optional
            Wait for the workspace group to go into 'Terminated' mode before returning
        wait_interval : int, optional
            Number of seconds between each server check
        wait_timeout : int, optional
            Total number of seconds to check server before giving up

        Raises
        ------
        ManagementError
            If timeout is reached

        """
        if self._manager is None:
            raise ManagementError(
                msg='No workspace manager is associated with this object.',
            )
        self._manager._delete(f'workspaceGroups/{self.id}', params=dict(force=force))
        if wait_on_terminated:
            while True:
                self.refresh()
                if self.terminated_at is not None:
                    break
                if wait_timeout <= 0:
                    raise ManagementError(
                        msg='Exceeded waiting time for WorkspaceGroup to terminate',
                    )
                time.sleep(wait_interval)
                wait_timeout -= wait_interval

    def create_workspace(
        self,
        name: str,
        size: Optional[str] = None,
        auto_suspend: Optional[Dict[str, Any]] = None,
        cache_config: Optional[int] = None,
        enable_kai: Optional[bool] = None,
        wait_on_active: bool = False,
        wait_interval: int = 10,
        wait_timeout: int = 600,
    ) -> Workspace:
        """
        Create a new workspace.

        Parameters
        ----------
        name : str
            Name of the workspace
        size : str, optional
            Workspace size in workspace size notation (S-00, S-1, etc.)
        auto_suspend : Dict[str, Any], optional
            Auto suspend settings for the workspace. If this field is not
            provided, no settings will be enabled.
        cache_config : int, optional
            Specifies the multiplier for the persistent cache associated
            with the workspace. If specified, it enables the cache configuration
            multiplier. It can have one of the following values: 1, 2, or 4.
        enable_kai : bool, optional
            Whether to create a SingleStore Kai-enabled workspace
        wait_on_active : bool, optional
            Wait for the workspace to be active before returning
        wait_timeout : int, optional
            Maximum number of seconds to wait before raising an exception
            if wait=True
        wait_interval : int, optional
            Number of seconds between each polling interval

        Returns
        -------
        :class:`Workspace`

        """
        if self._manager is None:
            raise ManagementError(
                msg='No workspace manager is associated with this object.',
            )

        out = self._manager.create_workspace(
            name=name,
            workspace_group=self,
            size=size,
            auto_suspend=snake_to_camel_dict(auto_suspend),
            cache_config=cache_config,
            enable_kai=enable_kai,
            wait_on_active=wait_on_active,
            wait_interval=wait_interval,
            wait_timeout=wait_timeout,
        )

        return out

    @property
    def workspaces(self) -> NamedList[Workspace]:
        """Return a list of available workspaces."""
        if self._manager is None:
            raise ManagementError(
                msg='No workspace manager is associated with this object.',
            )
        res = self._manager._get('workspaces', params=dict(workspaceGroupID=self.id))
        return NamedList(
            [Workspace.from_dict(item, self._manager) for item in res.json()],
        )


class StarterWorkspace(object):
    """
    SingleStoreDB starter workspace definition.

    This object is not instantiated directly. It is used in the results
    of API calls on the :class:`WorkspaceManager`. Existing starter workspaces are
    accessed by either :attr:`WorkspaceManager.starter_workspaces` or by calling
    :meth:`WorkspaceManager.get_starter_workspace`.

    See Also
    --------
    :meth:`WorkspaceManager.get_starter_workspace`
    :attr:`WorkspaceManager.starter_workspaces`

    """

    name: str
    id: str

    def __init__(
        self,
        name: str,
        id: str,
    ):
        #: Name of the starter workspace
        self.name = name

        #: Unique ID of the starter workspace
        self.id = id

        self._manager: Optional[WorkspaceManager] = None

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)

    @classmethod
    def from_dict(
        cls, obj: Dict[str, Any], manager: 'WorkspaceManager',
    ) -> 'StarterWorkspace':
        """
        Construct a StarterWorkspace from a dictionary of values.

        Parameters
        ----------
        obj : dict
            Dictionary of values
        manager : WorkspaceManager, optional
            The WorkspaceManager the StarterWorkspace belongs to

        Returns
        -------
        :class:`StarterWorkspace`

        """
        out = cls(
            name=obj['name'],
            id=obj['virtualWorkspaceID'],
        )
        out._manager = manager
        return out

    @property
    def organization(self) -> Organization:
        if self._manager is None:
            raise ManagementError(
                msg='No workspace manager is associated with this object.',
            )
        return self._manager.organization

    @property
    def stage(self) -> Stage:
        """Stage manager."""
        if self._manager is None:
            raise ManagementError(
                msg='No workspace manager is associated with this object.',
            )
        return Stage(self.id, self._manager)

    stages = stage

    @property
    def starter_workspaces(self) -> NamedList[StarterWorkspace]:
        """Return a list of available starter workspaces."""
        if self._manager is None:
            raise ManagementError(
                msg='No workspace manager is associated with this object.',
            )
        res = self._manager._get('sharedtier/virtualWorkspaces')
        return NamedList(
            [StarterWorkspace.from_dict(item, self._manager) for item in res.json()],
        )


class Billing(object):
    """Billing information."""

    COMPUTE_CREDIT = 'compute_credit'
    STORAGE_AVG_BYTE = 'storage_avg_byte'

    HOUR = 'hour'
    DAY = 'day'
    MONTH = 'month'

    def __init__(self, manager: Manager):
        self._manager = manager

    def usage(
        self,
        start_time: datetime.datetime,
        end_time: datetime.datetime,
        metric: Optional[str] = None,
        aggregate_by: Optional[str] = None,
    ) -> List[BillingUsageItem]:
        """
        Get usage information.

        Parameters
        ----------
        start_time : datetime.datetime
            Start time for usage interval
        end_time : datetime.datetime
            End time for usage interval
        metric : str, optional
            Possible metrics are ``mgr.billing.COMPUTE_CREDIT`` and
            ``mgr.billing.STORAGE_AVG_BYTE`` (default is all)
        aggregate_by : str, optional
            Aggregate type used to group usage: ``mgr.billing.HOUR``,
            ``mgr.billing.DAY``, or ``mgr.billing.MONTH``

        Returns
        -------
        List[BillingUsage]

        """
        res = self._manager._get(
            'billing/usage',
            params={
                k: v for k, v in dict(
                    metric=snake_to_camel(metric),
                    startTime=from_datetime(start_time),
                    endTime=from_datetime(end_time),
                    aggregate_by=aggregate_by.lower() if aggregate_by else None,
                ).items() if v is not None
            },
        )
        return [
            BillingUsageItem.from_dict(x, self._manager)
            for x in res.json()['billingUsage']
        ]


class Organizations(object):
    """Organizations."""

    def __init__(self, manager: Manager):
        self._manager = manager

    @property
    def current(self) -> Organization:
        """Get current organization."""
        res = self._manager._get('organizations/current').json()
        return Organization.from_dict(res, self._manager)


class WorkspaceManager(Manager):
    """
    SingleStoreDB workspace manager.

    This class should be instantiated using :func:`singlestoredb.manage_workspaces`.

    Parameters
    ----------
    access_token : str, optional
        The API key or other access token for the workspace management API
    version : str, optional
        Version of the API to use
    base_url : str, optional
        Base URL of the workspace management API

    See Also
    --------
    :func:`singlestoredb.manage_workspaces`

    """

    #: Workspace management API version if none is specified.
    default_version = config.get_option('management.version') or 'v1'

    #: Base URL if none is specified.
    default_base_url = config.get_option('management.base_url') \
        or 'https://api.singlestore.com'

    #: Object type
    obj_type = 'workspace'

    @property
    def workspace_groups(self) -> NamedList[WorkspaceGroup]:
        """Return a list of available workspace groups."""
        res = self._get('workspaceGroups')
        return NamedList([WorkspaceGroup.from_dict(item, self) for item in res.json()])

    @property
    def starter_workspaces(self) -> NamedList[StarterWorkspace]:
        """Return a list of available starter workspaces."""
        res = self._get('sharedtier/virtualWorkspaces')
        return NamedList([StarterWorkspace.from_dict(item, self) for item in res.json()])

    @property
    def organizations(self) -> Organizations:
        """Return the organizations."""
        return Organizations(self)

    @property
    def organization(self) -> Organization:
        """ Return the current organization."""
        return self.organizations.current

    @property
    def billing(self) -> Billing:
        """Return the current billing information."""
        return Billing(self)

    @ttl_property(datetime.timedelta(hours=1))
    def regions(self) -> NamedList[Region]:
        """Return a list of available regions."""
        res = self._get('regions')
        return NamedList([Region.from_dict(item, self) for item in res.json()])

    def create_workspace_group(
        self,
        name: str,
        region: Union[str, Region],
        firewall_ranges: List[str],
        admin_password: Optional[str] = None,
        backup_bucket_kms_key_id: Optional[str] = None,
        data_bucket_kms_key_id: Optional[str] = None,
        expires_at: Optional[str] = None,
        smart_dr: Optional[bool] = None,
        allow_all_traffic: Optional[bool] = None,
        update_window: Optional[Dict[str, int]] = None,
    ) -> WorkspaceGroup:
        """
        Create a new workspace group.

        Parameters
        ----------
        name : str
            Name of the workspace group
        region : str or Region
            ID of the region where the workspace group should be created
        firewall_ranges : list[str]
            List of allowed CIDR ranges. An empty list indicates that all
            inbound requests are allowed.
        admin_password : str, optional
            Admin password for the workspace group. If no password is supplied,
            a password will be generated and retured in the response.
        backup_bucket_kms_key_id : str, optional
            Specifies the KMS key ID associated with the backup bucket.
            If specified, enables Customer-Managed Encryption Keys (CMEK)
            encryption for the backup bucket of the workspace group.
            This feature is only supported in workspace groups deployed in AWS.
        data_bucket_kms_key_id : str, optional
            Specifies the KMS key ID associated with the data bucket.
            If specified, enables Customer-Managed Encryption Keys (CMEK)
            encryption for the data bucket and Amazon Elastic Block Store
            (EBS) volumes of the workspace group. This feature is only supported
            in workspace groups deployed in AWS.
        expires_at : str, optional
            The timestamp of when the workspace group will expire.
            If the expiration time is not specified,
            the workspace group will have no expiration time.
            At expiration, the workspace group is terminated and all the data is lost.
            Expiration time can be specified as a timestamp or duration.
            Example: "2021-01-02T15:04:05Z07:00", "2021-01-02", "3h30m"
        smart_dr : bool, optional
            Enables Smart Disaster Recovery (SmartDR) for the workspace group.
            SmartDR is a disaster recovery solution that ensures seamless and
            continuous replication of data from the primary region to a secondary region
        allow_all_traffic : bool, optional
            Allow all traffic to the workspace group
        update_window : Dict[str, int], optional
            Specify the day and hour of an update window: dict(day=0-6, hour=0-23)

        Returns
        -------
        :class:`WorkspaceGroup`

        """
        if isinstance(region, Region):
            region = region.id
        res = self._post(
            'workspaceGroups', json=dict(
                name=name, regionID=region,
                adminPassword=admin_password,
                backupBucketKMSKeyID=backup_bucket_kms_key_id,
                dataBucketKMSKeyID=data_bucket_kms_key_id,
                firewallRanges=firewall_ranges or [],
                expiresAt=expires_at,
                smartDR=smart_dr,
                allowAllTraffic=allow_all_traffic,
                updateWindow=snake_to_camel_dict(update_window),
            ),
        )
        return self.get_workspace_group(res.json()['workspaceGroupID'])

    def create_workspace(
        self,
        name: str,
        workspace_group: Union[str, WorkspaceGroup],
        size: Optional[str] = None,
        auto_suspend: Optional[Dict[str, Any]] = None,
        cache_config: Optional[int] = None,
        enable_kai: Optional[bool] = None,
        wait_on_active: bool = False,
        wait_interval: int = 10,
        wait_timeout: int = 600,
    ) -> Workspace:
        """
        Create a new workspace.

        Parameters
        ----------
        name : str
            Name of the workspace
        workspace_group : str or WorkspaceGroup
            The workspace ID of the workspace
        size : str, optional
            Workspace size in workspace size notation (S-00, S-1, etc.)
        auto_suspend : Dict[str, Any], optional
            Auto suspend settings for the workspace. If this field is not
            provided, no settings will be enabled.
        cache_config : int, optional
            Specifies the multiplier for the persistent cache associated
            with the workspace. If specified, it enables the cache configuration
            multiplier. It can have one of the following values: 1, 2, or 4.
        enable_kai : bool, optional
            Whether to create a SingleStore Kai-enabled workspace
        wait_on_active : bool, optional
            Wait for the workspace to be active before returning
        wait_timeout : int, optional
            Maximum number of seconds to wait before raising an exception
            if wait=True
        wait_interval : int, optional
            Number of seconds between each polling interval

        Returns
        -------
        :class:`Workspace`

        """
        if isinstance(workspace_group, WorkspaceGroup):
            workspace_group = workspace_group.id
        res = self._post(
            'workspaces', json=dict(
                name=name,
                workspaceGroupID=workspace_group,
                size=size,
                autoSuspend=snake_to_camel_dict(auto_suspend),
                cacheConfig=cache_config,
                enableKai=enable_kai,
            ),
        )
        out = self.get_workspace(res.json()['workspaceID'])
        if wait_on_active:
            out = self._wait_on_state(
                out,
                'Active',
                interval=wait_interval,
                timeout=wait_timeout,
            )
        return out

    def get_workspace_group(self, id: str) -> WorkspaceGroup:
        """
        Retrieve a workspace group definition.

        Parameters
        ----------
        id : str
            ID of the workspace group

        Returns
        -------
        :class:`WorkspaceGroup`

        """
        res = self._get(f'workspaceGroups/{id}')
        return WorkspaceGroup.from_dict(res.json(), manager=self)

    def get_workspace(self, id: str) -> Workspace:
        """
        Retrieve a workspace definition.

        Parameters
        ----------
        id : str
            ID of the workspace

        Returns
        -------
        :class:`Workspace`

        """
        res = self._get(f'workspaces/{id}')
        return Workspace.from_dict(res.json(), manager=self)

    def get_starter_workspace(self, id: str) -> StarterWorkspace:
        """
        Retrieve a starter workspace definition.

        Parameters
        ----------
        id : str
            ID of the starter workspace

        Returns
        -------
        :class:`StarterWorkspace`

        """
        res = self._get(f'sharedtier/virtualWorkspaces/{id}')
        return StarterWorkspace.from_dict(res.json(), manager=self)


def manage_workspaces(
    access_token: Optional[str] = None,
    version: Optional[str] = None,
    base_url: Optional[str] = None,
    *,
    organization_id: Optional[str] = None,
) -> WorkspaceManager:
    """
    Retrieve a SingleStoreDB workspace manager.

    Parameters
    ----------
    access_token : str, optional
        The API key or other access token for the workspace management API
    version : str, optional
        Version of the API to use
    base_url : str, optional
        Base URL of the workspace management API
    organization_id : str, optional
        ID of organization, if using a JWT for authentication

    Returns
    -------
    :class:`WorkspaceManager`

    """
    return WorkspaceManager(
        access_token=access_token, base_url=base_url,
        version=version, organization_id=organization_id,
    )
