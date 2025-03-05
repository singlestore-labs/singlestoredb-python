#!/usr/bin/env python
"""SingleStore Cloud Files Management."""
from __future__ import annotations

import datetime
import errno
import glob
import io
import os
import re
from abc import ABC
from abc import abstractmethod
import stat
import sys
import time
from typing import Any
from typing import BinaryIO
from typing import Dict
from typing import List
from typing import Optional
from typing import TextIO
from typing import Union

import pyfuse3
import trio

from .. import config
from ..exceptions import ManagementError
from .manager import Manager
from .utils import PathLike
from .utils import to_datetime
from .utils import vars_to_str

PERSONAL_SPACE = 'personal'
SHARED_SPACE = 'shared'
MODELS_SPACE = 'models'


class FilesObject(object):
    """
    File / folder object.

    It can belong to either a workspace stage or personal/shared space.

    This object is not instantiated directly. It is used in the results
    of various operations in ``WorkspaceGroup.stage``, ``FilesManager.personal_space``,
    ``FilesManager.shared_space`` and ``FilesManager.models_space`` methods.

    """

    def __init__(
        self,
        name: str,
        path: str,
        size: int,
        type: str,
        format: str,
        mimetype: str,
        created: Optional[datetime.datetime],
        last_modified: Optional[datetime.datetime],
        writable: bool,
        content: Optional[List[str]] = None,
    ):
        #: Name of file / folder
        self.name = name

        if type == 'directory':
            path = re.sub(r'/*$', r'', str(path)) + '/'

        #: Path of file / folder
        self.path = path

        #: Size of the object (in bytes)
        self.size = size

        #: Data type: file or directory
        self.type = type

        #: Data format
        self.format = format

        #: Mime type
        self.mimetype = mimetype

        #: Datetime the object was created
        self.created_at = created

        #: Datetime the object was modified last
        self.last_modified_at = last_modified

        #: Is the object writable?
        self.writable = writable

        #: Contents of a directory
        self.content: List[str] = content or []

        self._location: Optional[FileLocation] = None

    @classmethod
    def from_dict(
        cls,
        obj: Dict[str, Any],
        location: FileLocation,
    ) -> FilesObject:
        """
        Construct a FilesObject from a dictionary of values.

        Parameters
        ----------
        obj : dict
            Dictionary of values
        location : FileLocation
            FileLocation object to use as the parent

        Returns
        -------
        :class:`FilesObject`

        """
        out = cls(
            name=obj['name'],
            path=obj['path'],
            size=obj['size'],
            type=obj['type'],
            format=obj['format'],
            mimetype=obj['mimetype'],
            created=to_datetime(obj.get('created')),
            last_modified=to_datetime(obj.get('last_modified')),
            writable=bool(obj['writable']),
        )
        out._location = location
        return out

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)

    def open(
        self,
        mode: str = 'r',
        encoding: Optional[str] = None,
    ) -> Union[io.StringIO, io.BytesIO]:
        """
        Open a file path for reading or writing.

        Parameters
        ----------
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
        if self._location is None:
            raise ManagementError(
                msg='No FileLocation object is associated with this object.',
            )

        if self.is_dir():
            raise IsADirectoryError(
                f'directories can not be read or written: {self.path}',
            )

        return self._location.open(self.path, mode=mode, encoding=encoding)

    def download(
        self,
        local_path: Optional[PathLike] = None,
        *,
        overwrite: bool = False,
        encoding: Optional[str] = None,
    ) -> Optional[Union[bytes, str]]:
        """
        Download the content of a file path.

        Parameters
        ----------
        local_path : Path or str
            Path to local file target location
        overwrite : bool, optional
            Should an existing file be overwritten if it exists?
        encoding : str, optional
            Encoding used to convert the resulting data

        Returns
        -------
        bytes or str or None

        """
        if self._location is None:
            raise ManagementError(
                msg='No FileLocation object is associated with this object.',
            )

        return self._location.download_file(
            self.path, local_path=local_path,
            overwrite=overwrite, encoding=encoding,
        )

    download_file = download

    def remove(self) -> None:
        """Delete the file."""
        if self._location is None:
            raise ManagementError(
                msg='No FileLocation object is associated with this object.',
            )

        if self.type == 'directory':
            raise IsADirectoryError(
                f'path is a directory; use rmdir or removedirs {self.path}',
            )

        self._location.remove(self.path)

    def rmdir(self) -> None:
        """Delete the empty directory."""
        if self._location is None:
            raise ManagementError(
                msg='No FileLocation object is associated with this object.',
            )

        if self.type != 'directory':
            raise NotADirectoryError(
                f'path is not a directory: {self.path}',
            )

        self._location.rmdir(self.path)

    def removedirs(self) -> None:
        """Delete the directory recursively."""
        if self._location is None:
            raise ManagementError(
                msg='No FileLocation object is associated with this object.',
            )

        if self.type != 'directory':
            raise NotADirectoryError(
                f'path is not a directory: {self.path}',
            )

        self._location.removedirs(self.path)

    def rename(self, new_path: PathLike, *, overwrite: bool = False) -> None:
        """
        Move the file to a new location.

        Parameters
        ----------
        new_path : Path or str
            The new location of the file
        overwrite : bool, optional
            Should path be overwritten if it already exists?

        """
        if self._location is None:
            raise ManagementError(
                msg='No FileLocation object is associated with this object.',
            )
        out = self._location.rename(self.path, new_path, overwrite=overwrite)
        self.name = out.name
        self.path = out.path
        return None

    def exists(self) -> bool:
        """Does the file / folder exist?"""
        if self._location is None:
            raise ManagementError(
                msg='No FileLocation object is associated with this object.',
            )
        return self._location.exists(self.path)

    def is_dir(self) -> bool:
        """Is the object a directory?"""
        return self.type == 'directory'

    def is_file(self) -> bool:
        """Is the object a file?"""
        return self.type != 'directory'

    def abspath(self) -> str:
        """Return the full path of the object."""
        return str(self.path)

    def basename(self) -> str:
        """Return the basename of the object."""
        return self.name

    def dirname(self) -> str:
        """Return the directory name of the object."""
        return re.sub(r'/*$', r'', os.path.dirname(re.sub(r'/*$', r'', self.path))) + '/'

    def getmtime(self) -> float:
        """Return the last modified datetime as a UNIX timestamp."""
        if self.last_modified_at is None:
            return 0.0
        return self.last_modified_at.timestamp()

    def getctime(self) -> float:
        """Return the creation datetime as a UNIX timestamp."""
        if self.created_at is None:
            return 0.0
        return self.created_at.timestamp()
    

class FilesObjectTextWriter(io.StringIO):
    """StringIO wrapper for writing to FileLocation."""

    def __init__(self, buffer: Optional[str], location: FileLocation, path: PathLike):
        self._location = location
        self._path = path
        super().__init__(buffer)

    def close(self) -> None:
        """Write the content to the path."""
        self._location._upload(self.getvalue(), self._path)
        super().close()


class FilesObjectTextReader(io.StringIO):
    """StringIO wrapper for reading from FileLocation."""


class FilesObjectBytesWriter(io.BytesIO):
    """BytesIO wrapper for writing to FileLocation."""

    def __init__(self, buffer: bytes, location: FileLocation, path: PathLike):
        self._location = location
        self._path = path
        super().__init__(buffer)

    def close(self) -> None:
        """Write the content to the file path."""
        self._location._upload(self.getvalue(), self._path)
        super().close()


class FilesObjectBytesReader(io.BytesIO):
    """BytesIO wrapper for reading from FileLocation."""


class Inode:
    def __init__(self, fs, parent: int, name: str, type:str, id=None):
        self.fs = fs

        if id is None:
            id = fs.next_id
            fs.next_id += 1

        self.name = name
        self.parent = parent
        self.id = id
        self.type = type
        self._children = None

    def __repr__(self):
        return f"Inode({self.id}, \"{self.name}\", {self._children})"
    
    def getNameWithoutTrailingSlash(self):
        if self.name == "":
            return self.name
        if self.name.endswith("/"):
            return self.name[:-1]
        return self.name

    def getStagePath(self):
        if self.parent is None:
            return self.name
        stagepath = self.parent.getStagePath() + self.name
        if self.type == 'directory':
            stagepath += "/"
        return stagepath
    
    def isDir(self):
        return self.name == "" or self.getStagePath().endswith("/")
    
    def isFile(self):
        return not self.isDir()
    
    @property
    def children(self) -> List[int]:
        if self._children is None:
            self._children = []
            children = self.fs.stage.listdir(self.getStagePath())
            for child in children:
                childInode = Inode(self.fs, self, child.name, child.type)
                self.fs.inodes[childInode.id] = childInode
                self._children.append(childInode.id)
        return self._children
    
    def unlink(self, id):
        assert self._children is not None
        self._children.remove(id)

class SinglestoreFS(pyfuse3.Operations):
    def __init__(self, fileLocation: FileLocation):
        super(SinglestoreFS, self).__init__()
        self.next_id = pyfuse3.ROOT_INODE + 1

        """
        How to use:
        workspaceManager = s2.manage_workspaces(access_token, base_url=base_url)
        workspaceGroup = workspaceManager.get_workspace_group(workspace_group)
        return workspaceGroup.stage
        """
        
        self.stage = fileLocation

        self.inodes = {
            pyfuse3.ROOT_INODE: Inode(self, None, "", 'directory', pyfuse3.ROOT_INODE),
        }

    async def getattr(self, id, ctx=None):
        inode = self.inodes[id]
        info = self.stage.info(inode.getStagePath())
        
        entry = pyfuse3.EntryAttributes()
        if inode.isDir():
            entry.st_mode = (stat.S_IFDIR | 0o555)
            entry.st_size = 0
        elif inode.isFile():
            entry.st_mode = (stat.S_IFREG | 0o555)
            entry.st_size = info.size
        else:
            raise pyfuse3.FUSEError(errno.ENOENT)
        if info.writable:
            entry.st_mode |= 0o222

        entry.st_atime_ns = time.time_ns() # Current timestamp
        entry.st_ctime_ns = 0
        if info.created_at is not None:
            entry.st_ctime_ns = info.created_at.timestamp()*1e9
        entry.st_mtime_ns = 0
        if info.last_modified_at is not None:
            entry.st_mtime_ns = info.last_modified_at.timestamp()*1e9
        entry.st_gid = os.getgid() # TODO: check
        entry.st_uid = os.getuid() # TODO: check
        entry.st_ino = id

        return entry

    async def lookup(self, parent_id, name, ctx=None):
        parent_inode = self.inodes[parent_id]

        assert parent_inode.isDir()

        for child_id in parent_inode.children:
            child_inode = self.inodes[child_id]
            if child_inode.getNameWithoutTrailingSlash() == name.decode():
                return await self.getattr(child_id)
        raise pyfuse3.FUSEError(errno.ENOENT)

    async def opendir(self, id, ctx):
        if not id in self.inodes:
            raise pyfuse3.FUSEError(errno.ENOENT)
        return id

    async def readdir(self, fh, start_id, token):
        if not fh in self.inodes:
            raise pyfuse3.FUSEError(errno.ENOENT)

        inode = self.inodes[fh]

        assert inode.isDir()

        children = {child: self.inodes[child] for child in inode.children if child > start_id}

        for child in children.values():
            pyfuse3.readdir_reply(
                token,
                child.getNameWithoutTrailingSlash().encode(),
                await self.getattr(child.id),
                child.id
            )
        return

    async def open(self, id, flags, ctx):
        return pyfuse3.FileInfo(fh=id)

    async def read(self, fh, off, size):
        inode = self.inodes[fh]
        assert inode.isFile()
        fileContent = self.stage.download_file(inode.getStagePath())
        return fileContent[off:off+size]
    
    async def create(self, parent_id, name, mode, flags, ctx):
        parent_inode = self.inodes[parent_id]
        assert parent_inode.isDir()
        stagePath = parent_inode.getStagePath() + name.decode()
        self.stage.open(stagePath, "w").close()
        inode = Inode(self, parent_inode, name.decode(), 'file')
        self.inodes[inode.id] = inode
        self.inodes[parent_id]._children.append(inode.id)
        return pyfuse3.FileInfo(fh=inode.id), await self.getattr(inode.id)

    async def setattr(self, id, attr, fields, fh, ctx):
        return await self.getattr(id)

    async def write(self, fh, offset, data):
        inode = self.inodes[fh]
        assert inode.isFile()
        fileContent = self.stage.download_file(inode.getStagePath())
        newFileContent = fileContent[:offset] + data + fileContent[offset+len(data):]
        with self.stage.open(inode.getStagePath(), "wb") as f:
            return f.write(newFileContent)
    
    async def unlink(self, parent_id, name, ctx):
        parent_inode = self.inodes[parent_id]
        attr = await self.lookup(parent_id, name)
        inode = self.inodes[attr.st_ino]
        assert inode.isFile()
        self.stage.remove(inode.getStagePath())
        parent_inode.unlink(inode.id)
        del self.inodes[inode.id]
        return
    
    async def mkdir(self, parent_id, name, mode, ctx):
        parent_inode = self.inodes[parent_id]
        assert parent_inode.isDir()
        stagePath = parent_inode.getStagePath() + name.decode() + "/"
        self.stage.mkdir(stagePath)
        inode = Inode(self, parent_inode, name.decode() + "/", 'directory')
        self.inodes[inode.id] = inode
        parent_inode._children.append(inode.id)
        return await self.getattr(inode.id)
    
    async def rename(self, parent_id, name, newparent_id, newname, ctx):
        parent_inode = self.inodes[parent_id]
        newparent_inode = self.inodes[newparent_id]
        assert parent_inode.isDir()
        assert newparent_inode.isDir()
        attr = await self.lookup(parent_id, name)
        inode = self.inodes[attr.st_ino]
        self.stage.rename(inode.getStagePath(), newparent_inode.getStagePath() + newname.decode())
        inode.parent = newparent_inode
        inode.name = newname.decode()
        newparent_inode._children.append(inode.id)
        parent_inode.unlink(inode.id)
        return
    
    async def rmdir(self, parent_id, name, ctx):
        parent_inode = self.inodes[parent_id]
        assert parent_inode.isDir()
        attr = await self.lookup(parent_id, name)
        inode = self.inodes[attr.st_ino]
        assert inode.isDir()
        self.stage.rmdir(inode.getStagePath())
        parent_inode.unlink(inode.id)
        del self.inodes[inode.id]
        return

class FileLocation(ABC):
    @abstractmethod
    def open(
        self,
        path: PathLike,
        mode: str = 'r',
        encoding: Optional[str] = None,
    ) -> Union[io.StringIO, io.BytesIO]:
        pass

    @abstractmethod
    def upload_file(
        self,
        local_path: Union[PathLike, TextIO, BinaryIO],
        path: PathLike,
        *,
        overwrite: bool = False,
    ) -> FilesObject:
        pass

    @abstractmethod
    def upload_folder(
        self,
        local_path: PathLike,
        path: PathLike,
        *,
        overwrite: bool = False,
        recursive: bool = True,
        include_root: bool = False,
        ignore: Optional[Union[PathLike, List[PathLike]]] = None,
    ) -> FilesObject:
        pass

    @abstractmethod
    def _upload(
        self,
        content: Union[str, bytes, TextIO, BinaryIO],
        path: PathLike,
        *,
        overwrite: bool = False,
    ) -> FilesObject:
        pass

    @abstractmethod
    def mkdir(self, path: PathLike, overwrite: bool = False) -> FilesObject:
        pass

    @abstractmethod
    def rename(
        self,
        old_path: PathLike,
        new_path: PathLike,
        *,
        overwrite: bool = False,
    ) -> FilesObject:
        pass

    @abstractmethod
    def info(self, path: PathLike) -> FilesObject:
        pass

    @abstractmethod
    def exists(self, path: PathLike) -> bool:
        pass

    @abstractmethod
    def is_dir(self, path: PathLike) -> bool:
        pass

    @abstractmethod
    def is_file(self, path: PathLike) -> bool:
        pass

    @abstractmethod
    def listdir(
        self,
        path: PathLike = '/',
        *,
        recursive: bool = False,
    ) -> List[FilesObject]:
        pass

    @abstractmethod
    def download_file(
        self,
        path: PathLike,
        local_path: Optional[PathLike] = None,
        *,
        overwrite: bool = False,
        encoding: Optional[str] = None,
    ) -> Optional[Union[bytes, str]]:
        pass

    @abstractmethod
    def download_folder(
        self,
        path: PathLike,
        local_path: PathLike = '.',
        *,
        overwrite: bool = False,
    ) -> None:
        pass

    @abstractmethod
    def remove(self, path: PathLike) -> None:
        pass

    @abstractmethod
    def removedirs(self, path: PathLike) -> None:
        pass

    @abstractmethod
    def rmdir(self, path: PathLike) -> None:
        pass

    @abstractmethod
    def __str__(self) -> str:
        pass

    @abstractmethod
    def __repr__(self) -> str:
        pass

    def mount(self, mountpoint) -> None:
        """Mount to folder"""
        fs = SinglestoreFS(self)
        fuse_options = set(pyfuse3.default_options)
        # fuse_options.add('fsname=singlestore_fs')
        fuse_options.add('debug')
        pyfuse3.init(fs, mountpoint, fuse_options)

        try:
            trio.run(pyfuse3.main)
        except:
            pyfuse3.close(unmount=True)

        # pyfuse3.close()

class FilesManager(Manager):
    """
    SingleStoreDB files manager.

    This class should be instantiated using :func:`singlestoredb.manage_files`.

    Parameters
    ----------
    access_token : str, optional
        The API key or other access token for the files management API
    version : str, optional
        Version of the API to use
    base_url : str, optional
        Base URL of the files management API

    See Also
    --------
    :func:`singlestoredb.manage_files`

    """

    #: Management API version if none is specified.
    default_version = config.get_option('management.version') or 'v1'

    #: Base URL if none is specified.
    default_base_url = config.get_option('management.base_url') \
        or 'https://api.singlestore.com'

    #: Object type
    obj_type = 'file'

    @property
    def personal_space(self) -> FileSpace:
        """Return the personal file space."""
        return FileSpace(PERSONAL_SPACE, self)

    @property
    def shared_space(self) -> FileSpace:
        """Return the shared file space."""
        return FileSpace(SHARED_SPACE, self)

    @property
    def models_space(self) -> FileSpace:
        """Return the models file space."""
        return FileSpace(MODELS_SPACE, self)


def manage_files(
    access_token: Optional[str] = None,
    version: Optional[str] = None,
    base_url: Optional[str] = None,
    *,
    organization_id: Optional[str] = None,
) -> FilesManager:
    """
    Retrieve a SingleStoreDB files manager.

    Parameters
    ----------
    access_token : str, optional
        The API key or other access token for the files management API
    version : str, optional
        Version of the API to use
    base_url : str, optional
        Base URL of the files management API
    organization_id : str, optional
        ID of organization, if using a JWT for authentication

    Returns
    -------
    :class:`FilesManager`

    """
    return FilesManager(
        access_token=access_token, base_url=base_url,
        version=version, organization_id=organization_id,
    )


class FileSpace(FileLocation):
    """
    FileSpace manager.

    This object is not instantiated directly.
    It is returned by ``FilesManager.personal_space``, ``FilesManager.shared_space``
    or ``FileManger.models_space``.

    """

    def __init__(self, location: str, manager: FilesManager):
        self._location = location
        self._manager = manager

    def open(
        self,
        path: PathLike,
        mode: str = 'r',
        encoding: Optional[str] = None,
    ) -> Union[io.StringIO, io.BytesIO]:
        """
        Open a file path for reading or writing.

        Parameters
        ----------
        path : Path or str
            The file path to read / write
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
            raise ManagementError(msg='modifying an existing file is not supported')

        if 'w' in mode or 'x' in mode:
            exists = self.exists(path)
            if exists:
                if 'x' in mode:
                    raise FileExistsError(f'file path already exists: {path}')
                self.remove(path)
            if 'b' in mode:
                return FilesObjectBytesWriter(b'', self, path)
            return FilesObjectTextWriter('', self, path)

        if 'r' in mode:
            content = self.download_file(path)
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
        local_path: Union[PathLike, TextIO, BinaryIO],
        path: PathLike,
        *,
        overwrite: bool = False,
    ) -> FilesObject:
        """
        Upload a local file.

        Parameters
        ----------
        local_path : Path or str or file-like
            Path to the local file or an open file object
        path : Path or str
            Path to the file
        overwrite : bool, optional
            Should the ``path`` be overwritten if it exists already?

        """
        if isinstance(local_path, (TextIO, BinaryIO)):
            pass
        elif not os.path.isfile(local_path):
            raise IsADirectoryError(f'local path is not a file: {local_path}')

        if self.exists(path):
            if not overwrite:
                raise OSError(f'file path already exists: {path}')

            self.remove(path)

        if isinstance(local_path, (TextIO, BinaryIO)):
            return self._upload(local_path, path, overwrite=overwrite)
        return self._upload(open(local_path, 'rb'), path, overwrite=overwrite)

    def upload_folder(
        self,
        local_path: PathLike,
        path: PathLike,
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
        path : Path or str
            Path of folder to upload to
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

        if not path:
            path = local_path

        ignore_files = set()
        if ignore:
            if isinstance(ignore, list):
                for item in ignore:
                    ignore_files.update(glob.glob(str(item), recursive=recursive))
            else:
                ignore_files.update(glob.glob(str(ignore), recursive=recursive))

        for dir_path, _, files in os.walk(str(local_path)):
            for fname in files:
                if ignore_files and fname in ignore_files:
                    continue

                local_file_path = os.path.join(dir_path, fname)
                remote_path = os.path.join(
                    path,
                    local_file_path.lstrip(str(local_path)),
                )
                self.upload_file(
                    local_path=local_file_path,
                    path=remote_path,
                    overwrite=overwrite,
                )
        return self.info(path)

    def _upload(
        self,
        content: Union[str, bytes, TextIO, BinaryIO],
        path: PathLike,
        *,
        overwrite: bool = False,
    ) -> FilesObject:
        """
        Upload content to a file.

        Parameters
        ----------
        content : str or bytes or file-like
            Content to upload
        path : Path or str
            Path to the file
        overwrite : bool, optional
            Should the ``path`` be overwritten if it exists already?

        """
        if self.exists(path):
            if not overwrite:
                raise OSError(f'file path already exists: {path}')
            self.remove(path)

        self._manager._put(
            f'files/fs/{self._location}/{path}',
            files={'file': content},
            headers={'Content-Type': None},
        )

        return self.info(path)

    def mkdir(self, path: PathLike, overwrite: bool = False) -> FilesObject:
        """
        Make a directory in the file space.

        Parameters
        ----------
        path : Path or str
            Path of the folder to create
        overwrite : bool, optional
            Should the file path be overwritten if it exists already?

        Returns
        -------
        FilesObject

        """
        raise ManagementError(
            msg='Operation not supported: directories are currently not allowed '
                'in Files API',
        )

    mkdirs = mkdir

    def rename(
        self,
        old_path: PathLike,
        new_path: PathLike,
        *,
        overwrite: bool = False,
    ) -> FilesObject:
        """
        Move the file to a new location.

        Parameters
        -----------
        old_path : Path or str
            Original location of the path
        new_path : Path or str
            New location of the path
        overwrite : bool, optional
            Should the ``new_path`` be overwritten if it exists already?

        """
        if not self.exists(old_path):
            raise OSError(f'file path does not exist: {old_path}')

        if str(old_path).endswith('/') or str(new_path).endswith('/'):
            raise ManagementError(
                msg='Operation not supported: directories are currently not allowed '
                    'in Files API',
            )

        if self.exists(new_path):
            if not overwrite:
                raise OSError(f'file path already exists: {new_path}')

            self.remove(new_path)

        self._manager._patch(
            f'files/fs/{self._location}/{old_path}',
            json=dict(newPath=new_path),
        )

        return self.info(new_path)

    def info(self, path: PathLike) -> FilesObject:
        """
        Return information about a file location.

        Parameters
        ----------
        path : Path or str
            Path to the file

        Returns
        -------
        FilesObject

        """
        res = self._manager._get(
            re.sub(r'/+$', r'/', f'files/fs/{self._location}/{path}'),
            params=dict(metadata=1),
        ).json()

        return FilesObject.from_dict(res, self)

    def exists(self, path: PathLike) -> bool:
        """
        Does the given file path exist?

        Parameters
        ----------
        path : Path or str
            Path to file object

        Returns
        -------
        bool

        """
        try:
            self.info(path)
            return True
        except ManagementError as exc:
            if exc.errno == 404:
                return False
            raise

    def is_dir(self, path: PathLike) -> bool:
        """
        Is the given file path a directory?

        Parameters
        ----------
        path : Path or str
            Path to file object

        Returns
        -------
        bool

        """
        try:
            return self.info(path).type == 'directory'
        except ManagementError as exc:
            if exc.errno == 404:
                return False
            raise

    def is_file(self, path: PathLike) -> bool:
        """
        Is the given file path a file?

        Parameters
        ----------
        path : Path or str
            Path to file object

        Returns
        -------
        bool

        """
        try:
            return self.info(path).type != 'directory'
        except ManagementError as exc:
            if exc.errno == 404:
                return False
            raise

    def _listdir(self, path: PathLike, *, recursive: bool = False) -> List[FilesObject]:
        """
        Return the files in a directory.

        Parameters
        ----------
        path : Path or str
            Path to the folder
        recursive : bool, optional
            Should folders be listed recursively?

        """
        res = self._manager._get(
            f'files/fs/{self._location}/{path}',
        ).json()

        if recursive:
            out = []
            for item in res['content'] or []:
                out.append(FilesObject(item, self))
                if item['type'] == 'directory':
                    out.extend(self._listdir(item['path'], recursive=recursive))
            return out
        return [FilesObject(x, self) for x in res['content'] or []]

    def listdir(
        self,
        path: PathLike = '/',
        *,
        recursive: bool = False,
    ) -> List[FilesObject]:
        """
        List the files / folders at the given path.

        Parameters
        ----------
        path : Path or str, optional
            Path to the file location

        Returns
        -------
        List[str]

        """
        path = re.sub(r'^(\./|/)+', r'', str(path))
        path = re.sub(r'/+$', r'', path) + '/'

        if not self.is_dir(path):
            raise NotADirectoryError(f'path is not a directory: {path}')

        out = self._listdir(path, recursive=recursive)
        return out

    def download_file(
        self,
        path: PathLike,
        local_path: Optional[PathLike] = None,
        *,
        overwrite: bool = False,
        encoding: Optional[str] = None,
    ) -> Optional[Union[bytes, str]]:
        """
        Download the content of a file path.

        Parameters
        ----------
        path : Path or str
            Path to the file
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
        if self.is_dir(path):
            raise IsADirectoryError(f'file path is a directory: {path}')

        out = self._manager._get(
            f'files/fs/{self._location}/{path}',
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
        path: PathLike,
        local_path: PathLike = '.',
        *,
        overwrite: bool = False,
    ) -> None:
        """
        Download a FileSpace folder to a local directory.

        Parameters
        ----------
        path : Path or str
            Directory path
        local_path : Path or str
            Path to local directory target location
        overwrite : bool, optional
            Should an existing directory / files be overwritten if they exist?

        """

        if local_path is not None and not overwrite and os.path.exists(local_path):
            raise OSError('target path already exists; use overwrite=True to replace')

        if not self.is_dir(path):
            raise NotADirectoryError(f'path is not a directory: {path}')

        files = self.listdir(path, recursive=True)
        for f in files:
            remote_path = os.path.join(path, f)
            if self.is_dir(remote_path):
                continue
            target = os.path.normpath(os.path.join(local_path, f))
            os.makedirs(os.path.dirname(target), exist_ok=True)
            self.download_file(remote_path, target, overwrite=overwrite)

    def remove(self, path: PathLike) -> None:
        """
        Delete a file location.

        Parameters
        ----------
        path : Path or str
            Path to the location

        """
        if self.is_dir(path):
            raise IsADirectoryError('file path is a directory')

        self._manager._delete(f'files/fs/{self._location}/{path}')

    def removedirs(self, path: PathLike) -> None:
        """
        Delete a folder recursively.

        Parameters
        ----------
        path : Path or str
            Path to the file location

        """
        if not self.is_dir(path):
            raise NotADirectoryError('path is not a directory')

        self._manager._delete(f'files/fs/{self._location}/{path}')

    def rmdir(self, path: PathLike) -> None:
        """
        Delete a folder.

        Parameters
        ----------
        path : Path or str
            Path to the file location

        """
        raise ManagementError(
            msg='Operation not supported: directories are currently not allowed '
                'in Files API',
        )

    def __str__(self) -> str:
        """Return string representation."""
        return vars_to_str(self)

    def __repr__(self) -> str:
        """Return string representation."""
        return str(self)
