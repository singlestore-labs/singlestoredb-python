#!/usr/bin/env python
"""SingleStore Cloud Files Management."""
from __future__ import annotations

import datetime
import io
import os
import re
from abc import ABC
from abc import abstractmethod
from typing import Any
from typing import BinaryIO
from typing import Dict
from typing import List
from typing import Optional
from typing import TextIO
from typing import Union

from ..exceptions import ManagementError
from .utils import PathLike
from .utils import to_datetime
from .utils import vars_to_str


class FilesObject(object):
    """
    File / folder object.

    It can belong to either a workspace stage or personal/shared space.

    This object is not instantiated directly. It is used in the results
    of various operations in ``WorkspaceGroup.stage`` methods.

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
    ) -> List[str]:
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
