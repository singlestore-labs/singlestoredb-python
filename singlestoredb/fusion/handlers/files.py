#!/usr/bin/env python3
from typing import Any
from typing import Dict
from typing import Optional

from .. import result
from ..handler import SQLHandler
from ..result import FusionSQLResult
from .utils import dt_isoformat
from .utils import get_file_space


class ShowFilesHandler(SQLHandler):
    """
    Generic handler for listing files in a personal/shared space.
    """  # noqa: E501

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        file_space = get_file_space(params)

        res = FusionSQLResult()
        res.add_field('Name', result.STRING)

        if params['extended']:
            res.add_field('Type', result.STRING)
            res.add_field('Size', result.INTEGER)
            res.add_field('Writable', result.STRING)
            res.add_field('CreatedAt', result.DATETIME)
            res.add_field('LastModifiedAt', result.DATETIME)

            files = []
            for x in file_space.listdir(
                params['at_path'] or '/',
                recursive=params['recursive'],
            ):
                info = file_space.info(x)
                files.append(
                    tuple([
                        x, info.type, info.size or 0, info.writable,
                        dt_isoformat(info.created_at),
                        dt_isoformat(info.last_modified_at),
                    ]),
                )
            res.set_rows(files)

        else:
            res.set_rows([(x,) for x in file_space.listdir(
                params['at_path'] or '/',
                recursive=params['recursive'],
            )])

        if params['like']:
            res = res.like(Name=params['like'])

        return res.order_by(**params['order_by']).limit(params['limit'])


class ShowPersonalFilesHandler(ShowFilesHandler):
    """
    SHOW PERSONAL FILES
        [ at_path ] [ <like> ]
        [ <order-by> ]
        [ <limit> ] [ recursive ] [ extended ];

    # File path to list
    at_path = AT '<path>'

    # Should the listing be recursive?
    recursive = RECURSIVE

    # Should extended attributes be shown?
    extended = EXTENDED

    Description
    -----------
    Displays a list of files in a personal/shared space.

    Arguments
    ---------
    * ``<path>``: A path in the personal/shared space.
    * ``<pattern>``: A pattern similar to SQL LIKE clause.
      Uses ``%`` as the wildcard character.

    Remarks
    -------
    * Use the ``LIKE`` clause to specify a pattern and return only the
      files that match the specified pattern.
    * The ``LIMIT`` clause limits the number of results to the
      specified number.
    * Use the ``ORDER BY`` clause to sort the results by the specified
      key. By default, the results are sorted in the ascending order.
    * The ``AT`` clause specifies the path in the personal/shared
      space to list the files from.
    * Use the ``RECURSIVE`` clause to list the files recursively.
    * To return more information about the files, use the ``EXTENDED``
      clause.

    Examples
    --------
    The following command lists the files at a specific path::

        SHOW PERSONAL FILES AT "/data/";

    The following command lists the files recursively with
    additional information::

        SHOW PERSONAL FILES RECURSIVE EXTENDED;

    See Also
    --------
    * ``SHOW SHARED FILES``
    * ``UPLOAD PERSONAL FILE``
    * ``UPLOAD SHARED FILE``
    * ``DOWNLOAD PERSONAL FILE``
    * ``DOWNLOAD SHARED FILE``

    """  # noqa: E501
    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        params['file_location'] = 'PERSONAL'
        return super().run(params)


class ShowSharedFilesHandler(ShowFilesHandler):
    """
    SHOW SHARED FILES
        [ at_path ] [ <like> ]
        [ <order-by> ]
        [ <limit> ] [ recursive ] [ extended ];

    # File path to list
    at_path = AT '<path>'

    # Should the listing be recursive?
    recursive = RECURSIVE

    # Should extended attributes be shown?
    extended = EXTENDED

    Description
    -----------
    Displays a list of files in a personal/shared space.

    Arguments
    ---------
    * ``<path>``: A path in the personal/shared space.
    * ``<pattern>``: A pattern similar to SQL LIKE clause.
      Uses ``%`` as the wildcard character.

    Remarks
    -------
    * Use the ``LIKE`` clause to specify a pattern and return only the
      files that match the specified pattern.
    * The ``LIMIT`` clause limits the number of results to the
      specified number.
    * Use the ``ORDER BY`` clause to sort the results by the specified
      key. By default, the results are sorted in the ascending order.
    * The ``AT`` clause specifies the path in the personal/shared
      space to list the files from.
    * Use the ``RECURSIVE`` clause to list the files recursively.
    * To return more information about the files, use the ``EXTENDED``
      clause.

    Examples
    --------
    The following command lists the files at a specific path::

        SHOW SHARED FILES AT "/data/";

    The following command lists the files recursively with
    additional information::

        SHOW SHARED FILES RECURSIVE EXTENDED;

    See Also
    --------
    * ``SHOW PERSONAL FILES``
    * ``UPLOAD PERSONAL FILE``
    * ``UPLOAD SHARED FILE``
    * ``DOWNLOAD PERSONAL FILE``
    * ``DOWNLOAD SHARED FILE``

    """  # noqa: E501
    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        params['file_location'] = 'SHARED'
        return super().run(params)


ShowPersonalFilesHandler.register(overwrite=True)
ShowSharedFilesHandler.register(overwrite=True)


class UploadFileHandler(SQLHandler):
    """
    Generic handler for uploading files to a personal/shared space.
    """  # noqa: E501

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        file_space = get_file_space(params)
        file_space.upload_file(
            params['local_path'], params['path'],
            overwrite=params['overwrite'],
        )
        return None


class UploadPersonalFileHandler(UploadFileHandler):
    """
    UPLOAD PERSONAL FILE TO path
        FROM local_path [ overwrite ];

    # Path to file
    path = '<filename>'

    # Path to local file
    local_path = '<local-path>'

    # Should an existing file be overwritten?
    overwrite = OVERWRITE

    Description
    -----------
    Uploads a file to a personal/shared space.

    Arguments
    ---------
    * ``<filename>``: The filename in the personal/shared space where the file is uploaded.
    * ``<local-path>``: The path to the file to upload in the local
      directory.

    Remarks
    -------
    * If the ``OVERWRITE`` clause is specified, any existing file at the
      specified path in the personal/shared space is overwritten.

    Examples
    --------
    The following command uploads a file to a personal/shared space and overwrite any
    existing files at the specified path::

        UPLOAD PERSONAL FILE TO 'stats.csv'
            FROM '/tmp/user/stats.csv' OVERWRITE;

    See Also
    --------
    * ``UPLOAD SHARED FILE``
    * ``DOWNLOAD PERSONAL FILE``
    * ``DOWNLOAD SHARED FILE``

    """  # noqa: E501

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        params['file_location'] = 'PERSONAL'
        return super().run(params)


class UploadSharedFileHandler(UploadFileHandler):
    """
    UPLOAD SHARED FILE TO path
        FROM local_path [ overwrite ];

    # Path to file
    path = '<filename>'

    # Path to local file
    local_path = '<local-path>'

    # Should an existing file be overwritten?
    overwrite = OVERWRITE

    Description
    -----------
    Uploads a file to a personal/shared space.

    Arguments
    ---------
    * ``<filename>``: The filename in the personal/shared space where the file is uploaded.
    * ``<local-path>``: The path to the file to upload in the local
      directory.

    Remarks
    -------
    * If the ``OVERWRITE`` clause is specified, any existing file at the
      specified path in the personal/shared space is overwritten.

    Examples
    --------
    The following command uploads a file to a personal/shared space and overwrite any
    existing files at the specified path::

        UPLOAD SHARED FILE TO 'stats.csv'
            FROM '/tmp/user/stats.csv' OVERWRITE;

    See Also
    --------
    * ``UPLOAD PERSONAL FILE``
    * ``DOWNLOAD PERSONAL FILE``
    * ``DOWNLOAD SHARED FILE``

    """  # noqa: E501

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        params['file_location'] = 'SHARED'
        return super().run(params)


UploadPersonalFileHandler.register(overwrite=True)
UploadSharedFileHandler.register(overwrite=True)


class DownloadFileHandler(SQLHandler):
    """
    Generic handler for downloading files from a personal/shared space.
    """  # noqa: E501

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        file_space = get_file_space(params)

        out = file_space.download_file(
            params['path'],
            local_path=params['local_path'] or None,
            overwrite=params['overwrite'],
            encoding=params['encoding'] or None,
        )

        if not params['local_path']:
            res = FusionSQLResult()
            if params['encoding']:
                res.add_field('Data', result.STRING)
            else:
                res.add_field('Data', result.BLOB)
            res.set_rows([(out,)])
            return res

        return None


class DownloadPersonalFileHandler(DownloadFileHandler):
    """
    DOWNLOAD PERSONAL FILE path
        [ local_path ]
        [ overwrite ]
        [ encoding ];

    # Path to file
    path = '<path>'

    # Path to local file
    local_path = TO '<local-path>'

    # Should an existing file be overwritten?
    overwrite = OVERWRITE

    # File encoding
    encoding = ENCODING '<encoding>'

    Description
    -----------
    Download a file from a personal/shared space.

    Arguments
    ---------
    * ``<path>``: The path to the file to download in a personal/shared space.
    * ``<encoding>``: The encoding to apply to the downloaded file.
    * ``<local-path>``: Specifies the path in the local directory
      where the file is downloaded.

    Remarks
    -------
    * If the ``OVERWRITE`` clause is specified, any existing file at
      the download location is overwritten.
    * By default, files are downloaded in binary encoding. To view
      the contents of the file on the standard output, use the
      ``ENCODING`` clause and specify an encoding.
    * If ``<local-path>`` is not specified, the file is displayed
      on the standard output.

    Examples
    --------
    The following command displays the contents of the file on the
    standard output::

        DOWNLOAD PERSONAL FILE '/data/stats.csv' ENCODING 'utf8';

    The following command downloads a file to a specific location and
    overwrites any existing file with the name ``stats.csv`` on the local storage::

        DOWNLOAD PERSONAL FILE '/data/stats.csv'
            TO '/tmp/data.csv' OVERWRITE;

    See Also
    --------
    * ``DOWNLOAD SHARED FILE``
    * ``UPLOAD PERSONAL FILE``
    * ``UPLOAD SHARED FILE``

    """  # noqa: E501

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        params['file_location'] = 'PERSONAL'
        return super().run(params)


class DownloadSharedFileHandler(DownloadFileHandler):
    """
    DOWNLOAD SHARED FILE path
        [ local_path ]
        [ overwrite ]
        [ encoding ];

    # Path to file
    path = '<path>'

    # Path to local file
    local_path = TO '<local-path>'

    # Should an existing file be overwritten?
    overwrite = OVERWRITE

    # File encoding
    encoding = ENCODING '<encoding>'

    Description
    -----------
    Download a file from a personal/shared space.

    Arguments
    ---------
    * ``<path>``: The path to the file to download in a personal/shared space.
    * ``<encoding>``: The encoding to apply to the downloaded file.
    * ``<local-path>``: Specifies the path in the local directory
      where the file is downloaded.

    Remarks
    -------
    * If the ``OVERWRITE`` clause is specified, any existing file at
      the download location is overwritten.
    * By default, files are downloaded in binary encoding. To view
      the contents of the file on the standard output, use the
      ``ENCODING`` clause and specify an encoding.
    * If ``<local-path>`` is not specified, the file is displayed
      on the standard output.

    Examples
    --------
    The following command displays the contents of the file on the
    standard output::

        DOWNLOAD SHARED FILE '/data/stats.csv' ENCODING 'utf8';

    The following command downloads a file to a specific location and
    overwrites any existing file with the name ``stats.csv`` on the local storage::

        DOWNLOAD SHARED FILE '/data/stats.csv'
            TO '/tmp/data.csv' OVERWRITE;

    See Also
    --------
    * ``DOWNLOAD PERSONAL FILE``
    * ``UPLOAD PERSONAL FILE``
    * ``UPLOAD SHARED FILE``

    """  # noqa: E501

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        params['file_location'] = 'SHARED'
        return super().run(params)


DownloadPersonalFileHandler.register(overwrite=True)
DownloadSharedFileHandler.register(overwrite=True)


class DropHandler(SQLHandler):
    """
    Generic handler for deleting files/folders from a personal/shared space.
    """  # noqa: E501

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        file_space = get_file_space(params)

        file_type = params['file_type']
        if not file_type:
            raise KeyError('file type was not specified')

        file_type = file_type.lower()
        if file_type not in ['file', 'folder']:
            raise ValueError('file type must be either FILE or FOLDER')

        if file_type == 'file':
            file_space.remove(params['path'])
        elif file_type == 'folder':
            if params['recursive']:
                file_space.removedirs(params['path'])
            else:
                file_space.rmdir(params['path'])

        return None


class DropPersonalHandler(DropHandler):
    """
    DROP PERSONAL <file-type> path
        [ recursive ];

    # Path to file
    path = '<path>'

    # Should folders be deleted recursively?
    recursive = RECURSIVE

    Description
    -----------
    Deletes a file/folder from a personal/shared space.

    Arguments
    ---------
    * ``<file-type>``: The type of the file, it can
      be either 'FILE' or 'FOLDER'.
    * ``<path>``: The path to the file to delete in a personal/shared space.

    Remarks
    -------
    * The ``RECURSIVE`` clause indicates that the specified folder
      is deleted recursively.

    Example
    --------
    The following commands delete a file/folder from a personal/shared space::

        DROP PERSONAL FILE '/data/stats.csv';
        DROP PERSONAL FOLDER '/data/' RECURSIVE;

    See Also
    --------
    * ``DROP SHARED FILE``
    * ``DROP SHARED FOLDER``

    """  # noqa: E501

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        params['file_location'] = 'PERSONAL'
        return super().run(params)


class DropSharedHandler(DropHandler):
    """
    DROP SHARED <file-type> path
        [ recursive ];

    # Path to file
    path = '<path>'

    # Should folders be deleted recursively?
    recursive = RECURSIVE

    Description
    -----------
    Deletes a file/folder from a personal/shared space.

    Arguments
    ---------
    * ``<file-type>``: The type of the file, it can
      be either 'FILE' or 'FOLDER'.
    * ``<path>``: The path to the file to delete in a personal/shared space.

    Remarks
    -------
    * The ``RECURSIVE`` clause indicates that the specified folder
      is deleted recursively.

    Example
    --------
    The following commands delete a file/folder from a personal/shared space::

        DROP SHARED FILE '/data/stats.csv';
        DROP SHARED FOLDER '/data/' RECURSIVE;

    See Also
    --------
    * ``DROP PERSONAL FILE``
    * ``DROP PERSONAL FOLDER``

    """  # noqa: E501

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        params['file_location'] = 'SHARED'
        return super().run(params)


DropPersonalHandler.register(overwrite=True)
DropSharedHandler.register(overwrite=True)


class CreateFolderHandler(SQLHandler):
    """
    Generic handler for creating folders in a personal/shared space.
    """

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        file_space = get_file_space(params)
        file_space.mkdir(params['path'], overwrite=params['overwrite'])
        return None


class CreatePersonalFolderHandler(CreateFolderHandler):
    """
    CREATE PERSONAL FOLDER path
        [ overwrite ];

    # Path to folder
    path = '<path>'

    # Should an existing folder be overwritten?
    overwrite = OVERWRITE

    Description
    -----------
    Creates a new folder at the specified path in a personal/shared space.

    Arguments
    ---------
    * ``<path>``: The path in a personal/shared space where the folder
      is created. The path must end with a trailing slash (/).

    Remarks
    -------
    * If the ``OVERWRITE`` clause is specified, any existing
      folder at the specified path is overwritten.

    Example
    -------
    The following command creates a folder in a personal/shared space::

        CREATE PERSONAL FOLDER `/data/csv/`;

    See Also
    --------
    * ``CREATE SHARED FOLDER``

    """

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        params['file_location'] = 'PERSONAL'
        return super().run(params)


class CreateSharedFolderHandler(CreateFolderHandler):
    """
    CREATE SHARED FOLDER path
        [ overwrite ];

    # Path to folder
    path = '<path>'

    # Should an existing folder be overwritten?
    overwrite = OVERWRITE

    Description
    -----------
    Creates a new folder at the specified path in a personal/shared space.

    Arguments
    ---------
    * ``<path>``: The path in a personal/shared space where the folder
      is created. The path must end with a trailing slash (/).

    Remarks
    -------
    * If the ``OVERWRITE`` clause is specified, any existing
      folder at the specified path is overwritten.

    Example
    -------
    The following command creates a folder in a personal/shared space::

        CREATE SHARED FOLDER `/data/csv/`;

    See Also
    --------
    * ``CREATE PERSONAL FOLDER``

    """

    def run(self, params: Dict[str, Any]) -> Optional[FusionSQLResult]:
        params['file_location'] = 'SHARED'
        return super().run(params)


CreatePersonalFolderHandler.register(overwrite=True)
CreateSharedFolderHandler.register(overwrite=True)
